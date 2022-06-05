use std::{
    collections::hash_map::HashMap,
    io
};
use std::sync::Arc;
use axum::{
    extract::Extension,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        TypedHeader,
    },
    http::{StatusCode, Uri},
    response::IntoResponse,
    routing::{get, get_service},
    Router,
};
use futures::{sink::SinkExt, stream::{StreamExt, SplitSink, SplitStream}};
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tower_http::services::ServeDir;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use minion_msg::{MinionMsg, MinionOps};

use uuid::Uuid as JobId;

use murray::actor;



#[derive(Clone, Debug)]
struct Job {
    id: JobId,
}


struct ServerState {
    sup_ch: mpsc::Sender<MinionSupActorMessages>,
}


async fn handle_error(_err: io::Error) -> impl IntoResponse {
    (StatusCode::INTERNAL_SERVER_ERROR, "Something went wrong...")
}


#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "gru=debug,tower_http=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let state = ServerState{
        sup_ch: MinionSupActor{}.start(),
    };
    let shared_state = Arc::new(state);
    
    let code_server = ServeDir::new("/tmp/gru");
    let app = Router::new()
        .nest("/code/", get_service(code_server).handle_error(handle_error))
            .route("/ws", get(ws_handler))
            .layer(Extension(shared_state))
            .layer(
                TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
            );

    // run it with hyper
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn fallback(uri: Uri) -> impl IntoResponse {
    (StatusCode::NOT_FOUND, format!("No route for {}", uri))
}

async fn ws_handler(
    Extension(state): Extension<Arc<ServerState>>,
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
) -> impl IntoResponse {
    if let Some(TypedHeader(user_agent)) = user_agent {
        println!("`{}` connected", user_agent.as_str());
    }

    let ch = state.sup_ch.clone();
    ws.on_upgrade(|s| async move {
        match ch.send(MinionSupActorMessages::Connection(MinionSupActorMessagesConnection{socket: s})).await {
            Ok(_) => (),
            Err(e) => {
                panic!("handle supervisor dying {:?}", e);
            }
        }
    })
}


#[derive(Debug)]
enum JobErrors {
    BadCode,
    MinionAborted,
    MinionUnavailable
}

actor! {
 MinionSup,
 Messages: {
    Connection {
        socket: WebSocket, 
    },
    Connected {
        id: minion_msg::MinionId,
        socket: WebSocket,
    },
    Job {
    },
    JobResult {
        job_id: JobId,
        result: Result<Vec<u8>,JobErrors>,
    },
 },
 State: {
    minions: HashMap<minion_msg::MinionId, Minion>, 
 }
}


impl MinionSupActor {

    async fn handle_connection(&self, state: &mut MinionSupActorState, payload: MinionSupActorMessagesConnection) { 
        let mut socket = payload.socket;
        let c = state.tx.clone();
            while let Some(msg) = socket.recv().await {
                if let Ok(msg) = msg {
                    match msg {
                        Message::Binary(b) => {
                            let m = minion_msg::from_bytes(&b).unwrap();
                            println!("op: {:?}", m.op);
                            match m.op {
                                MinionOps::Auth => {
                                    println!("handling {:?}", m.op);
                                    let m = MinionSupActorMessages::Connected(MinionSupActorMessagesConnected{
                                        id: m.payload.into(),
                                        socket,
                                    });
                                    return c.send(m).await.unwrap();
                                }
                                _ => {
                                    println!("invalid message in connect {:?}", m.op);
                                    socket.close().await.unwrap_or_default();
                                    return;
                                }
                            }
                        }
                        _ => {
                            println!("invalid message type");
                            socket.close().await.unwrap_or_default();
                            return;
                        }
                    }
                } else {
                    return;
                    // XXX premature disconnection 
                }
            }
    }

    async fn handle_connected(&self, state: &mut MinionSupActorState, payload: MinionSupActorMessagesConnected) { 
        let id = payload.id;
        let socket = payload.socket;
        // XXX HANDLE RECONNECT
        let mi = Minion {
            actor_ch: MinionActor{}.start(Some(state.tx.clone()), &id),
            id: id.clone(),
        };
        let c = mi.actor_ch.clone();
        c.try_send(MinionActorMessages::Connected(MinionActorMessagesConnected{socket})).unwrap();
        if state.minions.is_none() {
            state.minions = Some(HashMap::new());
        }
        if let Some(mm) = state.minions.as_mut() {
            mm.insert(id.clone(), mi);
        } else {
            panic!("MinionSup lost minion cache")
        }
    }

    async fn handle_job(&self, state: &mut MinionSupActorState, payload: MinionSupActorMessagesJob) { 

        // XXX TODO
    }

    async fn handle_jobresult(&self, state: &mut MinionSupActorState, payload: MinionSupActorMessagesJobResult) { 
        println!("job {:?} completed with result {:?}", payload.job_id, payload.result );
    }
}


#[derive(Debug)]
struct Minion {
    id: minion_msg::MinionId,
    actor_ch: mpsc::Sender<MinionActorMessages>,
}



actor! {
    Minion,
    Options: {
        sup: MinionSup,
        id: minion_msg::MinionId,
    },
    Messages: {
    Connected { socket: WebSocket },
    Exec { job: Job },
    Ret { result: Result<Vec<u8>, minion_msg::MinionErrors> },
    },
    State: {
    sender: SplitSink<WebSocket, Message>,
    onjob: uuid::Uuid,
    }
}



impl MinionActor {
    async fn handle_connected(&self, state: &mut MinionActorState, payload: MinionActorMessagesConnected) { 

        let (mut sender, mut receiver) = payload.socket.split();
        state.sender = Some(sender);
        self.handle_socket(&state, receiver);

        let c = state.tx.clone();
        tokio::spawn(async move { 
            c.send(MinionActorMessages::Exec(MinionActorMessagesExec{job: Job{id: uuid::Uuid::new_v4()}})).await;
        });
    }

    async fn handle_exec(&self, state: &mut MinionActorState, payload: MinionActorMessagesExec) { 
        if state.onjob != None {
            match state.sup_ch.as_mut() {
                Some(c) => {
                    c.send(
                        MinionSupActorMessages::JobResult(MinionSupActorMessagesJobResult{ 
                            job_id: payload.job.id, 
                            result: Err(JobErrors::MinionUnavailable)
                        })).await.unwrap_or_default()
                },
                None => ()
            }
            return;
        }

        match state.sender.as_mut() {
            Some(s) => {
                let x =  s.send(
                    Message::Binary(minion_msg::to_vec(
                            &minion_msg::MinionMsg{ 
                                op: minion_msg::MinionOps::Exec,
                                payload: "/code/basicjob".into(),
                            }
                    ).unwrap()) ).await;
                match x {
                    Ok(_) => {
                        state.onjob = Some(payload.job.id.clone());
                        ()
                    },
                    Err(_) => {
                        // XXX does this actually close ? might need to poison pill the
                        // receiver
                        s.close().await.unwrap_or_default();
                        state.sender = None; 
                    }
                }
            },
            None => {
                panic!();
            }
        }
    }

    async fn handle_ret(&self, state: &mut MinionActorState, payload: MinionActorMessagesRet) { 
        let j = match state.onjob {
                    Some(j) => j,
                    None => {
                        print!("{:?} got res back but no job ? {:?}", state.id, payload.result);
                        // so, something should happen here 
                        return;
                    }
                };
                state.onjob = None;

                match state.sup_ch.as_mut() {
                    Some(c) => c.send(MinionSupActorMessages::JobResult( MinionSupActorMessagesJobResult{ 
                        job_id: j, 
                        result: payload.result.map_err(|e| {
                            match e {
                                minion_msg::MinionErrors::BadCode => JobErrors::BadCode,
                                minion_msg::MinionErrors::ExecFault => JobErrors::MinionAborted,
                            }
                        }),
                    })).await.unwrap_or_default(), // XXX what to do if sup goes away ? 
                    None => (),
                }
            }

    fn handle_socket(&self, state: &MinionActorState, mut receiver: SplitStream<WebSocket>) {
        let id = state.id.clone();
        let tx = state.tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = receiver.next().await {
                if let Ok(msg) = msg {
                    match msg {
                        Message::Text(t) => {
                            println!("client sent str: {:?}", t);
                        }
                        Message::Binary(b) => {
                            println!("client sent binary data");
                            let m = minion_msg::from_bytes(&b).unwrap();
                            println!("op: {:?}", m.op);
                            match m.op {
                                MinionOps::Ret => {
                                    println!("minion returned {:?}", m.payload);
                                    tx.send(MinionActorMessages::Ret(MinionActorMessagesRet{ result: Ok(m.payload) })).await.unwrap_or_default();
                                },
                                _ => {
                                    println!("minion {:?} bad message {:?}", id, m.op);
                                }
                            }
                        }
                        Message::Ping(_) => {
                            println!("socket ping");
                        }
                        Message::Pong(_) => {
                            println!("socket pong");
                        }
                        Message::Close(_) => {
                            println!("client disconnected");
                            return;
                        }
                    }
                } else {
                    println!("client disconnected");
                    return;
                }
            }
        });
    }
}

//    (module
//      (type $t0 (func (param i32) (result i32)))
//      (type $t1 (func (param) (result i32)))
//      (func $add_one (export "add_one") (type $t0) (param $p0 i32) (result i32)
//        get_local $p0
//        i32.const 1
//        i32.add)
//      (func $run (export "run") (type $t1) (result i32)
//        i32.const 42
//        call $add_one))



#[cfg(test)]
mod tests {

}
