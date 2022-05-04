
use std::collections::hash_map::HashMap;
use std::sync::Arc;

use axum::{
    extract::Extension,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        TypedHeader,
    },
    http::{StatusCode, Uri},
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{sink::SinkExt, stream::{StreamExt, SplitSink, SplitStream}};
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tower_http::{
    trace::{DefaultMakeSpan, TraceLayer},
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use uuid::{uuid, Uuid};

use minion_msg::{MinionMsg, MinionOps};

use uuid::Uuid as JobId;

#[derive(Clone, Debug)]
struct Job {
    id: JobId,
}


struct ServerState {
    sup_ch: mpsc::Sender<MinionSupMessages>,
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
    
    let app = Router::new()
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
        match ch.send(MinionSupMessages::Connection{socket: s}).await {
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

#[derive(Debug)]
enum MinionSupMessages {
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
    }
}

struct MinionSupActor {
}

struct MinionSupState {
    minions: HashMap<minion_msg::MinionId, Minion>,
    rx: mpsc::Receiver<MinionSupMessages>,
    tx: mpsc::Sender<MinionSupMessages>,
}


impl MinionSupActor {
    fn start(self) -> mpsc::Sender<MinionSupMessages> {
        let (tx, rx) = mpsc::channel(100); 
        
        let mut state = MinionSupState{
            minions: HashMap::new(),
            rx: rx,
            tx: tx.clone(),
        };


        tokio::spawn(async move {
            while let Some(msg) = state.rx.recv().await {
                self.handle(&mut state, msg);
            }
        });

        tx
    }
}


impl MinionSupActor {
    fn handle(&self, state: &mut MinionSupState, msg: MinionSupMessages) -> () {
        match msg {
            MinionSupMessages::Connection { mut socket } => {
                let c = state.tx.clone();
                tokio::spawn(async move {
                    while let Some(msg) = socket.recv().await {
                        if let Ok(msg) = msg {
                            match msg {
                                Message::Binary(b) => {
                                    let m = minion_msg::from_bytes(&b).unwrap();
                                    println!("op: {:?}", m.op);
                                    match m.op {
                                        MinionOps::Auth => {
                                            println!("handling {:?}", m.op);
                                            let m = MinionSupMessages::Connected {
                                                id: m.payload.into(),
                                                socket: socket,
                                            };
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
                });
            },
            MinionSupMessages::Connected { id, socket } => {
                // XXX HANDLE RECONNECT
                let mi = Minion {
                    actor_ch: MinionActor{}.start(Some(state.tx.clone()), &id),
                    id: id.clone(),
                };
                let c = mi.actor_ch.clone();
                c.try_send(MinionMessages::Connected{socket}).unwrap();
                state.minions.insert(id.clone(), mi);
            },
            MinionSupMessages::Job {  } => {
                // XXX TODO
            }
            MinionSupMessages::JobResult { job_id , result } => {
                println!("job {:?} completed with result {:?}", job_id, result );
            },
        }
    }
}


struct Minion {
    id: minion_msg::MinionId,
    actor_ch: mpsc::Sender<MinionMessages>,
}


#[derive(Debug)]
enum MinionMessages {
    Connected{ socket: WebSocket },
    Exec{ job: Job },
    Ret{ result: Result<Vec<u8>, minion_msg::MinionErrors> },
}

struct MinionActor {
}

struct MinionState {
    id: minion_msg::MinionId,
    rx: mpsc::Receiver<MinionMessages>,
    tx: mpsc::Sender<MinionMessages>,
    sender: Option<SplitSink<WebSocket, Message>>,
    onjob: Option<uuid::Uuid>,
    sup_ch: Option<mpsc::Sender<MinionSupMessages>>,
}

impl MinionActor {
    fn start(mut self, sup: Option<mpsc::Sender<MinionSupMessages>>, id: &minion_msg::MinionId) -> mpsc::Sender<MinionMessages> {
        let (tx, rx) = mpsc::channel(100); 
        

        let mut state = MinionState{
            id: id.clone(),
            rx: rx,
            tx: tx.clone(),
            sender: None,
            onjob: None,
            sup_ch: sup,
        };
    

        tokio::spawn(async move {
            while let Some(msg) = state.rx.recv().await {
                self.handle(&mut state, msg).await;
            }
        });

        tx 
    }
}

impl MinionActor {
    async fn handle(&self, state: &mut MinionState, msg: MinionMessages) -> () {
        match msg {
            MinionMessages::Connected{socket} => {
                let (mut sender, mut receiver) = socket.split();
                state.sender = Some(sender);
                self.handle_socket(&state, receiver); // XXXX

                state.tx.send(MinionMessages::Exec{job: Job{id: uuid::Uuid::new_v4()}}).await;
            },

            MinionMessages::Exec { job } => {
                if state.onjob != None {
                    match state.sup_ch.as_mut() {
                        Some(c) => c.send(
                            MinionSupMessages::JobResult { 
                                job_id: job.id, 
                                result: Err(JobErrors::MinionUnavailable)
                                    }).await.unwrap_or_default(),
                        None => ()
                    }
                    return;
                }

                let thejob = r#"
    (module
      (type $t0 (func (param i32) (result i32)))
      (type $t1 (func (param) (result i32)))
      (func $add_one (export "add_one") (type $t0) (param $p0 i32) (result i32)
        get_local $p0
        i32.const 1
        i32.add)
      (func $run (export "run") (type $t1) (result i32)
        i32.const 42
        call $add_one))
    "#;
                match state.sender.as_mut() {
                    Some(s) => {
                        match s.send(
                            Message::Binary(minion_msg::to_vec(
                                    &minion_msg::MinionMsg{ 
                                        op: minion_msg::MinionOps::Exec,
                                        payload: thejob.into(),
                                    }
                            ).unwrap()) ).await {
                            Ok(_) => {
                                state.onjob = Some(job.id.clone());
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
            },
            MinionMessages::Ret { result } => {
                let j = match state.onjob {
                    Some(j) => j,
                    None => {
                        print!("{:?} got res back but no job ? {:?}", state.id, result);
                        // so, something should happen here 
                        return;
                    }
                };
                state.onjob = None;

                match state.sup_ch.as_mut() {
                    Some(c) => c.send(MinionSupMessages::JobResult { 
                        job_id: j, 
                        result: result.map_err(|e| {
                            match e {
                                minion_msg::MinionErrors::BadCode => JobErrors::BadCode,
                                minion_msg::MinionErrors::ExecFault => JobErrors::MinionAborted,
                            }
                        }),
                    }).await.unwrap_or_default(), // XXX what to do if sup goes away ? 
                    None => (),
                }
            },
        }
    }

    fn handle_socket(&self, state: &MinionState, mut receiver: SplitStream<WebSocket>) {

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
                                    tx.send(MinionMessages::Ret { result: Ok(m.payload) }).await.unwrap_or_default();
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



