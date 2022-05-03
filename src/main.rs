
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

use minion_msg::{MinionMsg, MinionOps};




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
        ch.send(MinionSupMessages::Connection{socket: s}).await; //XXX
    })
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
                                            socket.close();
                                            return;
                                        }
                                    }
                                }
                                _ => {
                                    println!("invalid message type");
                                    socket.close();
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
                    actor_ch: MinionActor{}.start(&id, socket),
                    id: id.clone(),
                };
                let c = mi.actor_ch.clone();
                state.minions.insert(id.clone(), mi);
                
                tokio::spawn(async move  {
                    use std::time::Duration;
use tokio::time::sleep;
                    sleep(Duration::from_millis(3000)).await;
                    c.send(MinionMessages::Job).await;
                    println!("it's job time");
                });
                
            },
            _ => {
                panic!()
            }
        }
    }
}


struct Minion {
    id: minion_msg::MinionId,
    actor_ch: mpsc::Sender<MinionMessages>,
}

enum MinionSM {
    Idle,
    Working,
}

enum MinionMessages {
    Job,
}

struct MinionActor {
}

struct MinionState {
    id: minion_msg::MinionId,
    rx: mpsc::Receiver<MinionMessages>,
    state: MinionSM,
   // sockchan: mpsc::Sender<minion_msg::MinionMsg>,
    sender: SplitSink<WebSocket, Message>,
}

impl MinionActor {
    fn start(mut self, id: &minion_msg::MinionId, mut s: WebSocket) -> mpsc::Sender<MinionMessages> {
        let (tx, rx) = mpsc::channel(100); 
        
        let (mut sender, mut receiver) = s.split();

        let mut state = MinionState{
            id: id.clone(),
            state: MinionSM::Idle,
            rx: rx,
            //sockchan: self.handle_socket(socket),
            sender: sender,
        };
    
    self.handle_socket(&state, receiver); // XXXX

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
            MinionMessages::Job => {
                println!("{:?} Job", state.id);
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

                    state.sender.send(
                    Message::Binary(minion_msg::to_vec(
                            &minion_msg::MinionMsg{ 
                                op: minion_msg::MinionOps::Exec,
                                payload: thejob.into(),
                            }
                    ).unwrap())
                ).await;// XXX
            }
        }
    }

    fn handle_socket(&self, state: &MinionState, mut receiver: SplitStream<WebSocket>) {

        let id = state.id.clone();
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
                                    println!("minion returned {:?}", m.payload)
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



