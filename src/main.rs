use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        TypedHeader,
    },
    http::{StatusCode, Uri},
    response::IntoResponse,
    routing::{get, get_service},
    Router,
};
use std::net::SocketAddr;
use tower_http::{
    services::ServeDir,
    trace::{DefaultMakeSpan, TraceLayer},
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use minion_msg::{MinionMsg, MinionOps};

#[tokio::main]
async fn main() {
    let module_wat = r#"
    (module
      (type $t0 (func (param i32) (result i32)))
      (func $add_one (export "add_one") (type $t0) (param $p0 i32) (result i32)
        get_local $p0
        i32.const 1
        i32.add))
    "#;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "gru=debug,tower_http=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let app = Router::new()
        .route("/ws", get(ws_handler))
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
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
) -> impl IntoResponse {
    if let Some(TypedHeader(user_agent)) = user_agent {
        println!("`{}` connected", user_agent.as_str());
    }

    ws.on_upgrade(handle_socket)
}

enum MinionStates {
    Connected,
    Authenticated,
    Idle,
    Working,
}
struct Minion {
    id: i128,
    state: MinionStates, 
}


async fn handle_socket(mut socket: WebSocket) {
    let state = Minion {id: 0, state: MinionStates::Connected};

    if let Some(msg) = socket.recv().await {
        if let Ok(msg) = msg {
            match msg {
                Message::Text(t) => {
                    println!("client sent str: {:?}", t);
                }
                Message::Binary(b) => {
                    println!("client sent binary data");
                    let m = minion_msg::from_bytes(&b).unwrap();
                    println!("op: {:?}", m.op);

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

    loop {
        if socket
            .send(Message::Text(String::from("Hi!")))
            .await
            .is_err()
        {
            println!("client disconnected");
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

