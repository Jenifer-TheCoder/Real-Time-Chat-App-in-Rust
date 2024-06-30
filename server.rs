use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use futures_util::{StreamExt, SinkExt};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use warp::Filter;
use warp::fs::File;
use std::convert::Infallible;
#[tokio::main]
async fn main() {
    // Set up the broadcast channel
    let (tx, _rx) = broadcast::channel(100);
    let tx = Arc::new(Mutex::new(tx));
    // WebSocket handler
    let tx_ws = tx.clone();
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .map(move |ws: warp::ws::Ws| {
            let tx = tx_ws.clone();
            ws.on_upgrade(move |websocket| handle_connection(websocket, tx))
        });
    // Static file handler
    let static_route = warp::path::end()
        .and(warp::fs::file("static/index.html"));
    // Combine routes
    let routes = ws_route.or(static_route);
    println!("Server listening on 127.0.0.1:8080");
    // Start the server
    warp::serve(routes)
        .run(([127, 0, 0, 1], 8080))
        .await;
}
async fn handle_connection(ws: warp::ws::WebSocket, tx: Arc<Mutex<broadcast::Sender<String>>>) {
    let (mut ws_sender, mut ws_receiver) = ws.split();
    // Subscribe to the broadcast channel
    let mut rx = tx.lock().unwrap().subscribe();
    // Task to send broadcast messages to the client
    tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if ws_sender.send(warp::ws::Message::text(msg)).await.is_err() {
                break;
            }
        }
    });
    // Process incoming messages
    while let Some(result) = ws_receiver.next().await {
        match result {
            Ok(message) => {
                if let Ok(text) = message.to_str() {
                    println!("Received message: {}", text);
                    tx.lock().unwrap().send(text.to_string()).expect("Failed to broadcast message");
                }
            },
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
        }
    }
}