use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tokio::sync::Notify;
use tokio::time::timeout;
// std::sync::Mutex is not Send, so not able to use in tokio
use futures::lock::Mutex;

fn instant_to_u64(instant: Instant) -> u64 {
    let duration_since_epoch =
        instant.duration_since(Instant::now().checked_sub(Duration::from_secs(0)).unwrap());
    duration_since_epoch.as_micros() as u64
}

#[allow(dead_code)]
fn u64_to_instant(u64_time: u64) -> Instant {
    let duration_since_epoch = Duration::from_micros(u64_time);
    Instant::now().checked_sub(duration_since_epoch).unwrap()
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Request {
    id: u64,
    payload: String,
    sent_at: u64, // Instant does not implement Serialize
    retries: u32,
}

// Use echo server to test, so just use same type as request
type Response = Request;

#[derive(Debug, Clone)]
struct RpcClient {
    requests: Arc<Mutex<Vec<Request>>>,
    socket: Arc<UdpSocket>,
    notify: Arc<Notify>,
    exit: Arc<AtomicBool>,
    tx: Option<broadcast::Sender<()>>,
}

async fn send_data(socket: &UdpSocket, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    socket.send(data).await?;
    Ok(())
}

impl RpcClient {
    async fn new(
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let socket = Arc::new(UdpSocket::bind(local_addr).await?);
        socket.connect(remote_addr).await?;
        let (tx, _) = broadcast::channel(1);
        Ok(Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            socket,
            notify: Arc::new(Notify::new()),
            exit: Arc::new(AtomicBool::new(false)),
            tx: Some(tx),
        })
    }

    async fn recv_task_func(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = [0; 1024];
        let notify = Arc::clone(&self.notify);
        let socket = Arc::clone(&self.socket);
        let requests = Arc::clone(&self.requests);
        let exit = Arc::clone(&self.exit);
        let mut rx = self.tx.as_ref().unwrap().subscribe();

        loop {
            tokio::select! {
                _ = rx.recv() => {
                    println!("Receiver task got signal");
                    if exit.load(std::sync::atomic::Ordering::Relaxed) {
                        println!("Receiver task exiting");
                        break;
                    }
                }
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((size, _remote)) => {
                            let response: Response = bincode::deserialize(&buf[0..size]).unwrap();
                            let mut requests = requests.lock().await;
                            if let Some(index) = requests.iter().position(|r| r.id == response.id) {
                                // Remove the request from the list
                                let request = requests.remove(index);
                                // Process the response here
                                println!("Received response for request {:?}", request);
                                notify.notify_waiters();
                            }
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            // No data available, continue looping
                        }
                        Err(e) => {
                            // Other errors, print the error and exit the loop
                            eprintln!("Error receiving message: {}", e);
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn send_task_func(&self) -> Result<(), Box<dyn std::error::Error>> {
        let notify = Arc::clone(&self.notify);
        let socket = Arc::clone(&self.socket);
        let requests = Arc::clone(&self.requests);
        let exit = Arc::clone(&self.exit);
        let mut rx = self.tx.as_ref().unwrap().subscribe();

        loop {
            tokio::select! {
                _ = rx.recv() => {
                    println!("Sender task got signal");
                    if exit.load(std::sync::atomic::Ordering::Relaxed) {
                        println!("Sender task exiting");
                        break;
                    }
                }
                _ = notify.notified() => {
                    loop {
                        let mut requests = requests.lock().await;
                        if requests.len() == 0 {
                            println!("Sender task, no pending request to send");
                            break;
                        }
                        if requests[0].retries == 0 {
                            println!("Request {:?} exceeded retry times", requests[0]);
                            requests.remove(0);
                            continue;
                        };
                        println!("Sender task is posting request {:?}", requests[0]);
                        requests[0].retries -= 1;
                        let bytes = bincode::serialize(&requests[0]).unwrap();
                        send_data(&socket, &bytes).await.unwrap();
                        drop(requests);
                        match timeout(Duration::from_millis(100), notify.notified()).await {
                            Ok(_) => {
                                println!("Sender task got notification");
                            },
                            Err(_) => {
                                println!("Sender task timed out waiting for notification, try resend request");
                            },
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn post_request(&self, request: Request) -> Result<(), Box<dyn std::error::Error>> {
        let requests = Arc::clone(&self.requests);
        let mut requests = requests.lock().await;
        println!("Posting request {:?}", request);
        requests.push(request);
        self.notify.clone().notify_waiters();
        Ok(())
    }

    fn stop(&self) {
        self.exit.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(tx) = &self.tx {
            let _ = tx.send(());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let remote_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();
    let rpc_client = RpcClient::new(local_addr, remote_addr).await?;

    let receiver = rpc_client.clone();
    let receive_task = tokio::spawn(async move {
        receiver.recv_task_func().await.unwrap();
    });

    let sender = rpc_client.clone();
    let resend_task = tokio::spawn(async move {
        sender.send_task_func().await.unwrap();
    });

    for i in 0..10 {
        rpc_client
            .post_request(Request {
                id: i,
                payload: "Hello, world!".to_owned(),
                sent_at: instant_to_u64(Instant::now()),
                retries: 5,
            })
            .await
            .unwrap();
    }

    // Sleep some time for the communication to finish
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    rpc_client.stop();
    receive_task.await.unwrap();
    resend_task.await.unwrap();

    Ok(())
}

// Test this with local echo server running as:
// ~$ ncat -l 8081 --keep-open --udp --exec "/bin/cat"
// $ cargo run
//      Running `target/debug/playground`
// Posting request Request { id: 0, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Posting request Request { id: 1, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Posting request Request { id: 2, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Posting request Request { id: 3, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Posting request Request { id: 4, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Posting request Request { id: 5, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Posting request Request { id: 6, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Posting request Request { id: 7, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Posting request Request { id: 8, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Sender task is posting request Request { id: 0, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Posting request Request { id: 9, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Sender task got notification
// Received response for request Request { id: 0, payload: "Hello, world!", sent_at: 0, retries: 4 }
// Sender task is posting request Request { id: 1, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Received response for request Request { id: 1, payload: "Hello, world!", sent_at: 0, retries: 4 }
// Sender task got notification
// Sender task is posting request Request { id: 2, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Received response for request Request { id: 2, payload: "Hello, world!", sent_at: 0, retries: 4 }
// Sender task got notification
// Sender task is posting request Request { id: 3, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Received response for request Request { id: 3, payload: "Hello, world!", sent_at: 0, retries: 4 }
// Sender task got notification
// Sender task is posting request Request { id: 4, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Received response for request Request { id: 4, payload: "Hello, world!", sent_at: 0, retries: 4 }
// Sender task got notification
// Sender task is posting request Request { id: 5, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Received response for request Request { id: 5, payload: "Hello, world!", sent_at: 0, retries: 4 }
// Sender task got notification
// Sender task is posting request Request { id: 6, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Received response for request Request { id: 6, payload: "Hello, world!", sent_at: 0, retries: 4 }
// Sender task got notification
// Sender task is posting request Request { id: 7, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Received response for request Request { id: 7, payload: "Hello, world!", sent_at: 0, retries: 4 }
// Sender task got notification
// Sender task is posting request Request { id: 8, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Received response for request Request { id: 8, payload: "Hello, world!", sent_at: 0, retries: 4 }
// Sender task got notification
// Sender task is posting request Request { id: 9, payload: "Hello, world!", sent_at: 0, retries: 5 }
// Received response for request Request { id: 9, payload: "Hello, world!", sent_at: 0, retries: 4 }
// Sender task got notification
// Sender task, no pending request to send
// Receiver task got signal
// Receiver task exiting
// Sender task got signal
// Sender task exiting
