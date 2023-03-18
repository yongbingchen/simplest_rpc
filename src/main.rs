use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize};
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
    responses: Arc<Mutex<Vec<Response>>>,
    socket: Arc<UdpSocket>,
    notify: Arc<Notify>,
    exit: Arc<AtomicBool>,
    started: Arc<AtomicUsize>,
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
            responses: Arc::new(Mutex::new(Vec::new())),
            socket,
            notify: Arc::new(Notify::new()),
            exit: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicUsize::new(0)),
            tx: Some(tx),
        })
    }

    async fn recv_task_func(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = [0; 1024];
        let notify = Arc::clone(&self.notify);
        let socket = Arc::clone(&self.socket);
        let requests = Arc::clone(&self.requests);
        let responses = Arc::clone(&self.responses);
        let exit = Arc::clone(&self.exit);
        let started = Arc::clone(&self.started);
        let mut rx = self.tx.as_ref().unwrap().subscribe();
        started.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

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
                                let mut responses = responses.lock().await;
                                responses.push(response);
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
        started.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn send_task_func(&self) -> Result<(), Box<dyn std::error::Error>> {
        let notify = Arc::clone(&self.notify);
        let socket = Arc::clone(&self.socket);
        let requests = Arc::clone(&self.requests);
        let exit = Arc::clone(&self.exit);
        let started = Arc::clone(&self.started);
        let mut rx = self.tx.as_ref().unwrap().subscribe();
        started.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

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
        started.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn response_handler_task_func(&self) -> Result<(), Box<dyn std::error::Error>> {
        let notify = Arc::clone(&self.notify);
        let responses = Arc::clone(&self.responses);
        let exit = Arc::clone(&self.exit);
        let mut rx = self.tx.as_ref().unwrap().subscribe();

        loop {
            tokio::select! {
                _ = rx.recv() => {
                    println!("Response handler task got signal");
                    if exit.load(std::sync::atomic::Ordering::Relaxed) {
                        println!("Response handler task exiting");
                        break;
                    }
                }
                _ = notify.notified() => {
                    loop {
                        let mut responses = responses.lock().await;
                        if responses.len() == 0 {
                            println!("Response task, no pending response to handle");
                            break;
                        }
                        let response = responses.remove(0);
                        println!("Response handler is handling response {:?}", response);
                        // TODO: map callback function with request ID, and call the callback here
                   }
                }
            }
        }
        Ok(())
    }


    async fn post_request(&self, request: Request, timeout_ms: u64) -> Result<Box<Vec<u8>>, Box<dyn std::error::Error>> {
        let requests = Arc::clone(&self.requests);
        let notify = Arc::clone(&self.notify);
        let mut requests = requests.lock().await;
        println!("Posting request {:?}", request);
        requests.push(request);
        notify.notify_waiters();

        // Wait for response with timeout
        let response = Box::new(vec![1, 2, 3]);
        match timeout(Duration::from_millis(timeout_ms), notify.notified()).await {
            Ok(_) => {
                println!("Sender task got notification");
            },
            Err(_) => {
                println!("Sender task timed out waiting for notification, try resend request");
            },
        }

        Ok(response)
    }

    fn stop(&self) {
        self.exit.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(tx) = &self.tx {
            let _ = tx.send(());
        }
    }

    async fn wait_for_tasks_start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let started = Arc::clone(&self.started);
        while 2 != started.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok(())
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

    rpc_client.wait_for_tasks_start().await?;

    // Try issue multiple requests concurrently
    let responses = tokio::join! {
        rpc_client
            .post_request(Request {
                id: 1234,
                payload: format!("hello, world! {}", 1234).to_owned(),
                sent_at: instant_to_u64(Instant::now()),
                retries: 5,
            }, 1000),
        rpc_client
            .post_request(Request {
                id: 5678,
                payload: format!("hello, world! {}", 5678).to_owned(),
                sent_at: instant_to_u64(Instant::now()),
                retries: 5,
            }, 1000),
    };
    println!("Final responses: {:?}", responses);

    // Sleep some time for the communication to finish
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    rpc_client.stop();
    receive_task.await.unwrap();
    resend_task.await.unwrap();

    Ok(())
}

// Test this with local echo server running as:
// ~$ ncat -l 8081 --keep-open --udp --exec "/bin/cat"
