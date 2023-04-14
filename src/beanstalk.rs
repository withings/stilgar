use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use serde_yaml;
use log;

/// Queue size limit for messages to a Beanstalk channel
const BEANSTALK_MESSAGE_QUEUE_SIZE: usize = 128;

/// A beanstalkd handle
pub struct Beanstalk {
    /// TCP stream to beanstalkd
    stream: TcpStream,
    /// The receiving end for messages, used by run_channel
    rx: mpsc::Receiver<ClientMessage>,
    /// The transmitting end, use by whatever to interact with beanstalkd
    tx: mpsc::Sender<ClientMessage>,
}

/// A beanstalkd error
#[derive(Error, Debug)]
pub enum BeanstalkError {
    #[error("the internal queue to Beanstalk is not available: {0}")]
    QueueUnvailable(String),
    #[error("a return channel has failed to receive a Beanstalk response: {0}")]
    ReturnChannelFailure(String),
    #[error("unexpected response from Beanstalk for command {0}: {1}")]
    UnexpectedResponse(String, String),
    #[error("beanstalk communication error: {0}")]
    CommunicationError(String),
    #[error("job reservation timeout")]
    ReservationTimeout,
}

/// Convenience struct: copy of the channel's transmissiting end, with some methods
#[derive(Clone)]
pub struct BeanstalkProxy {
    request_tx: mpsc::Sender<ClientMessage>
}

/// Convenience type: Beanstalk operation result
pub type BeanstalkResult = Result<String, BeanstalkError>;

/// A command which can be sent over the Beanstalk channel
struct ClientMessage {
    /// A oneshot for the reply
    return_tx: oneshot::Sender<BeanstalkResult>,
    /// The actual command body
    body: ClientMessageBody,
}

/// A command's body
enum ClientMessageBody {
    /// Sending an actual command (eg. PUT, RESERVE, ...)
    Command(String),
    /// Asking the channel for n more bytes (eg. after a RESERVE, to get the job payload)
    Continuation(usize),
}

impl Beanstalk {
    /// Connects to beanstalkd
    pub async fn connect(addr: &String) -> std::io::Result<Self> {
        log::debug!("connecting to beanstalkd at {}", addr);
        TcpStream::connect(addr).await.map(|stream| {
            let (tx, rx) = mpsc::channel::<ClientMessage>(BEANSTALK_MESSAGE_QUEUE_SIZE);
            log::debug!("connected to beanstalkd at {}", addr);
            Self { stream, rx, tx }
        })
    }

    /// Provides a clone of the channel's tx, for use by any task
    pub fn proxy(&self) -> BeanstalkProxy {
        BeanstalkProxy { request_tx: self.tx.clone() }
    }

    /// The channel which owns the actual connection and processes messages
    /// Note the &mut: by taking a mut reference, this function
    /// prevents anything else from altering the Beanstalk struct
    pub async fn run_channel(&mut self) {
        log::debug!("running beanstalkd channel");

        let (read, mut write) = self.stream.split();
        let mut bufreader = BufReader::new(read);

        while let Some(message) = self.rx.recv().await {
            /* Reply to the other task with... */
            message.return_tx.send(match message.body {
                ClientMessageBody::Command(c) => {
                    /* Text command: let's send that and wait for a single line reply */
                    let mut response = String::new();
                    write.write_all(c.as_bytes()).await
                        .and(bufreader.read_line(&mut response).await)
                        .map(|_| response)
                        .map_err(|e| BeanstalkError::CommunicationError(e.to_string()))
                },
                ClientMessageBody::Continuation(n) => {
                    /* Asking for more bytes, let's read exactly that */
                    let mut buffer = vec![0 as u8; n+2];
                    bufreader.read_exact(&mut buffer).await
                        .map(|_| String::from_utf8_lossy(&buffer).trim().to_string())
                        .map_err(|e| BeanstalkError::CommunicationError(e.to_string()))
                }
            }).ok();
        }
    }
}

/// A beanstalk job
pub struct Job {
    pub id: u64,
    pub payload: String
}

/// beanstalkd statistics
#[derive(Serialize, Deserialize)]
pub struct Statistics {
    #[serde(rename = "current-jobs-ready")]
    pub jobs_ready: usize,
    #[serde(rename = "current-jobs-reserved")]
    pub jobs_reserved: usize,
    #[serde(rename = "current-jobs-delayed")]
    pub jobs_delayed: usize,
    #[serde(rename = "total-jobs")]
    pub total_jobs: usize,
    #[serde(rename = "current-connections")]
    pub current_connections: usize,
    pub uptime: u64,
}


impl BeanstalkProxy {
    /// Low level channel exchange: send a message body over the channel and wait for a reply
    async fn exchange(&self, body: ClientMessageBody) -> BeanstalkResult {
        let (tx, rx) = oneshot::channel::<BeanstalkResult>();
        self.request_tx.send(ClientMessage { return_tx: tx, body }).await
            .map_err(|e| BeanstalkError::QueueUnvailable(e.to_string()))?;
        rx.await.map_err(|e| BeanstalkError::ReturnChannelFailure(e.to_string()))?
    }

    /// Convenience function: exchange for Command messages
    async fn send_command(&self, command: String) -> BeanstalkResult {
        self.exchange(ClientMessageBody::Command(command)).await
    }

    /// Convenience function: exchange for Continuation messages
    async fn expect_data(&self, length: usize) -> BeanstalkResult {
        self.exchange(ClientMessageBody::Continuation(length)).await
    }

    /// Ask beanstalk to USE a tube on this connection
    pub async fn use_tube(&self, tube: &str) -> BeanstalkResult {
        log::debug!("using tube {}", tube);
        let using = self.send_command(format!("use {}\r\n", tube)).await?;
        match using.starts_with("USING ") {
            true => Ok(using),
            false => Err(BeanstalkError::UnexpectedResponse("use".to_string(), using))
        }
    }

    /// Ask beanstalk to WATCH a tube on this connection
    pub async fn watch_tube(&self, tube: &str) -> BeanstalkResult {
        log::debug!("watching tube {}", tube);
        let watching = self.send_command(format!("watch {}\r\n", tube)).await?;
        match watching.starts_with("WATCHING ") {
            true => Ok(watching),
            false => Err(BeanstalkError::UnexpectedResponse("watch".to_string(), watching))
        }
    }

    /// Put a job into the queue
    pub async fn put(&self, job: String) -> BeanstalkResult {
        log::debug!("putting beanstalkd job, {} byte(s)", job.len());
        let inserted = self.send_command(format!("put 0 0 60 {}\r\n{}\r\n", job.len(), job)).await?;
        match inserted.starts_with("INSERTED ") {
            true => Ok(inserted),
            false => Err(BeanstalkError::UnexpectedResponse("put".to_string(), inserted))
        }
    }

    /// Reserve a job from the queue
    pub async fn reserve(&self) -> Result<Job, BeanstalkError> {
        let command_response = self.send_command(String::from("reserve-with-timeout 5\r\n")).await?;
        let parts: Vec<&str> = command_response.trim().split(" ").collect();

        if parts.len() == 1 && parts[0] == "TIMED_OUT" {
            return Err(BeanstalkError::ReservationTimeout);
        }

        if parts.len() != 3 || parts[0] != "RESERVED" {
            return Err(BeanstalkError::UnexpectedResponse("reserve".to_string(), command_response));
        }

        let id = parts[1].parse::<u64>()
            .map_err(|_| BeanstalkError::UnexpectedResponse("reserve".to_string(), command_response.clone()))?;

        let bytes = parts[2].parse::<usize>()
            .map_err(|_| BeanstalkError::UnexpectedResponse("reserve".to_string(), command_response.clone()))?;

        Ok(Job {
            id,
            payload: self.expect_data(bytes).await?
        })
    }

    /// Delete a job from the queue
    pub async fn delete(&self, id: u64) -> BeanstalkResult {
        log::debug!("deleting job ID {}", id);
        let deleted = self.send_command(format!("delete {}\r\n", id)).await?;
        match deleted.starts_with("DELETED") {
            true => Ok(deleted),
            false => Err(BeanstalkError::UnexpectedResponse("delete".to_string(), deleted))
        }
    }

    /// Get server stats
    pub async fn stats(&self) -> Result<Statistics, BeanstalkError> {
        let command_response = self.send_command(String::from("stats\r\n")).await?;
        let parts: Vec<&str> = command_response.trim().split(" ").collect();

        if parts.len() != 2 || parts[0] != "OK" {
            return Err(BeanstalkError::UnexpectedResponse("stats-tube".to_string(), command_response));
        }

        let bytes = parts[1].parse::<usize>()
            .map_err(|_| BeanstalkError::UnexpectedResponse("stats-tube".to_string(), command_response.clone()))?;

        let stats_yaml = self.expect_data(bytes).await?;
        serde_yaml::from_str(&stats_yaml)
            .map_err(|e| BeanstalkError::UnexpectedResponse("stats-tube".to_string(), e.to_string()))
    }
}
