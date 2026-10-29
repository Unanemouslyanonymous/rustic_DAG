//! Peer to peer WebSocket mesh.
//!
//! Every node runs one WebSocket server and keeps one outbound connection to
//! every other committee member, so each pair of nodes is linked by two
//! unidirectional streams. Outbound links reconnect with exponential backoff
//! and never lose the message that was in flight when a link dropped.
//!
//! Connections are authenticated with a challenge-response: the server sends
//! a fresh nonce, the dialer signs `(from, to, nonce)` with its committee
//! key. Clients skip the signature and can only submit transactions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::config::Committee;
use crate::crypto::{KeyPair, PublicKey};
use crate::messages::{decode, encode, Challenge, ClientMessage, Handshake, Hello, PeerMessage};
use crate::types::Transaction;

/// What the network layer hands to the core.
#[derive(Debug)]
pub enum Inbound {
    Peer(PublicKey, PeerMessage),
    Client(Vec<Transaction>),
}

/// Cheap handle for sending to peers. Sends never block: if a peer's queue
/// is full the message is dropped and the sync logic recovers it later.
#[derive(Clone)]
pub struct Network {
    peers: Arc<HashMap<PublicKey, mpsc::Sender<Bytes>>>,
}

impl Network {
    pub fn from_channels(peers: HashMap<PublicKey, mpsc::Sender<Bytes>>) -> Self {
        Network { peers: Arc::new(peers) }
    }

    pub fn send(&self, to: &PublicKey, msg: &PeerMessage) {
        if let Some(tx) = self.peers.get(to) {
            Self::push(to, tx, encode(msg));
        }
    }

    pub fn broadcast(&self, msg: &PeerMessage) {
        let bytes = encode(msg);
        for (to, tx) in self.peers.iter() {
            Self::push(to, tx, bytes.clone());
        }
    }

    fn push(to: &PublicKey, tx: &mpsc::Sender<Bytes>, bytes: Bytes) {
        if let Err(mpsc::error::TrySendError::Full(_)) = tx.try_send(bytes) {
            warn!("outbound queue to {to} is full, dropping message");
        }
    }
}

/// Spawns one dialer per peer into `tasks` and returns the sending handle.
pub fn spawn_dialers(
    keypair: &KeyPair,
    committee: &Committee,
    capacity: usize,
    tasks: &mut JoinSet<()>,
) -> Network {
    let mut peers = HashMap::new();
    for (name, address) in committee.others(&keypair.name) {
        let (tx, rx) = mpsc::channel(capacity);
        peers.insert(name, tx);
        tasks.spawn(dialer(keypair.clone(), name, address, rx));
    }
    Network::from_channels(peers)
}

async fn dialer(keypair: KeyPair, peer: PublicKey, address: String, mut rx: mpsc::Receiver<Bytes>) {
    let url = format!("ws://{address}");
    let mut backlog: Option<Bytes> = None;
    let mut delay = Duration::from_millis(50);
    loop {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => {
                let (mut sink, mut stream) = ws.split();
                let challenge = match stream.next().await {
                    Some(Ok(Message::Binary(b))) => decode::<Challenge>(&b).ok(),
                    _ => None,
                };
                match challenge {
                    Some(ch) if ch.server == peer => {
                        let hello = Handshake::Peer(Hello::new(&keypair, &ch));
                        if sink.send(Message::Binary(encode(&hello))).await.is_ok() {
                            debug!("connected to {peer} at {address}");
                            delay = Duration::from_millis(50);
                            loop {
                                if backlog.is_none() {
                                    tokio::select! {
                                        m = rx.recv() => match m {
                                            Some(m) => backlog = Some(m),
                                            None => return,
                                        },
                                        incoming = stream.next() => match incoming {
                                            Some(Ok(_)) => continue,
                                            _ => break,
                                        },
                                    }
                                }
                                let m = backlog.take().expect("set above");
                                if sink.send(Message::Binary(m.clone())).await.is_err() {
                                    backlog = Some(m);
                                    break;
                                }
                            }
                            debug!("lost connection to {peer}");
                        }
                    }
                    Some(ch) => warn!("{address} claims to be {} but we expected {peer}", ch.server),
                    None => debug!("no challenge from {address}"),
                }
            }
            Err(e) => debug!("cannot reach {peer} at {address}: {e}"),
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(2));
    }
}

/// Accept loop. Connection handlers live in a `JoinSet` owned by this task,
/// so aborting it tears down every inbound connection as well.
pub async fn listen(
    listener: TcpListener,
    me: PublicKey,
    committee: Arc<Committee>,
    tx: mpsc::Sender<Inbound>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, addr)) => {
                    let _ = stream.set_nodelay(true);
                    let committee = committee.clone();
                    let tx = tx.clone();
                    connections.spawn(async move {
                        if let Err(e) = serve(stream, me, committee, tx).await {
                            debug!("connection from {addr} closed: {e}");
                        }
                    });
                }
                Err(e) => warn!("accept failed: {e}"),
            },
            Some(_) = connections.join_next() => {}
        }
    }
}

async fn serve(
    stream: TcpStream,
    me: PublicKey,
    committee: Arc<Committee>,
    tx: mpsc::Sender<Inbound>,
) -> anyhow::Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut sink, mut stream) = ws.split();

    let mut nonce = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut nonce);
    let challenge = Challenge { server: me, nonce };
    sink.send(Message::Binary(encode(&challenge))).await?;

    let first = match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
        Ok(Some(Ok(Message::Binary(b)))) => b,
        _ => anyhow::bail!("no handshake"),
    };
    match decode::<Handshake>(&first)? {
        Handshake::Peer(hello) => {
            anyhow::ensure!(committee.contains(&hello.from), "{} is not in the committee", hello.from);
            anyhow::ensure!(hello.from != me && hello.to == me, "misaddressed hello");
            hello.verify(&challenge)?;
            info!("peer {} connected", hello.from);
            while let Some(frame) = stream.next().await {
                match frame? {
                    Message::Binary(b) => match decode::<PeerMessage>(&b) {
                        Ok(msg) => {
                            if tx.send(Inbound::Peer(hello.from, msg)).await.is_err() {
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            warn!("garbage from {}: {e}", hello.from);
                            anyhow::bail!("protocol violation");
                        }
                    },
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
        Handshake::Client => {
            while let Some(frame) = stream.next().await {
                match frame? {
                    Message::Binary(b) => {
                        let ClientMessage::Transactions(txs) = decode(&b)?;
                        if tx.send(Inbound::Client(txs)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

/// Minimal client connection used by the CLI client and the benchmark.
pub struct ClientConnection {
    sink: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
        Message,
    >,
}

impl ClientConnection {
    pub async fn connect(address: &str) -> anyhow::Result<Self> {
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{address}")).await?;
        let (mut sink, mut stream) = ws.split();
        match stream.next().await {
            Some(Ok(Message::Binary(b))) => {
                decode::<Challenge>(&b)?;
            }
            _ => anyhow::bail!("{address} did not send a challenge"),
        }
        sink.send(Message::Binary(encode(&Handshake::Client))).await?;
        // Drain whatever the server sends so control frames get handled.
        tokio::spawn(async move { while let Some(Ok(_)) = stream.next().await {} });
        Ok(ClientConnection { sink })
    }

    pub async fn submit(&mut self, txs: Vec<Transaction>) -> anyhow::Result<()> {
        self.sink.send(Message::Binary(encode(&ClientMessage::Transactions(txs)))).await?;
        Ok(())
    }
}
