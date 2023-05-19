use anyhow::{Context, Result, anyhow};
use base64::prelude::{Engine as _, BASE64_STANDARD_NO_PAD};
use hmac::Mac as _;
use priority_queue::PriorityQueue;
use std::{future, mem, task};
use std::collections::{HashMap, VecDeque};
use std::cmp::Reverse;
use std::future::Future as _;
use std::pin::Pin;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

use crate::database::Database;
use super::super::ProtocolError;
use super::Server;

pub enum Handle {
    Available(Box<Stream>),
    Acquired,
    Expired,
}

pub struct Stream {
    db: Option<Arc<dyn Database>>,
    stream_id: u64,
    baton_seq: u64,
}

pub struct Guard<'srv> {
    server: &'srv Server,
    stream: Option<Box<Stream>>,
    release: bool,
}

pub async fn acquire<'srv>(server: &'srv Server, baton: Option<&str>) -> Result<Guard<'srv>> {
    let stream = match baton {
        Some(baton) => {
            let (stream_id, baton_seq) = decode_baton(server, baton)?;

            let mut streams = server.streams.lock();
            let handle = streams.get_mut(&stream_id);
            match handle {
                None => {
                    return Err(ProtocolError::BatonInvalid)
                        .context(format!("Stream handle for {stream_id} was not found"));
                }
                Some(Handle::Acquired) => {
                    return Err(ProtocolError::BatonReused)
                        .context(format!("Stream handle for {stream_id} is acquired"));
                }
                Some(Handle::Expired) => {
                    return Err(ProtocolError::BatonStreamExpired)
                        .context(format!("Stream handle for {stream_id} is expired"));
                }
                Some(Handle::Available(stream)) => {
                    if stream.baton_seq != baton_seq {
                        return Err(ProtocolError::BatonReused)
                            .context(format!("Expected baton seq {}, received {baton_seq}", stream.baton_seq));
                    }
                }
            };

            let Handle::Available(mut stream) = mem::replace(handle.unwrap(), Handle::Acquired) else {
                unreachable!()
            };

            tracing::debug!("Stream {stream_id} was acquired with baton seq {baton_seq}");
            stream.baton_seq = stream.baton_seq.wrapping_add(1);
            unmark_expire(server, stream.stream_id);
            stream
        }
        None => {
            let db = server.db_factory.create().await
                .context("Could not create a database connection")?;

            let mut streams = server.streams.lock();
            let stream = Box::new(Stream {
                db: Some(db),
                stream_id: gen_stream_id(&mut streams),
                baton_seq: rand::random(),
            });
            streams.insert(stream.stream_id, Handle::Acquired);
            tracing::debug!("Stream {} was created with baton seq {}", stream.stream_id, stream.baton_seq);
            stream
        }
    };
    Ok(Guard { server, stream: Some(stream), release: false })
}

impl<'srv> Guard<'srv> {
    pub fn get_db(&self) -> Result<Arc<dyn Database>, ProtocolError> {
        let stream = self.stream.as_ref().unwrap();
        stream.db.clone().ok_or(ProtocolError::BatonStreamClosed)
    }

    pub fn release(mut self) -> Option<String> {
        let stream = self.stream.as_ref().unwrap();
        if stream.db.is_some() {
            self.release = true; // tell destructor to make the stream available again
            Some(encode_baton(self.server, stream.stream_id, stream.baton_seq))
        } else {
            None
        }
    }
}

impl<'srv> Drop for Guard<'srv> {
    fn drop(&mut self) {
        let stream = self.stream.take().unwrap();
        let stream_id = stream.stream_id;

        let mut streams = self.server.streams.lock();
        let Some(handle) = streams.remove(&stream_id) else {
            panic!("Dropped a Guard for stream {stream_id}, \
                but Server does not contain a handle to it");
        };
        if !matches!(handle, Handle::Acquired) {
            panic!("Dropped a Guard for stream {stream_id}, \
                but Server contained handle that is not acquired");
        }

        if self.release {
            streams.insert(stream_id, Handle::Available(stream));
            mark_expire(self.server, stream_id);
            tracing::debug!("Stream {stream_id} was released for further use");
        } else {
            tracing::debug!("Stream {stream_id} was closed");
        }
    }
}



fn gen_stream_id(streams: &mut HashMap<u64, Handle>) -> u64 {
    for _ in 0..10 {
        let stream_id = rand::random();
        if !streams.contains_key(&stream_id) {
            return stream_id
        }
    }
    panic!("Failed to generate a free stream id with rejection sampling")
}

fn decode_baton(server: &Server, baton_str: &str) -> Result<(u64, u64)> {
    let baton_data = BASE64_STANDARD_NO_PAD.decode(baton_str).map_err(|err| {
        anyhow!(ProtocolError::BatonInvalid)
            .context(format!("Could not base64-decode baton: {err}"))
    })?;

    if baton_data.len() != 48 {
        return Err(ProtocolError::BatonInvalid)
            .context(format!("Baton has invalid size of {} bytes", baton_data.len()))
    }

    let message = &baton_data[0..16];
    let received_mac = &baton_data[16..48];

    let mut hmac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&server.baton_key).unwrap();
    hmac.update(message);
    hmac.verify_slice(received_mac).map_err(|_| {
        anyhow!(ProtocolError::BatonInvalid).context("Invalid MAC on baton")
    })?;

    let stream_id = u64::from_be_bytes(message[0..8].try_into().unwrap());
    let baton_seq = u64::from_be_bytes(message[8..16].try_into().unwrap());
    Ok((stream_id, baton_seq))
}

fn encode_baton(server: &Server, stream_id: u64, baton_seq: u64) -> String {
    let mut message = [0; 16];
    message[0..8].copy_from_slice(&stream_id.to_be_bytes());
    message[8..16].copy_from_slice(&baton_seq.to_be_bytes());

    let mut hmac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&server.baton_key).unwrap();
    hmac.update(&message);
    let mac = hmac.finalize().into_bytes();

    let mut baton_data = [0; 48];
    baton_data[0..16].copy_from_slice(&message);
    baton_data[16..48].copy_from_slice(&mac);
    BASE64_STANDARD_NO_PAD.encode(&baton_data)
}



const EXPIRATION: Duration = Duration::from_secs(10);
const CLEANUP: Duration = Duration::from_secs(300);

pub struct ExpireState {
    expire_queue: PriorityQueue<u64, Reverse<Instant>>,
    cleanup_queue: VecDeque<(u64, Instant)>,
    sleep: Pin<Box<tokio::time::Sleep>>,
    waker: Option<task::Waker>,
    round_base: Instant,
}

impl ExpireState {
    pub fn new() -> Self {
        Self {
            expire_queue: PriorityQueue::new(),
            cleanup_queue: VecDeque::new(),
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            waker: None,
            round_base: Instant::now(),
        }
    }
}

fn unmark_expire(server: &Server, stream_id: u64) {
    let mut state = server.expire_state.lock();
    state.expire_queue.remove(&stream_id);
}

fn mark_expire(server: &Server, stream_id: u64) {
    let mut state = server.expire_state.lock();
    let expire_at = roundup_instant(&state, Instant::now() + EXPIRATION);
    if state.sleep.deadline() > expire_at {
        state.waker.take().map(|waker| waker.wake());
    }
    state.expire_queue.push(stream_id, Reverse(expire_at));
}

pub async fn run_expire(server: &Server) {
    future::poll_fn(|cx| {
        pump_expire(server, cx);
        task::Poll::Pending
    }).await
}

fn pump_expire(server: &Server, cx: &mut task::Context) {
    let mut streams = server.streams.lock();
    let mut state = server.expire_state.lock();
    let now = Instant::now();

    let wakeup_at = loop {
        let stream_id = match state.expire_queue.peek() {
            Some((&stream_id, &Reverse(expire_at))) => {
                if expire_at <= now {
                    stream_id
                } else {
                    break expire_at
                }
            }
            None => break now + Duration::from_secs(60),
        };
        state.expire_queue.pop();

        let handle = streams.get_mut(&stream_id);
        if !matches!(handle, Some(Handle::Available(_))) {
            continue
        }
        *handle.unwrap() = Handle::Expired;
        tracing::debug!("Stream {stream_id} was expired");

        let cleanup_at = roundup_instant(&state, now + CLEANUP);
        state.cleanup_queue.push_back((stream_id, cleanup_at));
    };

    loop {
        let stream_id = match state.cleanup_queue.front() {
            Some(&(stream_id, cleanup_at)) if cleanup_at <= now => stream_id,
            _ => break,
        };
        state.cleanup_queue.pop_front();

        let handle = streams.remove(&stream_id);
        assert!(matches!(handle, Some(Handle::Expired)));
        tracing::debug!("Stream {stream_id} was cleaned up after expiration");
    }

    state.sleep.as_mut().reset(wakeup_at);
    state.waker = Some(cx.waker().clone());
    let _: task::Poll<()> = state.sleep.as_mut().poll(cx);
}

fn roundup_instant(state: &ExpireState, instant: Instant) -> Instant {
    let duration_s = (instant - state.round_base).as_secs();
    state.round_base + Duration::from_secs(duration_s + 1)
}
