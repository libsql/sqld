use anyhow::{Context, Result, anyhow};
use base64::prelude::{Engine as _, BASE64_STANDARD_NO_PAD};
use hmac::Mac as _;
use std::collections::HashMap;
use std::sync::Arc;

use crate::database::Database;
use super::super::ProtocolError;
use super::Server;

pub struct Handle(Option<Box<Stream>>);

struct Stream {
    db: Option<Arc<dyn Database>>,
    stream_id: u64,
    baton_seq: u64,
}

pub struct Guard<'srv> {
    server: &'srv Server,
    stream: Option<Box<Stream>>,
}

pub async fn acquire<'srv>(server: &'srv Server, baton: Option<&str>) -> Result<Guard<'srv>> {
    let stream = match baton {
        Some(baton) => {
            let (stream_id, baton_seq) = decode_baton(server, baton)?;

            let mut streams = server.streams.lock();
            let Some(handle) = streams.get_mut(&stream_id) else {
                return Err(ProtocolError::BatonInvalid).context(format!("Stream {stream_id} was not found"));
            };
            let Some(stream) = handle.0.as_ref() else {
                return Err(ProtocolError::BatonInUse).context(format!("Stream {stream_id} is in use"));
            };
            if stream.baton_seq != baton_seq {
                return Err(ProtocolError::BatonReused)
                    .context(format!("Expected baton seq {}, received {baton_seq}", stream.baton_seq));
            }

            let mut stream = handle.0.take().unwrap();
            stream.baton_seq = stream.baton_seq.wrapping_add(1);
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
            streams.insert(stream.stream_id, Handle(None));

            stream
        }
    };
    Ok(Guard { server, stream: Some(stream) })
}

impl<'srv> Guard<'srv> {
    pub fn get_db(&self) -> Result<Arc<dyn Database>, ProtocolError> {
        let stream = self.stream.as_ref().expect("Guard has been released");
        stream.db.clone().ok_or(ProtocolError::BatonStreamClosed)
    }

    pub fn release(mut self) -> Option<String> {
        let Some(stream) = self.stream.take() else {
            panic!("Guard has already been released");
        };
        let stream_id = stream.stream_id;

        let mut streams = self.server.streams.lock();
        let Some(handle) = streams.remove(&stream_id) else {
            panic!("Released a Guard for stream {stream_id}, \
                but Server does not contain a handle to it");
        };
        if handle.0.is_some() {
            panic!("Released a Guard for stream {stream_id}, \
                but Server contained handle to a different stream");
        }

        if stream.db.is_some() {
            let baton = encode_baton(self.server, stream_id, stream.baton_seq);
            streams.insert(stream_id, Handle(Some(stream)));
            Some(baton)
        } else {
            None
        }
    }
}

impl<'srv> Drop for Guard<'srv> {
    fn drop(&mut self) {
        let Some(stream) = self.stream.take() else {
            return
        };
        let stream_id = stream.stream_id;

        let mut streams = self.server.streams.lock();
        let Some(handle) = streams.remove(&stream_id) else {
            panic!("Dropped a Guard for stream {stream_id}, \
                but Server does not contain a handle to it");
        };
        if handle.0.is_some() {
            panic!("Dropped a Guard for stream {stream_id}, \
                but Server contained a handle to a different stream");
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
