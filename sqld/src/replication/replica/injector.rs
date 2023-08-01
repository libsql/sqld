use sqld_libsql_bindings::ffi;
use std::path::Path;

use crate::replication::replica::hook::{SQLITE_CONTINUE_REPLICATION, SQLITE_EXIT_REPLICATION};

use super::hook::{InjectorHookCtx, INJECTOR_METHODS};

pub struct FrameInjector<'a> {
    conn: sqld_libsql_bindings::Connection<'a>,
}

impl<'a> FrameInjector<'a> {
    pub fn new(db_path: &Path, hook_ctx: &'a mut InjectorHookCtx) -> anyhow::Result<Self> {
        let conn = sqld_libsql_bindings::Connection::open(
            db_path,
            (ffi::SQLITE_OPEN_READWRITE
                | ffi::SQLITE_OPEN_CREATE
                | ffi::SQLITE_OPEN_URI
                | ffi::SQLITE_OPEN_NOMUTEX) as i32,
            &INJECTOR_METHODS,
            hook_ctx,
        )
        .map_err(|rc| libsql::Error::LibError(rc))?;

        Ok(Self { conn })
    }

    pub fn step(&mut self) -> anyhow::Result<bool> {
        self.conn.execute("pragma writable_schema=on", ())?;
        let res = self.conn.execute("create table __dummy__ (dummy);", ());

        match res {
            Ok(_) => panic!("replication hook was not called"),
            Err(e) => {
                if let libsql::Error::LibError(rc) = e {
                    if rc == SQLITE_EXIT_REPLICATION {
                        self.conn.execute("pragma writable_schema=reset", ())?;
                        return Ok(false);
                    }
                    if rc == SQLITE_CONTINUE_REPLICATION {
                        return Ok(true);
                    }
                }
                anyhow::bail!(e);
            }
        }
    }
}
