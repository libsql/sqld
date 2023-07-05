use std::path::Path;

use crate::replication::replica::hook::{SQLITE_CONTINUE_REPLICATION, SQLITE_EXIT_REPLICATION};

use super::hook::{InjectorHookCtx, INJECTOR_METHODS};

pub struct FrameInjector<'a> {
    conn: libsql_sys::Connection<'a>,
}

impl<'a> FrameInjector<'a> {
    pub fn new(db_path: &Path, hook_ctx: &'a mut InjectorHookCtx) -> anyhow::Result<Self> {
        let conn: libsql_sys::Connection<'_> = libsql_sys::Connection::open(
            db_path,
            libsql_sys::ffi::SQLITE_OPEN_READWRITE
                | libsql_sys::ffi::SQLITE_OPEN_CREATE
                | libsql_sys::ffi::SQLITE_OPEN_URI
                | libsql_sys::ffi::SQLITE_OPEN_NOMUTEX,
            &INJECTOR_METHODS,
            hook_ctx,
        )?;

        Ok(Self { conn })
    }

    pub fn step(&mut self) -> anyhow::Result<bool> {
        self.conn.pragma_update(None, "writable_schema", "on")?;
        let res = self.conn.execute("create table __dummy__ (dummy);", ());

        match res {
            Ok(_) => panic!("replication hook was not called"),
            Err(e) => {
                if let Some(e) = e.sqlite_error() {
                    if e.extended_code == SQLITE_EXIT_REPLICATION {
                        self.conn.pragma_update(None, "writable_schema", "reset")?;
                        return Ok(false);
                    }
                    if e.extended_code == SQLITE_CONTINUE_REPLICATION {
                        return Ok(true);
                    }
                }
                anyhow::bail!(e);
            }
        }
    }
}
