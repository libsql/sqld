#![allow(improper_ctypes)]

pub mod ffi;
pub mod wal_hook;

use std::{ffi::CString, marker::PhantomData, ops::Deref};

pub use crate::wal_hook::WalMethodsHook;
pub use once_cell::sync::Lazy;

pub use libsql;

use self::{
    ffi::{libsql_wal_methods, libsql_wal_methods_find},
    wal_hook::WalHook,
};

pub fn get_orig_wal_methods() -> anyhow::Result<*mut libsql_wal_methods> {
    let orig: *mut libsql_wal_methods = unsafe { libsql_wal_methods_find(std::ptr::null()) };
    if orig.is_null() {
        anyhow::bail!("no underlying methods");
    }

    Ok(orig)
}

pub struct Connection<'a> {
    conn: libsql::Connection,
    _pth: PhantomData<&'a mut ()>,
}

impl Deref for Connection<'_> {
    type Target = libsql::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl<'a> Connection<'a> {
    /// returns a dummy, in-memory connection. For testing purposes only
    pub fn test(_: &mut ()) -> Self {
        let conn = libsql::Database::open(":memory:")
            .unwrap()
            .connect()
            .unwrap();
        Self {
            conn,
            _pth: PhantomData,
        }
    }

    /// Opens a database with the regular wal methods in the directory pointed to by path
    pub fn open<W: WalHook>(
        path: impl AsRef<std::path::Path>,
        flags: std::ffi::c_int,
        // we technically _only_ need to know about W, but requiring a static ref to the wal_hook ensures that
        // it has been instanciated and lives for long enough
        _wal_hook: &'static WalMethodsHook<W>,
        hook_ctx: &'a mut W::Context,
    ) -> Result<Self, std::ffi::c_int> {
        let path = path.as_ref().join("data");
        tracing::trace!(
            "Opening a connection with regular WAL at {}",
            path.display()
        );

        let conn_str = format!("file:{}?_journal_mode=WAL", path.display());
        let filename = CString::new(conn_str).unwrap();
        let mut db: *mut ffi::sqlite3 = std::ptr::null_mut();

        unsafe {
            // We pass a pointer to the WAL methods data to the database connection. This means
            // that the reference must outlive the connection. This is guaranteed by the marker in
            // the returned connection.
            let rc = ffi::libsql_open_v2(
                filename.as_ptr(),
                &mut db as *mut _,
                flags,
                std::ptr::null_mut(),
                W::name().as_ptr(),
                hook_ctx as *mut _ as *mut _,
            );

            if rc != 0 {
                ffi::sqlite3_close(db);
                return Err(rc);
            }

            assert!(!db.is_null());
            ffi::sqlite3_busy_timeout(db, 5000);
        };

        let conn = libsql::Connection::from_handle(db);

        Ok(Connection {
            conn,
            _pth: PhantomData,
        })
    }
}
