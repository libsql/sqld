use std::ffi::{c_int, CStr};

use rusqlite::ffi::{libsql_wal as Wal, PgHdr};
use sqld_libsql_bindings::ffi::types::XWalFrameFn;
use sqld_libsql_bindings::init_static_wal_method;
use sqld_libsql_bindings::wal_hook::WalHook;

use crate::database::frame::FrameBorrowed;
use crate::database::libsql::replication_log::WAL_PAGE_SIZE;

use super::headers::Headers;
use super::{FrameBuffer, InjectorCommitHandler};

// Those are custom error codes returned by the replicator hook.
pub const LIBSQL_INJECT_FATAL: c_int = 200;
/// Injection succeeded, left on a open txn state
pub const LIBSQL_INJECT_OK_TXN: c_int = 201;
/// Injection succeeded
pub const LIBSQL_INJECT_OK: c_int = 202;

pub struct InjectorHookCtx {
    /// shared frame buffer
    buffer: FrameBuffer,
    /// currently in a txn
    is_txn: bool,
    commit_handler: Box<dyn InjectorCommitHandler>,
}

impl InjectorHookCtx {
    pub fn new(
        buffer: FrameBuffer,
        injector_commit_handler: impl InjectorCommitHandler + 'static,
    ) -> Self {
        Self {
            buffer,
            is_txn: false,
            commit_handler: Box::new(injector_commit_handler),
        }
    }

    fn inject_pages(
        &mut self,
        sync_flags: i32,
        orig: XWalFrameFn,
        wal: *mut Wal,
    ) -> anyhow::Result<()> {
        self.is_txn = true;
        let buffer = self.buffer.borrow();
        let (mut headers, last_frame_no, size_after) =
            make_page_header(buffer.iter().map(|f| &**f));
        if size_after != 0 {
            self.commit_handler.pre_commit(last_frame_no)?;
        }

        let ret = unsafe {
            orig(
                wal,
                WAL_PAGE_SIZE,
                headers.as_ptr(),
                size_after,
                (size_after != 0) as _,
                sync_flags,
            )
        };

        if ret == 0 {
            debug_assert!(headers.all_applied());
            drop(headers);
            if size_after != 0 {
                self.commit_handler.post_commit(last_frame_no)?;
                self.is_txn = false;
            }
            tracing::trace!("applied frame batch");

            Ok(())
        } else {
            anyhow::bail!("failed to apply pages");
        }
    }
}

/// Turn a list of `WalFrame` into a list of PgHdr.
/// The caller has the responsibility to free the returned headers.
/// return (headers, last_frame_no, size_after)
fn make_page_header<'a>(
    frames: impl Iterator<Item = &'a FrameBorrowed>,
) -> (Headers<'a>, u64, u32) {
    let mut first_pg: *mut PgHdr = std::ptr::null_mut();
    let mut current_pg;
    let mut last_frame_no = 0;
    let mut size_after = 0;

    let mut headers_count = 0;
    let mut prev_pg: *mut PgHdr = std::ptr::null_mut();
    for frame in frames {
        if frame.header().frame_no > last_frame_no {
            last_frame_no = frame.header().frame_no;
            size_after = frame.header().size_after;
        }

        let page = PgHdr {
            pPage: std::ptr::null_mut(),
            pData: frame.page().as_ptr() as _,
            pExtra: std::ptr::null_mut(),
            pCache: std::ptr::null_mut(),
            pDirty: std::ptr::null_mut(),
            pPager: std::ptr::null_mut(),
            pgno: frame.header().page_no,
            pageHash: 0,
            flags: 0x02, // PGHDR_DIRTY - it works without the flag, but why risk it
            nRef: 0,
            pDirtyNext: std::ptr::null_mut(),
            pDirtyPrev: std::ptr::null_mut(),
        };
        headers_count += 1;
        current_pg = Box::into_raw(Box::new(page));
        if first_pg.is_null() {
            first_pg = current_pg;
        }
        if !prev_pg.is_null() {
            unsafe {
                (*prev_pg).pDirty = current_pg;
            }
        }
        prev_pg = current_pg;
    }

    tracing::trace!("built {headers_count} page headers");

    let headers = unsafe { Headers::new(first_pg) };
    (headers, last_frame_no, size_after)
}

init_static_wal_method!(INJECTOR_METHODS, InjectorHook);

/// The injector hook hijacks a call to xframes, and replace the content of the call with it's own
/// frames.
/// The Caller must first call `set_frames`, passing the frames to be injected, then trigger a call
/// to xFrames from the libsql connection (see dummy write in `injector`), and can then collect the
/// result on the injection with `take_result`
pub enum InjectorHook {}

unsafe impl WalHook for InjectorHook {
    type Context = InjectorHookCtx;

    fn on_frames(
        wal: &mut Wal,
        _page_size: c_int,
        _page_headers: *mut PgHdr,
        _size_after: u32,
        _is_commit: c_int,
        sync_flags: c_int,
        orig: XWalFrameFn,
    ) -> c_int {
        let wal_ptr = wal as *mut _;
        let ctx = Self::wal_extract_ctx(wal);
        let ret = ctx.inject_pages(sync_flags, orig, wal_ptr);
        if let Err(e) = ret {
            tracing::error!("fatal replication error: {e}");
            return LIBSQL_INJECT_FATAL;
        }

        ctx.buffer.borrow_mut().clear();

        if !ctx.is_txn {
            LIBSQL_INJECT_OK
        } else {
            LIBSQL_INJECT_OK_TXN
        }
    }

    fn name() -> &'static CStr {
        CStr::from_bytes_with_nul(b"frame_injector_hook\0").unwrap()
    }
}