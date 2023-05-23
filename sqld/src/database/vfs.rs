use std::collections::HashMap;
use std::ffi::{c_char, c_double, c_int, c_void, CStr, CString};
use std::fs::{File, OpenOptions};
use std::iter::repeat;
use std::ops::Deref;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, UNIX_EPOCH};

use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};

static FS: Lazy<Fs> = Lazy::new(|| Fs {
    inner: Arc::new(FsInner {
        openened_files: Default::default(),
    }),
});

static VFS: Lazy<VfsBox> = Lazy::new(|| init_vfs(&*FS));

pub fn init() {
    unsafe {
        rusqlite::ffi::sqlite3_vfs_register(&VFS.0 as *const _ as *mut _, 1);
    }
}

use rand::Fill;
use rusqlite::ffi::{
    sqlite3_file, sqlite3_io_methods, sqlite3_syscall_ptr, sqlite3_vfs, SQLITE_BUSY,
    SQLITE_FCNTL_CHUNK_SIZE, SQLITE_FCNTL_EXTERNAL_READER, SQLITE_FCNTL_GET_LOCKPROXYFILE,
    SQLITE_FCNTL_HAS_MOVED, SQLITE_FCNTL_LAST_ERRNO, SQLITE_FCNTL_LOCKSTATE,
    SQLITE_FCNTL_LOCK_TIMEOUT, SQLITE_FCNTL_MMAP_SIZE, SQLITE_FCNTL_PERSIST_WAL,
    SQLITE_FCNTL_POWERSAFE_OVERWRITE, SQLITE_FCNTL_SET_LOCKPROXYFILE, SQLITE_FCNTL_SIZE_HINT,
    SQLITE_FCNTL_TEMPFILENAME, SQLITE_FCNTL_VFSNAME, SQLITE_IOERR_FSTAT, SQLITE_IOERR_FSYNC,
    SQLITE_IOERR_READ, SQLITE_IOERR_SHORT_READ, SQLITE_IOERR_TRUNCATE, SQLITE_IOERR_WRITE,
    SQLITE_NOTFOUND, SQLITE_OK, SQLITE_SHM_NLOCK, SQLITE_SHM_SHARED, SQLITE_SHM_UNLOCK,
};

const SQLD_IO_METHODS: sqlite3_io_methods = init_io_methods();

#[derive(Clone)]
#[repr(C)]
struct SqldFile {
    io_methods: *const sqlite3_io_methods,
    inner: Arc<SqldFileInner>,
}

impl Deref for SqldFile {
    type Target = SqldFileInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

struct RegularFile {
    file: File,
}

impl Drop for SqldFileInner {
    fn drop(&mut self) {
        self.fs.inner.openened_files.write().remove(&self.name);
    }
}

struct SharedMemory {
    mem: RwLock<Vec<u8>>,
    section_locks: Mutex<[i32; SQLITE_SHM_NLOCK as usize]>,
    shared_mask: AtomicU32,
    exclusive_mask: AtomicU32,
}

struct SqldFileInner {
    name: CString,
    fs: Fs,
    kind: FileKind,
}

enum FileKind {
    Regular(RegularFile),
    SharedMemory(SharedMemory),
}

impl FileKind {
    fn as_regular(&self) -> Option<&RegularFile> {
        match self {
            FileKind::Regular(ref f) => Some(f),
            FileKind::SharedMemory(_) => None,
        }
    }

    fn as_shared(&self) -> Option<&SharedMemory> {
        match self {
            FileKind::Regular(_) => None,
            FileKind::SharedMemory(ref s) => Some(s),
        }
    }
}

#[derive(Clone)]
struct Fs {
    inner: Arc<FsInner>,
}

struct FsInner {
    openened_files: RwLock<HashMap<CString, Weak<SqldFileInner>>>,
}

const fn init_io_methods() -> sqlite3_io_methods {
    sqlite3_io_methods {
        iVersion: 3,
        xClose: Some(x_close),
        xRead: Some(x_read),
        xWrite: Some(x_write),
        xTruncate: Some(x_truncate),
        xSync: Some(x_sync),
        xFileSize: Some(x_file_size),
        xLock: Some(x_lock),
        xUnlock: Some(x_unlock),
        xCheckReservedLock: Some(x_check_reserved_lock),
        xFileControl: Some(x_file_control),
        xSectorSize: Some(x_sector_size),
        xDeviceCharacteristics: Some(x_device_characteristics),
        xShmMap: Some(x_shm_map),
        xShmLock: Some(x_shm_lock),
        xShmBarrier: Some(x_shm_barrier),
        xShmUnmap: Some(x_shm_unmap),
        xFetch: Some(x_fetch),
        xUnfetch: Some(x_unfetch),
    }
}

unsafe extern "C" fn x_close(f: *mut sqlite3_file) -> c_int {
    // decrement refcount on the file
    let _ = *(f as *mut SqldFile);
    0
}

unsafe extern "C" fn x_read(
    f: *mut sqlite3_file,
    buffer: *mut c_void,
    n: i32,
    offset: i64,
) -> c_int {
    let f = &*(f as *mut SqldFile);
    let reg_file = f.kind.as_regular().unwrap();
    let buf = std::slice::from_raw_parts_mut(buffer as _, n as _);
    // dbg!(offset);
    // dbg!(&f.name);
    match reg_file.file.read_at(buf, offset as _) {
        Ok(amt) => {
            if n as usize != amt {
                SQLITE_IOERR_SHORT_READ
            } else {
                SQLITE_OK
            }
        }
        Err(_) => SQLITE_IOERR_READ,
    }
}

unsafe extern "C" fn x_write(
    f: *mut sqlite3_file,
    buffer: *const c_void,
    n: i32,
    offset: i64,
) -> c_int {
    let f = &*(f as *mut SqldFile);
    let reg_file = f.kind.as_regular().unwrap();
    let buf = std::slice::from_raw_parts(buffer as *const u8, n as _);
    // dbg!(offset);
    // dbg!(&f.name);
    match reg_file.file.write_all_at(buf, offset as _) {
        Ok(_) => SQLITE_OK,
        Err(_) => SQLITE_IOERR_WRITE,
    }
}

unsafe extern "C" fn x_truncate(f: *mut sqlite3_file, n: i64) -> i32 {
    let f = &*(f as *mut SqldFile);
    let reg_file = f.kind.as_regular().unwrap();
    match reg_file.file.set_len(n as _) {
        Ok(()) => SQLITE_OK,
        Err(_) => SQLITE_IOERR_TRUNCATE,
    }
}

unsafe extern "C" fn x_sync(f: *mut sqlite3_file, _flag: i32) -> i32 {
    let f = &*(f as *mut SqldFile);
    let reg_file = f.kind.as_regular().unwrap();
    match reg_file.file.sync_data() {
        Ok(_) => SQLITE_OK,
        Err(_) => SQLITE_IOERR_FSYNC,
    }
}

unsafe extern "C" fn x_file_size(f: *mut sqlite3_file, out: *mut i64) -> i32 {
    let f = &*(f as *mut SqldFile);
    let reg_file = f.kind.as_regular().unwrap();
    match reg_file.file.metadata() {
        Ok(m) => {
            out.write(m.len() as i64);
            SQLITE_OK
        }
        Err(_) => SQLITE_IOERR_FSTAT,
    }
}
unsafe extern "C" fn x_lock(_f: *mut sqlite3_file, _level: i32) -> i32 {
    SQLITE_OK
}

unsafe extern "C" fn x_unlock(_f: *mut sqlite3_file, _flag: i32) -> i32 {
    SQLITE_OK
}

unsafe extern "C" fn x_check_reserved_lock(_f: *mut sqlite3_file, out: *mut i32) -> i32 {
    *out = 0;
    SQLITE_OK
}

unsafe extern "C" fn x_file_control(f: *mut sqlite3_file, op: i32, arg: *mut c_void) -> i32 {
    let f = &*(f as *mut SqldFile);
    match dbg!(op) {
        // #if defined(__linux__) && defined(SQLITE_ENABLE_BATCH_ATOMIC_WRITE)
        //     case SQLITE_FCNTL_BEGIN_ATOMIC_WRITE: {
        //       int rc = osIoctl(pFile->h, F2FS_IOC_START_ATOMIC_WRITE);
        //       return rc ? SQLITE_IOERR_BEGIN_ATOMIC : SQLITE_OK;
        //     }
        //     case SQLITE_FCNTL_COMMIT_ATOMIC_WRITE: {
        //       int rc = osIoctl(pFile->h, F2FS_IOC_COMMIT_ATOMIC_WRITE);
        //       return rc ? SQLITE_IOERR_COMMIT_ATOMIC : SQLITE_OK;
        //     }
        //     case SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE: {
        //       int rc = osIoctl(pFile->h, F2FS_IOC_ABORT_VOLATILE_WRITE);
        //       return rc ? SQLITE_IOERR_ROLLBACK_ATOMIC : SQLITE_OK;
        //     }
        // #endif /* __linux__ && SQLITE_ENABLE_BATCH_ATOMIC_WRITE */
        SQLITE_FCNTL_LOCKSTATE => {
            dbg!();
            // *(int*)pArg = pFile->eFileLock;
            return SQLITE_OK;
        }
        SQLITE_FCNTL_LAST_ERRNO => {
            dbg!();
            // *(int*)pArg = pFile->lastErrno;
            return SQLITE_OK;
        }
        SQLITE_FCNTL_CHUNK_SIZE => {
            dbg!();
            // pFile->szChunk = *(int *)pArg;
            return SQLITE_OK;
        }
        SQLITE_FCNTL_SIZE_HINT => {
            dbg!(&f.name);
            f.kind
                .as_regular()
                .unwrap()
                .file
                .set_len(*(arg as *const u64))
                .unwrap();
            // int rc;
            // SimulateIOErrorBenign(1);
            // rc = fcntlSizeHint(pFile, *(i64 *)pArg);
            // SimulateIOErrorBenign(0);
            return SQLITE_OK;
        }
        SQLITE_FCNTL_PERSIST_WAL => {
            dbg!();
            // unixModeBit(pFile, UNIXFILE_PERSIST_WAL, (int*)pArg);
            return SQLITE_OK;
        }
        SQLITE_FCNTL_POWERSAFE_OVERWRITE => {
            dbg!();
            // unixModeBit(pFile, UNIXFILE_PSOW, (int*)pArg);
            return SQLITE_OK;
        }
        SQLITE_FCNTL_VFSNAME => {
            dbg!();
            // *(char**)pArg = sqlite3_mprintf("%s", pFile->pVfs->zName);
            return SQLITE_OK;
        }
        SQLITE_FCNTL_TEMPFILENAME => {
            dbg!();
            // char *zTFile = sqlite3_malloc64( pFile->pVfs->mxPathname );
            // if( zTFile ){
            //   unixGetTempname(pFile->pVfs->mxPathname, zTFile);
            //   *(char**)pArg = zTFile;
            // }
            return SQLITE_OK;
        }
        SQLITE_FCNTL_HAS_MOVED => {
            dbg!();
            // *(int*)pArg = fileHasMoved(pFile);
            return SQLITE_OK;
        }
        SQLITE_FCNTL_LOCK_TIMEOUT => {
            dbg!();
            // int iOld = pFile->iBusyTimeout;
            // pFile->iBusyTimeout = *(int*)pArg;
            // *(int*)pArg = iOld;
            return SQLITE_OK;
        }
        SQLITE_FCNTL_MMAP_SIZE => {
            *(arg as *mut i64) = 1024 * 1024 * 10; // 10MiB
            return SQLITE_OK;
            // i64 newLimit = *(i64*)pArg;
            // int rc = SQLITE_OK;
            // if( newLimit>sqlite3GlobalConfig.mxMmap ){
            //   newLimit = sqlite3GlobalConfig.mxMmap;
            // }
            //
            // /* The value of newLimit may be eventually cast to (size_t) and passed
            // ** to mmap(). Restrict its value to 2GB if (size_t) is not at least a
            // ** 64-bit type. */
            // if( newLimit>0 && sizeof(size_t)<8 ){
            //   newLimit = (newLimit & 0x7FFFFFFF);
            // }
            //
            // *(i64*)pArg = pFile->mmapSizeMax;
            // if( newLimit>=0 && newLimit!=pFile->mmapSizeMax && pFile->nFetchOut==0 ){
            //   pFile->mmapSizeMax = newLimit;
            //   if( pFile->mmapSize>0 ){
            //     unixUnmapfile(pFile);
            //     rc = unixMapfile(pFile, -1);
            //   }
            // }
        }
        // #ifdef SQLITE_DEBUG
        //     /* The pager calls this method to signal that it has done
        //     ** a rollback and that the database is therefore unchanged and
        //     ** it hence it is OK for the transaction change counter to be
        //     ** unchanged.
        //     */
        //     case SQLITE_FCNTL_DB_UNCHANGED: {
        //       ((unixFile*)id)->dbUpdate = 0;
        //       return SQLITE_OK;
        //     }
        SQLITE_FCNTL_SET_LOCKPROXYFILE | SQLITE_FCNTL_GET_LOCKPROXYFILE => {
            // return proxyFileControl(id, op, pArg);
            return SQLITE_OK;
        }
        //
        SQLITE_FCNTL_EXTERNAL_READER => {
            // #ifndef SQLITE_OMIT_WAL
            //       return unixFcntlExternalReader((unixFile*)id, (int*)pArg);
            // #else
            //       *(int*)pArg = 0;
            return SQLITE_OK;
        }
        _ => SQLITE_NOTFOUND,
    }
}

extern "C" fn x_sector_size(_f: *mut sqlite3_file) -> i32 {
    0
}

extern "C" fn x_device_characteristics(_f: *mut sqlite3_file) -> i32 {
    SQLITE_NOTFOUND
}

unsafe extern "C" fn x_shm_map(
    f: *mut sqlite3_file,
    i_pg: i32,
    page_size: i32,
    extend: i32,
    p: *mut *mut c_void,
) -> i32 {
    let f = &*(f as *mut SqldFile);
    let shared = f.kind.as_shared().unwrap();
    let offset = i_pg as usize * page_size as usize;
    //todo make sure this is the wal index
    //todo: only return section if we have the lock?
    let mem = shared.mem.upgradable_read();
    let wal_index_ptr = mem.as_ptr() as *mut u8;
    if offset >= mem.len() {
        if extend != 0 {
            let mut mem = RwLockUpgradableReadGuard::upgrade(mem);
            let mem_len = mem.len();
            mem.extend(repeat(0).take((offset + page_size as usize) - mem_len));
        } else {
            p.write(std::ptr::null_mut());
            return SQLITE_OK;
        }
    }

    p.write(wal_index_ptr.offset(offset as _) as *mut c_void);

    SQLITE_OK
}

unsafe extern "C" fn x_shm_lock(f: *mut sqlite3_file, offset: i32, n: i32, flags: i32) -> i32 {
    let f = &*(f as *mut SqldFile);
    let shared = f.kind.as_shared().unwrap();
    dbg!(&f.name);

    let offset = offset as usize;
    let n = n as usize;

    let mut locks = shared.section_locks.lock();
    // this is the mask of all the lock we are interested in changing
    let mask = (1 << offset + n) - (1 << offset);
    let shared_mask = shared.shared_mask.load(Ordering::Relaxed);
    let exclusive_mask = shared.exclusive_mask.load(Ordering::Relaxed);
    if flags & SQLITE_SHM_UNLOCK != 0 {
        let mut unlock = true;
        for i in offset..offset + n {
            // we can unlock iif all the lock we are trying to unlock are exclusive (locks[i] == -1),
            // if we are the last owner of a shared lock (locks[i] == 1), or if the lock is already
            // unlocked (lock[i] == 0).
            if locks[i] > (shared_mask & 1 << i != 0) as i32 {
                unlock = false;
            }
        }

        if unlock {
            // unlock all the locks.
            locks[offset..offset + n].iter_mut().for_each(|x| *x = 0);
        } else {
            assert_eq!(n, 1);
            assert!(locks[offset] > 1);
        }

        // unset the locks
        shared.exclusive_mask.fetch_and(!mask, Ordering::Relaxed);
        shared.shared_mask.fetch_and(!mask, Ordering::Relaxed);
    } else if flags & SQLITE_SHM_SHARED != 0 {
        assert_eq!(n, 1);
        if shared_mask & mask == 0 {
            if locks[offset] < 0 {
                return SQLITE_BUSY;
            }

            shared.shared_mask.fetch_or(mask, Ordering::Relaxed);
            locks[offset] += 1;
        }
    } else {
        for i in offset..offset + n {
            // we can unlock iif all the lock we are trying to unlock are exclusive (locks[i] == -1),
            // if we are the last owner of a shared lock (locks[i] == 1), or if the lock is already
            // unlocked (lock[i] == 0).
            if locks[i] != 0 && (exclusive_mask & 1 << i) == 0 {
                return SQLITE_BUSY;
            }

            shared.exclusive_mask.fetch_or(mask, Ordering::Relaxed);
            locks[offset..offset + n].iter_mut().for_each(|x| *x = -1);
        }
    }

    SQLITE_OK
}
extern "C" fn x_shm_barrier(_f: *mut sqlite3_file) {
    // No need for of barrier, since we're all in the same process?
}
unsafe extern "C" fn x_shm_unmap(_f: *mut sqlite3_file, _flags: c_int) -> c_int {
    SQLITE_OK
}
extern "C" fn x_fetch(
    _f: *mut sqlite3_file,
    _offset: i64,
    _amount: i32,
    _pp: *mut *mut c_void,
) -> i32 {
    panic!("not sure what this does")
}
extern "C" fn x_unfetch(_f: *mut sqlite3_file, _offset: i64, _p: *mut c_void) -> i32 {
    panic!("not sure what this does either");
}

unsafe impl Send for VfsBox {}
unsafe impl Sync for VfsBox {}

struct VfsBox(sqlite3_vfs);

fn init_vfs(fs: &'static Fs) -> VfsBox {
    VfsBox(sqlite3_vfs {
        iVersion: 3,
        szOsFile: std::mem::size_of::<SqldFile>() as _,
        mxPathname: 300, // 300 bytes for a path name ough to be enough
        pNext: std::ptr::null_mut(),
        zName: CStr::from_bytes_with_nul(b"sqld_vfs\0").unwrap().as_ptr(),
        pAppData: fs as *const _ as *mut _,
        xOpen: Some(x_open),
        xDelete: Some(x_delete),
        xAccess: Some(x_access),
        xFullPathname: Some(x_full_pathname),
        xDlOpen: Some(x_dl_open),
        xDlError: Some(x_dl_error),
        xDlSym: Some(x_dl_sym),
        xDlClose: Some(x_dl_close),
        xRandomness: Some(x_randomness),
        xSleep: Some(x_sleep),
        xCurrentTime: Some(x_current_time),
        xGetLastError: Some(x_get_last_error),
        xCurrentTimeInt64: Some(x_get_current_time_u64),
        xSetSystemCall: Some(x_set_sys_call),
        xGetSystemCall: Some(x_get_syscall),
        xNextSystemCall: Some(x_next_syscall),
    })
}

enum FileType {
    Shm,
    Db,
    Wal,
}

fn path_type(p: &Path) -> FileType {
    let fname = p.file_name().unwrap().to_str().unwrap();
    if fname.ends_with("-shm") {
        FileType::Shm
    } else if fname.ends_with("-wal") {
        FileType::Wal
    } else {
        FileType::Db
    }
}

unsafe extern "C" fn x_open(
    vfs: *mut sqlite3_vfs,
    path: *const c_char,
    file: *mut sqlite3_file,
    _flags: i32,
    _out_flags: *mut i32,
) -> i32 {
    let fs = &*((*vfs).pAppData as *const Fs);
    let path_str = CStr::from_ptr(path).to_owned();
    let path = Path::new(path_str.to_str().unwrap());

    let lock = fs.inner.openened_files.upgradable_read();
    if let Some(maybe_file) = lock.get(&path_str) {
        if let Some(inner) = maybe_file.upgrade() {
            (file as *mut SqldFile).write(SqldFile {
                io_methods: &SQLD_IO_METHODS as *const _,
                inner,
            });
            return SQLITE_OK;
        }
    }

    let kind = match path_type(path) {
        FileType::Shm => FileKind::SharedMemory(SharedMemory {
            mem: RwLock::new(vec![0; SQLITE_SHM_NLOCK as usize * 4096]),
            section_locks: Mutex::new([0; SQLITE_SHM_NLOCK as usize]),
            shared_mask: AtomicU32::new(0),
            exclusive_mask: AtomicU32::new(0),
        }),
        FileType::Db | FileType::Wal => {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(path)
                .unwrap();
            FileKind::Regular(RegularFile { file })
        }
    };

    let inner = Arc::new(SqldFileInner {
        name: path_str.clone(),
        fs: fs.clone(),
        kind,
    });

    let mut lock = RwLockUpgradableReadGuard::upgrade(lock);
    lock.insert(path_str.to_owned(), Arc::downgrade(&inner));

    (file as *mut SqldFile).write(SqldFile {
        io_methods: &SQLD_IO_METHODS as *const _,
        inner,
    });

    SQLITE_OK
}

extern "C" fn x_delete(_vfs: *mut sqlite3_vfs, _path: *const c_char, _sync_dir: i32) -> i32 {
    SQLITE_OK
}

unsafe extern "C" fn x_access(
    _vfs: *mut sqlite3_vfs,
    path: *const c_char,
    _flags: i32,
    out: *mut i32,
) -> i32 {
    // FIXME: report accurate file permissions.
    let path = Path::new(CStr::from_ptr(path).to_str().unwrap());
    *out = if !path.exists() { 0 } else { 1 };

    SQLITE_OK
}

unsafe extern "C" fn x_full_pathname(
    _vfs: *mut sqlite3_vfs,
    path: *const c_char,
    n_out: i32,
    p_out: *mut c_char,
) -> i32 {
    let current = std::env::current_dir().unwrap();
    let path = Path::new(CStr::from_ptr(path).to_str().unwrap());
    let absolute = current.join(path);
    dbg!(path);
    let p_str = absolute.to_str().unwrap();

    assert!(p_str.len() + 1 < n_out as usize);

    p_out.copy_from(p_str.as_ptr() as *const _, p_str.len());
    p_out.offset(p_str.len() as _).write(0); // null terminate

    SQLITE_OK
}
extern "C" fn x_dl_open(_vfs: *mut sqlite3_vfs, _fname: *const c_char) -> *mut c_void {
    todo!()
}

extern "C" fn x_dl_error(_vfs: *mut sqlite3_vfs, _n_bytes: i32, _msg: *mut c_char) {
    todo!()
}

unsafe extern "C" fn x_dl_sym(
    _vfs: *mut sqlite3_vfs,
    _p: *mut c_void,
    _sym: *const c_char,
) -> Option<unsafe extern "C" fn(*mut sqlite3_vfs, *mut c_void, *const c_char)> {
    todo!()
}

extern "C" fn x_dl_close(_vfs: *mut sqlite3_vfs, _p: *mut c_void) {
    todo!()
}

unsafe extern "C" fn x_randomness(_vfs: *mut sqlite3_vfs, n_bytes: i32, out: *mut c_char) -> c_int {
    std::slice::from_raw_parts_mut(out, n_bytes as usize)
        .try_fill(&mut rand::thread_rng())
        .unwrap();

    SQLITE_OK
}

extern "C" fn x_sleep(_vfs: *mut sqlite3_vfs, us: c_int) -> c_int {
    std::thread::sleep(Duration::from_micros(us as _));
    SQLITE_OK
}

unsafe extern "C" fn x_current_time(_vfs: *mut sqlite3_vfs, out: *mut c_double) -> c_int {
    *out = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        / 86400000.0;
    SQLITE_OK
}

extern "C" fn x_get_last_error(_vfs: *mut sqlite3_vfs, _size: c_int, _msg: *mut c_char) -> c_int {
    std::io::Error::last_os_error().raw_os_error().unwrap()
}

unsafe extern "C" fn x_get_current_time_u64(_vfs: *mut sqlite3_vfs, out: *mut i64) -> c_int {
    *out = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as _;
    SQLITE_OK
}

unsafe extern "C" fn x_set_sys_call(
    _vfs: *mut sqlite3_vfs,
    _name: *const i8,
    _p: Option<unsafe extern "C" fn()>,
) -> c_int {
    todo!()
}
extern "C" fn x_get_syscall(_vfs: *mut sqlite3_vfs, _name: *const c_char) -> sqlite3_syscall_ptr {
    todo!()
}
extern "C" fn x_next_syscall(_vfs: *mut sqlite3_vfs, _name: *const c_char) -> *const c_char {
    todo!()
}
