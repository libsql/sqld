use std::collections::HashMap;

use tokio::{sync::{mpsc, oneshot}, task::{JoinSet, block_in_place}};

pub mod config;

type ExecFn = Box<dyn FnOnce(&mut dyn libsqlx::Connection) + Send>;

#[derive(Clone)]
struct ConnectionId {
    id: u32,
    close_sender: mpsc::Sender<()>,
}

enum AllocationMessage {
    /// Execute callback against connection
    Exec {
        connection_id: ConnectionId,
        exec: ExecFn,
    },
    /// Create a new connection, execute the callback and return the connection id.
    NewConnExec {
        exec: ExecFn,
        ret: oneshot::Sender<ConnectionId>,
    }
}

enum Database {}

impl Database {
    fn connect(&self) -> Box<dyn libsqlx::Connection + Send> {
        todo!();
    }
}

pub struct Allocation {
    inbox: mpsc::Receiver<AllocationMessage>,
    database: Database,
    /// senders to the spawned connections
    connections: HashMap<u32, mpsc::Sender<ExecFn>>,
    /// spawned connection futures, returning their connection id on completion.
    connections_futs: JoinSet<u32>,
    next_conn_id: u32,
    max_concurrent_connections: u32,
}

impl Allocation {
    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        AllocationMessage::Exec { connection_id, exec } => {
                            if let Some(sender) = self.connections.get(&connection_id.id) {
                                if let Err(_) = sender.send(exec).await {
                                    tracing::debug!("connection {} closed.", connection_id.id);
                                    self.connections.remove_entry(&connection_id.id);
                                }
                            }
                        },
                        AllocationMessage::NewConnExec { exec, ret } => {
                            let id = self.new_conn_exec(exec).await;
                            let _ = ret.send(id);
                        },
                    }
                },
                maybe_id = self.connections_futs.join_next() => {
                    if let Some(Ok(id)) = maybe_id {
                        self.connections.remove_entry(&id);
                    }
                },
                else => break,
            }
        }
    }

    async fn new_conn_exec(&mut self, exec: ExecFn) -> ConnectionId {
        let id = self.next_conn_id();
        let conn = block_in_place(|| self.database.connect());
        let (close_sender, exit) = mpsc::channel(1);
        let (exec_sender, exec_receiver) = mpsc::channel(1);
        let conn = Connection {
            id,
            conn,
            exit,
            exec: exec_receiver,
        };


        self.connections_futs.spawn(conn.run());
        // This should never block!
        assert!(exec_sender.try_send(exec).is_ok());
        assert!(self.connections.insert(id, exec_sender).is_none());

        ConnectionId {
            id,
            close_sender,
        }
    }

    fn next_conn_id(&mut self) -> u32 {
        loop {
            self.next_conn_id = self.next_conn_id.wrapping_add(1);
            if !self.connections.contains_key(&self.next_conn_id) {
                return self.next_conn_id
            }
        }
    }
}

struct Connection {
    id: u32,
    conn: Box<dyn libsqlx::Connection + Send>,
    exit: mpsc::Receiver<()>,
    exec: mpsc::Receiver<ExecFn>,
}

impl Connection {
    async fn run(mut self) -> u32 {
        loop {
            tokio::select! {
                _ = self.exit.recv() => break,
                Some(exec) = self.exec.recv() => {
                    tokio::task::block_in_place(|| exec(&mut *self.conn));
                }
            }
        }

        self.id
    }
}
