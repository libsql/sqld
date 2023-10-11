---------------------------- MODULE wal_replication ---------------------------
\* A formal specification of write-ahead log (WAL) replication algorithm.
\*
\* The algorithm assumes the presence of a write-ahead log (WAL), like the one
\* used in SQLite, where transactions append modified pages to a WAL. Each
\* modified page within the WAL is referred to as a frame and is assigned a
\* monotonically increasing frame index.
\*
\* A write is not considered durable when it is on the local server log, which
\* represents fsync'd SQLite WAL, because we assume volumes can disappear in
\* disaster recovery scenarios. A write is durable when it appears in the
\* durable log, which represents AWS S3 or similar service. We write
\* asynchronously to the durable storage, which is why on recovery we will lose
\* non-durable writes and trim the local logs.

EXTENDS Naturals, FiniteSetsExt, Sequences, SequencesExt, Bags, Functions, TLC

VARIABLE
    txID,
    commitIndex,
    commitServer,
    messages,
    logs,
    durableLog

----------------------------------------------------------------------------------
\* Cluster topology.

CONSTANT
    Servers

CONSTANTS
    Node1,
    Node2,
    Node3

IsPrimary(d) ==
    d = Node1

Primary == Node1

----------------------------------------------------------------------------------
\* Message passing. We assume an ordered networking with no duplicates.

InitMessageVar ==
    messages = [ s \in Servers |-> <<>>]

WithMessage(m, msgs) ==
    IF \E i \in 1..Len(msgs[m.dest]) : msgs[m.dest][i] = m THEN
        msgs
    ELSE
        [ msgs EXCEPT ![m.dest] = Append(@, m) ]

WithoutMessage(m, msgs) ==
    IF \E i \in 1..Len(msgs[m.dest]) : msgs[m.dest][i] = m THEN
        [ msgs EXCEPT ![m.dest] = RemoveAt(@, SelectInSeq(@, LAMBDA e: e = m)) ]
    ELSE
        msgs

Messages ==
    UNION { Range(messages[s]) : s \in Servers }

MessagesTo(dest, source) ==
    IF \E i \in 1..Len(messages[dest]) : messages[dest][i].source = source THEN
        {messages[dest][SelectInSeq(messages[dest], LAMBDA e: e.source = source)]}
    ELSE
        {}

Send(m) ==
    /\ messages' = WithMessage(m, messages)

Discard(m) ==
    messages' = WithoutMessage(m, messages)

----------------------------------------------------------------------------------
\* Protocol

\* Message types:
CONSTANTS
    GetFramesMsg,
    ExecuteMsg

RecoverLog(s) ==
    /\ logs' = [logs EXCEPT ![s] = durableLog]

Recover ==
    /\ \A s \in Servers: RecoverLog(s)
    /\ commitIndex = IF Len(durableLog) > 0 THEN Max(ToSet(durableLog)) ELSE 0
    /\ UNCHANGED(<<txID, commitIndex, commitServer, messages, durableLog>>)

SyncLog(s) ==
    /\ logs' = [logs EXCEPT ![s] = logs[Primary]]

SyncDurable(l) ==
    /\ durableLog' = durableLog \o l

AppendToLog(s, i) ==
    /\ logs' = [logs EXCEPT ![s] = Append(logs[s], i)]

HandleExecuteMsg(m) ==
    /\ IF IsPrimary(m.dest) THEN
           \* Append the write to the local WAL.
           /\ AppendToLog(m.dest, commitIndex + 1)
       ELSE
           \* Append the write to the WAL on the primary...
           /\ AppendToLog(Primary, commitIndex + 1)
           \* ...but also sync local WAL for read your writes.
           /\ SyncLog(m.dest)
    /\ SyncDurable(logs'[Primary])
    /\ commitIndex' = commitIndex + 1
    /\ commitServer' = m.dest
    /\ Discard(m)
    /\ UNCHANGED(<<txID>>)

RcvExecuteMsg(i, j) ==
    \E m \in MessagesTo(i, j) :
        /\ m.type = ExecuteMsg
        /\ HandleExecuteMsg(m)

HandleGetFramesMsg(m) ==
    /\ IsPrimary(m.dest)
    /\ SyncLog(m.source)
    /\ Discard(m)
    /\ UNCHANGED(<<txID, commitServer, commitIndex, durableLog>>)

RcvGetFramesMsg(i, j) ==
    \E m \in MessagesTo(i, j) :
        /\ m.type = GetFramesMsg
        /\ HandleGetFramesMsg(m)

Receive(i, j) ==
    \/ RcvGetFramesMsg(i, j)
    \/ RcvExecuteMsg(i, j)

SendGetFrames(s) ==
    LET
        msg == [
            type   |-> GetFramesMsg,
            dest   |-> Primary,
            source |-> s
        ]
    IN
    /\ Send(msg)
    /\ UNCHANGED(<<txID, commitIndex, commitServer, logs, durableLog>>)

SendExecute(i, j) ==
    LET
        msg == [
            type   |-> ExecuteMsg,
            txId   |-> txID,
            dest   |-> i,
            source |-> j
        ]
    IN
    /\ txID' = txID + 1
    /\ Send(msg)
    /\ UNCHANGED(<<commitIndex, commitServer, logs, durableLog>>)

Next ==
    \/ Recover
    \/ \E i, j \in Servers: SendExecute(i, j)
    \/ \E s \in Servers:    SendGetFrames(s)
    \/ \E i, j \in Servers: Receive(i, j)

Init ==
    /\ txID = 0
    /\ commitIndex = 0
    /\ commitServer = Primary
    /\ InitMessageVar
    /\ logs = [s \in Servers |-> <<>>]
    /\ durableLog = <<>>

----------------------------------------------------------------------------------
\* Invariants

ReadYourWritesInv ==
    commitIndex = 0 \/ Contains(logs[commitServer], commitIndex)

LogsAreContinuousInv ==
    \A s \in Servers: Len(logs[s]) = 0 \/ \A i \in 1..Max(ToSet(logs[s])) : Contains(logs[s], i)

NoServerIsAheadOfPrimaryInv ==
    \A s \in Servers: Len(logs[s]) <= Len(logs[Primary])

NoDurableFramesLostInv ==
    \A i \in 1..commitIndex : i \in ToSet(durableLog)

----------------------------------------------------------------------------------
\* Temporal properties

WriteLivenessProp == <>(commitIndex > 0)

ReplicationProp ==
    [] (commitIndex > 0 => \A s \in Servers : <> (Len(logs[s]) > 0))

====
