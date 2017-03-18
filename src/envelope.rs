use std::fmt::Debug;
use pid::Pid;
use correlation_id::CorrelationId;
use msg::Msg;

/// Envelopes are the the message type received by actors
///
/// Envelopes are routable to processes and services on all nodes.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope<T: ActorMsg>
    pub to: Pid,
    pub from: Pid,
    pub msg: MsgType<T>,
    pub correlation_id: CorrelationId
}

pub enum MsgType<T: ActorMsg> {
    User(T),
    Rabble(Msg)
}
