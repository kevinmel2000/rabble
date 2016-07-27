use std::fmt::Debug;
use rustc_serialize::{Encodable, Decodable};
use pid::Pid;
use correlation_id::CorrelationId;
use msg::Msg;

/// Envelopes are routable to processes on all nodes and threads running on the same node as this
/// process.
#[derive(Debug, Clone, Eq, PartialEq, RustcEncodable, RustcDecodable)]
pub struct Envelope<T: Encodable + Decodable + Debug + Clone> {
    pub to: Pid,
    pub from: Pid,
    pub msg: Msg<T>,
    pub correlation_id: Option<CorrelationId>
}

impl<T: Encodable + Decodable + Debug + Clone> Envelope<T> {
    pub fn new(to: Pid, from: Pid, msg: Msg<T>, c_id: Option<CorrelationId>) -> Envelope<T> {
        Envelope {
            to: to,
            from: from,
            msg: msg,
            correlation_id: c_id
        }
    }
}
