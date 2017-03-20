use std::convert::From;
use amy::Notification;
use msgpack::{Encoder, Decoder};
use orset::{ORSet, Delta};
use node_id::NodeId;
use envelope::Envelope;
use correlation_id::CorrelationId;
use user_msg::UserMsg;
use pb_messages;

/// Messages sent to the Cluster Server
pub enum ClusterMsg<T: UserMsg> {
    PollNotifications(Vec<Notification>),
    Join(NodeId),
    Leave(NodeId),
    Envelope(Envelope<T>),
    Shutdown
}

/// A message sent between nodes in Rabble.
///
#[derive(Debug, Clone)]
pub enum ExternalMsg<T: UserMsg> {
   Members {from: NodeId, orset: ORSet<NodeId>},
   Ping,
   Envelope(Envelope<T>),
   Delta(Delta<NodeId>)
}

impl<T: UserMsg> From<pb_messages::ClusterServerMsg> for ExternalMsg<T> {
    fn from(pb_msg: pb_messages::ClusterServerMsg) -> ExternalMsg<T> {
        if pb_msg.has_envelope() {
            return ExternalMsg::Envelope(pb.take_envelope().into());
        }
        if pb_msg.has_ping() {
            return ExternalMsg::Ping;
        }
        if pb_msg.has_orset() {
            let pb_orset = pb_msg.take_orset();
            let from = pb_orset.take_from().into();
            // ORsets are serialized as msgpack data still
            let serialized_orset = pb_orset.take_orset();
            let mut decoder = Decoder::new(&serialized_orset[..]);
        }
        if pb_msg.has_delta() {
        }
}
