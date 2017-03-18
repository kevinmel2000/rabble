use std::fmt::Debug;
use correlation_id::CorrelationId;
use metrics::Metric;
use pb_messages::PbMsg;

type Name = &'static str;

#[derive(Debug, Clone, PartialEq)]
pub enum Msg {
    Req(Req),
    Rpy(Rpy),
    Unknown
}

/// A request message sent to pids. Not all requests have replies.
pub enum Req {
    GetMetrics,
    StartTimer(u64),// time in ms
    CancelTimer,
    Shutdown,
    GetProcesses,
    GetServices
}

/// A reply message sent to pids. 
pub enum Rpy {
    Timeout,
    Metrics(Vec<(Name, Metric)>),
    Processes(Vec<Pid>),
    Services(Vec<Pid>),
    Members(Vec<(NodeId, Connected)>)
}
    
impl From<PbMsg> for Msg {
    fn from(pb_msg: PbMsg) -> Self {


        /* Requests */
        if pb_msg.has_start_timer() {
            return Msg::Req(Req::StartTimer(pb_msg.get_start_timer()));
        }
        if pb_msg.has_cancel_timer() {
            return Msg::Req(Req::CancelTimer);
        }
        if pb_msg.has_get_metrics() {
            return Msg::Req(Req::GetMetrics);
        }
        if pb_msg.has_shutdown() {
            return Msg::Req(Req::Shutdown);
        }
        if pb_msg.has_get_processes() {
            return Msg::Req(Req::GetProcesses);
        }
        if pb_msg.has_get_services() {
            return Msg::Req(Req::GetProcesses);
        }

        /* Replies */
        if pb_msg.has_timeout() {
            return Msg::Rpy(Rpy::Timeout);
        }
        if pb_msg.has_metrics() {
            let pb_metrics = pb_msg.take_metrics();
            let metrics = pb_metrics.map(|m| {
                if m.has_gauge() {
                    return Metric::Gauge(m.get_gauge());
                }
                if m.has_counter() {
                    return Metric::Counter(m.get_counter());
                }
                // TODO: Add histogram support
            }).collect();
            return Msg::Rpy(Rpy::Metrics(metrics));
        }
        if pb_msg.has_processes() {
            let pids = pb_msg.take_processes()
                .take_pids()
                .into_iter()
                .map(|p| p.into()).collect();
            return Msg::Rpy(Rpy::Processes(pids));
        }
        if pb_msg.has_services() {
            let pids = pb_msg.take_services()
                .take_pids()
                .into_iter()
                .map(|p| p.into()).collect();
            return Msg::Rpy(Rpy::Services(pids));
        }
        if pb_msg.has_members() {
            let members = pb_msg.take_members()
                .take_members()
                .take_members()
                .into_iter()
                .map(|m| (m.take_node().into(), m.get_connected())).collect();
            return Msg::Rpy(Rpy::Members(members));
        }

        Msg::Unknown

    }
}
