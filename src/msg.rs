use std::fmt::Debug;
use correlation_id::CorrelationId;
use metrics::Metric;
use pb_messages::{self, PbMsg};
use user_msg::UserMsg;

type Name = &'static str;

#[derive(Debug, Clone, PartialEq)]
pub enum Msg<T: UserMsg> {
    Req(Req),
    Rpy(Rpy),
    User(T),
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
    
impl<T: UserMsg> From<PbMsg> for Msg<T> {
    fn from(pb_msg: PbMsg) -> Msg<T> {

        /* The user level message type */
        if pb_msg.has_user_msg() {
            let encoded = pb_msg.take_user_msg();
            return Msg::User(T::from_bytes(encoded));
        }

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
                .into_iter()
                .map(|m| (m.take_node().into(), m.get_connected())).collect();
            return Msg::Rpy(Rpy::Members(members));
        }

        Msg::Unknown

    }
}

impl<T: UserMsg> From<Msg<T>> for PbMsg {
    fn from(msg: Msg<T>) -> PbMsg {
        let mut pbmsg = PbMsg::new();
        match msg {
            Msg::User(user_msg) => {
                let bytes = user_msg.to_bytes();
                pbmsg.set_user_msg(bytes);
            },
            Msg::Req(Req::GetMetrics) => {
                pbmsg.set_get_metrics(true);
            },
            Msg::Req(Req::StartTimer(time_in_ms)) => {
                pbmsg.set_start_timer(time_in_ms);
            },
            Msg::Req(Req::CancelTimer) => {
                pbmsg.set_cancel_timer(true);
            },
            Msg::Req(Req::Shutdown) => {
                pbmsg.set_shutdown(true);
            },
            Msg::Req(Req::GetProcesses) => {
                pbmsg.set_get_processes(true);
            },
            Msg::Req(Req::GetServices) => {
                pbmsg.set_get_services(true);
            },
            Msg::Rpy(Rpy::Timeout) => {
                pbmsg.set_timeout(true);
            },
            Msg::Rpy(Rpy::Metrics(metrics)) => {
                let mut pb_metrics = pb_messages::Metrics::new();
                pb_metrics.set_metrics(metrics.map(|m| {
                    let mut metric = pb_messages::Metric::new();
                    match m {
                        Gauge(val) => metric.set_gauge(val),
                        Counter(val) => metric.set_counter(val)
                        // TODO: Add histogram support
                    }
                    metric
                }).collect());
                pb_msg.set_metrics(pb_metrics);
            },
            Msg::Rpy(Rpy::Processes(pids)) => {
                let mut processes = pb_messages::Pids::new();
                processes.set_pids(pids.map(|p| p.into()).collect());
                pb_msg.set_processes(processes);
            },
            Msg::Rpy(Rpy::Services(pids)) => {
                let mut services = pb_messages::Pids::new();
                services.set_pids(pids.map(|p| p.into()).collect());
                pb_msg.set_services(services);
            },
            Msg::Rpy(Rpy::Members(members)) => {
                let mut pb_members = pb_messages::Members::new();
                pb_members.set_members(members.map|(node_id, connected)| {
                    let mut member = pb_messages::Member::new();
                    member.set_node(node_id.into());
                    member.set_connected(connected);
                    member
                }).collect();
                pb_msg.set_members(pb_members);
            },
            Msg::Unknown => unreachable
        }
        pb_msg
    }
}
