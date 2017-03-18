use pid::Pid;
use envelope::Envelope;
use actor_msg::ActorMsg;

pub trait Process<T: ActorMsg> : Send {

    /// Initialize process state if necessary
    fn init(&mut self, _executor_pid: Pid) -> Vec<Envelope<Self::Msg>> {
        Vec::new()
    }

    /// Handle messages from other actors
    fn handle(&mut self, envelope: Envelope<T>) -> &mut Vec<Envelope<T>>;
}
