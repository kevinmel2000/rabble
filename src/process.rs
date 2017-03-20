use pid::Pid;
use envelope::Envelope;
use user_msg::UserMsg;

pub trait Process<T: UserMsg> : Send {

    /// Initialize process state if necessary
    fn init(&mut self, _executor_pid: Pid) -> Vec<Envelope<T>> {
        Vec::new()
    }

    /// Handle messages from other actors
    fn handle(&mut self, envelope: Envelope<T>) -> &mut Vec<Envelope<T>>;
}
