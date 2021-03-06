extern crate rabble;
#[macro_use]
extern crate assert_matches;

use std::thread;

use rabble::{
    NodeId,
    Service,
    ThreadHandler,
    CorrelationId,
    Msg,
    Pid
};

#[test]
fn single_service_and_handler_get_executor_status() {
    let node_id = NodeId {name: "node1".to_string(), addr: "127.0.0.1:11000".to_string()};
    let (node, handles) = rabble::rouse::<u64>(node_id, None);
    let pid = Pid {
        name: "test-service".to_string(),
        group: Some("Service".to_string()),
        node: node.id.clone()
    };
    let pid2 = pid.clone();
    let handler = ThreadHandler::new(move |_node, envelope| {
        assert_eq!(envelope.to, pid);
        assert_matches!(envelope.msg, Msg::ExecutorStatus(_));
    });
    let mut service = Service::new(pid2.clone(), node.clone(), handler).unwrap();
    node.executor_status(CorrelationId::pid(pid2)).unwrap();
    thread::spawn(move || {
        service.wait();
    });

    node.shutdown();

    for h in handles {
        h.join().unwrap();
    }
}
