use std::convert::{From, Into};

pub trait ActorMsg: Debug + Clone + PartialEq + From<PbMsg> + Into<PbMsg> {}
