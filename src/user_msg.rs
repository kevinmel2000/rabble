pub trait UserMsg: Debug + Clone + PartialEq {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(Vec<u8>) -> Self;
}
