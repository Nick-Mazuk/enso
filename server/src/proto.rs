mod protocol {
    include!(concat!(env!("OUT_DIR"), "/protocol.rs"));
}

pub mod google {
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
}

pub use protocol::*;
