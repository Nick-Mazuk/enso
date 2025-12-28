#[allow(clippy::pedantic)]
#[allow(clippy::nursery)]
#[allow(clippy::all)]
mod protocol {
    include!(concat!(env!("OUT_DIR"), "/protocol.rs"));
}

#[allow(clippy::pedantic)]
#[allow(clippy::nursery)]
#[allow(clippy::all)]
pub mod google {
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
}

#[allow(clippy::pedantic)]
#[allow(clippy::nursery)]
#[allow(clippy::all)]
pub use protocol::*;
