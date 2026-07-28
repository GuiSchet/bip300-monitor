//! Generated CUSF mainchain API types.

pub mod cusf {
    pub mod common {
        pub mod v1 {
            tonic::include_proto!("cusf.common.v1");
        }
    }

    pub mod mainchain {
        pub mod v1 {
            tonic::include_proto!("cusf.mainchain.v1");
        }
    }
}

pub use cusf::common::v1 as common;
pub use cusf::mainchain::v1 as mainchain;
