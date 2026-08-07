mod bootstrap;
mod client;
mod proto;

pub use bootstrap::*;
pub use client::*;

/// Activation-demand client, for the gateway to tell the control plane which
/// scaled-to-zero services have requests waiting.
pub mod activation {
    pub use crate::proto::activation::v1alpha1::*;
}
