//! Cloud Provider Integrations
//!
//! Monitor AWS, Azure, GCP, DigitalOcean, Linode, IBM, Oracle, and Alibaba Cloud.

pub mod alibaba;
pub mod aws;
pub mod azure;
pub mod digitalocean;
pub mod gcp;
pub mod ibm;
pub mod linode;
pub mod oracle;

pub use alibaba::AlibabaCloudIntegration;
pub use aws::AwsIntegration;
pub use azure::AzureIntegration;
pub use digitalocean::DigitalOceanIntegration;
pub use gcp::GcpIntegration;
pub use ibm::IbmCloudIntegration;
pub use linode::LinodeIntegration;
pub use oracle::OracleCloudIntegration;
