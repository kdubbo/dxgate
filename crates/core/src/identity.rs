use serde::{Deserialize, Serialize};

pub const NODE_TYPE_ROUTER: &str = "router";
pub const DEFAULT_DNS_DOMAIN: &str = "svc.cluster.local";
pub const DEFAULT_CLUSTER_ID: &str = "Kubernetes";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouterIdentity {
    pub pod_name: String,
    pub namespace: String,
    pub pod_ip: String,
    pub node_name: Option<String>,
    pub cluster_id: String,
    pub dns_domain: String,
}

impl RouterIdentity {
    pub fn node_id(&self) -> String {
        format!(
            "{NODE_TYPE_ROUTER}~{}~{}.{}~{}.{}",
            self.pod_ip, self.pod_name, self.namespace, self.namespace, self.dns_domain
        )
    }

    pub fn metadata(&self) -> RouterMetadata {
        RouterMetadata {
            generator: "dxgate".to_string(),
            cluster_id: self.cluster_id.clone(),
            namespace: self.namespace.clone(),
            node_name: self.node_name.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouterMetadata {
    #[serde(rename = "GENERATOR")]
    pub generator: String,
    #[serde(rename = "CLUSTER_ID")]
    pub cluster_id: String,
    #[serde(rename = "NAMESPACE")]
    pub namespace: String,
    #[serde(rename = "KUBE_NODE_NAME", skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_dubbo_router_node_id() {
        let id = RouterIdentity {
            pod_name: "dxgate-abc".into(),
            namespace: "dubbo-system".into(),
            pod_ip: "10.0.0.10".into(),
            node_name: Some("node-a".into()),
            cluster_id: DEFAULT_CLUSTER_ID.into(),
            dns_domain: DEFAULT_DNS_DOMAIN.into(),
        };

        assert_eq!(
            id.node_id(),
            "router~10.0.0.10~dxgate-abc.dubbo-system~dubbo-system.svc.cluster.local"
        );
    }
}
