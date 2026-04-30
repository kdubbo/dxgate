pub mod cluster {
    pub mod v1 {
        #![allow(dead_code)]
        tonic::include_proto!("cluster.v1");
    }
}

pub mod core {
    pub mod v1 {
        #![allow(dead_code)]
        tonic::include_proto!("core.v1");
    }
}

pub mod endpoint {
    pub mod v1 {
        #![allow(dead_code)]
        tonic::include_proto!("endpoint.v1");
    }
}

pub mod extensions {
    pub mod filters {
        pub mod http {
            pub mod router {
                pub mod v1 {
                    #![allow(dead_code)]
                    tonic::include_proto!("extensions.filters.http.router.v1");
                }
            }
        }

        pub mod network {
            pub mod http_connection_manager {
                pub mod v1 {
                    #![allow(dead_code)]
                    tonic::include_proto!("extensions.filters.network.http_connection_manager.v1");
                }
            }
        }
    }

    pub mod transport_sockets {
        pub mod tls {
            pub mod v1 {
                #![allow(dead_code)]
                tonic::include_proto!("extensions.transport_sockets.tls.v1");
            }
        }
    }
}

pub mod google {
    pub mod rpc {
        #![allow(dead_code)]
        tonic::include_proto!("google.rpc");
    }
}

pub mod listener {
    pub mod v1 {
        #![allow(dead_code)]
        tonic::include_proto!("listener.v1");
    }
}

pub mod route {
    pub mod v1 {
        #![allow(dead_code)]
        tonic::include_proto!("route.v1");
    }
}

pub mod service {
    pub mod discovery {
        pub mod v1 {
            #![allow(dead_code)]
            tonic::include_proto!("service.discovery.v1");
        }
    }

    pub mod secret {
        pub mod v1 {
            #![allow(dead_code)]
            tonic::include_proto!("service.secret.v1");
        }
    }
}

pub mod r#type {
    pub mod matcher {
        pub mod v1 {
            #![allow(dead_code)]
            tonic::include_proto!("r#type.matcher.v1");
        }
    }
}
