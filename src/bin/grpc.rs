use tonic::{transport::Server, Request, Response, Status};

use crate::proto::event_store_server::{EventStore, EventStoreServer};
use crate::proto::{AppendEventReply, AppendEventRequest};

pub mod io {
    pub mod cloudevents {
        pub mod v1 {
            tonic::include_proto!("io.cloudevents.v1");
        }
    }
}
pub mod proto {
    tonic::include_proto!("hematite");
}

#[derive(Debug, Default)]
pub struct HematiteStore {}

#[tonic::async_trait]
impl EventStore for HematiteStore {
    async fn append_event(
        &self,
        _request: Request<AppendEventRequest>,
    ) -> Result<Response<AppendEventReply>, Status> {
        let reply = AppendEventReply {
            revision: 0,
        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let store = HematiteStore::default();

    Server::builder()
        .add_service(EventStoreServer::new(store))
        .serve(addr)
        .await?;

    Ok(())
}
