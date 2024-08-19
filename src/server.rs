//! The gRPC server.
//!

use std::{collections::HashMap, sync::Mutex};

use crate::{log, rpc::kv_store::*, SERVER_ADDR};
use anyhow::Result;
use tonic::{transport::Server, Request, Response, Status};

pub struct KvStore {
    store: Mutex<HashMap<String, String>>,
}

#[tonic::async_trait]
impl kv_store_server::KvStore for KvStore {
    async fn example(
        &self,
        req: Request<ExampleRequest>,
    ) -> Result<Response<ExampleReply>, Status> {
        log::info!("Received example request.");
        Ok(Response::new(ExampleReply {
            output: req.into_inner().input + 1,
        }))
    }

    #[doc = " TODO: Protocol buffers"]
    #[doc = ""]
    #[doc = " Function to insert a key value pair into the server"]
    async fn insert_key_value_pair(
        &self,
        request: tonic::Request<InsertKeyValueRequest>,
    ) -> Result<tonic::Response<InsertKeyResponse>, tonic::Status> {
        let inner_request = request.into_inner();

        // Access the key and value fields
        let key = inner_request.key;
        let value = inner_request.value;

        let mut store = self.store.lock().unwrap();
        store.insert(key, value);

        // Create a response with the status
        let response = InsertKeyResponse { status: 1 };

        // Return the response wrapped in a tonic::Response
        Ok(Response::new(response))
    }

    #[doc = " function that gets key and returns value"]
    #[doc = " TODO : convert to stream and try"]
    async fn get_value_from_key(
        &self,
        request: tonic::Request<RetrieveValueRequest>,
    ) -> Result<tonic::Response<RetireveValueResponse>, tonic::Status> {
        let inner_request = request.into_inner();

        // Extract the key from the request
        let key = inner_request.key;
        let store = self.store.lock().unwrap();

        // Retrieve the value from the store
        match store.get_key_value(&key) {
            Some(value) => {
                // Create a response with the retrieved value
                let response = RetireveValueResponse {
                    value: (*(value.1)).to_string(),
                };

                // Return the response wrapped in a tonic::Response
                Ok(Response::new(response))
            }
            None => {
                // If the key does not exist, return a NotFound status
                Err(Status::not_found("Key not found"))
            }
        }
    }

    // TODO: RPC implementation
}

pub async fn start() -> Result<()> {
    let svc = kv_store_server::KvStoreServer::new(KvStore {
        store: Mutex::new(HashMap::new()),
    });

    log::info!("Starting KV store server.");
    Server::builder()
        .add_service(svc)
        .serve(SERVER_ADDR.parse().unwrap())
        .await?;
    Ok(())
}
