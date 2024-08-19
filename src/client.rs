//! The gRPC client.
//!

use crate::{rpc::kv_store::*, SERVER_ADDR};
use anyhow::Result;
use tonic::transport::Channel;

async fn connect() -> Result<kv_store_client::KvStoreClient<Channel>> {
    Ok(kv_store_client::KvStoreClient::connect(format!("http://{}", SERVER_ADDR)).await?)
}

// Client methods. DO NOT MODIFY THEIR SIGNATURES.
pub async fn example(input: u32) -> Result<u32> {
    let mut client = connect().await?;

    Ok(client
        .example(ExampleRequest { input })
        .await?
        .into_inner()
        .output)
}
pub async fn echo(msg: String) -> Result<String> {
    println!("{:?}", msg);

    Ok((msg))
}
pub async fn put(key: Vec<u8>, value: Vec<u8>) -> Result<()> {
    let mut client = connect().await?;
    let res = client
        .insert_key_value_pair(InsertKeyValueRequest {
            key: String::from_utf8(key).unwrap(),
            value: String::from_utf8(value).unwrap(),
        })
        .await?;

    println!("Success!!");
    Ok(())
}
pub async fn get(key: Vec<u8>) -> Result<Vec<u8>> {
    let mut client = connect().await?;

    let res = client
        .get_value_from_key(RetrieveValueRequest {
            key: String::from_utf8(key).unwrap(),
        })
        .await?
        .into_inner();

    let value = res.value.into_bytes();

    println!("Value is {:?}", value);
    Ok((value))
}
