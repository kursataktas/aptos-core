use anyhow::Result;
use cloud_storage::{ListRequest, Object};
use futures_util::TryStreamExt;
use reqwest::{Body, Client};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_json::{self, Result as SerdeResult};
use futures::join;  


const MAX_REQUESTS: usize = 100;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestData {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    pub payload: Vec<ByteBuf>,
}


// Read from GCP bucket
pub async fn read_from_gcp_bucket() -> Result<()> {
    let sharded = "http://10.128.0.4:8080";
    let non_sharded = "http://10.128.0.5:8080";
    let mut all_objects =
        Box::pin(Object::list("request-replay-prod", ListRequest::default()).await?);

    let mut cnt = 0;
    while let Some(object_list) = all_objects.try_next().await? {
        for object in object_list.items {
            let object_data =
                String::from_utf8(Object::download("request-replay-prod", &object.name).await?)?;
            // split by new line number if exists
            let lines = object_data.split('\n').collect::<Vec<&str>>();
            for line in lines {
                let request_data: SerdeResult<RequestData> =
                    serde_json::from_slice(line.as_bytes());
                cnt += 1;
                match request_data {
                    Ok(data) => {
                        println!("Request {}", cnt);
                        let shard_response = send_request_to_server(&sharded, data.clone()).await?;
                        let non_shard_response = send_request_to_server(&non_sharded, data).await?;  
                      
                        if shard_response != non_shard_response {  
                            println!("Request {} has different responses", cnt);
                            println!("Shared response: {}", shard_response);  
                            println!("Non-shared response: {}", non_shard_response); 
                            panic!("Responses are different");
                        }
                    },
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            if cnt >= MAX_REQUESTS {
                return Ok(());
            }
        }
    }

    Ok(())
}

pub async fn send_request_to_server(host: &str, request_data: RequestData) -> Result<String> {
    let client = Client::new();
    let suffix = request_data.uri;
    let new_uri = format!("{}{}", host, suffix);
    let method = request_data.method.parse()?;
    println!("Sending {} request to: {}", method, new_uri);
    let mut request_builder = client.request(method, new_uri);
    for (key, value) in request_data.headers {
        request_builder = request_builder.header(key, value);
    }
    let body = Body::from(
        request_data
            .payload
            .into_iter()
            .flat_map(|bytebuf| bytebuf.to_vec())
            .collect::<Vec<_>>(),
    );
    let request = request_builder.body(body);
    let response = request.send().await?;
    // get the content of the response
    let response = response.text().await?;
    println!("Response content {:?}", response);
    Ok(response)
}

pub fn main() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(read_from_gcp_bucket())
        .unwrap();
}
