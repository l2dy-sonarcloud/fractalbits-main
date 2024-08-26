use fake::Fake;
use std::collections::HashMap;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let url = "http://localhost:3000";
    let client = reqwest::Client::new();
    let total = 2 * 1024 * 1024;
    let mut kvs = HashMap::with_capacity(total);

    // Put key&value pairs
    let before = Instant::now();
    for i in 0..total {
        let key = (4..30).fake::<String>();
        let value = (4..30).fake::<String>();
        let _res = client
            .post(format!("{url}/{key}"))
            .body(format!("{value}"))
            .send()
            .await
            .unwrap();
        kvs.insert(key, value);
        if i != 0 && i % 10000 == 0 {
            println!("inserted {i} kv pairs");
        }
    }
    println!("Elapsed time: {:.2?}", before.elapsed());

    let before = Instant::now();
    // Get values back
    let mut i = 0;
    for key in kvs.keys() {
        let _res = client.get(format!("{url}/{key}")).send().await.unwrap();
        if i != 0 && i % 10000 == 0 {
            println!("fetched {i} kv pairs");
        }
        i += 1;
    }
    println!("Elapsed time: {:.2?}", before.elapsed());
}
