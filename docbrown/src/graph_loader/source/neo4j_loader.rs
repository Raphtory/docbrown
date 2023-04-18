
use neo4rs::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use futures::stream::*;
use uuid::Uuid;

#[tokio::test]
async fn test_neo_connection() {
    let uri = "127.0.0.1:7687";
    let user = "neo4j";
    let pass = "password";
    let id = Uuid::new_v4().to_string();

    let graph = Arc::new(Graph::new(&uri, user, pass).await.unwrap());
    let mut result = graph.run(
        query("CREATE (p:Person {id: $id})").param("id", id.clone())
    ).await.unwrap();

    let mut handles = Vec::new();
    let mut count = Arc::new(AtomicU32::new(0));
    for _ in 1..=42 {
        let graph = graph.clone();
        let id = id.clone();
        let count = count.clone();
        let handle = tokio::spawn(async move {
            let mut result = graph.execute(
                query("MATCH (p:Person {id: $id}) RETURN p").param("id", id)
            ).await.unwrap();
            while let Ok(Some(row)) = result.next().await {
                dbg!(row);
                count.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;
    assert_eq!(count.load(Ordering::Relaxed), 42);
}



