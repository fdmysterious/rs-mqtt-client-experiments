use std::error::Error;
use std::time::Duration;
use std::sync::Arc;

use tokio::{task,time};
use rumqttc::{self, AsyncClient, MqttOptions, QoS, Event, Incoming, EventLoop};

use async_trait::async_trait;

use pretty_env_logger;

use indexmap::IndexMap;
use std::collections::HashMap;

use log;


// https://internals.rust-lang.org/t/allowing-calling-static-methods-through-trait-objects/10417/5

#[async_trait]
pub trait MqttHandler {
    async fn call_async(&self, client: AsyncClient);
    fn path() -> &'static str where Self: Sized;
    async fn subscribe(client: &mut AsyncClient) where Self: Sized;
}

pub struct MqttRouter {
    routes: IndexMap<String, Box<dyn MqttHandler + Send + Sync>>,
}

impl MqttRouter {
    pub fn new() -> Self {
        Self {
            routes: IndexMap::new(),
        }
    }

    pub async fn add_route<T>(&mut self, client: &mut AsyncClient, handler: T)
    where
        T: MqttHandler + Send + Sync + 'static,
    {
        let path = <T as MqttHandler>::path();

        log::info!("Add route: {:?}", path);

        self.routes.insert(String::from(path), Box::new(handler));
        let _ = <T as MqttHandler>::subscribe(client).await;
    }

    pub async fn handle_request(&self, client: AsyncClient, path: String)
    {
        match self.routes.get(&path) {
            Some(handler) => {
                handler.call_async(client).await;
            }

            None => {
                println!("No route found for path: {}", path);
            }
        }
    }
}

#[derive(Clone, Copy)]
struct HelloHandler;

#[async_trait]
impl MqttHandler for HelloHandler {
    async fn call_async(&self, client: AsyncClient) {
        println!("Hello handler!");

        let resp = "Hello back!";
        client.publish("hello/back", QoS::AtLeastOnce, false, resp.as_bytes())
            .await
            .unwrap();
    }

    fn path() -> &'static str {
        "hello/world"
    }

    async fn subscribe(client: &mut AsyncClient) {
        log::debug!("Subscribe hello handler!");
        client.subscribe(Self::path(), QoS::AtMostOnce).await.unwrap();
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    log::info!("Hello world!");

    let mqttoptions = MqttOptions::new("mqtt-async-connector", "localhost", 1883);
    let (mut client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    let mut router = MqttRouter::new();

    router.add_route(&mut client, HelloHandler).await;
    let router = Arc::new(router);

    loop {
        let event = eventloop.poll().await;

        match &event {
            Ok(v) => {
                log::trace!("Event = {v:?}");

                if let Event::Incoming(Incoming::Publish(pub_event)) = v {
                    log::debug!("> Publish event!");

                    let new_client  = client.clone();
                    let topic       = String::from(&pub_event.topic);
                    let router_arc  = router.clone();
                    tokio::spawn(async move {
                        router_arc.handle_request(new_client, topic).await;
                    });
                }
            }

            Err(e) => {
                println!("Error = {e:?}");
                return Ok(());
            }
        }
    }
}

async fn hello_handler(client: AsyncClient) {
    println!("Hello handler!");

    let resp = "Hello back!";
    client.publish("hello/back", QoS::AtLeastOnce, false, resp.as_bytes())
        .await
        .unwrap();
}

async fn handle(topic: String, client: AsyncClient) -> Result<(), ()> {
    println!("Handle !");
    match topic.as_str() {
        "hello/world" => {
            hello_handler(client).await;
            Ok(())
        },

        _ => {
            Err(())
        }
    }
}
