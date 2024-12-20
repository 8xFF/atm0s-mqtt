use std::net::SocketAddr;

use mqtt::{QualityOfService, TopicName};
use poem::{
    listener::TcpListener,
    middleware::{Cors, Tracing},
    EndpointExt, Route, Server,
};
use tokio::sync::{
    mpsc::{channel, Receiver},
    oneshot,
};

use crate::hub::HubPublishError;

mod emqx;

pub enum HttpCommand {
    Publish {
        topic: TopicName,
        payload: Vec<u8>,
        qos: QualityOfService,
        retain: bool,
        tx: oneshot::Sender<Result<(), HubPublishError>>,
    },
}

pub fn run_http(listen: SocketAddr) -> Receiver<HttpCommand> {
    let (cmd_tx, cmd_rx) = channel(10);

    tokio::spawn(async move {
        let emqx_route = emqx::route(cmd_tx).with(Cors::new()).with(Tracing);
        Server::new(TcpListener::bind(listen)).run(Route::new().nest("/api", emqx_route)).await
    });

    cmd_rx
}
