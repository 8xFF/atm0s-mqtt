use std::{net::SocketAddr, str::FromStr};

use atm0s_mqtt::{run_http, HttpCommand, MqttBroker, WebHook, WebhookConfig};
use atm0s_small_p2p::{P2pNetwork, P2pNetworkConfig, PeerAddress, SharedKeyHandshake};
use clap::Parser;
use mqtt::packet::{PublishPacket, QoSWithPacketIdentifier};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::select;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const HUB_SERVICE_ID: u16 = 100;
const KV_SERVICE: u16 = 101;

pub const DEFAULT_CLUSTER_CERT: &[u8] = include_bytes!("../dev-certs/dev.cluster.cert");
pub const DEFAULT_CLUSTER_KEY: &[u8] = include_bytes!("../dev-certs/dev.cluster.key");

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// UDP/TCP port for serving QUIC/TCP connection for SDN network
    #[arg(env, long)]
    sdn_peer_id: u64,

    /// UDP/TCP port for serving QUIC/TCP connection for SDN network
    #[arg(env, long, default_value = "0.0.0.0:11111")]
    sdn_listen: SocketAddr,

    /// Seeds
    #[arg(env, long, value_delimiter = ',')]
    sdn_seeds: Vec<String>,

    /// Allow it broadcast address to other peers
    /// This allows other peer can active connect to this node
    /// This option is useful with high performance relay node
    #[arg(env, long)]
    sdn_advertise_address: Option<SocketAddr>,

    /// Sdn secure code
    #[arg(env, long, default_value = "insecure")]
    sdn_secure_code: String,

    /// Mqtt tcp listen
    #[arg(env, long, default_value = "0.0.0.0:1883")]
    mqtt_listen: SocketAddr,

    /// Http listen
    #[arg(env, long, default_value = "0.0.0.0:8080")]
    http_listen: SocketAddr,

    /// Webhook workers number
    #[arg(env, long, default_value = "4")]
    webhook_workers: usize,

    /// Mqtt authentication endpoint
    #[arg(env, long)]
    authentication_endpoint: Option<String>,

    /// Mqtt authorization endpoint
    #[arg(env, long)]
    authorization_endpoint: Option<String>,

    /// Mqtt event endpoint
    #[arg(env, long)]
    event_endpoint: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider().install_default().expect("should install ring as default");
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }
    let args: Args = Args::parse();
    tracing_subscriber::registry().with(fmt::layer()).with(EnvFilter::from_default_env()).init();

    let priv_key = PrivatePkcs8KeyDer::from(DEFAULT_CLUSTER_KEY.to_vec());
    let cert = CertificateDer::from(DEFAULT_CLUSTER_CERT.to_vec());

    let mut p2p = P2pNetwork::new(P2pNetworkConfig {
        peer_id: args.sdn_peer_id.into(),
        listen_addr: args.sdn_listen,
        advertise: args.sdn_advertise_address.map(|a| a.into()),
        priv_key,
        cert,
        tick_ms: 100,
        seeds: args.sdn_seeds.into_iter().map(|s| PeerAddress::from_str(s.as_str()).expect("should parse address")).collect::<Vec<_>>(),
        secure: SharedKeyHandshake::from(args.sdn_secure_code.as_str()),
    })
    .await
    .expect("should create network");

    let hub_service = p2p.create_service(HUB_SERVICE_ID.into());
    let kv_service = p2p.create_service(KV_SERVICE.into());

    let hook = WebHook::new(
        args.webhook_workers,
        WebhookConfig {
            authentication_endpoint: args.authentication_endpoint,
            authorization_endpoint: args.authorization_endpoint,
            event_endpoint: args.event_endpoint,
        },
    );
    let mut mqtt_broker = MqttBroker::new(args.mqtt_listen, hub_service, kv_service, hook).await?;
    let mut http_cmd_rx = run_http(args.http_listen);

    let mut pkt_id: u16 = 0;

    loop {
        select! {
            _ = p2p.recv() => {}
            _ = mqtt_broker.recv() => {}
            cmd = http_cmd_rx.recv() => match cmd.expect("should have command") {
                HttpCommand::Publish { topic, payload, qos, retain, tx } => {
                    let mut packet = PublishPacket::new(topic, QoSWithPacketIdentifier::new(qos, pkt_id), payload);
                    packet.set_retain(retain);
                    pkt_id += 1;
                    let res = mqtt_broker.publish(packet).await;
                    let _ = tx.send(res);
                },
            }
        }
    }
}
