use std::{collections::HashMap, str::FromStr};

use atm0s_small_p2p::{
    replicate_kv_service::{KvEvent, ReplicatedKvService},
    P2pService, P2pServiceEvent, PeerId,
};
use leg::LegControl;

mod leg;
mod registry;

pub use leg::{Leg, LegId, LegOutput};
use mqtt::{packet::PublishPacket, Decodable, Encodable};
use registry::{SubscribeResult, Topic, UnsubscribeResult};
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

#[derive(Debug)]
pub enum HubPublishError {
    NoSubscribers,
    TopicInvalid,
}

pub struct Hub {
    hub_service: P2pService,
    replicated_kv_service: ReplicatedKvService<String, bool>,
    local_registry: registry::Registry<LegId>,
    remote_registry: registry::Registry<PeerId>,
    legs: HashMap<LegId, UnboundedSender<LegOutput>>,
    control_tx: UnboundedSender<(LegId, LegControl)>,
    control_rx: UnboundedReceiver<(LegId, LegControl)>,
    leg_id_seq: LegId,
}

impl Hub {
    pub fn new(hub_service: P2pService, kv_service: P2pService) -> Hub {
        let (control_tx, control_rx) = tokio::sync::mpsc::unbounded_channel();
        Hub {
            hub_service,
            local_registry: registry::Registry::default(),
            remote_registry: registry::Registry::default(),
            replicated_kv_service: ReplicatedKvService::new(kv_service, 1_000_000, 10_000),
            legs: HashMap::new(),
            control_tx,
            control_rx,
            leg_id_seq: LegId::default(),
        }
    }

    pub fn create_leg(&mut self) -> Leg {
        let leg_id = self.leg_id_seq;
        self.leg_id_seq += 1;
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        self.legs.insert(leg_id, event_tx);
        Leg::new(leg_id, self.control_tx.clone(), event_rx)
    }

    pub async fn publish(&mut self, pkt: PublishPacket) -> Result<(), HubPublishError> {
        //TODO save retain message
        let mut has_subscribers = false;
        let topic_str: &str = pkt.topic_name();
        let topic = Topic::from_str(topic_str).map_err(|_e| HubPublishError::TopicInvalid)?;
        for leg_id in self.local_registry.get(&topic).into_iter().flatten() {
            has_subscribers = true;
            log::info!("[Hub] publish {topic_str} to local leg {leg_id:?}");
            let event_tx = self.legs.get(leg_id).expect("should have leg with leg_id");
            let _ = event_tx.send(LegOutput::Publish(pkt.clone()));
        }
        let dests = self.remote_registry.get(&topic).into_iter().flatten().cloned().collect::<Vec<_>>();
        if !dests.is_empty() {
            has_subscribers = true;
            let mut buf = Vec::new();
            pkt.encode(&mut buf).expect("should encode packet");
            let requester = self.hub_service.requester();
            let topic_str = topic_str.to_owned();
            tokio::spawn(async move {
                for dest_peer in dests {
                    log::info!("[Hub] publish {topic_str} to remote node {dest_peer:?}");
                    let _ = requester.send_unicast(dest_peer, buf.clone()).await;
                }
            });
        }
        has_subscribers.then_some(()).ok_or(HubPublishError::NoSubscribers)
    }

    pub async fn recv(&mut self) -> Option<()> {
        select! {
            event = self.control_rx.recv() => {
                let (leg_id, control) = event?;
                match control {
                    LegControl::Subscribe(topic_str) => {
                        log::info!("[Hub] local leg {leg_id:?} subscribe {topic_str}");
                        if let Ok(topic) = Topic::from_str(&topic_str) {
                            if self.local_registry.subscribe(&topic, leg_id).eq(&Some(SubscribeResult::NodeAdded)) {
                                // first time we subscribe to this topic then need to add to replicated kv
                                self.replicated_kv_service.set(topic_str, true);
                            }
                        }
                        Some(())
                    },
                    LegControl::Unsubscribe(topic_str) => {
                        log::info!("[Hub] local leg {leg_id:?} unsubscribe {topic_str}");
                        if let Ok(topic) = Topic::from_str(&topic_str) {
                            if self.local_registry.unsubscribe(&topic, leg_id).eq(&Some(UnsubscribeResult::NodeRemoved)) {
                                // no more we subscribe to this topic then need to remove from replicated kv
                                self.replicated_kv_service.del(topic_str.to_owned());
                            }
                        }
                        Some(())
                    },
                    LegControl::Publish(pkt) => {
                        let _ = self.publish(pkt).await;
                        Some(())
                    },
                }
            },
            event = self.hub_service.recv() => match event? {
                P2pServiceEvent::Unicast(peer_id, vec) => {
                    if let Ok(pkt) = PublishPacket::decode(&mut vec.as_slice()) {
                        let topic_str: &str = pkt.topic_name();
                        if let Ok(topic) = Topic::from_str(topic_str) {
                            for leg_id in self.local_registry.get(&topic).into_iter().flatten() {
                                log::info!("[Hub] forward {topic_str} to local leg {leg_id:?} from remote {peer_id:?}");
                                let event_tx = self.legs.get(leg_id).expect("should have leg with leg_id");
                                let _ = event_tx.send(LegOutput::Publish(pkt.clone()));
                            }
                        }
                    } else {
                        log::warn!("invalid publish packet");
                    }

                    Some(())
                },
                P2pServiceEvent::Broadcast(..) => Some(()),
                P2pServiceEvent::Stream(..) => Some(()),
            },
            event = self.replicated_kv_service.recv() => match event? {
                KvEvent::Set(Some(remote), k, _) => {
                    log::info!("[Hub] remote {remote:?} subscribe {k}");
                    if let Ok(topic) = Topic::from_str(&k) {
                        self.remote_registry.subscribe(&topic, remote);
                    }
                    Some(())
                },
                KvEvent::Del(Some(remote), k) => {
                    log::info!("[Hub] remote {remote:?} unsubscribe {k}");
                    if let Ok(topic) = Topic::from_str(&k) {
                        self.remote_registry.unsubscribe(&topic, remote);
                    }
                    Some(())
                },
                _ => Some(())
            },
        }
    }
}
