use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    str::FromStr,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeResult {
    NodeAdded,
    TargetAdded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsubscribeResult {
    NodeRemoved,
    TargetRemoved,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TopicPart {
    SingleWildcard,
    MultiWildcard,
    Value(String),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Topic(Vec<TopicPart>);

#[derive(Debug)]
struct RegistryNode<T> {
    children: HashMap<TopicPart, Box<RegistryNode<T>>>,
    targets: HashSet<T>,
}

impl<T> Default for RegistryNode<T> {
    fn default() -> Self {
        Self {
            children: HashMap::new(),
            targets: HashSet::new(),
        }
    }
}

#[derive(Debug)]
pub struct Registry<T> {
    root: RegistryNode<T>,
    get_buf: Vec<T>,
}

impl<T> Default for Registry<T> {
    fn default() -> Self {
        Self {
            root: RegistryNode::default(),
            get_buf: Vec::new(),
        }
    }
}

impl<T> Registry<T>
where
    T: Eq + Hash + Clone,
{
    /// return true if the topic is new
    pub fn subscribe(&mut self, topic: &Topic, leg_id: T) -> Option<SubscribeResult> {
        self.root.subscribe(&topic.0, leg_id)
    }

    /// return true if the topic exists
    pub fn unsubscribe(&mut self, topic: &Topic, leg_id: T) -> Option<UnsubscribeResult> {
        self.root.unsubscribe(&topic.0, leg_id)
    }

    /// TODO: avoid use temporary buffer here
    pub fn get(&mut self, topic: &Topic) -> Option<impl Iterator<Item = &T>> {
        self.get_buf.clear();
        self.root.get(&topic.0, &mut self.get_buf)?;
        Some(self.get_buf.iter())
    }

    /// return true if the registry is empty
    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.root.is_empty()
    }
}

impl Topic {
    fn is_valid(&self) -> bool {
        let len = self.0.len();
        len > 0
            && self.0.iter().enumerate().all(|(index, part)| match part {
                // only accept MultiWildcard at the end
                TopicPart::MultiWildcard => index == len - 1,
                _ => true,
            })
    }
}

impl FromStr for Topic {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('/').map(|s| match s {
            "+" => TopicPart::SingleWildcard,
            "#" => TopicPart::MultiWildcard,
            _ => TopicPart::Value(s.to_string()),
        });
        let topic = Topic(parts.collect());
        topic.is_valid().then_some(topic).ok_or("invalid topic")
    }
}

impl<T> RegistryNode<T>
where
    T: Eq + Hash + Clone,
{
    pub fn subscribe(&mut self, parts: &[TopicPart], target: T) -> Option<SubscribeResult> {
        if let Some(next) = parts.first() {
            if let Some(child) = self.children.get_mut(next) {
                child.subscribe(&parts[1..], target)
            } else {
                let mut node = Box::new(RegistryNode::default());
                node.subscribe(&parts[1..], target);
                self.children.insert(next.clone(), node);
                Some(SubscribeResult::NodeAdded)
            }
        } else {
            self.targets.insert(target).then_some(SubscribeResult::TargetAdded)
        }
    }

    pub fn unsubscribe(&mut self, parts: &[TopicPart], target: T) -> Option<UnsubscribeResult> {
        if let Some(next) = parts.first() {
            if let Some(child) = self.children.get_mut(next) {
                let res = child.unsubscribe(&parts[1..], target);
                if child.children.is_empty() && child.targets.is_empty() {
                    self.children.remove(next);
                    Some(UnsubscribeResult::NodeRemoved)
                } else {
                    res
                }
            } else {
                None
            }
        } else {
            self.targets.remove(&target).then_some(UnsubscribeResult::TargetRemoved)
        }
    }

    pub fn get(&self, parts: &[TopicPart], dest: &mut Vec<T>) -> Option<()> {
        if let Some(next) = parts.first() {
            match next {
                TopicPart::SingleWildcard | TopicPart::MultiWildcard => None,
                TopicPart::Value(_) => {
                    let exact_child = self.children.get(next).and_then(|c| c.get(&parts[1..], dest));
                    let single_wildcard_child = self.children.get(&TopicPart::SingleWildcard).and_then(|c| c.get(&parts[1..], dest));
                    let multi_wildcard_child = self.children.get(&TopicPart::MultiWildcard).and_then(|c| c.get(&[], dest));

                    exact_child.or(single_wildcard_child).or(multi_wildcard_child)
                }
            }
        } else {
            dest.extend(self.targets.iter().cloned());
            Some(())
        }
    }

    /// return true if the registry is empty
    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.children.is_empty() && self.targets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::{hash::Hash, str::FromStr};

    use crate::hub::registry::{SubscribeResult, Topic, TopicPart, UnsubscribeResult};

    use super::Registry;

    impl TopicPart {
        fn value(v: &str) -> TopicPart {
            TopicPart::Value(v.to_string())
        }
    }

    fn get_vec<T: Clone + Hash + Eq + Ord>(registry: &mut Registry<T>, topic: &str) -> Option<Vec<T>> {
        registry.get(&Topic::from_str(topic).expect("should parse topic")).map(|t| {
            let mut res = t.cloned().collect::<Vec<_>>();
            res.sort();
            res
        })
    }

    #[test]
    fn test_topic() {
        assert_eq!(Topic::from_str("a/b/c"), Ok(Topic(vec![TopicPart::value("a"), TopicPart::value("b"), TopicPart::value("c")])));
        assert_eq!(Topic::from_str("a/b/#"), Ok(Topic(vec![TopicPart::value("a"), TopicPart::value("b"), TopicPart::MultiWildcard])));
        assert_eq!(
            Topic::from_str("a/b/+/c"),
            Ok(Topic(vec![TopicPart::value("a"), TopicPart::value("b"), TopicPart::SingleWildcard, TopicPart::value("c")]))
        );
        assert!(Topic::from_str("a/b/#/c").is_err());
    }

    #[test]
    fn registry_single() {
        let mut registry: Registry<u16> = Registry::default();
        assert_eq!(registry.subscribe(&Topic::from_str("a").expect("should parse topic"), 1), Some(SubscribeResult::NodeAdded));
        assert_eq!(registry.subscribe(&Topic::from_str("a").expect("should parse topic"), 1), None);

        assert_eq!(get_vec(&mut registry, "a"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "b"), None);

        assert_eq!(registry.unsubscribe(&Topic::from_str("a").expect("should parse topic"), 1), Some(UnsubscribeResult::NodeRemoved));
        assert_eq!(registry.unsubscribe(&Topic::from_str("a").expect("should parse topic"), 1), None);
        assert_eq!(get_vec(&mut registry, "a"), None);

        assert!(registry.is_empty());
    }

    #[test]
    fn registry_multi() {
        let mut registry: Registry<u16> = Registry::default();
        assert_eq!(registry.subscribe(&Topic::from_str("a/b/c").expect("should parse topic"), 1), Some(SubscribeResult::NodeAdded));
        assert_eq!(registry.subscribe(&Topic::from_str("a/b/c").expect("should parse topic"), 2), Some(SubscribeResult::TargetAdded));
        assert_eq!(registry.subscribe(&Topic::from_str("a/b/c").expect("should parse topic"), 3), Some(SubscribeResult::TargetAdded));

        assert_eq!(get_vec(&mut registry, "a/b/c"), Some(vec![1, 2, 3]));
        assert_eq!(get_vec(&mut registry, "a/b/d"), None);

        assert_eq!(registry.unsubscribe(&Topic::from_str("a/b/c").expect("should parse topic"), 1), Some(UnsubscribeResult::TargetRemoved));
        assert_eq!(get_vec(&mut registry, "a/b/c"), Some(vec![2, 3]));
        assert_eq!(registry.unsubscribe(&Topic::from_str("a/b/c").expect("should parse topic"), 2), Some(UnsubscribeResult::TargetRemoved));
        assert_eq!(get_vec(&mut registry, "a/b/c"), Some(vec![3]));
        assert_eq!(registry.unsubscribe(&Topic::from_str("a/b/c").expect("should parse topic"), 3), Some(UnsubscribeResult::NodeRemoved));
        assert_eq!(get_vec(&mut registry, "a/b/c"), None);

        assert!(registry.is_empty());
    }

    #[test]
    fn registry_single_wildcard_middle() {
        let mut registry: Registry<u16> = Registry::default();
        assert_eq!(registry.subscribe(&Topic::from_str("a/+/c").expect("should parse topic"), 1), Some(SubscribeResult::NodeAdded));

        assert_eq!(get_vec(&mut registry, "a/b/c"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "a/a/c"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "b/a/c"), None);

        assert_eq!(registry.unsubscribe(&Topic::from_str("a/+/c").expect("should parse topic"), 1), Some(UnsubscribeResult::NodeRemoved));

        assert_eq!(get_vec(&mut registry, "a/b/c"), None);
        assert_eq!(get_vec(&mut registry, "a/a/c"), None);
        assert_eq!(get_vec(&mut registry, "b/a/c"), None);

        assert!(registry.is_empty());
    }

    #[test]
    fn registry_single_wildcard_begin() {
        let mut registry: Registry<u16> = Registry::default();
        assert_eq!(registry.subscribe(&Topic::from_str("+/b/c").expect("should parse topic"), 1), Some(SubscribeResult::NodeAdded));

        assert_eq!(get_vec(&mut registry, "a/b/c"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "w/b/c"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "w/f/c"), None);

        assert_eq!(registry.unsubscribe(&Topic::from_str("+/b/c").expect("should parse topic"), 1), Some(UnsubscribeResult::NodeRemoved));

        assert_eq!(get_vec(&mut registry, "a/b/c"), None);
        assert_eq!(get_vec(&mut registry, "w/b/c"), None);
        assert_eq!(get_vec(&mut registry, "w/f/c"), None);

        assert!(registry.is_empty());
    }

    #[test]
    fn registry_single_wildcard_end() {
        let mut registry: Registry<u16> = Registry::default();
        assert_eq!(registry.subscribe(&Topic::from_str("a/b/+").expect("should parse topic"), 1), Some(SubscribeResult::NodeAdded));

        assert_eq!(get_vec(&mut registry, "a/b/c"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "a/b/w"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "a/b/w/z"), None);

        assert_eq!(registry.unsubscribe(&Topic::from_str("a/b/+").expect("should parse topic"), 1), Some(UnsubscribeResult::NodeRemoved));

        assert_eq!(get_vec(&mut registry, "a/b/c"), None);
        assert_eq!(get_vec(&mut registry, "a/b/w"), None);
        assert_eq!(get_vec(&mut registry, "a/b/w/z"), None);

        assert!(registry.is_empty());
    }

    #[test]
    fn registry_multi_wildcard_short() {
        let mut registry: Registry<u16> = Registry::default();

        assert_eq!(registry.subscribe(&Topic::from_str("#").expect("should parse topic"), 1), Some(SubscribeResult::NodeAdded));

        assert_eq!(get_vec(&mut registry, "a/b/c"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "a/b/c/d"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "a/b/c/d/e"), Some(vec![1]));

        assert_eq!(registry.unsubscribe(&Topic::from_str("#").expect("should parse topic"), 1), Some(UnsubscribeResult::NodeRemoved));

        assert_eq!(get_vec(&mut registry, "a/b/c"), None);
        assert_eq!(get_vec(&mut registry, "a/b/c/d"), None);
        assert_eq!(get_vec(&mut registry, "a/b/c/d/e"), None);

        assert!(registry.is_empty());
    }

    #[test]
    fn registry_multi_wildcard_long() {
        let mut registry: Registry<u16> = Registry::default();

        assert_eq!(registry.subscribe(&Topic::from_str("a/b/#").expect("should parse topic"), 1), Some(SubscribeResult::NodeAdded));

        assert_eq!(get_vec(&mut registry, "a/b/c"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "a/b/c/d"), Some(vec![1]));
        assert_eq!(get_vec(&mut registry, "a/c/c/d/e"), None);

        assert_eq!(registry.unsubscribe(&Topic::from_str("a/b/#").expect("should parse topic"), 1), Some(UnsubscribeResult::NodeRemoved));

        assert_eq!(get_vec(&mut registry, "a/b/c"), None);
        assert_eq!(get_vec(&mut registry, "a/b/c/d"), None);
        assert_eq!(get_vec(&mut registry, "a/c/c/d/e"), None);

        assert!(registry.is_empty());
    }
}
