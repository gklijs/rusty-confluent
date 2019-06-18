use avro_rs::types::Value;
use futures::future::Future;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use schema_registry_converter::schema_registry::SubjectNameStrategy;
use schema_registry_converter::Encoder;

pub struct RecordProducer {
    producer: FutureProducer,
    encoder: Encoder,
}

impl<'a> RecordProducer {
    pub fn send(
        &'a mut self,
        topic: &'a str,
        value_values: Vec<(&'static str, Value)>,
        value_strategy: SubjectNameStrategy,
    ) -> Result<(i32, i64), (KafkaError, OwnedMessage)> {
        let payload = match self.encoder.encode(value_values, &value_strategy) {
            Ok(v) => v,
            Err(e) => panic!("Error getting payload: {}", e),
        };
        let fr = FutureRecord {
            topic,
            partition: None,
            payload: Some(&payload),
            key: Some("key1"),
            timestamp: None,
            headers: None,
        };
        self.producer.send(fr, 0).wait().unwrap()
    }
}

pub fn get_producer(brokers: &str, schema_registry_url: String) -> RecordProducer {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "60000")
        .set("queue.buffering.max.messages", "10")
        .create()
        .expect("Producer creation error");
    let encoder = Encoder::new(schema_registry_url);
    RecordProducer { producer, encoder }
}
