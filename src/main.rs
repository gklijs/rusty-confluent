#[macro_use]
extern crate log;
extern crate avro_rs;
extern crate clap;
extern crate futures;
extern crate rdkafka;
extern crate schema_registry_converter;

#[macro_use]
extern crate serde_derive;
extern crate failure;

mod kafka_producer;

use clap::{App, Arg};
use futures::*;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

use crate::kafka_producer::get_producer;
use avro_rs::{from_value, to_value, types::Record, types::Value, Codec, Reader, Schema, Writer};
use failure::Error;
use rdkafka::message::OwnedHeaders;
use schema_registry_converter::schema_registry::SubjectNameStrategy;
use schema_registry_converter::schema_registry::SuppliedSchema;
use schema_registry_converter::Encoder;

pub fn get_schema_registry_url() -> String {
    "localhost:8081".into()
}

pub fn get_brokers() -> &'static str {
    "127.0.0.1:9092"
}

pub fn get_raw_schema() -> &'static str {
    let raw_schema = r#"
        {
            "namespace": "com.rusty.test",
            "type": "record",
            "name": "teamorganization",
            "fields": [
                {"name": "founder", "type": "string"},
                {"name": "co_founder", "type": "string"},
                {"name": "employees", "type": {
                    "type": "array",
                    "items": {
                        "name": "Employee",
                        "type": "record",
                        "fields": [
                            {"name": "firstName", "type": "string"},
                            {"name": "lastName", "type": "string"},
                            {"name": "age", "type": "int"},
                            {"name": "employee_id", "type": "long"}
                        ]
                    }}
                }
            ]
        }
    "#;
    raw_schema
}

pub fn get_employee_schema() -> SuppliedSchema {
    let raw_schema = get_raw_schema();
    SuppliedSchema::new(raw_schema.to_string())
}

pub fn run_test(
    topic: &str,
    value_strategy: SubjectNameStrategy,
    value_values: Vec<(&'static str, Value)>,
) {
    let mut producer = get_producer(get_brokers(), get_schema_registry_url());
    producer.send(topic, value_values, value_strategy);
}

fn main() -> () {
    let employee = Value::Record(vec![
        ("firstName".to_string(), Value::String("Mary".to_owned())),
        ("lastName".to_string(), Value::String("X".to_owned())),
        ("age".to_string(), Value::Int(52)),
        ("employee_id".to_string(), Value::Long(234321534)),
    ]);

    let employee_two = Value::Record(vec![
        ("firstName".to_string(), Value::String("Ron".to_owned())),
        ("lastName".to_string(), Value::String("P".to_owned())),
        ("age".to_string(), Value::Int(23)),
        ("employee_id".to_string(), Value::Long(341523413)),
    ]);

    let employee_three = Value::Record(vec![
        (
            "firstName".to_string(),
            Value::String("Rachelle".to_owned()),
        ),
        ("lastName".to_string(), Value::String("W".to_owned())),
        ("age".to_string(), Value::Int(45)),
        ("employee_id".to_string(), Value::Long(122334234)),
    ]);

    let employee_four = Value::Record(vec![
        ("firstName".to_string(), Value::String("Alexa".to_owned())),
        ("lastName".to_string(), Value::String("T".to_owned())),
        ("age".to_string(), Value::Int(31)),
        ("employee_id".to_string(), Value::Long(656343463)),
    ]);

    let mut employee_vec: Vec<(Value)> = Vec::new();
    employee_vec.push(employee);
    employee_vec.push(employee_two);
    employee_vec.push(employee_three);
    employee_vec.push(employee_four);

    let topic = "TEST1";
    let value_strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(get_employee_schema());
    run_test(
        topic,
        value_strategy,
        vec![
            ("founder", Value::String("Brian".to_string())),
            ("co_founder", Value::String("Mary".to_string())),
            ("employees", Value::Array(employee_vec)),
        ],
    )
}
