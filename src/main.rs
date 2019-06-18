#[macro_use]
extern crate log;
extern crate clap;
extern crate futures;
extern crate rdkafka;
extern crate avro_rs;
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

use rdkafka::message::OwnedHeaders;
use avro_rs::{Codec, Reader, Schema, Writer, from_value, types::Record, types::Value, to_value};
use failure::Error;
use schema_registry_converter::schema_registry::SubjectNameStrategy;
use schema_registry_converter::schema_registry::SuppliedSchema;
use schema_registry_converter::Encoder;
use crate::kafka_producer::get_producer;

#[derive(Debug, Serialize, Deserialize)]
struct TeamOrg {
	founder: String,
	co_founder: String,
	employees: Vec<Employee>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Employee {
	firstName: String,
	lastName: String,
	age: i32,
	employee_id: i64,
}

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

pub fn run_test(topic: &str, value_strategy: SubjectNameStrategy, payload_value: avro_rs::types::Value){
    let mut producer = get_producer(get_brokers(), get_schema_registry_url());
    let value_values = vec!(("teamorganization", payload_value));
    producer.send(topic, value_values, value_strategy);
}

fn main() -> Result<(), Error> {
	let employee = Employee {
		firstName: "Mary".to_owned(),
		lastName: "X".to_owned(),
		age: 52,
		employee_id: 234321534,
	};

	let employee_two = Employee {
		firstName: "Ron".to_owned(),
		lastName: "P".to_owned(),
		age: 23,
		employee_id: 341523413,
	};

	let employee_three = Employee {
		firstName: "Rachelle".to_owned(),
		lastName: "W".to_owned(),
		age: 45,
		employee_id: 122334234,
	};

	let employee_four = Employee {
		firstName: "Alexa".to_owned(),
		lastName: "T".to_owned(),
		age: 31,
		employee_id: 656343463,
	};

	let mut employee_vec: Vec<Employee> = Vec::new();
	employee_vec.push(employee);
	employee_vec.push(employee_two);
	employee_vec.push(employee_three);
	employee_vec.push(employee_four);

	let test = TeamOrg {
		founder: "Brian".to_owned(),
		co_founder: "Mary".to_owned(),
		employees: employee_vec,
	};

	let schema = Schema::parse_str(get_raw_schema())?;
	let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);
    writer.append_ser(test)?;

    writer.flush().unwrap();

    let input = writer.into_inner();
    let reader = Reader::with_schema(&schema, &input[..])?;

    let topic = "TEST1";

    for record in reader {
        let value_strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(get_employee_schema());
        run_test(topic, value_strategy, record.unwrap());
    }
    Ok(())
}