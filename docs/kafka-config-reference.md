# Kafka Infrastructure — Reference Guide

How Kafka is configured in this project, explained for future reference.

---

## Overview

Kafka is the event broker. Every time an employee is created, updated, or deleted, an event is published to Kafka. A consumer picks it up and saves it to MongoDB.

```
Producer (EmployeeEventProducer)
        ↓
   employee_topic  (Kafka)
        ↓
Consumer (EmployeeEventConsumer)
        ↓
     MongoDB
```

---

## Docker Compose Services

### Zookeeper
```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.6.1
  environment:
    ZOOKEEPER_CLIENT_PORT: 2181
    ZOOKEEPER_TICK_TIME: 2000
```

| Config | Value | What it means |
|---|---|---|
| `ZOOKEEPER_CLIENT_PORT` | `2181` | Port Kafka connects to Zookeeper on |
| `ZOOKEEPER_TICK_TIME` | `2000` | Heartbeat interval in ms — how often Zookeeper checks if nodes are alive |

**What it does:** Zookeeper is Kafka's coordinator. It tracks which brokers are alive, who leads each partition, and manages cluster membership. Being phased out in newer Kafka (KRaft mode) but still required in Confluent 7.6.1.

---

### Kafka Broker
```yaml
kafka:
  ports:
    - "9092:9092"    # external — your laptop talks to Kafka here
    - "29092:29092"  # internal — containers talk to each other here
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    KAFKA_LOG_RETENTION_HOURS: 168
```

| Config | Value | What it means |
|---|---|---|
| `KAFKA_BROKER_ID` | `1` | Unique ID for this broker — needed when you have multiple brokers |
| `KAFKA_ZOOKEEPER_CONNECT` | `zookeeper:2181` | Where to find Zookeeper |
| `KAFKA_ADVERTISED_LISTENERS` | see below | The two addresses Kafka tells clients to connect to |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `PLAINTEXT` | Which listener brokers use to talk to each other |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1` | Only 1 broker in dev, so replicas = 1. **Never use 1 in production** |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `true` | Kafka creates topics automatically if they don't exist |
| `KAFKA_LOG_RETENTION_HOURS` | `168` | Keep messages for 7 days then delete |

#### The Two Listeners Explained

This is the most confusing part of any Kafka setup:

```
PLAINTEXT://kafka:29092        ← used by containers (Spring app, Schema Registry)
PLAINTEXT_HOST://localhost:9092 ← used by your laptop (CLI tools, Kafka UI)
```

Inside Docker, containers refer to each other by service name (`kafka`). Outside Docker (your laptop), you use `localhost`. Two listeners solves both cases.

That's why `application.yml` uses:
```yaml
spring.kafka.bootstrap-servers: kafka:29092
```
And tests use the Testcontainers-mapped port dynamically.

---

### Schema Registry
```yaml
schema-registry:
  environment:
    SCHEMA_REGISTRY_HOST_NAME: schema-registry
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka:29092
    SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
```

| Config | Value | What it means |
|---|---|---|
| `SCHEMA_REGISTRY_HOST_NAME` | `schema-registry` | How it identifies itself to clients |
| `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` | `PLAINTEXT://kafka:29092` | Schema Registry stores schemas in a Kafka topic internally — this is where it connects |
| `SCHEMA_REGISTRY_LISTENERS` | `http://0.0.0.0:8081` | Exposes REST API on port 8081 |

**What it does:** Acts as a central contract store for Avro schemas. Producers register the schema before sending. Consumers fetch it before deserialising. Both sides are guaranteed to agree on the message shape.

---

### Topic Init
```yaml
kafka-init:
  command:
    - |
      kafka-topics --bootstrap-server kafka:29092 \
        --create --if-not-exists \
        --topic employee_topic \
        --partitions 3 \
        --replication-factor 1
```

Creates `employee_topic` with:

| Setting | Value | What it means |
|---|---|---|
| `--partitions 3` | 3 | Topic is split into 3 buckets — allows up to 3 consumers in the same group to read in parallel |
| `--replication-factor 1` | 1 | Only 1 copy of each message (dev only — use ≥ 2 in production) |
| `--if-not-exists` | — | Safe to run multiple times — won't fail if topic already exists |

---

### Schema Registry Init
```yaml
schema-registry-init:
  volumes:
    - ./src/main/avro/message.avsc:/schemas/employee.avsc:ro
    - ./scripts/register-schema.sh:/register-schema.sh:ro
  entrypoint: ["/bin/sh", "/register-schema.sh"]
```

Mounts your `message.avsc` file into the container and runs `register-schema.sh` which POSTs it to the Schema Registry REST API. This pre-registers the schema so the producer doesn't have to do it on first message.

---

### Control Center
```yaml
control-center:
  ports:
    - "9021:9021"
  environment:
    CONTROL_CENTER_BOOTSTRAP_SERVERS: kafka:29092
    CONTROL_CENTER_REPLICATION_FACTOR: 1
```

Web UI for Kafka. Access at **http://localhost:9021**. Shows topics, messages, consumer groups, lag, and schema registry.

---

## Startup Order

```
Zookeeper (healthy)
    ↓
Kafka (healthy)
    ├── Schema Registry (started)
    │       └── schema-registry-init (registers message.avsc)
    ├── kafka-init (creates employee_topic)
    └── App (starts last — all dependencies healthy)
```

---

## application.yml — Kafka Config

```yaml
spring:
  kafka:
    bootstrap-servers: kafka:29092
    properties:
      schema.registry.url: http://schema-registry:8081
      specific.avro.reader: true
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      acks: all
      retries: 3
    consumer:
      group-id: kafka-training-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      enable-auto-commit: false
    topic:
      employee: employee_topic
```

| Config | Value | What it means |
|---|---|---|
| `bootstrap-servers` | `kafka:29092` | Entry point — where the app connects to discover the cluster |
| `schema.registry.url` | `http://schema-registry:8081` | Where to register/fetch Avro schemas |
| `specific.avro.reader` | `true` | Deserialise into generated Java classes (`EmployeeEvent`) not generic records |
| `value-serializer` | `KafkaAvroSerializer` | Serialise message value as Avro binary |
| `acks: all` | `all` | Producer waits for all replicas to confirm before considering a send successful |
| `retries: 3` | `3` | Retry failed sends up to 3 times |
| `group-id` | `kafka-training-group` | Consumer group name — Kafka tracks offset per group |
| `auto-offset-reset` | `earliest` | If no offset exists yet, start from the beginning of the topic |
| `enable-auto-commit` | `false` | Don't auto-commit offsets — Spring Kafka manages this manually for reliability |

