# Kafka Data Gen Tools

This repository provides a deployable REST api for the purpose of producing
dynamic data to kafka topics for functionality and load testing purposes.

## Dependencies

- [kafkajs](https://kafka.js.org/) ([npm](https://www.npmjs.com/package/kafkajs))
- [faker](https://github.com/Marak/Faker.js#readme) ([npm](https://www.npmjs.com/package/faker))

# API

### `GET /api/agents`

List currently running data generation agents.

### `POST /api/agents`

Create new data generation agent.

# Models

## Agent

## Scenario

## Generation Strategy

### GenerationStrategy.CONSTANT

### GenerationStrategy.FAKER
