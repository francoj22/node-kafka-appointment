/**
 * Franco Gutierrez | SUGI FKMS LTD | 2025
 * Kafka Configuration Module
 */
const { Kafka } = require('kafkajs');

// Kafka configuration
const kafka = new Kafka({
  clientId: 'appointment-service',
  brokers: ['localhost:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

// Topic configuration
const TOPICS = {
  APPOINTMENTS: 'appointments-topic',
  APPOINTMENTS_CREATED: 'appointments-created',
  APPOINTMENTS_UPDATED: 'appointments-updated',
  APPOINTMENTS_CANCELLED: 'appointments-cancelled'
};

module.exports = {
  kafka,
  TOPICS
};
