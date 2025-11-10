const { kafka, TOPICS } = require('../config/kafka.config');
const Appointment = require('../models/appointment.model');

class AppointmentProducer {
  constructor() {
    this.producer = kafka.producer({
      allowAutoTopicCreation: true,
      transactionTimeout: 30000
    });
    this.isConnected = false;
  }

  async connect() {
    try {
      await this.producer.connect();
      this.isConnected = true;
      console.log('Kafka Producer connected successfully');
    } catch (error) {
      console.error('Failed to connect Kafka Producer:', error);
      throw error;
    }
  }

  async disconnect() {
    try {
      await this.producer.disconnect();
      this.isConnected = false;
      console.log('Kafka Producer disconnected');
    } catch (error) {
      console.error('Failed to disconnect Kafka Producer:', error);
      throw error;
    }
  }

  async createAppointment(appointmentData) {
    try {
      const appointment = new Appointment(appointmentData);
      const validation = appointment.validate();

      if (!validation.isValid) {
        throw new Error(`Validation failed: ${validation.errors.join(', ')}`);
      }

      const message = {
        key: appointment.id,
        value: JSON.stringify(appointment.toJSON()),
        headers: {
          eventType: 'APPOINTMENT_CREATED',
          timestamp: new Date().toISOString()
        }
      };

      const result = await this.producer.send({
        topic: TOPICS.APPOINTMENTS_CREATED,
        messages: [message]
      });

      console.log('Appointment created and sent to Kafka:', {
        appointmentId: appointment.id,
        topic: TOPICS.APPOINTMENTS_CREATED,
        partition: result[0].partition,
        offset: result[0].offset
      });

      return appointment;
    } catch (error) {
      console.error('Failed to create appointment:', error);
      throw error;
    }
  }

  async updateAppointment(appointmentId, updates) {
    try {
      const updatedData = {
        ...updates,
        id: appointmentId,
        updatedAt: new Date().toISOString()
      };

      const message = {
        key: appointmentId,
        value: JSON.stringify(updatedData),
        headers: {
          eventType: 'APPOINTMENT_UPDATED',
          timestamp: new Date().toISOString()
        }
      };

      const result = await this.producer.send({
        topic: TOPICS.APPOINTMENTS_UPDATED,
        messages: [message]
      });

      console.log('Appointment updated and sent to Kafka:', {
        appointmentId,
        topic: TOPICS.APPOINTMENTS_UPDATED,
        partition: result[0].partition,
        offset: result[0].offset
      });

      return updatedData;
    } catch (error) {
      console.error('Failed to update appointment:', error);
      throw error;
    }
  }

  async cancelAppointment(appointmentId, reason) {
    try {
      const cancellationData = {
        id: appointmentId,
        status: 'CANCELLED',
        cancellationReason: reason,
        cancelledAt: new Date().toISOString()
      };

      const message = {
        key: appointmentId,
        value: JSON.stringify(cancellationData),
        headers: {
          eventType: 'APPOINTMENT_CANCELLED',
          timestamp: new Date().toISOString()
        }
      };

      const result = await this.producer.send({
        topic: TOPICS.APPOINTMENTS_CANCELLED,
        messages: [message]
      });

      console.log('Appointment cancelled and sent to Kafka:', {
        appointmentId,
        topic: TOPICS.APPOINTMENTS_CANCELLED,
        partition: result[0].partition,
        offset: result[0].offset
      });

      return cancellationData;
    } catch (error) {
      console.error('Failed to cancel appointment:', error);
      throw error;
    }
  }

  async sendBatch(appointments) {
    try {
      const messages = appointments.map(appointmentData => {
        const appointment = new Appointment(appointmentData);
        return {
          key: appointment.id,
          value: JSON.stringify(appointment.toJSON()),
          headers: {
            eventType: 'APPOINTMENT_CREATED',
            timestamp: new Date().toISOString()
          }
        };
      });

      const result = await this.producer.send({
        topic: TOPICS.APPOINTMENTS_CREATED,
        messages
      });

      console.log(`Batch of ${appointments.length} appointments sent to Kafka`);
      return result;
    } catch (error) {
      console.error('Failed to send batch appointments:', error);
      throw error;
    }
  }
}

module.exports = AppointmentProducer;
