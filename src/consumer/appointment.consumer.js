/**
 * Franco Gutierrez | SUGI FKMS LTD | 2025
 * Appointment Consumer Module
 */
const { kafka, TOPICS } = require('../config/kafka.config');

class AppointmentConsumer {
  constructor(groupId = 'appointment-consumer-group') {
    this.consumer = kafka.consumer({
      groupId,
      sessionTimeout: 30000,
      heartbeatInterval: 3000
    });
    this.isConnected = false;
  }

  async connect() {
    try {
      await this.consumer.connect();
      this.isConnected = true;
      console.log('Kafka Consumer connected successfully');
    } catch (error) {
      console.error('Failed to connect Kafka Consumer:', error);
      throw error;
    }
  }

  async disconnect() {
    try {
      await this.consumer.disconnect();
      this.isConnected = false;
      console.log('Kafka Consumer disconnected');
    } catch (error) {
      console.error('Failed to disconnect Kafka Consumer:', error);
      throw error;
    }
  }

  async subscribe(topics = [TOPICS.APPOINTMENTS_CREATED]) {
    try {
      for (const topic of topics) {
        await this.consumer.subscribe({
          topic,
          fromBeginning: true
        });
        console.log(`Subscribed to topic: ${topic}`);
      }
    } catch (error) {
      console.error('Failed to subscribe to topics:', error);
      throw error;
    }
  }

  async run(handlers = {}) {
    try {
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const appointment = JSON.parse(message.value.toString());
            const eventType = message.headers?.eventType?.toString() || 'UNKNOWN';

            console.log('\n' + '='.repeat(60));
            console.log(`Received message from topic: ${topic}`);
            console.log(`Partition: ${partition} | Offset: ${message.offset}`);
            console.log(`Event Type: ${eventType}`);
            console.log(`Timestamp: ${new Date().toISOString()}`);
            console.log('Message Key:', message.key?.toString());
            console.log('Appointment Data:', JSON.stringify(appointment, null, 2));
            console.log('====================' +  '\n');

            // Handle different event types
            switch (topic) {
              case TOPICS.APPOINTMENTS_CREATED:
                if (handlers.onAppointmentCreated) {
                  await handlers.onAppointmentCreated(appointment, message);
                } else {
                  await this.defaultAppointmentCreatedHandler(appointment);
                }
                break;

              case TOPICS.APPOINTMENTS_UPDATED:
                if (handlers.onAppointmentUpdated) {
                  await handlers.onAppointmentUpdated(appointment, message);
                } else {
                  await this.defaultAppointmentUpdatedHandler(appointment);
                }
                break;

              case TOPICS.APPOINTMENTS_CANCELLED:
                if (handlers.onAppointmentCancelled) {
                  await handlers.onAppointmentCancelled(appointment, message);
                } else {
                  await this.defaultAppointmentCancelledHandler(appointment);
                }
                break;

              default:
                console.log(`No handler for topic: ${topic}`);
            }
          } catch (error) {
            console.error('Error processing message:', error);
            // In production, you might want to send to a dead letter queue
          }
        }
      });

      console.log('Consumer is running and listening for messages...');
    } catch (error) {
      console.error('Failed to run consumer:', error);
      throw error;
    }
  }

  // Default handlers
  async defaultAppointmentCreatedHandler(appointment) {
    console.log('Processing new appointment:');
    console.log(`Patient: ${appointment.patientName}`);
    console.log(`Doctor: ${appointment.doctorName}`);
    console.log(`Date: ${appointment.appointmentDate} at ${appointment.appointmentTime}`);
    console.log(`Status: ${appointment.status}`);

    
    console.log('Save to database');
    console.log('Send confirmation email');
    console.log('Update calendar');
    console.log('Trigger notifications');
  }

  async defaultAppointmentUpdatedHandler(appointment) {
    console.log('Processing appointment update:');
    console.log(`Appointment ID: ${appointment.id}`);
    console.log(`Updated at: ${appointment.updatedAt}`);
    console.log(`Update database record`);
    console.log('Send Notification');
    console.log('Sync with Calendar')
  }

  async defaultAppointmentCancelledHandler(appointment) {
    console.log('Processing appointment cancellation:');
    console.log(`Appointment ID: ${appointment.id}`);
    console.log(`Cancelled at: ${appointment.cancelledAt}`);
    console.log(`Reason: ${appointment.cancellationReason || 'Not specified'}`);

    console.log('Update database record');
    console.log('Send cancellation notification');
    console.log('Free up calendar slot');
    console.log('Process refunds if applicable');
  }

  async seekToBeginning() {
  }

  async seekToBeginning() {
    try {
      const topics = await this.consumer.topics();
      for (const topic of topics) {
        await this.consumer.seek({ topic, partition: 0, offset: '0' });
      }
      console.log('Consumer seeked to beginning of all partitions');
    } catch (error) {
      console.error('Failed to seek to beginning:', error);
      throw error;
    }
  }
}

module.exports = AppointmentConsumer;
