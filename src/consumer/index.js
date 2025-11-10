/**
 * Franco Gutierrez | SUGI FKMS LTD | 2025
 * Appointment Consumer Demo
 */
const AppointmentConsumer = require('./appointment.consumer');
const { TOPICS } = require('../config/kafka.config');

async function startConsumer() {
  // Create consumer with a unique group ID
  const consumer = new AppointmentConsumer('appointment-service-group-123');

  try {
    console.log('Starting Appointment Consumer...\n');

    // Connect to Kafka
    await consumer.connect();

    // Subscribe to all appointment topics
    await consumer.subscribe([
      TOPICS.APPOINTMENTS_CREATED,
      TOPICS.APPOINTMENTS_UPDATED,
      TOPICS.APPOINTMENTS_CANCELLED
    ]);

    console.log('\n' + '='.repeat(60));
    console.log('Consumer is ready and listening for messages...');
    console.log('=========\n');

    // Define custom handlers for different appointment events
    const handlers = {
      onAppointmentCreated: async (appointment, message) => {
        console.log('Custom Handler: New Appointment Created');
        console.log(`ID: ${appointment.id}`);
        console.log(`Patient: ${appointment.patientName}`);
        console.log(`Doctor: ${appointment.doctorName}`);
        console.log(`Scheduled: ${appointment.appointmentDate} at ${appointment.appointmentTime}`);

        // Simulate business logic
        console.log('Saving to database...');
        console.log('Sending confirmation email...');
        console.log('Updating calendar...');
        console.log('Processing completed\n');
      },

      onAppointmentUpdated: async (appointment, message) => {
        console.log('Custom Handler: Appointment Updated');
        console.log(`ID: ${appointment.id}`);
        console.log(`Updated At: ${appointment.updatedAt}`);

        // Simulate business logic
        console.log('Updating database record...');
        console.log('Notifying patient and doctor...');
        console.log('Syncing calendar changes...');
        console.log('Processing completed\n');
      },

      onAppointmentCancelled: async (appointment, message) => {
        console.log('Custom Handler: Appointment Cancelled');
        console.log(`ID: ${appointment.id}`);
        console.log(`Reason: ${appointment.cancellationReason || 'Not specified'}`);

        // Simulate business logic
        console.log('Marking appointment as cancelled in database...');
        console.log('Sending cancellation notification...');
        console.log('Freeing up time slot...');
        console.log('Processing refund if applicable...');
        console.log('Processing completed\n');
      }
    };

    // Start consuming messages with custom handlers
    await consumer.run(handlers);

    console.log('Press Ctrl+C to stop the consumer\n');

  } catch (error) {
    console.error('Error starting consumer:', error);
    process.exit(1);
  }
}

// Handle graceful shutdown
let isShuttingDown = false;

async function gracefulShutdown(consumer) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  console.log('\n\nShutting down consumer gracefully...');

  try {
    if (consumer) {
      await consumer.disconnect();
    }
    console.log('Consumer disconnected successfully');
    process.exit(0);
  } catch (error) {
    console.error('Error during shutdown:', error);
    process.exit(1);
  }
}

process.on('SIGINT', () => gracefulShutdown(null));
process.on('SIGTERM', () => gracefulShutdown(null));

// Catch unhandled errors
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

// Start the consumer
startConsumer().catch(error => {
  console.error('Failed to start consumer:', error);
  process.exit(1);
});
