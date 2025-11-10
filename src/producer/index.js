const AppointmentProducer = require('./appointment.producer');

// Sample appointment data
const sampleAppointments = [
  {
    patientName: 'Tony Stark',
    doctorName: 'Dr. Sarah Smith',
    appointmentDate: '2025-11-15',
    appointmentTime: '10:00 AM',
    reason: 'Regular checkup',
    status: 'SCHEDULED'
  },
  {
    patientName: 'Jimmy Neutron',
    doctorName: 'Dr. Michael Johnson',
    appointmentDate: '2025-11-16',
    appointmentTime: '02:30 PM',
    reason: 'Follow-up consultation',
    status: 'SCHEDULED'
  },
  {
    patientName: 'Sara Connor',
    doctorName: 'Dr. Emily Davis',
    appointmentDate: '2025-11-17',
    appointmentTime: '11:00 AM',
    reason: 'Lab results review',
    status: 'SCHEDULED'
  }
];

async function demonstrateProducer() {
  const producer = new AppointmentProducer();

  try {
    // Connect to Kafka
    console.log('Starting Appointment Producer...\n');
    await producer.connect();

    // Wait a bit for connection to stabilize
    await new Promise(resolve => setTimeout(resolve, 2000));

    console.log('\n' + '======================');
    console.log('Demo: Creating Individual Appointments');
    console.log('==============\n');

    // Create individual appointments
    for (const appointmentData of sampleAppointments) {
      const appointment = await producer.createAppointment(appointmentData);
      console.log(`Created appointment for ${appointment.patientName}`);
      await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second between messages
    }

    console.log('\n' + '==============');
    console.log('Demo: Updating an Appointment');
    console.log('==============================\n');

    // Update an appointment (you would normally have the appointment ID from creation)
    await new Promise(resolve => setTimeout(resolve, 2000));
    const updateData = {
      patientName: 'Tony Stark',
      appointmentDate: '2025-11-18',
      appointmentTime: '03:00 PM',
      status: 'CONFIRMED'
    };
    await producer.updateAppointment('sample-appointment-id', updateData);

    console.log('\n=====================');
    console.log('Demo: Cancelling an Appointment');
    console.log('========================\n');

    // Cancel an appointment
    await new Promise(resolve => setTimeout(resolve, 2000));
    await producer.cancelAppointment('sample-appointment-id-2', 'Patient requested reschedule');

    console.log('\n======================');
    console.log('Demo: Batch Creating Appointments');
    console.log('================');

    // Send batch appointments
    await new Promise(resolve => setTimeout(resolve, 2000));
    const batchAppointments = [
      {
        patientName: 'Alice Cooper',
        doctorName: 'Dr. James Wilson',
        appointmentDate: '2025-11-20',
        appointmentTime: '09:00 AM',
        reason: 'Annual physical'
      },
      {
        patientName: 'Bob Martin',
        doctorName: 'Dr. Lisa Anderson',
        appointmentDate: '2025-11-21',
        appointmentTime: '01:00 PM',
        reason: 'Vaccination'
      }
    ];
    await producer.sendBatch(batchAppointments);

    console.log('\n' + '==================');
    console.log('All demo operations completed successfully!');
    console.log('========\n');

    // Keep the producer running for a bit
    console.log('Producer will remain active for 10 seconds...');
    await new Promise(resolve => setTimeout(resolve, 10000));

  } catch (error) {
    console.error('Error in producer demo:', error);
  } finally {
    await producer.disconnect();
    console.log('\nProducer demo finished. Exiting...');
    process.exit(0);
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n\nReceived SIGINT, shutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n\nReceived SIGTERM, shutting down gracefully...');
  process.exit(0);
});

// Run the demo
demonstrateProducer().catch(console.error);
