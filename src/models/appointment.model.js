/**
 * Franco Gutierrez | SUGI FKMS LTD | 2025
 * Appointment Model Module
 */
const { v4: uuidv4 } = require('uuid');

class Appointment {
  constructor(data) {
    this.id = data.id || uuidv4();
    this.patientName = data.patientName;
    this.doctorName = data.doctorName;
    this.appointmentDate = data.appointmentDate;
    this.appointmentTime = data.appointmentTime;
    this.reason = data.reason;
    this.status = data.status || 'SCHEDULED'; // SCHEDULED, CONFIRMED, CANCELLED, COMPLETED
    this.createdAt = data.createdAt || new Date().toISOString();
    this.updatedAt = data.updatedAt || new Date().toISOString();
  }

  validate() {
    const errors = [];

    if (!this.patientName) {
      errors.push('Patient name is required');
    }

    if (!this.doctorName) {
      errors.push('Doctor name is required');
    }

    if (!this.appointmentDate) {
      errors.push('Appointment date is required');
    }

    if (!this.appointmentTime) {
      errors.push('Appointment time is required');
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }

  toJSON() {
    return {
      id: this.id,
      patientName: this.patientName,
      doctorName: this.doctorName,
      appointmentDate: this.appointmentDate,
      appointmentTime: this.appointmentTime,
      reason: this.reason,
      status: this.status,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt
    };
  }
}

module.exports = Appointment;
