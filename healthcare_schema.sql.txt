-- =====================================================
-- Healthcare.gammal.tech Database Schema (Production Ready)
-- =====================================================

CREATE SCHEMA IF NOT EXISTS healthcare;

-- =================================
-- Users Table
-- =================================
CREATE TABLE healthcare.users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    phone_number VARCHAR(20),
    date_of_birth DATE,
    gender VARCHAR(10),
    insurance_id UUID,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =================================
-- Doctors Table
-- =================================
CREATE TABLE healthcare.doctors (
    doctor_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name VARCHAR(255) NOT NULL,
    specialty VARCHAR(100),
    email VARCHAR(255) UNIQUE NOT NULL,
    phone_number VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =================================
-- Appointments Table
-- =================================
CREATE TABLE healthcare.appointments (
    appointment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES healthcare.users(user_id),
    doctor_id UUID REFERENCES healthcare.doctors(doctor_id),
    appointment_time TIMESTAMP NOT NULL,
    status VARCHAR(50) DEFAULT 'Scheduled',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =================================
-- Medical Records Table
-- =================================
CREATE TABLE healthcare.medical_records (
    record_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES healthcare.users(user_id),
    doctor_id UUID REFERENCES healthcare.doctors(doctor_id),
    diagnosis TEXT,
    treatment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =================================
-- Insurance Table
-- =================================
CREATE TABLE healthcare.insurance (
    insurance_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_name VARCHAR(255) NOT NULL,
    policy_number VARCHAR(100) UNIQUE NOT NULL,
    coverage_details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =================================
-- Statistics / Analytics Tables
-- =================================
CREATE TABLE healthcare.analytics_diseases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    disease_name VARCHAR(255) NOT NULL,
    patient_count INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE healthcare.analytics_doctor_visits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doctor_id UUID REFERENCES healthcare.doctors(doctor_id),
    visit_count INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =================================
-- Platform Information (Healthcare.gammal.tech)
-- =================================
CREATE TABLE healthcare.healthcare_gammal_tech (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    domain VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial record
INSERT INTO healthcare.healthcare_gammal_tech (name, domain, contact_email)
VALUES (
    'Healthcare.gammal.tech',
    'https://healthcare.gammal.tech',
    'support@healthcare.gammal.tech'
);
