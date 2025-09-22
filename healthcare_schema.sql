-- =============================================
-- 🏥 Healthcare System Database Schema
-- Production-ready version
-- =============================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE patients (
    patient_id SERIAL PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT,
    encrypted_ssn BYTEA NOT NULL,
    birth_date DATE NOT NULL CHECK (birth_date <= CURRENT_DATE),
    gender VARCHAR(10) CHECK (gender IN ('Male','Female','Other')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE doctors (
    doctor_id SERIAL PRIMARY KEY,
    full_name TEXT NOT NULL,
    specialty TEXT NOT NULL,
    email TEXT UNIQUE,
    phone TEXT
);

CREATE TABLE diseases (
    disease_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT
);

CREATE TABLE patient_diagnoses (
    diagnosis_id SERIAL PRIMARY KEY,
    patient_id INT REFERENCES patients(patient_id),
    disease_id INT REFERENCES diseases(disease_id),
    diagnosis_date DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE TABLE appointments (
    appointment_id SERIAL PRIMARY KEY,
    patient_id INT REFERENCES patients(patient_id),
    doctor_id INT REFERENCES doctors(doctor_id),
    appointment_date TIMESTAMP NOT NULL,
    status VARCHAR(20) CHECK (status IN ('Scheduled','Completed','Cancelled'))
);

CREATE TABLE visits (
    visit_id SERIAL PRIMARY KEY,
    patient_id INT REFERENCES patients(patient_id),
    doctor_id INT REFERENCES doctors(doctor_id),
    visit_date DATE NOT NULL,
    notes TEXT
);

CREATE TABLE insurance_companies (
    company_id SERIAL PRIMARY KEY,
    company_name TEXT NOT NULL,
    contact_email TEXT,
    contact_phone TEXT
);

CREATE TABLE insurance_policies (
    policy_id SERIAL PRIMARY KEY,
    patient_id INT REFERENCES patients(patient_id),
    company_id INT REFERENCES insurance_companies(company_id),
    policy_number TEXT UNIQUE NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

CREATE TABLE insurance_claims (
    claim_id SERIAL PRIMARY KEY,
    policy_id INT REFERENCES insurance_policies(policy_id),
    visit_id INT REFERENCES visits(visit_id),
    claim_date DATE DEFAULT CURRENT_DATE,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) CHECK (status IN ('Pending','Approved','Rejected'))
);

CREATE TABLE audit_log (
    log_id SERIAL PRIMARY KEY,
    table_name TEXT,
    record_id INT,
    operation TEXT CHECK (operation IN ('INSERT','UPDATE','DELETE')),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Statistics
SELECT COUNT(DISTINCT patient_id) AS diabetes_patients
FROM patient_diagnoses d
JOIN diseases ds ON d.disease_id = ds.disease_id
WHERE ds.name ILIKE 'diabetes';

SELECT COUNT(DISTINCT patient_id) AS hypertension_patients
FROM patient_diagnoses d
JOIN diseases ds ON d.disease_id = ds.disease_id
WHERE ds.name ILIKE 'hypertension';

SELECT doctor_id, COUNT(*) AS total_visits
FROM visits
GROUP BY doctor_id;

SELECT company_id,
       SUM(CASE WHEN status='Approved' THEN 1 ELSE 0 END)::decimal / COUNT(*) * 100 AS approval_rate
FROM insurance_claims
GROUP BY company_id;
