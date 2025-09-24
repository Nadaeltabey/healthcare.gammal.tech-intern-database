-- =========================
-- Healthcare.gammal.tech
-- Unified Production-Ready SQL Foundation (PostgreSQL)
-- Prereqs: pgcrypto, uuid-ossp
-- IMPORTANT: replace placeholder secrets with secret-manager values before production.
-- =========================

-- 0. Extensions & schema
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP SCHEMA IF EXISTS healthcare CASCADE;
CREATE SCHEMA healthcare;
SET search_path = healthcare, public;

-- =========================
-- 1. Roles (minimal)
-- =========================
DO $$ BEGIN
  EXECUTE 'CREATE ROLE healthcare_admin NOINHERIT LOGIN';
EXCEPTION WHEN others THEN PERFORM 1; END$$;
DO $$ BEGIN
  EXECUTE 'CREATE ROLE healthcare_etl NOINHERIT LOGIN';
EXCEPTION WHEN others THEN PERFORM 1; END$$;
DO $$ BEGIN
  EXECUTE 'CREATE ROLE healthcare_clinician NOINHERIT LOGIN';
EXCEPTION WHEN others THEN PERFORM 1; END$$;
DO $$ BEGIN
  EXECUTE 'CREATE ROLE healthcare_researcher NOINHERIT LOGIN';
EXCEPTION WHEN others THEN PERFORM 1; END$$;
DO $$ BEGIN
  EXECUTE 'CREATE ROLE healthcare_viewer NOINHERIT LOGIN';
EXCEPTION WHEN others THEN PERFORM 1; END$$;

-- =========================
-- 2. Config / Secrets placeholder
-- =========================
CREATE TABLE config (
  key text PRIMARY KEY,
  value text NOT NULL,
  updated_at timestamptz default now()
);

INSERT INTO config(key, value) VALUES
  ('data_salt', 'CHANGE_ME_SAVE_IN_SECRETS_MANAGER') ON CONFLICT (key) DO NOTHING,
  ('pgp_sym_key', 'CHANGE_ME_PGP_KEY_IN_SECRETS') ON CONFLICT (key) DO NOTHING;

CREATE OR REPLACE FUNCTION cfg_get(k text) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT value FROM config WHERE key = k;
$$;

-- =========================
-- 3. Documentation tables (folder naming, DQ rules)
-- =========================
CREATE TABLE doc_folder_structure (
  id serial PRIMARY KEY,
  area text NOT NULL,
  recommended_structure text NOT NULL,
  example text,
  created_at timestamptz default now()
);

INSERT INTO doc_folder_structure(area, recommended_structure, example) VALUES
('data lake', '/data/incoming/yyyy=YYYY/mm=MM/dd=DD/', 'visits_clinicA_20250922_b001.csv'),
('archive', '/data/raw/yyyy=YYYY/mm=MM/', 'raw_visits_2025-09-'),
('processed', '/data/processed/parquet/<dataset>/year=YYYY/', 'cleaned_visits/year=2025/');

CREATE TABLE doc_dq_rules (
  rule_id serial PRIMARY KEY,
  short_desc text NOT NULL,
  sql_check text NOT NULL,
  severity text DEFAULT 'warning', -- warning|error
  created_at timestamptz default now()
);

INSERT INTO doc_dq_rules(short_desc, sql_check, severity) VALUES
('visit_date must exist', 'SELECT COUNT(*) FROM cleaned_visits WHERE visit_date IS NULL;', 'error'),
('systolic range [60,250]', 'SELECT COUNT(*) FROM cleaned_visits WHERE systolic_bp < 60 OR systolic_bp > 250;', 'error'),
('diastolic range [30,160]', 'SELECT COUNT(*) FROM cleaned_visits WHERE diastolic_bp < 30 OR diastolic_bp > 160;', 'error'),
('heart_rate range [20,250]', 'SELECT COUNT(*) FROM cleaned_visits WHERE heart_rate < 20 OR heart_rate > 250;', 'warning'),
('no plain-PII in public tables', 'manual review: scan known columns and raw_payload', 'error');

-- =========================
-- 4. Reference tables & mappings (facilities, users, clinicians, doctors, insurers)
-- =========================
CREATE TABLE facility (
  facility_id bigserial PRIMARY KEY,
  name text NOT NULL UNIQUE,
  code text,
  address text,
  phone text,
  created_at timestamptz default now()
);

CREATE TABLE user_account (
  user_id bigserial PRIMARY KEY,
  username text NOT NULL UNIQUE,
  full_name text,
  role text NOT NULL, -- 'admin','etl','clinician','researcher','viewer'
  email text,
  created_at timestamptz default now()
);

CREATE TABLE clinician_facility (
  user_id bigint NOT NULL REFERENCES user_account(user_id) ON DELETE CASCADE,
  facility_id bigint NOT NULL REFERENCES facility(facility_id) ON DELETE CASCADE,
  PRIMARY KEY (user_id, facility_id)
);

-- doctors table
CREATE TABLE doctor (
  doctor_id bigserial PRIMARY KEY,
  full_name text NOT NULL,
  license_no text,
  specialty text,
  facility_id bigint REFERENCES facility(facility_id),
  created_at timestamptz default now()
);

-- insurers & patient-insurance
CREATE TABLE insurer (
  insurer_id bigserial PRIMARY KEY,
  name text NOT NULL UNIQUE,
  code text,
  contact jsonb,
  created_at timestamptz default now()
);

CREATE TABLE patient_insurance (
  patient_hash text NOT NULL REFERENCES patient(patient_hash) ON DELETE CASCADE, -- patient defined later
  insurer_id bigint NOT NULL REFERENCES insurer(insurer_id),
  policy_number text,
  start_date date,
  end_date date,
  active boolean DEFAULT true,
  primary key (patient_hash, insurer_id)
);

-- =========================
-- 5. Raw ingestion stores (visits + competitors)
-- =========================
CREATE TABLE raw_visits (
  raw_id bigserial PRIMARY KEY,
  source_file text,
  row_num int,
  raw_payload jsonb NOT NULL,
  imported_at timestamptz default now()
);
CREATE INDEX idx_raw_imported_at ON raw_visits(imported_at);

CREATE TABLE competitor_stats_raw (
  raw_id bigserial PRIMARY KEY,
  source_file text,
  row_num int,
  raw_payload jsonb NOT NULL,
  imported_at timestamptz default now()
);

-- =========================
-- 6. PII table (encrypted) - tightly controlled
-- =========================
CREATE TABLE pii_patients (
  pii_id bigserial PRIMARY KEY,
  patient_id_text text, -- original id from source (kept encrypted if needed)
  name_encrypted bytea,
  phone_encrypted bytea,
  email_encrypted bytea,
  extra jsonb,
  created_at timestamptz default now()
);

CREATE OR REPLACE FUNCTION encrypt_pii(plain text) RETURNS bytea LANGUAGE sql AS $$
  SELECT pgp_sym_encrypt(plain, cfg_get('pgp_sym_key'));
$$;

CREATE OR REPLACE FUNCTION decrypt_pii(cipher bytea) RETURNS text LANGUAGE sql AS $$
  SELECT pgp_sym_decrypt(cipher, cfg_get('pgp_sym_key'));
$$;

-- =========================
-- 7. De-identified patient table
-- =========================
CREATE TABLE patient (
  patient_hash text PRIMARY KEY,
  created_at timestamptz default now(),
  notes text
);

CREATE OR REPLACE FUNCTION make_patient_hash(patient_id_text text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE salt text;
BEGIN
  salt := cfg_get('data_salt');
  IF salt IS NULL THEN
    RAISE EXCEPTION 'data_salt not set in config. Use secret manager in production.';
  END IF;
  RETURN substr(encode(digest(coalesce(patient_id_text,'' ) || salt, 'sha256'), 'hex'),1,16);
END;
$$;

-- =========================
-- 8. Cleaned visits (partitioned) — expanded with doctor_id & insurer_id
-- =========================
CREATE TABLE cleaned_visits (
  visit_id bigserial NOT NULL,
  patient_hash text NOT NULL REFERENCES patient(patient_hash),
  facility_id bigint REFERENCES facility(facility_id),
  doctor_id bigint REFERENCES doctor(doctor_id),
  insurer_id bigint REFERENCES insurer(insurer_id),
  visit_date date,
  dob date,
  age_at_visit numeric(5,1),
  gender text,
  systolic_bp numeric,
  diastolic_bp numeric,
  heart_rate numeric,
  glucose_mg_dl numeric,
  cholesterol_mg_dl numeric,
  diagnosis_code text,
  diagnosis_type text,
  source_raw_id bigint REFERENCES raw_visits(raw_id),
  was_imputed boolean DEFAULT false,
  outlier_flag boolean DEFAULT false,
  created_at timestamptz DEFAULT now(),
  PRIMARY KEY (visit_id, visit_date)
) PARTITION BY RANGE (visit_date);

CREATE OR REPLACE PROCEDURE create_cleaned_partition_for_year(y int) LANGUAGE plpgsql AS $$
DECLARE
  part_name text := format('cleaned_visits_y%s', y);
  start_date date := make_date(y,1,1);
  end_date date := make_date(y+1,1,1);
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
    EXECUTE format('CREATE TABLE %I PARTITION OF cleaned_visits FOR VALUES FROM (%L) TO (%L);',
                   part_name, start_date, end_date);
  END IF;
END;
$$;

CALL create_cleaned_partition_for_year(EXTRACT(YEAR FROM now())::int - 1);
CALL create_cleaned_partition_for_year(EXTRACT(YEAR FROM now())::int);
CALL create_cleaned_partition_for_year(EXTRACT(YEAR FROM now())::int + 1);

CREATE INDEX IF NOT EXISTS idx_cleaned_patient_hash ON cleaned_visits(patient_hash);
CREATE INDEX IF NOT EXISTS idx_cleaned_visit_date ON cleaned_visits(visit_date);
CREATE INDEX IF NOT EXISTS idx_cleaned_facility ON cleaned_visits(facility_id);
CREATE INDEX IF NOT EXISTS idx_cleaned_doctor ON cleaned_visits(doctor_id);

-- =========================
-- 9. Aggregates & report tables (patient_summary + claim summary)
-- =========================
CREATE TABLE patient_summary (
  patient_hash text PRIMARY KEY,
  num_visits int,
  first_visit date,
  last_visit date,
  avg_systolic numeric(5,1),
  avg_diastolic numeric(5,1),
  avg_hr numeric(5,1),
  avg_glucose numeric(6,1),
  avg_cholesterol numeric(6,1),
  predominant_diag text,
  last_facility_id bigint REFERENCES facility(facility_id),
  age_at_last_visit numeric(5,1),
  risk_level text,
  last_insurer_id bigint REFERENCES insurer(insurer_id),
  updated_at timestamptz default now()
);

CREATE TABLE claim_summary (
  claim_id bigserial PRIMARY KEY,
  patient_hash text REFERENCES patient(patient_hash),
  insurer_id bigint REFERENCES insurer(insurer_id),
  claim_date date,
  amount numeric(12,2),
  status text,
  created_at timestamptz default now()
);

CREATE TABLE audit_log (
  audit_id bigserial PRIMARY KEY,
  action text NOT NULL,
  object_type text,
  object_id text,
  performed_by text,
  details jsonb,
  performed_at timestamptz default now()
);

CREATE TABLE data_quality_report (
  dq_id bigserial PRIMARY KEY,
  run_at timestamptz default now(),
  total_raw bigint,
  total_cleaned bigint,
  missing_visit_date bigint,
  missing_diagnosis bigint,
  outlier_systolic bigint,
  outlier_diastolic bigint,
  outlier_hr bigint,
  pct_imputed numeric(5,2),
  notes text,
  report jsonb
);

-- =========================
-- 10. Helper functions: parsing, conversions, age, DQ helpers
-- =========================
CREATE OR REPLACE FUNCTION parse_date_multi(txt text) RETURNS date LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE d date;
BEGIN
  IF txt IS NULL OR trim(txt) = '' THEN RETURN NULL; END IF;
  BEGIN d := to_date(txt, 'YYYY-MM-DD'); RETURN d; EXCEPTION WHEN others THEN NULL; END;
  BEGIN d := to_date(txt, 'DD-MM-YYYY'); RETURN d; EXCEPTION WHEN others THEN NULL; END;
  BEGIN d := to_date(txt, 'MM/DD/YYYY'); RETURN d; EXCEPTION WHEN others THEN NULL; END;
  BEGIN d := to_date(txt, 'YYYYMMDD'); RETURN d; EXCEPTION WHEN others THEN NULL; END;
  BEGIN RETURN (txt::timestamp)::date; EXCEPTION WHEN others THEN NULL; END;
END;
$$;

CREATE OR REPLACE FUNCTION glucose_to_mgdL(val numeric, unit text) RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN val IS NULL THEN NULL WHEN unit ILIKE 'mmol%' THEN round(val*18::numeric,1) ELSE round(val::numeric,1) END;
$$;

CREATE OR REPLACE FUNCTION compute_age(dob date, on_date date) RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN dob IS NULL OR on_date IS NULL THEN NULL ELSE round(((on_date - dob)::numeric/365.25)::numeric,1) END;
$$;

-- =========================
-- 11. ETL: process single raw row -> cleaned (idempotent) with doctor+insurer handling
-- =========================
CREATE OR REPLACE FUNCTION process_raw_visit(raw_id_in bigint, performed_by text DEFAULT 'etl_processor') RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  row jsonb;
  pid text;
  p_hash text;
  fac_name text;
  fac_id bigint;
  visit_dt date;
  dob_dt date;
  s_bp numeric;
  d_bp numeric;
  hr numeric;
  g_raw numeric;
  g_unit text;
  chol numeric;
  med_systolic numeric;
  med_diastolic numeric;
  med_hr numeric;
  med_glucose numeric;
  med_chol numeric;
  was_imputed boolean := false;
  outlier boolean := false;
  doc_name text;
  doc_id bigint;
  ins_name text;
  ins_id bigint;
BEGIN
  SELECT raw_payload INTO row FROM raw_visits WHERE raw_id = raw_id_in;
  IF row IS NULL THEN
    RAISE NOTICE 'raw_id % not found', raw_id_in;
    RETURN;
  END IF;

  -- extract fields
  pid := (row->>'patient_id');
  fac_name := coalesce(row->>'facility','unknown');
  visit_dt := parse_date_multi(row->>'visit_date');
  dob_dt := parse_date_multi(row->>'dob');
  s_bp := NULLIF(regexp_replace(coalesce(row->>'systolic_bp',''), '[^0-9.-]', '', 'g'), '')::numeric;
  d_bp := NULLIF(regexp_replace(coalesce(row->>'diastolic_bp',''), '[^0-9.-]', '', 'g'), '')::numeric;
  hr := NULLIF(regexp_replace(coalesce(row->>'heart_rate',''), '[^0-9.-]', '', 'g'), '')::numeric;
  g_raw := NULLIF(regexp_replace(coalesce(row->>'glucose',''), '[^0-9.-]', '', 'g'), '')::numeric;
  g_unit := row->>'glucose_unit';
  chol := NULLIF(regexp_replace(coalesce(row->>'cholesterol_mg_dl',''), '[^0-9.-]', '', 'g'), '')::numeric;

  -- doctor & insurer (from raw payload if present)
  doc_name := nullif(trim(coalesce(row->>'doctor_name', row->>'physician')),'');
  ins_name := nullif(trim(coalesce(row->>'insurer','insurance_provider')),'');

  -- ensure facility exists
  INSERT INTO facility(name) VALUES (fac_name) ON CONFLICT (name) DO NOTHING;
  SELECT facility_id INTO fac_id FROM facility WHERE name = fac_name LIMIT 1;

  -- ensure doctor exists (simple upsert by name+facility)
  IF doc_name IS NOT NULL THEN
    INSERT INTO doctor(full_name, facility_id) VALUES (doc_name, fac_id)
      ON CONFLICT (full_name) DO NOTHING;
    SELECT doctor_id INTO doc_id FROM doctor WHERE full_name = doc_name AND (facility_id = fac_id OR facility_id IS NULL) LIMIT 1;
  ELSE
    doc_id := NULL;
  END IF;

  -- ensure insurer exists
  IF ins_name IS NOT NULL THEN
    INSERT INTO insurer(name) VALUES (ins_name) ON CONFLICT (name) DO NOTHING;
    SELECT insurer_id INTO ins_id FROM insurer WHERE name = ins_name LIMIT 1;
  ELSE
    ins_id := NULL;
  END IF;

  -- compute medians for imputation
  SELECT
    COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY systolic_bp), 120)::numeric,
    COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY diastolic_bp), 80)::numeric,
    COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY heart_rate), 75)::numeric,
    COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY glucose_mg_dl), 100)::numeric,
    COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY cholesterol_mg_dl), 180)::numeric
  INTO med_systolic, med_diastolic, med_hr, med_glucose, med_chol
  FROM cleaned_visits;

  -- unit conversion
  IF g_raw IS NOT NULL THEN
    g_raw := glucose_to_mgdL(g_raw, g_unit);
  END IF;

  -- impute / validate ranges
  IF s_bp IS NULL OR s_bp < 60 OR s_bp > 250 THEN s_bp := med_systolic; was_imputed := true; END IF;
  IF d_bp IS NULL OR d_bp < 30 OR d_bp > 160 THEN d_bp := med_diastolic; was_imputed := true; END IF;
  IF hr IS NULL OR hr < 20 OR hr > 250 THEN hr := med_hr; was_imputed := true; END IF;
  IF g_raw IS NULL OR g_raw < 20 OR g_raw > 2000 THEN g_raw := med_glucose; was_imputed := true; END IF;
  IF chol IS NULL OR chol < 30 OR chol > 2000 THEN chol := med_chol; was_imputed := true; END IF;

  -- simple outlier detection
  IF s_bp > 180 OR hr > 140 OR g_raw > 500 THEN outlier := true; END IF;

  -- create patient hash & record patient & optional PII
  p_hash := make_patient_hash(pid);
  INSERT INTO patient(patient_hash) VALUES (p_hash) ON CONFLICT (patient_hash) DO NOTHING;

  IF pid IS NOT NULL AND pid <> '' THEN
    IF NOT EXISTS (SELECT 1 FROM pii_patients WHERE patient_id_text = pid) THEN
      INSERT INTO pii_patients(patient_id_text, name_encrypted, phone_encrypted, email_encrypted, extra)
      VALUES (
        pid,
        encrypt_pii(coalesce(row->>'name','')),
        encrypt_pii(coalesce(row->>'phone','')),
        encrypt_pii(coalesce(row->>'email','')),
        row - ARRAY['patient_id','name','phone','email']
      );
    END IF;
  END IF;

  -- link patient to insurer if insurer info present and policy_number present
  IF ins_id IS NOT NULL AND row->>'policy_number' IS NOT NULL THEN
    INSERT INTO patient_insurance(patient_hash, insurer_id, policy_number, start_date, end_date, active)
    VALUES (p_hash, ins_id, row->>'policy_number', parse_date_multi(row->>'policy_start'), parse_date_multi(row->>'policy_end'), true)
    ON CONFLICT (patient_hash, insurer_id) DO UPDATE SET policy_number = EXCLUDED.policy_number, start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date, active = EXCLUDED.active;
  END IF;

  -- idempotent insert/update into cleaned_visits
  PERFORM 1 FROM cleaned_visits WHERE patient_hash = p_hash AND visit_date = visit_dt AND facility_id = fac_id LIMIT 1;
  IF FOUND THEN
    UPDATE cleaned_visits
    SET
      dob = COALESCE(dob, dob_dt),
      age_at_visit = COALESCE(age_at_visit, compute_age(dob_dt, visit_dt)),
      gender = COALESCE(gender, row->>'gender'),
      systolic_bp = COALESCE(systolic_bp, s_bp),
      diastolic_bp = COALESCE(diastolic_bp, d_bp),
      heart_rate = COALESCE(heart_rate, hr),
      glucose_mg_dl = COALESCE(glucose_mg_dl, g_raw),
      cholesterol_mg_dl = COALESCE(cholesterol_mg_dl, chol),
      diagnosis_code = COALESCE(diagnosis_code, row->>'diagnosis_code'),
      diagnosis_type = COALESCE(diagnosis_type, CASE WHEN left(coalesce(row->>'diagnosis_code',''),1) IN ('I','E','J') THEN 'chronic' ELSE 'other' END),
      was_imputed = was_imputed OR was_imputed,
      outlier_flag = outlier_flag OR outlier,
      source_raw_id = COALESCE(source_raw_id, raw_id_in),
      doctor_id = COALESCE(doctor_id, doc_id),
      insurer_id = COALESCE(insurer_id, ins_id),
      created_at = now()
    WHERE patient_hash = p_hash AND visit_date = visit_dt AND facility_id = fac_id;
  ELSE
    INSERT INTO cleaned_visits(
      patient_hash, facility_id, doctor_id, insurer_id, visit_date, dob, age_at_visit, gender,
      systolic_bp, diastolic_bp, heart_rate, glucose_mg_dl, cholesterol_mg_dl,
      diagnosis_code, diagnosis_type, source_raw_id, was_imputed, outlier_flag, created_at
    ) VALUES (
      p_hash, fac_id, doc_id, ins_id, visit_dt, dob_dt, compute_age(dob_dt, visit_dt), row->>'gender',
      s_bp, d_bp, hr, g_raw, chol,
      row->>'diagnosis_code',
      CASE WHEN left(coalesce(row->>'diagnosis_code',''),1) IN ('I','E','J') THEN 'chronic' ELSE 'other' END,
      raw_id_in, was_imputed, outlier, now()
    );
  END IF;

  -- audit
  INSERT INTO audit_log(action, object_type, object_id, performed_by, details)
  VALUES ('process_raw_visit', 'raw_visits', raw_id_in::text, performed_by, jsonb_build_object('patient_id', pid, 'p_hash', p_hash));

END;
$$;

-- batch ETL
CREATE OR REPLACE FUNCTION process_all_raw(performed_by text DEFAULT 'etl_batch') RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  rid bigint;
  processed int := 0;
BEGIN
  FOR rid IN SELECT raw_id FROM raw_visits
             WHERE raw_id NOT IN (SELECT COALESCE(source_raw_id,0) FROM cleaned_visits)
  LOOP
    PERFORM process_raw_visit(rid, performed_by);
    processed := processed + 1;
  END LOOP;
  RETURN processed;
END;
$$;

-- =========================
-- 12. Competitor ETL & tables (process competitor raw into competitor_stats)
-- =========================
CREATE TABLE competitor_stats (
  comp_stat_id bigserial PRIMARY KEY,
  company_name text NOT NULL,
  metric_date date NOT NULL,
  avg_booking_time_minutes numeric(10,2),
  booking_volume int,
  avg_cancellation_rate numeric(5,2),
  source text,
  created_at timestamptz default now()
);

CREATE OR REPLACE FUNCTION insert_competitor_stat(comp jsonb) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  cname text := comp->>'company_name';
  mdate date := parse_date_multi(comp->>'metric_date');
  avg_bt numeric := NULLIF(comp->>'avg_booking_time_minutes','')::numeric;
  vol int := NULLIF(comp->>'booking_volume','')::int;
  cancel numeric := NULLIF(comp->>'avg_cancellation_rate','')::numeric;
  src text := comp->>'source';
BEGIN
  INSERT INTO competitor_stats(company_name, metric_date, avg_booking_time_minutes, booking_volume, avg_cancellation_rate, source)
  VALUES (cname, mdate, avg_bt, vol, cancel, src);
END;
$$;

CREATE OR REPLACE FUNCTION process_competitor_raw(raw_id_in bigint) RETURNS void LANGUAGE plpgsql AS $$
DECLARE r jsonb;
BEGIN
  SELECT raw_payload INTO r FROM competitor_stats_raw WHERE raw_id = raw_id_in;
  IF r IS NULL THEN RAISE NOTICE 'raw not found %', raw_id_in; RETURN; END IF;
  PERFORM insert_competitor_stat(r);
  INSERT INTO audit_log(action, object_type, object_id, performed_by, details)
    VALUES('process_competitor_raw','competitor_stats', raw_id_in::text, 'etl_competitor', jsonb_build_object('raw_id', raw_id_in));
END;
$$;

-- =========================
-- 13. Refresh patient_summary (aggregation) include insurance & last doctor
-- =========================
CREATE OR REPLACE FUNCTION refresh_patient_summary() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  TRUNCATE TABLE patient_summary;
  INSERT INTO patient_summary(
    patient_hash, num_visits, first_visit, last_visit,
    avg_systolic, avg_diastolic, avg_hr, avg_glucose, avg_cholesterol,
    predominant_diag, last_facility_id, age_at_last_visit, risk_level, last_insurer_id, updated_at
  )
  SELECT
    cv.patient_hash,
    COUNT(*) AS num_visits,
    MIN(cv.visit_date) AS first_visit,
    MAX(cv.visit_date) AS last_visit,
    round(AVG(cv.systolic_bp)::numeric,1) AS avg_systolic,
    round(AVG(cv.diastolic_bp)::numeric,1) AS avg_diastolic,
    round(AVG(cv.heart_rate)::numeric,1) AS avg_hr,
    round(AVG(cv.glucose_mg_dl)::numeric,1) AS avg_glucose,
    round(AVG(cv.cholesterol_mg_dl)::numeric,1) AS avg_cholesterol,
    (array_agg(cv.diagnosis_type ORDER BY cv.visit_date))[1] AS predominant_diag,
    (array_agg(cv.facility_id ORDER BY cv.visit_date DESC))[1] AS last_facility_id,
    MAX(cv.age_at_visit) AS age_at_last_visit,
    CASE
      WHEN (AVG(cv.systolic_bp) >= 140 OR AVG(cv.diastolic_bp) >= 90) AND AVG(cv.glucose_mg_dl) >= 180 THEN 'high'
      WHEN (AVG(cv.systolic_bp) >= 140 OR AVG(cv.diastolic_bp) >= 90) OR AVG(cv.glucose_mg_dl) >= 180 OR AVG(cv.cholesterol_mg_dl) >= 240 THEN 'moderate'
      ELSE 'low'
    END AS risk_level,
    (array_agg(cv.insurer_id ORDER BY cv.visit_date DESC))[1] AS last_insurer_id,
    now()
  FROM cleaned_visits cv
  GROUP BY cv.patient_hash;
END;
$$;

-- =========================
-- 14. Data Quality checks (run & store report)
-- =========================
CREATE OR REPLACE FUNCTION run_data_quality_checks(run_notes text DEFAULT 'automated') RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
  total_raw bigint;
  total_cleaned bigint;
  missing_visit_date bigint;
  missing_diagnosis bigint;
  outlier_systolic bigint;
  outlier_diastolic bigint;
  outlier_hr bigint;
  pct_imputed numeric;
  rep jsonb;
  dqid bigint;
BEGIN
  SELECT COUNT(*) INTO total_raw FROM raw_visits;
  SELECT COUNT(*) INTO total_cleaned FROM cleaned_visits;
  SELECT COUNT(*) INTO missing_visit_date FROM cleaned_visits WHERE visit_date IS NULL;
  SELECT COUNT(*) INTO missing_diagnosis FROM cleaned_visits WHERE diagnosis_code IS NULL OR diagnosis_code = '';
  SELECT COUNT(*) INTO outlier_systolic FROM cleaned_visits WHERE systolic_bp < 60 OR systolic_bp > 250;
  SELECT COUNT(*) INTO outlier_diastolic FROM cleaned_visits WHERE diastolic_bp < 30 OR diastolic_bp > 160;
  SELECT COUNT(*) INTO outlier_hr FROM cleaned_visits WHERE heart_rate < 20 OR heart_rate > 250;
  SELECT round(100.0 * sum(CASE WHEN was_imputed THEN 1 ELSE 0 END)::numeric / NULLIF(count(*),0),2) INTO pct_imputed FROM cleaned_visits;

  rep := jsonb_build_object(
    'total_raw', total_raw,
    'total_cleaned', total_cleaned,
    'missing_visit_date', missing_visit_date,
    'missing_diagnosis', missing_diagnosis,
    'outliers', jsonb_build_object('systolic', outlier_systolic, 'diastolic', outlier_diastolic, 'hr', outlier_hr),
    'pct_imputed', pct_imputed
  );

  INSERT INTO data_quality_report(total_raw, total_cleaned, missing_visit_date, missing_diagnosis,
    outlier_systolic, outlier_diastolic, outlier_hr, pct_imputed, notes, report)
  VALUES (total_raw, total_cleaned, missing_visit_date, missing_diagnosis,
    outlier_systolic, outlier_diastolic, outlier_hr, pct_imputed, run_notes, rep)
  RETURNING dq_id INTO dqid;

  INSERT INTO audit_log(action, object_type, object_id, performed_by, details)
    VALUES ('run_data_quality', 'data_quality_report', dqid::text, 'dq_runner', rep);

  RETURN dqid;
END;
$$;

-- =========================
-- 15. Views & Materialized Views for Dashboards (de-identified)
-- =========================
-- raw vs cleaned systolic avg
CREATE OR REPLACE VIEW vw_raw_vs_clean_systolic AS
SELECT 'raw'::text AS source, AVG( (regexp_replace(raw_payload->>'systolic_bp','[^0-9.-]','', 'g'))::numeric ) AS avg_systolic
FROM raw_visits
WHERE NULLIF(regexp_replace(raw_payload->>'systolic_bp','[^0-9.-]','', 'g'),'') IS NOT NULL
UNION ALL
SELECT 'cleaned'::text, AVG(systolic_bp) FROM cleaned_visits;

-- monthly avg glucose
CREATE MATERIALIZED VIEW mv_monthly_glucose AS
SELECT date_trunc('month', visit_date)::date AS month, round(avg(glucose_mg_dl)::numeric,1) AS avg_glucose
FROM cleaned_visits
GROUP BY date_trunc('month', visit_date)
ORDER BY month;

-- patients by risk
CREATE OR REPLACE VIEW vw_risk_counts AS
SELECT risk_level, count(*) AS cnt FROM patient_summary GROUP BY risk_level ORDER BY cnt DESC;

-- de-identified cleaned view for researchers
CREATE OR REPLACE VIEW vw_cleaned_visits_research AS
SELECT visit_id, patient_hash, facility_id, doctor_id, visit_date, age_at_visit, gender,
  systolic_bp, diastolic_bp, heart_rate, glucose_mg_dl, cholesterol_mg_dl, diagnosis_type, outlier_flag
FROM cleaned_visits;

-- disease counts (Diabetes E10-E14, Hypertension I10-I15 by ICD-10 start letter matching)
CREATE OR REPLACE VIEW vw_disease_counts AS
SELECT
  SUM(CASE WHEN diagnosis_code ILIKE 'E1%' OR diagnosis_code ILIKE 'E%' AND diagnosis_code IS NOT NULL THEN 1 ELSE 0 END) AS diabetes_count,
  SUM(CASE WHEN diagnosis_code ILIKE 'I1%' OR diagnosis_code ILIKE 'I%' AND diagnosis_code IS NOT NULL THEN 1 ELSE 0 END) AS hypertension_count,
  date_trunc('month', visit_date)::date AS month
FROM cleaned_visits
GROUP BY date_trunc('month', visit_date)
ORDER BY month DESC;

-- doctors most visited
CREATE OR REPLACE VIEW vw_doctor_visit_counts AS
SELECT d.doctor_id, d.full_name, d.specialty, count(cv.visit_id) AS visits
FROM cleaned_visits cv
LEFT JOIN doctor d ON d.doctor_id = cv.doctor_id
GROUP BY d.doctor_id, d.full_name, d.specialty
ORDER BY visits DESC;

-- materialized competitor booking per month
CREATE OR REPLACE VIEW vw_monthly_competitor_booking AS
SELECT date_trunc('month', metric_date)::date AS month,
       company_name,
       round(avg(avg_booking_time_minutes)::numeric,2) AS avg_booking_time_minutes,
       sum(booking_volume) AS total_bookings
FROM competitor_stats
GROUP BY 1,2
ORDER BY 1 DESC, 2;

CREATE MATERIALIZED VIEW mv_monthly_competitor_booking AS
SELECT * FROM vw_monthly_competitor_booking;

-- diabetes & hypertension totals (current snapshot)
CREATE OR REPLACE VIEW vw_disease_totals_snapshot AS
SELECT
  (SELECT COUNT(*) FROM cleaned_visits WHERE diagnosis_code ILIKE 'E%') AS total_diabetes_visits,
  (SELECT COUNT(*) FROM cleaned_visits WHERE diagnosis_code ILIKE 'I%') AS total_hypertension_visits,
  now() as snapshot_at;

-- insurer summary: number of patients per insurer
CREATE OR REPLACE VIEW vw_insurer_patient_counts AS
SELECT i.insurer_id, i.name, COUNT(DISTINCT ci.patient_hash) AS patient_count
FROM insurer i
LEFT JOIN cleaned_visits cv ON cv.insurer_id = i.insurer_id
LEFT JOIN patient_insurance ci ON ci.insurer_id = i.insurer_id
GROUP BY i.insurer_id, i.name
ORDER BY patient_count DESC;

-- =========================
-- 16. Row Level Security (RLS) & Policies
-- =========================
ALTER TABLE pii_patients ENABLE ROW LEVEL SECURITY;
CREATE POLICY pii_admin_policy ON pii_patients
  USING ( current_setting('app.current_user_role', true) = 'admin' )
  WITH CHECK ( current_setting('app.current_user_role', true) = 'admin' );

ALTER TABLE cleaned_visits ENABLE ROW LEVEL SECURITY;
CREATE POLICY clinician_select_policy ON cleaned_visits
  FOR SELECT USING (
    current_setting('app.current_user_role', true) = 'admin'
    OR (
      current_setting('app.current_user_role', true) = 'clinician'
      AND EXISTS (
        SELECT 1 FROM clinician_facility cf
         JOIN user_account ua ON ua.user_id = cf.user_id
         WHERE ua.username = current_user AND cf.facility_id = cleaned_visits.facility_id
      )
    )
  );

CREATE POLICY etl_rw_policy ON cleaned_visits
  FOR ALL USING ( current_setting('app.current_user_role', true) = 'admin' OR current_setting('app.current_user_role', true) = 'etl' )
  WITH CHECK ( current_setting('app.current_user_role', true) = 'admin' OR current_setting('app.current_user_role', true) = 'etl' );

CREATE POLICY admin_full_policy ON cleaned_visits
  FOR ALL USING ( current_setting('app.current_user_role', true) = 'admin' )
  WITH CHECK ( current_setting('app.current_user_role', true) = 'admin' );

REVOKE ALL ON vw_cleaned_visits_research FROM PUBLIC;
GRANT SELECT ON vw_cleaned_visits_research TO healthcare_researcher;
GRANT SELECT ON mv_monthly_glucose TO healthcare_researcher;
GRANT SELECT ON vw_risk_counts TO healthcare_researcher;
GRANT SELECT ON patient_summary TO healthcare_researcher;

-- =========================
-- 17. Grants (basic)
-- =========================
GRANT ALL ON SCHEMA healthcare TO healthcare_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA healthcare TO healthcare_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA healthcare TO healthcare_admin;

GRANT SELECT, INSERT ON raw_visits TO healthcare_etl;
GRANT SELECT, INSERT, UPDATE ON cleaned_visits TO healthcare_etl;
GRANT EXECUTE ON FUNCTION process_raw_visit(bigint, text) TO healthcare_etl;
GRANT EXECUTE ON FUNCTION process_all_raw(text) TO healthcare_etl;

GRANT SELECT ON vw_cleaned_visits_research TO healthcare_clinician;
GRANT SELECT ON vw_doctor_visit_counts TO healthcare_clinician;

GRANT SELECT ON mv_monthly_competitor_booking TO healthcare_researcher;
GRANT SELECT ON vw_monthly_competitor_booking TO healthcare_researcher;
GRANT SELECT ON vw_insurer_patient_counts TO healthcare_researcher;

GRANT SELECT ON patient_summary, data_quality_report TO healthcare_viewer;

-- =========================
-- 18. Maintenance helpers & scheduled tasks hints
-- =========================
CREATE OR REPLACE FUNCTION refresh_mv_monthly_glucose() RETURNS void LANGUAGE sql AS $$
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_glucose;
$$;

CREATE OR REPLACE FUNCTION refresh_mv_monthly_competitor_booking() RETURNS void LANGUAGE sql AS $$
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_competitor_booking;
$$;

-- Suggested scheduler tasks (Airflow/cron):
-- 1) CALL process_all_raw('etl_scheduler') hourly/daily
-- 2) SELECT refresh_patient_summary()
-- 3) PERFORM refresh_mv_monthly_glucose(); PERFORM refresh_mv_monthly_competitor_booking()
-- 4) SELECT run_data_quality_checks('scheduled run')

-- =========================
-- 19. Example sample inserts (demo) — remove in prod
-- =========================
INSERT INTO raw_visits(source_file, row_num, raw_payload) VALUES
  ('sample.csv', 1, '{"patient_id":"1001","name":"Ahmed Ali","dob":"1975-03-12","gender":"M","phone":"+201000000001","email":"a.ali@example.com","facility":"Central Clinic","doctor_name":"Dr. Hany","visit_date":"2025-09-10","systolic_bp":"130","diastolic_bp":"80","heart_rate":"72","glucose":"5.6","glucose_unit":"mmol/L","cholesterol_mg_dl":"190","diagnosis_code":"I10","insurer":"Alpha Insurance","policy_number":"POL123","notes":"note1"}'::jsonb),
  ('sample.csv', 2, '{"patient_id":"1002","name":"Mona S","dob":"1982-07-01","gender":"F","phone":"+201000000002","email":"m.s@example.com","facility":"Downtown Clinic","doctor_name":"Dr. Salma","visit_date":"2025-09-11","systolic_bp":"145","diastolic_bp":"95","heart_rate":"88","glucose":"200","glucose_unit":"mg/dL","cholesterol_mg_dl":"260","diagnosis_code":"E11","insurer":"Beta Insurance","policy_number":"POL999","notes":"note2"}'::jsonb);

INSERT INTO competitor_stats_raw(source_file, row_num, raw_payload) VALUES
  ('competitors.csv',1, '{"company_name":"CompetitorA","metric_date":"2025-09-01","avg_booking_time_minutes":"15.5","booking_volume":"1200","avg_cancellation_rate":"2.5","source":"scrape"}'::jsonb),
  ('competitors.csv',2, '{"company_name":"CompetitorB","metric_date":"2025-09-01","avg_booking_time_minutes":"10.2","booking_volume":"900","avg_cancellation_rate":"3.1","source":"api"}'::jsonb);

-- (Optionally) run ETL in safe env:
-- SELECT process_competitor_raw(1);
-- SELECT process_competitor_raw(2);
-- SELECT process_all_raw('manual_run');
-- SELECT refresh_patient_summary();
-- SELECT run_data_quality_checks('initial run');

-- =========================
-- 20. Useful monitoring queries
-- =========================
-- ETL lag:
-- SELECT count(*) FROM raw_visits rv WHERE NOT EXISTS (SELECT 1 FROM cleaned_visits cv WHERE cv.source_raw_id = rv.raw_id);

-- Latest DQ report:
-- SELECT * FROM data_quality_report ORDER BY run_at DESC LIMIT 1;

-- Patients by risk:
-- SELECT * FROM vw_risk_counts;

-- Doctors top:
-- SELECT * FROM vw_doctor_visit_counts LIMIT 10;

-- Competitor booking:
-- SELECT * FROM mv_monthly_competitor_booking ORDER BY month DESC LIMIT 12;

-- =========================
-- 21. Final notes & recommendations (production checklist)
-- =========================
/*
1) Move secrets to a secrets manager (Vault/KMS). Remove pgp key & salt from config table, or keep placeholders only.
2) Application should SET session-level context: SET app.current_user_role = 'clinician'; SET app.current_user_id = '...';
3) Add monitoring & alerts for ETL failures and DQ thresholds (Prometheus + Grafana or cloud monitoring).
4) Use WAL archiving & regular backups. Test restore.
5) Harden DB network access, enforce TLS, rotate credentials.
6) Add unit & integration tests for ETL functions (pytest + test DB).
7) Consider application-layer envelope encryption for extremely sensitive PII and KMS-wrapped keys.
8) Document retention policies & consent tracking.
*/
