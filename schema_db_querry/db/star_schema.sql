-- Active: 1767084789057@@127.0.0.1@3306@healthcare_db
-- Star Schema for Healthcare Analytics
-- Based on design_decisions.txt

-- 1. Dimension Tables

-- Dimension: Date
-- Purpose: Supports temporal analysis (daily, monthly, yearly) and performance filtering.
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    year INT,
    month INT,
    month_name VARCHAR(10),
    quarter INT,
    day_of_week VARCHAR(10),
    is_weekend BOOLEAN
);

-- Dimension: Specialty
-- Purpose: Lookup table for medical specialties to categorize providers.
CREATE TABLE dim_specialty (
    specialty_key INT AUTO_INCREMENT PRIMARY KEY,
    specialty_id INT, -- Source System ID
    specialty_name VARCHAR(100)
);

-- Dimension: Department
-- Purpose: Lookup table for hospital departments and physical locations.
CREATE TABLE dim_department (
    department_key INT AUTO_INCREMENT PRIMARY KEY,
    department_id INT, -- Source System ID
    department_name VARCHAR(100),
    floor INT
);

-- Dimension: Encounter Type
-- Purpose: Categorizes the setting of care (e.g., Inpatient, Outpatient, ER).
CREATE TABLE dim_encounter_type (
    encounter_type_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_type_name VARCHAR(50), -- e.g., 'Inpatient', 'Outpatient'
    encounter_class VARCHAR(50)      -- Grouping category if needed
);

-- Dimension: Patient
-- Purpose: Stores patient demographics and slowly changing attributes like age group.
CREATE TABLE dim_patient (
    patient_key INT AUTO_INCREMENT PRIMARY KEY,
    patient_id INT, -- Source System ID
    full_name VARCHAR(100),
    gender CHAR(1),
    date_of_birth DATE,
    age_group VARCHAR(10), -- Pre-calculated: '0-18', '19-65', '65+'
    current_flag BOOLEAN DEFAULT TRUE -- For Type 2 SCD
);

-- Dimension: Provider
-- Purpose: Stores healthcare provider details.
CREATE TABLE dim_provider (
    provider_key INT AUTO_INCREMENT PRIMARY KEY,
    provider_id INT, -- Source System ID
    provider_name VARCHAR(200),
    credential VARCHAR(20),
    specialty_name VARCHAR(100),
    department_name VARCHAR(100)
);

-- Dimension: Diagnosis
-- Purpose: Reference table for ICD-10 diagnosis codes and descriptions.
CREATE TABLE dim_diagnosis (
    diagnosis_key INT AUTO_INCREMENT PRIMARY KEY,
    diagnosis_id INT, -- Source System ID
    icd_code VARCHAR(10),
    diagnosis_description VARCHAR(200),
    category VARCHAR(50) -- Derived from ICD(International Classification of Diseases) code structure
);

-- Dimension: Procedure
-- Purpose: Reference table for CPT procedure codes and descriptions.
CREATE TABLE dim_procedure (
    procedure_key INT AUTO_INCREMENT PRIMARY KEY,
    procedure_id INT, -- Source System ID
    cpt_code VARCHAR(10),
    procedure_description VARCHAR(200),
    category VARCHAR(50)
);

-- 2. Fact Table

-- Fact Table: Encounters
-- Purpose: Central transactional table storing metrics (revenue, duration) at the encounter grain.
CREATE TABLE fact_encounters (
    encounter_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_id INT, -- Source System ID (useful for audit)
        -- Foreign Keys to Dimensions
    admission_date_key INT,
    discharge_date_key INT,
    patient_key INT,
    provider_key INT,
    department_key INT,
    encounter_type_key INT,
    /* Pre-aggregated Metrics */
    length_of_stay INT,              -- Days between admission and discharge date
    total_claim_amount DECIMAL(12, 2),
    total_allowed_amount DECIMAL(12, 2),
    diagnosis_count INT,             -- Count of associated diagnoses
    procedure_count INT,             -- Count of associated procedures
    /* Constraints */
    FOREIGN KEY (admission_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (discharge_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    /* Indexes  for Performance */
    INDEX idx_fact_admission_date (admission_date_key),
    INDEX idx_fact_discharge_date (discharge_date_key),
    INDEX idx_fact_provider (provider_key),
    INDEX idx_fact_patient (patient_key)
);
/*
3. Bridge Tables (Many-to-Many Resolution)

Bridge Table: Encounter Diagnoses
Purpose: Resolves Many-to-Many relationship between Encounters and Diagnoses.
 Tracks primary vs secondary.*/
CREATE TABLE bridge_encounter_diagnoses (
    bridge_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_key INT,
    diagnosis_key INT,
    diagnosis_sequence INT, -- 1 for Primary, 2 for Secondary, etc.
    
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
     -- Composite index for query performance
    INDEX idx_bridge_diag_enc (diagnosis_key, encounter_key)
);

-- Bridge Table: Encounter Procedures
-- Purpose: Resolves Many-to-Many relationship between Encounters and Procedures.
CREATE TABLE bridge_encounter_procedures (
    bridge_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_key INT,
    procedure_key INT,
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key),
    -- Composite index for query performance
    INDEX idx_bridge_proc_enc (procedure_key, encounter_key)
);

-- 2. Populate data Dimension and Fact Tables
DROP PROCEDURE IF EXISTS GenerateDimDate;
DELIMITER //
CREATE PROCEDURE GenerateDimDate()
BEGIN
    DECLARE currentDate DATE DEFAULT '2020-01-01';
    DECLARE endDate DATE DEFAULT '2040-12-31';
    DECLARE d_key INT;
    
    WHILE currentDate <= endDate DO
        SET d_key = YEAR(currentDate) * 10000 + MONTH(currentDate) * 100 + DAY(currentDate);
        
        INSERT INTO dim_date (date_key, full_date, year, month, month_name, quarter, day_of_week, is_weekend)
        VALUES (d_key, currentDate, YEAR(currentDate), 
        MONTH(currentDate), MONTHNAME(currentDate), 
        QUARTER(currentDate), DAYOFWEEK(currentDate), 
            CASE WHEN DAYOFWEEK(currentDate) IN (6,7) THEN TRUE ELSE FALSE END);
        
        SET currentDate = DATE_ADD(currentDate, INTERVAL 1 DAY);
    END WHILE;
END //
DELIMITER ;

-- 2.1 Populate Date Dimension
CALL GenerateDimDate();
-- 2.2 Populate Dimensions from Source Tables
-- populate dim_specialty from specialties source table
INSERT INTO dim_specialty (specialty_id, specialty_name)
SELECT DISTINCT specialty_id, specialty_name FROM specialties;

-- populate dim_department from departments source table

INSERT INTO dim_department (department_id, department_name, floor)
SELECT DISTINCT department_id, department_name, floor FROM departments;
-- populate dim_patient from patients source table

INSERT INTO dim_patient (patient_id, full_name, gender, date_of_birth, age_group)
SELECT patient_id, CONCAT(first_name, ' ', last_name), gender, date_of_birth,
    CASE 
      WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 5 THEN '0-4'
      WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 15 THEN '5-14'
      WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 45 THEN '15-44'
      WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 65 THEN '45-64'
      WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 75 THEN '65-74'
      ELSE '75+'
    END
FROM patients;

-- populate dim_provider from providers, dim_specialty, and dim_department source tables

INSERT INTO dim_provider (provider_id, provider_name, credential, specialty_name, department_name)
SELECT p.provider_id, CONCAT(p.first_name, ' ', p.last_name), NULL, s.specialty_name, d.department_name
FROM providers p
LEFT JOIN dim_specialty s ON p.specialty_id = s.specialty_id
LEFT JOIN dim_department d ON p.department_id = d.department_id;
-- populate dim_diagnosis from diagnoses source table

INSERT INTO dim_diagnosis (diagnosis_id, icd_code, diagnosis_description, category)
SELECT DISTINCT diagnosis_id, icd10_code, icd10_description, NULL FROM diagnoses;
-- populate dim_procedure from procedures source table

INSERT INTO dim_procedure (procedure_id, cpt_code, procedure_description, category)
SELECT DISTINCT procedure_id, cpt_code, cpt_description, NULL FROM procedures;

-- populate dim_encounter_type from encounters source table
INSERT INTO dim_encounter_type (encounter_type_name, encounter_class)
SELECT DISTINCT encounter_type, 'General' FROM encounters;

-- 2.3 Populate Fact Table
-- populate fact_encounters from encounters, billing, encounter_diagnoses, and encounter_procedures source tables

INSERT INTO fact_encounters (encounter_id, patient_key, provider_key, 
                             department_key, admission_date_key, discharge_date_key,
                              encounter_type_key, length_of_stay, total_claim_amount, 
                              total_allowed_amount, diagnosis_count, procedure_count)
SELECT e.encounter_id, dp.patient_key, dpr.provider_key, dd.department_key,
    YEAR(e.encounter_date) * 10000 + MONTH(e.encounter_date) * 100 + DAY(e.encounter_date),
    CASE WHEN e.discharge_date IS NOT NULL THEN YEAR(e.discharge_date) * 10000 + MONTH(e.discharge_date) * 100 + DAY(e.discharge_date) ELSE NULL END,
    det.encounter_type_key, DATEDIFF(e.discharge_date, e.encounter_date),
    COALESCE(b.total_claim, 0), COALESCE(b.total_allowed, 0), COALESCE(ed.diag_count, 0), COALESCE(ep.proc_count, 0)
FROM encounters e
JOIN dim_patient dp ON e.patient_id = dp.patient_id
JOIN dim_provider dpr ON e.provider_id = dpr.provider_id
LEFT JOIN dim_department dd ON e.department_id = dd.department_id
JOIN dim_encounter_type det ON e.encounter_type = det.encounter_type_name
LEFT JOIN (SELECT encounter_id, SUM(claim_amount) as total_claim, SUM(allowed_amount) as total_allowed FROM billing GROUP BY encounter_id) b ON e.encounter_id = b.encounter_id
LEFT JOIN (SELECT encounter_id, COUNT(*) as diag_count FROM encounter_diagnoses GROUP BY encounter_id) ed ON e.encounter_id = ed.encounter_id
LEFT JOIN (SELECT encounter_id, COUNT(*) as proc_count FROM encounter_procedures GROUP BY encounter_id) ep ON e.encounter_id = ep.encounter_id;

-- 2.4 Populate Bridge Tables

INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence)
SELECT fe.encounter_key, dd.diagnosis_key, ed.diagnosis_sequence FROM encounter_diagnoses ed 
JOIN fact_encounters fe ON ed.encounter_id = fe.encounter_id JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id;

INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key)
SELECT fe.encounter_key, dp.procedure_key FROM encounter_procedures ep JOIN fact_encounters fe ON ep.encounter_id = fe.encounter_id JOIN dim_procedure dp ON ep.procedure_id = dp.procedure_id;