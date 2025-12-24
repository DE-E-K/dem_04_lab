-- Transform healthcare OLTP to Star schema
DROP DATABASE IF EXISTS healthcare_db;
CREATE DATABASE IF NOT EXISTS healthcare_db;
USE healthcare_db;
CREATE TABLE patients (
  patient_id INT PRIMARY KEY,
  first_name VARCHAR (100),
  last_name VARCHAR (100),
  date_of_birth DATE,
  gender CHAR(1),
  mrn VARCHAR (20) UNIQUE
);

CREATE TABLE specialties (
  specialty_id INT PRIMARY KEY,
  specialty_name VARCHAR(100),
  specialty_code VARCHAR (10)
);

CREATE TABLE departments (
  department_id INT PRIMARY KEY,
  department_name VARCHAR(100),
  floor INT,
  capacity INT
);

CREATE TABLE providers (
  provider_id INT PRIMARY KEY,
  first_name VARCHAR (100),
  last_name VARCHAR (100),
  credential VARCHAR(20),
  specialty_id INT,
  department_id INT,
  FOREIGN KEY (specialty_id) REFERENCES specialties (specialty_id),
  FOREIGN KEY (department_id) REFERENCES departments (department_id)
);

CREATE TABLE encounters (
  encounter_id INT PRIMARY KEY,
  patient_id INT,
  provider_id INT,
  encounter_type VARCHAR (50), -- 'Outpatient', 'Inpatient', 'ER'
  encounter_date DATETIME,
  discharge_date DATETIME,
  department_id INT,
  FOREIGN KEY (patient_id) REFERENCES patients (patient_id),
  FOREIGN KEY (provider_id) REFERENCES providers (provider_id),
  FOREIGN KEY (department_id) REFERENCES departments (department_id),
  INDEX idx_encounter_date (encounter_date)
);

CREATE TABLE diagnoses (
  diagnosis_id INT PRIMARY KEY,
  icd10_code VARCHAR(10),
  icd10_description VARCHAR(200)
);

CREATE TABLE encounter_diagnoses (
  encounter_diagnosis_id INT PRIMARY KEY,
  encounter_id INT,
  diagnosis_id INT,
  diagnosis_sequence INT,
  FOREIGN KEY (encounter_id) REFERENCES encounters (encounter_id),
  FOREIGN KEY (diagnosis_id) REFERENCES diagnoses (diagnosis_id)
);

CREATE TABLE procedures (
  procedure_id INT PRIMARY KEY,
  cpt_code VARCHAR (10),
  cpt_description VARCHAR (200)
);

CREATE TABLE encounter_procedures (
  encounter_procedure_id INT PRIMARY KEY,
  encounter_id INT,
  procedure_id INT,
  procedure_date DATE,
  FOREIGN KEY (encounter_id) REFERENCES encounters (encounter_id),
  FOREIGN KEY (procedure_id) REFERENCES procedures (procedure_id)
);

CREATE TABLE billing (
  billing_id INT PRIMARY KEY,
  encounter_id INT,
  claim_amount DECIMAL (12, 2),
  allowed_amount DECIMAL (12, 2),
  claim_date DATE,
  claim_status VARCHAR (50),
  FOREIGN KEY (encounter_id) REFERENCES encounters (encounter_id),
  INDEX idx_claim_date (claim_date)
);

INSERT INTO specialties VALUES 
(1, 'Cardiology', 'CARD'), 
(2, 'Internal Medicine', 'IM'), 
(3, 'Emergency', 'ER');

INSERT INTO departments VALUES 
(1, 'Cardiology Unit', 3, 20), 
(2, 'Internal Medicine', 2, 30), 
(3, 'Emergency', 1, 45);

INSERT INTO providers VALUES 
(101, 'James', 'Chen', 'MD', 1, 1), 
(102, 'Sarah', 'Williams', 'MD', 2, 2), 
(103, 'Michael', 'Rodriguez', 'MD', 3, 3);

INSERT INTO patients VALUES 
(1001, 'John', 'Doe','1955-03-15', 'M', 'MRN001'),
(1002, 'Jane', 'Smith', '1962-07-22', 'F', 'MRN002'), 
(1003, 'Robert', 'Johnson', '1948-11-08', 'M', 'MRN003');

INSERT INTO diagnoses VALUES 
(3001, 'I10', 'Hypertension'), 
(3002, 'E11.9', 'Type 2 Diabetes'), 
(3003, 'I50.9', 'Heart Failure');

INSERT INTO procedures VALUES 
(4001, '99213', 'Office Visit'), 
(4002, '93000', 'EKG'), 
(4003, '71020', 'Chest X-ray');

INSERT INTO billing VALUES 
(14001, 7001, 350, 280, '2024-05-11', 'Paid'), 
(14002, 7002, 12500, 10000, '2024-06-08', 'Paid');
INSERT INTO encounters VALUES
(7001, 1001, 101, 'Outpatient', '2024-05-10 10:00:00', '2024-05-10 11:30:00', 1),
(7002, 1001, 101, 'Inpatient', '2024-06-02 14:00:00', '2024-06-06 09:00:00', 1),
(7003, 1002, 102, 'Outpatient', '2024-05-15 09:00:00', '2024-05-15 10:15:00', 2),
(7004, 1003, 103, 'ER', '2024-06-12 23:45:00', '2024-06-13 06:30:00', 3);

INSERT INTO encounter_diagnoses VALUES
(8001, 7001, 3001, 1),
(8002, 7001, 3002, 2),
(8003, 7002, 3001, 1),
(8004, 7002, 3003, 2),
(8005, 7003, 3002, 1),
(8006, 7004, 3001, 1);

INSERT INTO encounter_procedures VALUES
(9001, 7001, 4001, '2024-05-10'),
(9002, 7001, 4002, '2024-05-10'),
(9003, 7002, 4001, '2024-06-02'),
(9004, 7003, 4001, '2024-05-15');

-- Stored Procedure to Generate 10,000 Records per Table
-- ---------------------------------------------------------
DROP PROCEDURE IF EXISTS GenerateHealthcareData;
DELIMITER //
CREATE PROCEDURE GenerateHealthcareData()
BEGIN
  DECLARE i INT DEFAULT 15000; -- Start ID to avoid conflicts with existing data
  DECLARE max_id INT DEFAULT 30000; -- End ID (Generates exactly 15,000 records)

  -- Optimization: Start transaction to minimize disk I/O overhead
  START TRANSACTION;

  WHILE i < max_id DO
    -- 1. Specialties
    INSERT INTO specialties (specialty_id, specialty_name, specialty_code)
    VALUES (i, CONCAT('Specialty ', i), CONCAT('SP', i));

    -- 2. Departments
    INSERT INTO departments (department_id, department_name, floor, capacity)
    VALUES (i, CONCAT('Department ', i), FLOOR(1 + RAND() * 10), FLOOR(10 + RAND() * 100));

    -- 3. Providers
    INSERT INTO providers (provider_id, first_name, last_name, credential, specialty_id, department_id)
    VALUES (i, CONCAT('ProviderF', i), CONCAT('ProviderL', i), 'MD', i, i);

    -- 4. Patients
    INSERT INTO patients (patient_id, first_name, last_name, date_of_birth, gender, mrn)
    VALUES (i, CONCAT('PatientF', i), CONCAT('PatientL', i), DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND() * 25000) DAY), IF(RAND() > 0.5, 'M', 'F'), CONCAT('MRN', i));

    -- 5. Diagnoses
    INSERT INTO diagnoses (diagnosis_id, icd10_code, icd10_description)
    VALUES (i, CONCAT('ICD', i), CONCAT('Diagnosis Description ', i));

    -- 6. Procedures
    INSERT INTO procedures (procedure_id, cpt_code, cpt_description)
    VALUES (i, CONCAT('CPT', i), CONCAT('Procedure Description ', i));

    -- 7. Encounters
    INSERT INTO encounters (encounter_id, patient_id, provider_id, encounter_type, encounter_date, discharge_date, department_id)
    VALUES (i, i, i, ELT(FLOOR(1 + RAND() * 3), 'Outpatient', 'Inpatient', 'ER'), DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY), NOW(), i);

    -- 8. Encounter Diagnoses
    INSERT INTO encounter_diagnoses (encounter_diagnosis_id, encounter_id, diagnosis_id, diagnosis_sequence)
    VALUES (i, i, i, 1);

    -- 9. Encounter Procedures
    INSERT INTO encounter_procedures (encounter_procedure_id, encounter_id, procedure_id, procedure_date)
    VALUES (i, i, i, CURDATE());

    -- 10. Billing
    INSERT INTO billing (billing_id, encounter_id, claim_amount, allowed_amount, claim_date, claim_status)
    VALUES (i, i, ROUND(RAND() * 10000, 2), ROUND(RAND() * 8000, 2), CURDATE(), 'Submitted');

    SET i = i + 1;
  END WHILE;

  COMMIT;
END //
DELIMITER ;

CALL GenerateHealthcareData();

SELECT COUNT(*) FROM specialties;