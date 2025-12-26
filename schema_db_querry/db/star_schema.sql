-- Star Schema for Healthcare Analytics
-- Based on design_decisions.txt

-- 1. Dimension Tables

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

CREATE TABLE dim_specialty (
    specialty_key INT AUTO_INCREMENT PRIMARY KEY,
    specialty_id INT, -- Source System ID
    specialty_name VARCHAR(100)
);

CREATE TABLE dim_department (
    department_key INT AUTO_INCREMENT PRIMARY KEY,
    department_id INT, -- Source System ID
    department_name VARCHAR(100),
    floor INT
);

CREATE TABLE dim_encounter_type (
    encounter_type_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_type_name VARCHAR(50), -- e.g., 'Inpatient', 'Outpatient'
    encounter_class VARCHAR(50)      -- Grouping category if needed
);

CREATE TABLE dim_patient (
    patient_key INT AUTO_INCREMENT PRIMARY KEY,
    patient_id INT, -- Source System ID
    full_name VARCHAR(100),
    gender CHAR(1),
    date_of_birth DATE,
    age_group VARCHAR(10), -- Pre-calculated: '0-18', '19-65', '65+'
    current_flag BOOLEAN DEFAULT TRUE -- For Type 2 SCD
);

CREATE TABLE dim_provider (
    provider_key INT AUTO_INCREMENT PRIMARY KEY,
    provider_id INT, -- Source System ID
    provider_name VARCHAR(200),
    credential VARCHAR(20),
    specialty_name VARCHAR(100),
    department_name VARCHAR(100),
);

CREATE TABLE dim_diagnosis (
    diagnosis_key INT AUTO_INCREMENT PRIMARY KEY,
    diagnosis_id INT, -- Source System ID
    icd_code VARCHAR(10),
    diagnosis_description VARCHAR(200),
    category VARCHAR(50) -- Derived from ICD code structure
);

CREATE TABLE dim_procedure (
    procedure_key INT AUTO_INCREMENT PRIMARY KEY,
    procedure_id INT, -- Source System ID
    cpt_code VARCHAR(10),
    procedure_description VARCHAR(200),
    category VARCHAR(50)
);

-- 2. Fact Table

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

-- 3. Bridge Tables (Many-to-Many Resolution)

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

CREATE TABLE bridge_encounter_procedures (
    bridge_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_key INT,
    procedure_key INT,
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key),
    -- Composite index for query performance
    INDEX idx_bridge_proc_enc (procedure_key, encounter_key)
);