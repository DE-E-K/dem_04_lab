-- Active: 1767084789057@@127.0.0.1@3306@healthcare_db
USE healthcare_db;
-- For each month and specialty, show total encounters and unique patients by encounter type
EXPLAIN
SELECT
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS encounter_month,
    s.specialty_name,
    e.encounter_type,
    COUNT(e.encounter_id) AS total_encounters,
    COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY
    encounter_month,
    s.specialty_name,
    e.encounter_type
ORDER BY
    encounter_month,
    s.specialty_name;

-- Most common diagnosis-procedure combinations. Show ICD code, procedure code, and encounter count
EXPLAIN SELECT
    d.icd10_code,
    pr.cpt_code,
    COUNT(*) AS combination_count
FROM encounter_diagnoses ed
JOIN encounter_procedures ep ON ed.encounter_id = ep.encounter_id
JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
JOIN procedures pr ON ep.procedure_id = pr.procedure_id
GROUP BY
    d.icd10_code,
    pr.cpt_code
ORDER BY
    combination_count DESC
LIMIT 10;


--Requirement Which specialty has the highest readmission rate? (Inpatient discharge, return within 30 days).

EXPLAIN 
SELECT
    s.specialty_name,
    COUNT(DISTINCT e1.patient_id) AS total_discharges,
    COUNT(DISTINCT e2.patient_id) AS readmitted_patients,
    (COUNT(DISTINCT e2.patient_id) / COUNT(DISTINCT e1.patient_id)) * 100 AS readmission_rate
FROM encounters e1
JOIN providers p 
    ON e1.department_id = p.department_id
JOIN specialties s 
    ON p.specialty_id = s.specialty_id
LEFT JOIN encounters e2 
    ON e1.patient_id = e2.patient_id
    AND e2.encounter_date > e1.discharge_date
    AND e2.encounter_date <= DATE_ADD(e1.discharge_date, INTERVAL 30 DAY)
WHERE
    e1.encounter_type = 'Inpatient'
    AND e1.discharge_date IS NOT NULL
GROUP BY s.specialty_name
ORDER BY readmission_rate DESC;

-- Revenue by Specialty & Month
-- What we need: Total allowed amounts by specialty and month. Which specialties generate most revenue?

EXPLAIN 
SELECT
    DATE_FORMAT(b.claim_date, '%Y-%m') AS billing_month,
    s.specialty_name,
    SUM(b.allowed_amount) AS total_revenue
FROM billing b
JOIN encounters e ON b.encounter_id = e.encounter_id
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY
    billing_month,
    s.specialty_name
ORDER BY
    billing_month,
    total_revenue DESC;
