-- Active: 1767084789057@@127.0.0.1@3306@healthcare_db
use healthcare_db;

-- QUESTION 1: Monthly Encounters by Specialty
EXPLAIN
SELECT
    d.year,
    d.month,
    p.specialty_name,
    et.encounter_type_name,
    COUNT(f.encounter_key) AS total_encounters,
    COUNT(DISTINCT f.patient_key) AS unique_patients
FROM fact_encounters f
JOIN dim_date d ON f.admission_date_key = d.date_key
JOIN dim_provider p ON f.provider_key = p.provider_key
JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
GROUP BY
    d.year,
    d.month,
    p.specialty_name,
    et.encounter_type_name;

-- QUESTION 2: Top Diagnosis-Procedure Pairs
EXPLAIN SELECT
    dd.icd_code,
    dp.cpt_code,
    COUNT(*) AS combination_count
FROM bridge_encounter_diagnoses bed
JOIN bridge_encounter_procedures bep ON bed.encounter_key = bep.encounter_key
JOIN dim_diagnosis dd ON bed.diagnosis_key = dd.diagnosis_key
JOIN dim_procedure dp ON bep.procedure_key = dp.procedure_key
GROUP BY
    dd.icd_code,
    dp.cpt_code
ORDER BY
    combination_count DESC
LIMIT 10;

-- QUESTION 3: 30-Day Readmission Rate
EXPLAIN SELECT
    p.specialty_name,
    COUNT(DISTINCT f1.patient_key) AS total_discharges,
    COUNT(DISTINCT f2.patient_key) AS readmitted_patients,
    (COUNT(DISTINCT f2.patient_key) / COUNT(DISTINCT f1.patient_key) * 100) AS readmission_rate
FROM fact_encounters f1
JOIN dim_provider p ON f1.provider_key = p.provider_key
JOIN dim_encounter_type et ON f1.encounter_type_key = et.encounter_type_key
JOIN dim_date d1 ON f1.discharge_date_key = d1.date_key
LEFT JOIN fact_encounters f2 
    ON f1.patient_key = f2.patient_key 
    AND f2.admission_date_key > f1.discharge_date_key
LEFT JOIN dim_date d2 ON f2.admission_date_key = d2.date_key
WHERE et.encounter_type_name = 'Inpatient'
  AND f1.discharge_date_key IS NOT NULL
  AND (f2.encounter_key IS NULL OR d2.full_date <= DATE_ADD(d1.full_date, INTERVAL 30 DAY))
GROUP BY p.specialty_name
ORDER BY readmission_rate DESC;

-- QUESTION 4: Revenue by Specialty & Month
EXPLAIN SELECT
    d.year,
    d.month_name,
    p.specialty_name,
    SUM(f.total_allowed_amount) AS total_revenue
FROM fact_encounters f
JOIN dim_date d ON f.admission_date_key = d.date_key
JOIN dim_provider p ON f.provider_key = p.provider_key
GROUP BY
    d.year,
    d.month,
    d.month_name,
    p.specialty_name
ORDER BY
    d.year,
    d.month,
    total_revenue DESC;