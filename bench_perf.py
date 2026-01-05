
import mysql.connector
import time
import statistics
import getpass 

# Configuration
DB_CONFIG = {
    'user': 'root',
    'password': getpass.getpass(),
    'host': '127.0.0.1',
    'database': 'healthcare_db'
}

# Queries
QUERIES = [
    {
        "name": "1. Monthly Encounters (OLTP)",
        "type": "OLTP",
        "sql": """
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
        """
    },
    {
        "name": "1. Monthly Encounters (Star)",
        "type": "Star",
        "sql": """
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
                et.encounter_type_name
            ORDER BY
                d.year,
                d.month,
                p.specialty_name;
        """
    },
    {
        "name": "2. Top Diagnosis-Procedure (OLTP)",
        "type": "OLTP",
        "sql": """
            SELECT
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
        """
    },
    {
        "name": "2. Top Diagnosis-Procedure (Star)",
        "type": "Star",
        "sql": """
            SELECT
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
        """
    },
    {
        "name": "3. Readmission Rate (OLTP)",
        "type": "OLTP",
        "sql": """
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
        """
    },
    {
        "name": "3. Readmission Rate (Star)",
        "type": "Star",
        "sql": """
            SELECT
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
        """
    },
    {
        "name": "4. Revenue (OLTP)",
        "type": "OLTP",
        "sql": """
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
        """
    },
    {
        "name": "4. Revenue (Star)",
        "type": "Star",
        "sql": """
            SELECT
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
        """
    }
]

def run_benchmark():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Connected to database successfully.")
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return

    print(f"{'Query Name':<40} | {'Avg Time (s)':<15} | {'Min (s)':<10} | {'Max (s)':<10}")
    print("-" * 85)

    for q in QUERIES:
        times = []
        # Warmup
        try:
            cursor.execute(q['sql'])
            cursor.fetchall()
        except Exception as e:
            print(f"Error running {q['name']}: {e}")
            continue

        # Benchmark 5 runs
        for _ in range(5):
            start = time.perf_counter()
            cursor.execute(q['sql'])
            cursor.fetchall() # Fetch to ensure query is done
            end = time.perf_counter()
            times.append(end - start)
        
        avg_time = statistics.mean(times)
        min_time = min(times)
        max_time = max(times)
        
        print(f"{q['name']:<40} | {avg_time:.4f}    | {min_time:.4f}    | {max_time:.4f}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_benchmark()