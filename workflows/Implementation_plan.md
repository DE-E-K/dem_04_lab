---
title: Star Schema Workflow
description: Workflow to transform the Healthtech OLTP database into an optimized Star Schema.
---

This workflow guides you through the process of analyzing the existing schema, designing the star schema, and creating the necessary deliverables.

# Phase 1: Performance Analysis
1. **Setup**: Create `healthcare_db` database and load `dem04.sql` to establish the OLTP state.
2. Create a file named `query_analysis.txt`.
3. Analyze the 4 OLTP queries (Encounters by Specialty, Top Diagnosis-Procedure, Readmission, Revenue).
4. For each query, document:
   - The SQL query.
   - The JOIN chain.
   - Analysis of why it is slow (e.g., "7 joins + GROUP BY").
   - (Optional) Estimated execution time based on complexity.

# Phase 2: Design Decisions
1. Create a file named `design_decisions.txt`.
2. Document Decision 1: **Fact Table Grain**. (Recommended: One row per encounter).
3. Document Decision 2: **Dimension Tables**. List attributes for `dim_patient`, `dim_provider`, `dim_specialty`, `dim_department`, `dim_date`.
4. Document Decision 3: **Pre-Aggregated Metrics**. Explain why `diagnosis_count`, `total_allowed`, etc., are in the fact table.
5. Document Decision 4: **Bridge Tables**. Explain the use of bridge tables for multi-valued diagnoses/procedures.

# Phase 3: Star Schema Implementation
1. Create a file named `star_schema.sql`.
2. Write DDL statements for:
   - Dimension Tables: `dim_specialty`, `dim_department`, `dim_provider`, `dim_patient`, `dim_date`.
   - Fact Table: `fact_encounters` (ensure foreign keys to dimensions and correct metrics).
   - Bridge Tables: `bridge_encounter_diagnoses`, `bridge_encounter_procedures`.
   - Dimensions for Bridge: `dim_diagnosis`, `dim_procedure`.
3. Ensure Primary Keys (surrogate keys) and Foreign Keys are defined.

# Phase 4: Query Optimization
1. Create a file named `star_schema_queries.txt`.
2. Rewrite the 4 business questions using the new Star Schema tables.
3. Compare the new queries to the old ones (fewer joins, simpler logic).
4. Estimate the improvement factor.

# Phase 5: ETL Design
1. Create a file named `etl_design.txt`.
2. Write pseudocode for:
   - Loading `dim_patient` (from `patients`).
   - Loading `dim_date` (generating calendar data).
   - Loading `fact_encounters` (joining `encounters` with `billing` and looking up dimension keys).
   - Populating bridge tables.

# Phase 6: Reflection
1. Create a file named `reflection.md`.
2. Write a section on Trade-offs (Normalization vs Denormalization).
3. Write a section on Why the Star Schema is faster.