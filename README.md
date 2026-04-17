# DST Time Fix

## Overview

This project provides robust PL/SQL scripts to correct invalid `EFFECTIVE_DATE` values caused by U.S. Daylight Saving Time (DST) transitions. It specifically handles timestamps that fall within the non-existent 2:00–3:00 AM window during the spring DST shift by safely adjusting them forward by one hour.

## Key Features

The solution supports two table designs:

- **SCD Type-2 tables (with `RECORD_EXPIRE_DATE`)**  
  Implements a standard historical tracking approach by inserting corrected records and expiring the original rows within a single transaction.

- **Non-SCD tables (without `RECORD_EXPIRE_DATE`)**  
  Performs in-place updates while maintaining a persistent backup table for auditability and recovery.

## Why It’s Useful

The scripts dynamically scan all relevant `CONSUMER%` tables, apply U.S. DST rules across the past 50 years, and ensure data consistency without manual intervention. This makes it a practical solution for maintaining temporal data integrity in systems impacted by DST anomalies.

## Notes

- No automatic commit is performed—changes must be reviewed and explicitly committed.
- Backup and traceability are built in to ensure safe execution in production environments.
