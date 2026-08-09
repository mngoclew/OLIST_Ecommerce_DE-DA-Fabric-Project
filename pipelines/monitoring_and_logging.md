# Monitoring and Logging

## Overview

This project includes a lightweight logging approach to support pipeline monitoring and failure tracking in Microsoft Fabric.

The goal is to improve observability across the ETL workflow and make pipeline issues easier to diagnose.

---

## Logging Strategy

A log table is used to track pipeline execution events.

Typical logging fields include:

- `pipeline_name`
- `activity_name`
- `table_name`
- `status`
- `error_message`
- `log_time`

This log table captures both successful and failed activities depending on the pipeline design.

---

## Monitoring Use Cases

The logging layer supports the following use cases:

- Identify failed pipeline activities
- Trace which table or notebook failed
- Review execution timestamps
- Support debugging and issue investigation
- Improve pipeline reliability over time

---

## Example Failure Scenario

A pipeline failure can be recorded with information such as:

- Pipeline name: `pipeline_dim_seller`
- Activity name: `dim_seller`
- Table name: `dim_seller`
- Status: `FAILED`
- Error message: a notebook or transformation execution error
- Log time: ETL execution timestamp in `America/New_York`

---

## Benefits

This monitoring and logging design helps make the Microsoft Fabric project more production-oriented by adding:

- better pipeline visibility
- easier troubleshooting
- clearer execution history
- more robust ETL governance