# Microsoft Fabric Solution Architecture

## Overview

This project implements an end-to-end Data Engineering and Data Analytics solution using Microsoft Fabric.

The solution follows the Medallion Architecture pattern:

```text
Source Data
    |
    v
Bronze Lakehouse
    |
    v
Silver Transformation
    |
    v
Silver Lakehouse
    |
    v
Gold Transformation
    |
    v
Gold Lakehouse
    |
    v
Power BI Semantic Model
    |
    v
Power BI Reports
```

Microsoft Fabric Data Factory is used to orchestrate the ETL workflow, manage activity dependencies, refresh downstream analytical components, and support pipeline monitoring.

---

## Architecture Diagram

![Microsoft Fabric Medallion Architecture](medallion-architecture.png)

---

## Data Sources

The project uses the Brazilian E-Commerce Public Dataset by Olist.

The original dataset is supplemented with synthetic data for the 2019–2026 period to extend the analytical time horizon and support:

- Year-over-year analysis
- ETL scalability testing
- Pipeline orchestration testing
- Dashboard validation
- Time intelligence analysis

The raw datasets are not stored in this GitHub repository.

---

## Bronze Layer

The Bronze Lakehouse acts as the raw landing zone.

Source CSV files are preserved with minimal transformation before downstream processing.

Main source domains include:

- Customers
- Orders
- Order Items
- Products
- Sellers
- Payments
- Reviews
- Geolocation

---

## Silver Transformation

PySpark notebooks transform Bronze data into cleansed and standardized Silver tables.

Key transformation activities include:

- Duplicate removal
- Data cleaning
- Data type conversion
- Timestamp conversion
- Column standardization
- Audit column creation

---

## Silver Layer

The Silver Lakehouse contains cleansed and standardized business data.

Core tables include:

- `silver_customers`
- `silver_orders`
- `silver_order_items`
- `silver_products`
- `silver_sellers`
- `silver_payments`
- `silver_reviews`
- `silver_geolocation`

The Silver layer acts as the validated source for analytical transformations.

---

## Gold Transformation

PySpark and Spark SQL are used to transform Silver data into business-ready analytical tables.

The Gold transformation produces both dimension and fact tables for downstream reporting.

### Dimensions

- Customer
- Product
- Seller
- Date
- Location

### Facts

- Sales
- Payment
- Review
- Delivery
- Seller Delivery
- Customer Behavior

---

## Gold Layer

The Gold Lakehouse contains the analytical model used for business intelligence.

### Dimension Tables

- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_date`
- `dim_location`

### Fact Tables

- `fact_sales`
- `fact_payment`
- `fact_review`
- `fact_delivery`
- `fact_seller_delivery`
- `fact_customer_behavior`

The Gold layer provides business-ready analytical data for Power BI.

---

## Pipeline Orchestration

Microsoft Fabric Data Factory coordinates the ETL workflow.

The orchestration layer supports:

- Master ETL execution
- Pipeline dependencies
- Transformation sequencing
- Semantic model refresh
- Failure handling
- Execution monitoring

Detailed pipeline documentation is available in the `/pipelines` directory.

---

## Monitoring and Operations

The project includes monitoring and failure-handling logic using:

- Pipeline Run History
- Centralized `log_pipeline` table
- Error logging
- Email failure notifications

This improves visibility into pipeline execution and supports troubleshooting.

---

## Power BI Semantic Model

The Gold analytical tables are consumed by a Power BI semantic model.

The semantic layer supports:

- Table relationships
- DAX measures
- KPI calculations
- Time intelligence
- Year-over-year analysis

---

## Reporting Layer

The semantic model supports Power BI reporting across multiple analytical areas, including:

- Executive Dashboard
- Sales Performance
- Payment Analysis
- Delivery / SLA
- Customer Experience

The reporting layer provides business-facing insights generated from the Gold analytical model.