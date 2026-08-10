# Analytical Data Model

## Overview

The Gold layer of this project is designed as an analytical dimensional model for Power BI reporting.

The model separates descriptive business entities into dimension tables and analytical business processes into fact tables.

This structure supports reusable filtering, KPI calculation, time intelligence, and cross-domain business analysis.

---

## Model Structure

The analytical model contains five core dimensions:

- `dim_date`
- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_location`

and six analytical fact tables:

- `fact_sales`
- `fact_payment`
- `fact_delivery`
- `fact_review`
- `fact_seller_delivery`
- `fact_customer_behavior`

---

## Dimension Tables

### dim_date

Provides the shared calendar structure used for time-based analysis.

Typical analytical attributes include:

- Date
- Day
- Month
- Quarter
- Year
- Day of week
- Day type

The date dimension enables year-over-year and period-based analysis in Power BI.

---

### dim_customer

Stores reusable customer-level descriptive attributes.

It supports customer segmentation, geographic filtering, and customer-level reporting.

---

### dim_product

Stores reusable product attributes and product characteristics.

It supports:

- Product analysis
- Product category analysis
- Product size and weight analysis
- Sales performance by product

---

### dim_seller

Stores seller-level descriptive and geographic attributes.

It supports:

- Seller performance analysis
- Seller geographic analysis
- Revenue analysis by seller

---

### dim_location

Provides reusable geographic attributes derived from location data.

It supports geographic analysis by:

- ZIP code prefix
- City
- State
- Latitude
- Longitude

---

## Fact Tables

### fact_sales

The Sales fact table represents transactional sales activity.

**Grain:** one product sold by one seller within one order.

It supports analysis of:

- Revenue
- Product quantity
- Product value
- Shipping cost
- Seller performance
- Product performance
- Order status
- Sales trends

---

### fact_payment

The Payment fact table represents payment activity associated with customer orders.

It supports analysis of:

- Payment methods
- Payment sequence
- Installment behavior
- Payment amount

---

### fact_delivery

The Delivery fact table represents the order fulfillment and delivery lifecycle.

It supports analysis of:

- Order approval time
- Preparation time
- Shipping duration
- Delivery duration
- Estimated versus actual delivery
- On-time and late delivery performance

---

### fact_review

The Review fact table represents customer review activity.

It supports analysis of:

- Review score
- Review sentiment group
- Review coverage
- Review response time
- Customer satisfaction

---

### fact_seller_delivery

The Seller Delivery fact table focuses on seller-level fulfillment performance.

It supports analysis of:

- Seller preparation time
- Carrier handover performance
- Seller fulfillment status
- Seller delivery performance

---

### fact_customer_behavior

The Customer Behavior fact table provides customer-level behavioral metrics.

It supports analysis of:

- Total orders
- Delivered orders
- Canceled orders
- Product purchases
- Recognized revenue
- Shipping cost
- Review behavior
- Purchase frequency
- Customer value
- Returning customer behavior

---

## Dimensional Modeling Approach

Dimension tables provide reusable descriptive attributes while fact tables represent measurable business processes.

The Power BI semantic model uses these analytical tables to support filtering, aggregation, KPI calculations, and business reporting across multiple analytical domains.

---

## Analytical Domains

The model supports several reporting areas:

| Domain | Main Fact Table |
|---|---|
| Sales Performance | `fact_sales` |
| Payment Analysis | `fact_payment` |
| Delivery & Logistics | `fact_delivery` |
| Customer Experience | `fact_review` |
| Seller Fulfillment | `fact_seller_delivery` |
| Customer Behavior | `fact_customer_behavior` |

---

## Star Schema

The following diagram shows the analytical model used by the Power BI semantic layer.

![Star Schema](star-schema.png)