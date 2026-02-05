**Revenue Growth Payments Platform**

**Overview**

Revenue growth can appear healthy even when payment systems are silently failing underneath. This project models a realistic SaaS payments environment to uncover hidden revenue leakage, early churn signals, and actionable recovery opportunities.

The platform simulates production-scale billing, payment, and product events, transforms them using modern analytics engineering practices, and surfaces executive-grade business insights through focused dashboards.

This is not a visualization project, it is a decision intelligence system.

**Business Problems Addressed**

•	**Revenue Illusion**
Revenue can look strong while payment failures and retries increase underneath.

•	**Hidden Revenue Leakage**
Failed payments silently reduce realized revenue and increase operational friction.

**•	Late Churn Signals**
By the time customers cancel, recovery opportunities are already lost.

**•	Lack of Prioritization**
Teams don’t know which plans, months, or failure types to fix first.

**Key Business Questions**

**Revenue Health**
•	Is revenue growing, stable, or volatile?
•	Which plans drive growth?
•	Is growth driven by customers or retries?

**Payment Reliability**
•	Are failures increasing?
•	Are retries masking deeper issues?
•	Which plans and months are most affected?

**Customer Risk**
•	Do payment issues precede engagement decline?
•	Are failures correlated with cancel intent?
•	Which customers are at highest risk?

**Action & Impact**
•	How much revenue is recoverable?
•	Where should engineering focus first?
•	What is the expected business impact of fixing payment failures?

**Executive KPIs**
•	Total Revenue
•	Paying Customers
•	Payment Failure Rate %
•	Average Payment Attempts
•	Cancel Intent Rate %

KPIs are intentionally minimal and decision-oriented.

**Dashboards & Insights**

**Page 1 — Executive Revenue Health**
Purpose: Is revenue strong — and is it safe?
•	Monthly Revenue Trend
•	Revenue by Plan (Monthly)
•	Payment Failure Rate Trend
•	Average Payment Attempts Trend

Reveals whether growth is clean or driven by retry behavior.

**Page 2 — Risk & Root Cause**
Purpose: Where is revenue at risk and why?
•	Failure Rate by Plan
•	Failure Reasons Breakdown
•	Engagement Trend (Monthly)
•	Customer Risk Segment

Identifies fixable failure patterns and early churn signals.

**Page 3 — Action & Impact**
Purpose: What should we do next, and why does it matter?
•	Recoverable Revenue from Payment Failures
•	Executive narrative with findings and recommendations

Quantifies upside and supports prioritization decisions.

**Architecture**

**Data Generation**
•	Realistic synthetic SaaS data using probabilistic distributions
•	Customers, subscriptions, invoices, retries, failures, engagement

**Ingestion**
•	Python event generators
•	Batch inserts into PostgreSQL (Dockerized)

**Transformation**
•	dbt (Bronze → Staging → Gold marts)
•	Grain enforcement, sanity tests, value bounds

**Analytics**
•	Power BI semantic model
•	Dynamic tooltips with numeric + textual insights

**Data Models (Gold Layer)**
•	mart_revenue_monthly
•	mart_payment_reliability
•	mart_payment_reliability_plan_monthly
•	mart_failure_reason_monthly
•	mart_engagement
•	mart_customer_360
•	mart_failure_to_churn_signal

All marts are:
•	Grain-validated
•	Tested for nulls, bounds, and uniqueness
•	Business-aligned, not exploratory

**Data Quality & Testing**
•	Grain uniqueness tests (monthly / plan / customer)
•	Value bounds for rates and attempts
•	Not-null constraints on all business keys
•	Sanity checks validated in PostgreSQL

**Tech Stack**
•	Python - data generation & ingestion
•	PostgreSQL (Docker) - analytics warehouse
•	dbt - transformations & testing
•	Power BI - executive dashboards
•	Git - version control

**Why This Project Matters**
This project demonstrates:
•	Analytics engineering best practices
•	Business-first thinking
•	Executive communication skills
•	Ability to connect data → insight → action




