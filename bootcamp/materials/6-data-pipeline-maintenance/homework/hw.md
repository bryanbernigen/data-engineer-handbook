# Week 5 Data Pipeline Maintenance - Homework

## Overview

In this exercise, I am tasked with managing and maintaining five data pipelines. These pipelines are focused on tracking various business areas and providing key metrics to stakeholders. The five pipelines are:

1. **Profit**
   - Unit-level profit needed for experiments
   - Aggregate profit reported to investors
2. **Growth**
   - Aggregate growth reported to investors
   - Daily growth needed for experiments
3. **Engagement**
   - Aggregate engagement reported to investors

### Ownership of Pipelines

#### Primary and Secondary Owners

Ownership refers to the accountability for the performance and health of these pipelines. Each pipeline will have both primary and secondary owners. The primary owner will be the main point of contact responsible for ensuring the pipeline functions correctly, while the secondary owner will act as a backup in case the primary owner is unavailable.

**Profit Pipelines:**
- **Unit-level profit for experiments:**
  - **Primary Owner**: Data Engineer A
  - **Secondary Owner**: Data Engineer B
- **Aggregate profit reported to investors:**
  - **Primary Owner**: Data Engineer C
  - **Secondary Owner**: Data Engineer D

**Growth Pipelines:**
- **Aggregate growth reported to investors:**
  - **Primary Owner**: Data Engineer A
  - **Secondary Owner**: Data Engineer D
- **Daily growth for experiments:**
  - **Primary Owner**: Data Engineer B
  - **Secondary Owner**: Data Engineer C

**Engagement Pipeline:**
- **Aggregate engagement reported to investors:**
  - **Primary Owner**: Data Engineer C
  - **Secondary Owner**: Data Engineer B

### On-Call Schedule

An effective on-call schedule ensures that pipeline issues are addressed quickly and that there is minimal downtime or disruption. For this, we need to define an on-call rotation that ensures coverage while also considering holidays and time off.

#### General Assumptions:
- Each engineer is on-call for 1 week at a time.
- A backup owner (secondary) will handle issues if the primary owner is unavailable.
- On-call schedules should rotate in a fair manner, and holidays should be distributed evenly.

#### Proposed On-Call Schedule (Rotation):

| Week | Data Engineer A | Data Engineer B | Data Engineer C | Data Engineer D |
|------|-----------------|-----------------|-----------------|-----------------|
| 1    | Primary - Profit (Unit) / Growth (Aggregate) | Secondary - Growth (Daily) / Engagement | Secondary - Profit (Aggregate) | Secondary - Growth (Daily) |
| 2    | Secondary - Profit (Unit) | Primary - Growth (Daily) | Primary - Profit (Aggregate) / Engagement | Secondary - Growth (Daily) |
| 3    | Secondary - Profit (Unit) | Secondary - Engagement | Primary - Growth (Daily) | Primary - Profit (Aggregate) |
| 4    | Primary - Profit (Unit) / Growth (Aggregate) | Secondary - Growth (Daily) / Engagement | Secondary - Profit (Aggregate) | Secondary - Growth (Daily) |
| 5    | Secondary - Profit (Unit) | Primary - Growth (Daily) | Primary - Profit (Aggregate) / Engagement | Secondary - Growth (Daily) |

#### Holiday Coverage:
- **Holiday Exceptions**: On-call engineers can take time off during public holidays if there is a secondary engineer available to cover their responsibilities.
- **Weekend/Off-Hours Scenarios**: The secondary engineer will be responsible for handling urgent issues that arise outside of working hours.

##### Example Scenario:
- **Engineer A** is on-call for the first week but needs time off due to a public holiday.
- **Engineer B** (secondary) takes over as primary for that week, ensuring no gaps in coverage.

### Runbooks for Pipelines Reporting Metrics to Investors

Runbooks are detailed guides that define how each pipeline should operate and how to troubleshoot potential issues. Below are some general ideas for the runbooks for the pipelines that report metrics to investors (Profit - Aggregate, Growth - Aggregate, and Engagement - Aggregate).

#### 1. **Aggregate Profit Reported to Investors**

**Runbook Overview**:  
This pipeline aggregates unit-level profit data to provide an overall profit figure to stakeholders (investors). It runs at regular intervals (e.g., daily, weekly).

**What Could Go Wrong**:
- **Data Inconsistencies**: Missing or mismatched data could lead to inaccurate profit reports.
- **Slow Query Execution**: If queries for aggregating profits take too long, the report might not be ready on time.
- **Data Quality Issues**: Unusual trends, data spikes, or missing data can cause errors in the reports.
- **Pipeline Failures**: Unexpected downtime or errors could stop the pipeline from processing data.

**Key Actions**:
- Check logs for errors in data transformation or aggregation.
- Confirm data source availability (e.g., databases, APIs).
- Validate data integrity and consistency (e.g., null values, unexpected ranges).

#### 2. **Aggregate Growth Reported to Investors**

**Runbook Overview**:  
This pipeline aggregates growth metrics (e.g., user acquisition, revenue growth) to provide high-level growth metrics to investors.

**What Could Go Wrong**:
- **Growth Calculation Errors**: Incorrect formulae or aggregations could distort growth metrics.
- **Data Delays**: Delays in receiving or processing data could result in outdated reports.
- **Version Control Issues**: Outdated versions of the growth models may be used in the calculation.

**Key Actions**:
- Monitor for issues in the data processing workflow.
- Verify that the correct version of the model or aggregation method is being used.
- Regularly validate growth data using historical benchmarks.

#### 3. **Aggregate Engagement Reported to Investors**

**Runbook Overview**:  
This pipeline aggregates engagement metrics (e.g., website visits, app usage) to provide overall engagement insights to investors.

**What Could Go Wrong**:
- **Engagement Data Gaps**: Missing data for key engagement metrics could lead to incomplete reports.
- **Slow Data Processing**: Long processing times may delay the report, affecting timely delivery to investors.
- **Incorrect Metric Calculations**: Errors in aggregating engagement data could lead to misleading metrics.

**Key Actions**:
- Check for missing data or anomalies in the engagement metrics.
- Review the calculation method for engagement metrics to ensure consistency.
- Investigate delays in the data pipeline and optimize where necessary.

### Service Level Agreement (SLA)

To ensure the timely and effective resolution of issues, the following SLAs apply to each of the investor-reporting pipelines:

- **Critical Issues** (e.g., pipeline failure, incorrect data affecting investors):
  - **Response Time**: Within 30 minutes
  - **Resolution Time**: Within 4 hours
- **High Priority Issues** (e.g., minor data discrepancies, slow processing):
  - **Response Time**: Within 2 hours
  - **Resolution Time**: Within 12 hours
- **Medium Priority Issues** (e.g., non-urgent warnings, minor issues not affecting reports):
  - **Response Time**: Within 6 hours
  - **Resolution Time**: Within 24 hours

### Summary

In summary, this document outlines the structure and ownership of the five data pipelines, provides a fair on-call schedule for the data engineering team, and suggests high-level runbooks for maintaining the pipelines that report metrics to investors. Additionally, a Service Level Agreement (SLA) has been introduced to define response and resolution expectations for critical issues. By assigning clear ownership, planning for effective on-call rotations, and implementing SLAs, the team will be well-equipped to manage and maintain these critical business metrics pipelines.
