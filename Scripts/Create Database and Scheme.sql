/*
    Script Name     : Create Database and Schemas
    Description     : This script initializes the Data Warehouse environment.
                      It creates a new database (Datawarehouse) and three schemas:
                      Bronze, Silver, and Gold, following the Data Lakehouse layering concept.
*/

-- Switch context to master database (required to create a new database)
USE master;
GO 

-- Create the main Data Warehouse database
CREATE DATABASE Datawarehouse;
GO

-- Switch context to the newly created Data Warehouse database
USE Datawarehouse;
GO

/* ============================
   Create Schemas
   ============================ */

-- Bronze schema:
-- Stores raw data as ingested from source systems (CRM, ERP, etc.).
-- Minimal or no transformation; acts as staging area.
CREATE SCHEMA bronze;
GO

-- Silver schema:
-- Stores cleaned and standardized data after ETL transformations.
-- Designed for integration and ensuring data quality.
CREATE SCHEMA silver;
GO

-- Gold schema:
-- Stores curated, business-ready data.
-- Optimized for reporting, dashboards, and advanced analytics.
CREATE SCHEMA gold;
GO

