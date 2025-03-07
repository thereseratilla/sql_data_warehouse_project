/*
============================================================================
CREATE DATABASE AND SCHEMAS
===========================================================================

Purpose: 
	This script creates a new database called "DataWarehouse". If the database already exists, the existing database will be dropped and recreated.
	This script also creates three (3) schemas within the database representing the three (3) layers; Bronze, Silver and Gold.

Warning:
	Upon running the script, it drops the "DataWarehouse" database if it exists. 
	All data in the existing database will be deleted.
	Ensure proper backups before running the script.

*/



USE master;
GO

-- 1. Drop and create "DataWarehouse" database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- 2. Create the "DataWarehouse" database
 
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- 3. Create the 3 schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
