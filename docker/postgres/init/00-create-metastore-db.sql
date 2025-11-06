--------------------------------------------------------------------------------
-- Create Hive Metastore Database
--
-- This database stores metadata for all tables in the Data Lake catalog.
-- It is used by Hive Metastore service which Trino connects to.
--------------------------------------------------------------------------------

-- Create metastore database if it doesn't exist
CREATE DATABASE metastore;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE metastore TO pipeline;

\c metastore

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO pipeline;
