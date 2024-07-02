# Reusable Components for Data Operations

## Overview

This project repository consists of reusable components designed to handle various data operations efficiently within an enterprise setting. These components include a client for Google BigQuery operations, a configuration manager for environment-specific configurations, a client for secure interactions with the Google Secret Manager, and a client for handling SFTP operations. Each module is built to ensure robust and scalable data manipulation, configuration management, and secure communication.

### Components

#### BigQueryClient

- **Purpose**: Interacts with Google BigQuery to perform queries, manage tables, and load data.
- **Features**:
  - Execute SQL queries and return results as DataFrames.
  - Check for the existence of tables.
  - Load data from DataFrame to a specified BigQuery table.
  - Create and delete BigQuery datasets and tables.

#### ConfigManager

- **Purpose**: Manages configuration files based on different environments and business descriptions.
- **Features**:
  - Singleton pattern to ensure a single instance of configuration management.
  - Dynamically load and parse configuration files specific to the given environment and descriptor.

#### SFTPClient

- **Purpose**: Facilitates file transfer between local and remote servers using SFTP.
- **Features**:
  - Securely upload and download DataFrames as CSV files.
  - List files and directories in a specified path on the SFTP server.
  - Get the modification time of remote files.

#### SecretManager

- **Purpose**: Manages secrets like API keys and credentials using Google's Secret Manager.
- **Features**:
  - Create and retrieve secrets securely.
  - Ensure data integrity using CRC32C checksums.
  - Convert secret data to and from JSON format for application use.