# SSE ExecuteSQL Processor Bundle

A custom Apache NiFi processor bundle that extends the standard ExecuteSQL processor with environment validation capabilities.

## Overview

The SSE ExecuteSQL processor provides all the functionality of the standard ExecuteSQL processor, plus optional environment validation. When validation is enabled, it validates that the configured controller services match the expected environment configuration before executing SQL queries. This helps quickly detect when connection pools change and stop migration operations before data corruption occurs.

## Features

- **Full ExecuteSQL Compatibility**: When validation is disabled, behaves exactly like standard ExecuteSQL
- **Environment Validation**: When validation is enabled, validates controller services against expected environment configuration
- **Validation-Gated Execution**: When validation is enabled, SQL execution ONLY proceeds if environment validation passes
- **Flexible Validation Modes**: 
  - STRICT: Route to failure on mismatch (recommended for production)
  - WARNING: Log warning and continue (for testing/debugging)
- **Operation-based Validation**: Uses `operation_id` from FlowFile attributes to look up environment configuration
- **Comprehensive Logging**: Detailed validation results and error messages
- **FlowFile Attributes**: Adds validation status attributes for monitoring

## Requirements

- **Java**: 21 or 17+
- **Apache NiFi**: 2.1.0

## How It Works

1. **Validation Check**: If validation is enabled, the processor queries the SSE Engine database using the `operation_id` from FlowFile attributes
2. **Environment Lookup**: Retrieves environment configuration including source and target database details
3. **Connection Validation**: Compares actual controller service URLs with expected environment URLs
4. **Execution Decision**: 
   - **STRICT mode**: **SQL execution ONLY proceeds if validation passes**. If validation fails, FlowFile routes to `validation_failed` and SQL is never executed
   - **WARNING mode**: **SQL execution proceeds regardless of validation result**, but warnings are logged if validation fails
5. **SQL Execution**: **When validation is enabled, SQL execution is gated by validation results**. When validation is disabled, SQL executes normally like standard ExecuteSQL
- **Maven**: 3.6+
- **Database**: MySQL (for SSE Engine database)

## Project Structure

```
sse-execute-sql-bundle/
├── pom.xml                                    # Root POM
├── nifi-sse-execute-sql-processors/
│   ├── pom.xml                               # Processors module POM
│   ├── src/main/java/com/sse/processors/
│   │   └── SseExecuteSQL.java               # Main processor implementation
│   ├── src/main/resources/META-INF/services/
│   │   └── org.apache.nifi.processor.Processor
│   └── src/test/java/com/sse/processors/
│       └── SseExecuteSQLTest.java           # Unit tests
└── nifi-sse-execute-sql-nar/
    └── pom.xml                               # NAR module POM
```

## Building

### Prerequisites

1. **Java 21**: Ensure Java 21 is installed and `JAVA_HOME` is set
2. **Maven**: Install Maven 3.6 or later
3. **NiFi Extension Bundles Parent**: The project inherits from `nifi-extension-bundles:2.1.0`

### Build Commands

```bash
# Complete build with NAR file generation (recommended)
mvn clean install -DskipTests

# Clean and compile only (works outside NiFi environment)
mvn clean compile

# Run tests (requires NiFi environment)
mvn test

# Package processors only (works outside NiFi environment)
mvn clean package -pl nifi-sse-execute-sql-processors -DskipTests

# Verify NAR contents (after successful build)
jar -tf nifi-sse-execute-sql-nar/target/nifi-sse-execute-sql-nar-2.1.0.nar
```

**Note**: The `mvn clean install` command will automatically generate the NAR file and install all artifacts to your local Maven repository.

### Build Output

The build produces:
- `nifi-sse-execute-sql-processors/target/nifi-sse-execute-sql-processors-2.1.0.jar`
- `nifi-sse-execute-sql-nar/target/nifi-sse-execute-sql-nar-2.1.0.nar`

## Deployment

### 1. Copy NAR to NiFi

```bash
# Copy the NAR file to NiFi's lib directory
cp nifi-sse-execute-sql-nar/target/nifi-sse-execute-sql-nar-2.1.0.nar \
   /opt/nifi/nifi-current/lib/

# Or if using a different NiFi installation path
cp nifi-sse-execute-sql-nar/target/nifi-sse-execute-sql-nar-2.1.0.nar \
   /path/to/your/nifi/lib/
```

### 2. Restart NiFi

```bash
# Restart NiFi to load the new processor
/opt/nifi/nifi-current/bin/nifi.sh restart

# Or using systemctl if NiFi is installed as a service
sudo systemctl restart nifi
```

### 3. Verify Installation

1. Open NiFi UI (typically http://localhost:8080/nifi)
2. Drag a processor onto the canvas
3. Search for "SseExecuteSQL" or "SSE ExecuteSQL"
4. The processor should appear in the search results

## Configuration

### Controller Services Setup

Before using the processor, configure the required DBCP controller services:

#### 1. SSE Engine DBCP (DBCP_NCBA_SSE_ENGINE)
```
Database Connection URL: jdbc:mysql://localhost:3306/sse_engine
Database Driver Class Name: com.mysql.cj.jdbc.Driver
Database Driver Location(s): /path/to/mysql-connector.jar
Database User: sse_engine_user
Password: ********
```

#### 2. Source DBCP (DBCP_NCBA_SSE_SOURCE)
```
Database Connection URL: jdbc:oracle:thin:@source-host:1521/SOURCEDB
Database Driver Class Name: oracle.jdbc.OracleDriver
Database Driver Location(s): /path/to/ojdbc10.jar
Database User: source_user
Password: ********
```

#### 3. Target DBCP (DBCP_NCBA_SSE_TARGET)
```
Database Connection URL: jdbc:postgresql://target-host:5432/targetdb
Database Driver Class Name: org.postgresql.Driver
Database Driver Location(s): /path/to/postgresql-42.7.1.jar
Database User: target_user
Password: ********
```

### Processor Configuration

#### Standard ExecuteSQL Properties
Configure these properties as you would for standard ExecuteSQL:
- **Database Connection Pooling Service**: Select your target DBCP service
- **SQL Query**: Your SQL query to execute
- **Query Timeout**: Query timeout in seconds
- **Max Rows Per Flow File**: Limit rows per output file
- **Compression Format**: Compression for output files
- And other standard ExecuteSQL properties...

#### Validation Properties

| Property | Description | Required | Default |
|----------|-------------|----------|---------|
| **Enable Validation** | Master switch for validation | Yes | true |
| **Operation ID** | FlowFile attribute containing operation ID | Yes* | ${operation.id} |
| **SSE Engine Database Connection Pool** | DBCP service for SSE Engine database | Yes* | - |
| **Source Database Connection Pool** | DBCP service for source database validation | Yes* | - |
| **Target Database Connection Pool** | DBCP service for target database validation | Yes* | - |
| **Validation Mode** | STRICT or WARNING | Yes | STRICT |

*Required only when validation is enabled

### Example Configuration

```
Standard ExecuteSQL Properties:
- Database Connection Pooling Service: DBCP_NCBA_SSE_TARGET
- SQL Query: SELECT * FROM users WHERE id = ?
- Query Timeout: 30
- Max Rows Per Flow File: 1000

Validation Properties:
- Enable Validation: true
- Operation ID: ${operation.id}
- SSE Engine Database Connection Pool: DBCP_NCBA_SSE_ENGINE
- Source Database Connection Pool: DBCP_NCBA_SSE_SOURCE
- Target Database Connection Pool: DBCP_NCBA_SSE_TARGET
- Validation Mode: STRICT
```

## Usage Examples

### Example 1: Validation Disabled (Standard ExecuteSQL)

```
Properties:
- Enable Validation: false
- Database Connection Pooling Service: DBCP_NCBA_SSE_TARGET
- SQL Query: SELECT * FROM users WHERE status = 'active'
```

This configuration makes the processor behave exactly like standard ExecuteSQL. The SQL query will execute regardless of environment configuration.

### Example 2: Validation Enabled (STRICT Mode)

```
Properties:
- Enable Validation: true
- Operation ID: ${operation.id}
- SSE Engine Database Connection Pool: DBCP_NCBA_SSE_ENGINE
- Source Database Connection Pool: DBCP_NCBA_SSE_SOURCE
- Target Database Connection Pool: DBCP_NCBA_SSE_TARGET
- Validation Mode: STRICT
- Database Connection Pooling Service: DBCP_NCBA_SSE_TARGET
- SQL Query: SELECT * FROM users WHERE id = ?
```

This configuration validates environment before executing SQL. **The SQL query will ONLY execute if validation passes**. If validation fails, the FlowFile is routed to `validation_failed` and the SQL query is never executed.

### Example 3: Validation Enabled (WARNING Mode)

Same as Example 2, but with:
```
- Validation Mode: WARNING
```

This configuration validates environment before executing SQL. **The SQL query will execute regardless of validation result**, but warnings are logged if validation fails.

## FlowFile Attributes

The processor adds the following attributes to FlowFiles:

| Attribute | Description | When Added |
|-----------|-------------|------------|
| `validation.passed` | true/false | Always (when validation enabled) |
| `validation.environment.id` | Environment ID | When validation passes |
| `validation.environment.name` | Environment name | When validation passes |
| `validation.timestamp` | Validation timestamp | Always (when validation enabled) |
| `validation.error` | Error message | When validation fails |

## Relationships

The processor has three relationships:

| Relationship | Description |
|--------------|-------------|
| `success` | FlowFiles processed successfully |
| `failure` | FlowFiles that failed processing |
| `validation_failed` | FlowFiles that failed environment validation |

## Monitoring and Troubleshooting

### Logging

Add to `nifi/conf/logback.xml` for detailed logging:

```xml
<logger name="com.sse.processors" level="INFO"/>
<logger name="com.sse.processors.SseExecuteSQL" level="DEBUG"/>
```

### Key Log Messages

```
INFO  - Environment validation passed for operation_id=op123, environment=Production
WARN  - Environment validation warning for operation_id=op123: ... - Proceeding anyway
ERROR - Environment validation failed for operation_id=op123: Controller service mismatch
ERROR - Failed to query environment configuration: Connection refused
```

### Common Issues

1. **"No environment configuration found"**
   - Check operation_id exists in data_migration_records table
   - Verify operation has environment_id set
   - Check ENGINE DBCP connection

2. **"Controller service mismatch"**
   - Verify SOURCE/TARGET DBCP URLs match environment config
   - Check for URL normalization issues (extra parameters, case differences)
   - Review expected vs actual URLs in logs

3. **"Failed to extract JDBC URL"**
   - Verify SOURCE/TARGET DBCP services are enabled
   - Check database connectivity

4. **Processor not appearing in palette**
   - Verify NAR copied to lib directory
   - Check NiFi logs for NAR loading errors
   - Verify META-INF/services file exists and is correct

## Development

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=SseExecuteSQLTest

# Run with debug output
mvn test -Dtest=SseExecuteSQLTest -X
```

### Code Structure

The processor extends `AbstractExecuteSQL` and adds validation logic:

- **Properties**: Validation-specific property descriptors
- **Relationships**: Additional `validation_failed` relationship
- **onTrigger**: Overridden to add validation before ExecuteSQL processing
- **Helper Classes**: `ValidationResult`, `EnvironmentConfig`
- **Helper Methods**: Database querying, URL comparison, normalization

### Contributing

1. Follow NiFi coding standards
2. Add comprehensive comments for beginners
3. Include unit tests for new functionality
4. Update documentation for any changes

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review NiFi logs for detailed error messages
3. Verify database connectivity and configuration
4. Test with validation disabled first to isolate issues
