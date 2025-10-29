# Custom SSEExecuteSQL Processor Development Guide

## Overview

Develop a custom Apache NiFi processor that extends the standard `ExecuteSQL` processor with environment validation. This processor validates that the configured controller services match the expected environment configuration before executing SQL queries. 

**CRITICAL BEHAVIOR**: When validation is enabled, SQL execution is gated by validation results - the SQL query will ONLY execute if validation passes. This is the core safety feature that prevents data corruption.

**Key Requirement**: Validate environment configuration using `operation_id` from FlowFile attributes by querying the SSE Engine database directly. This helps quickly detect when connection pools change and stop migration operations before data corruption occurs.

## Prerequisites

- **Java Version**: Java 21
- **NiFi Version**: Apache NiFi 1.x or 2.x (specify based on your deployment)
- **Build Tool**: Maven
- **Database**: MySQL (sse_engine schema)

## System Context

### Architecture Overview

Your SSE (Sensys Search Engine) system uses NiFi for data migration operations with three key DBCP controller services:

1. **DBCP_NCBA_SSE_ENGINE**: Connects to `sse_engine` database for metadata queries
2. **DBCP_NCBA_SSE_SOURCE**: Connects to source database for reading data
3. **DBCP_NCBA_SSE_TARGET**: Connects to target database for executing SQL queries

### Process Flow

```
IncomingFlowFile (with operation_id attribute)
    ↓
SSEExecuteSQL Processor
    ↓
    1. Check if validation is enabled
    2. If disabled → Execute standard ExecuteSQL operation (SQL executes normally)
    3. If enabled → Extract operation_id from FlowFile attribute
    4. Query data_migration_records using ENGINE DBCP
    5. Get environment_id from operation
    6. Query environment_configurations and database_configurations
    7. Build expected source and target JDBC URLs
    8. Validate against SOURCE and TARGET DBCP services
    9. If validation passes → Execute standard ExecuteSQL operation (SQL executes)
    10. If validation fails → 
        - STRICT mode: Route to validation_failed (SQL is BLOCKED - never executes)
        - WARNING mode: Log warning and execute SQL anyway
```

## Database Schema Reference

### data_migration_records Table

From `V8__Create_Operation_Tracking_Tables.sql`:

```sql
CREATE TABLE data_migration_records (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    operation_id VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    record_id VARCHAR(255) NOT NULL,
    present_in_source BOOLEAN,
    present_in_target BOOLEAN,
    verified_source_time TIMESTAMP,
    verified_target_time TIMESTAMP,
    FOREIGN KEY (operation_id) REFERENCES operations(id)
);
```

### operations Table

From `V8__Create_Operation_Tracking_Tables.sql` + `V13__Add_Environment_Id.sql`:

```sql
CREATE TABLE operations (
    id VARCHAR(255) PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    environment_id BIGINT,  -- FK to environment_configurations
    progress DOUBLE PRECISION NOT NULL DEFAULT 0,
    start_time TIMESTAMP,
    last_updated_time TIMESTAMP,
    completion_time TIMESTAMP,
    error_message VARCHAR(1000),
    metadata TEXT
);
```

### environment_configurations Table

From `V2__Create_Dependencies_And_Environments.sql`:

```sql
CREATE TABLE environment_configurations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    source_config_id BIGINT NOT NULL,  -- FK to database_configurations
    target_config_id BIGINT NOT NULL,  -- FK to database_configurations
    status ENUM('DRAFT', 'VALIDATION_IN_PROGRESS', 'VALIDATION_FAILED', 
                'VALIDATION_PASSED', 'DEPLOYMENT_READY', 'DEPLOYED', 'INACTIVE'),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    validated_at TIMESTAMP NULL
);
```

### database_configurations Table

From `V1__Init_Database_Configurations.sql`:

```sql
CREATE TABLE database_configurations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    database_type VARCHAR(32) NOT NULL,  -- oracle, postgresql, mysql, sqlserver
    role VARCHAR(32) NOT NULL,
    host VARCHAR(128) NOT NULL,
    port INT NOT NULL,
    db_name VARCHAR(64) NOT NULL,
    encrypted_username TEXT NOT NULL,
    encrypted_password TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted BOOLEAN NOT NULL DEFAULT FALSE
);
```

## Custom Processor Implementation

### 1. Processor Class Structure

```java
package com.sse.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.ExecuteSQL;

import java.sql.*;
import java.util.*;

/**
 * Custom ExecuteSQL processor with environment validation.
 * Validates that source and target controller services match the expected
 * environment configuration before executing SQL queries.
 * 
 * When validation is disabled, functions as standard ExecuteSQL processor.
 * When validation is enabled, performs environment checks using operation_id
 * from FlowFile attributes to look up environment configuration from sse_engine database.
 */
@Tags({"sql", "select", "jdbc", "query", "database", "validation", "environment", "sse"})
@CapabilityDescription("Executes SQL queries with optional environment validation. " +
    "When validation is enabled, validates that controller services match the expected " +
    "environment configuration by querying the SSE Engine database using operation_id " +
    "from FlowFile attributes. When validation is disabled, functions as standard ExecuteSQL.")
@WritesAttributes({
    @WritesAttribute(attribute = "validation.passed", description = "true if validation passed"),
    @WritesAttribute(attribute = "validation.environment.id", description = "Environment ID used for validation"),
    @WritesAttribute(attribute = "validation.environment.name", description = "Environment name"),
    @WritesAttribute(attribute = "validation.timestamp", description = "Validation timestamp"),
    @WritesAttribute(attribute = "validation.error", description = "Validation error message if failed"),
    @WritesAttribute(attribute = "executesql.row.count", description = "Contains the number of rows returned by the query"),
    @WritesAttribute(attribute = "executesql.query.duration", description = "Combined duration of the query execution time and fetch time in milliseconds")
})
public class SSEExecuteSQL extends ExecuteSQL {
    
    // Additional properties beyond standard ExecuteSQL
    // ...
}
```

### 2. Property Descriptors

```java
// Operation ID from FlowFile attributes
public static final PropertyDescriptor OPERATION_ID = new PropertyDescriptor.Builder()
    .name("operation-id")
    .displayName("Operation ID")
    .description("The operation ID from FlowFile attributes. Used to look up environment configuration " +
                 "from sse_engine.data_migration_records table.")
    .required(true)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue("${operation.id}")  // Common attribute name
    .build();

// SSE Engine Database Connection (for querying environment metadata)
public static final PropertyDescriptor ENGINE_DBCP_SERVICE = new PropertyDescriptor.Builder()
    .name("engine-dbcp-service")
    .displayName("SSE Engine Database Connection Pool")
    .description("The DBCP Controller Service for connecting to the SSE Engine database " +
                 "(sse_engine schema) to query environment configurations. " +
                 "Typically DBCP_NCBA_SSE_ENGINE.")
    .required(true)
    .identifiesControllerService(DBCPService.class)
    .build();

// Source Controller Service (for validation)
public static final PropertyDescriptor SOURCE_DBCP_SERVICE = new PropertyDescriptor.Builder()
    .name("source-dbcp-service")
    .displayName("Source Database Connection Pool")
    .description("The DBCP Controller Service to validate against the environment's source configuration. " +
                 "Typically DBCP_NCBA_SSE_SOURCE.")
    .required(true)
    .identifiesControllerService(DBCPService.class)
    .build();

// Target Controller Service (for validation AND operation)
// Note: This should be the same as the DBCP service used by ExecuteSQL
public static final PropertyDescriptor TARGET_DBCP_SERVICE = new PropertyDescriptor.Builder()
    .name("target-dbcp-service")
    .displayName("Target Database Connection Pool")
    .description("The DBCP Controller Service to validate against the environment's target configuration " +
                 "AND use for the ExecuteSQL operation. Typically DBCP_NCBA_SSE_TARGET.")
    .required(true)
    .identifiesControllerService(DBCPService.class)
    .build();

// Validation Mode
public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor.Builder()
    .name("validation-mode")
    .displayName("Validation Mode")
    .description("Determines validation behavior: STRICT (route to failure on mismatch) or " +
                 "WARNING (log warning and continue)")
    .required(true)
    .defaultValue("STRICT")
    .allowableValues("STRICT", "WARNING")
    .build();

// Enable/Disable Validation (for testing/emergency bypass)
public static final PropertyDescriptor ENABLE_VALIDATION = new PropertyDescriptor.Builder()
    .name("enable-validation")
    .displayName("Enable Validation")
    .description("Enable or disable environment validation. Set to false only for testing or emergency bypass.")
    .required(true)
    .defaultValue("true")
    .allowableValues("true", "false")
    .build();

@Override
protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
    
    // Add custom properties at the beginning for visibility
    properties.add(0, ENABLE_VALIDATION);
    properties.add(1, OPERATION_ID);
    properties.add(2, ENGINE_DBCP_SERVICE);
    properties.add(3, SOURCE_DBCP_SERVICE);
    properties.add(4, TARGET_DBCP_SERVICE);
    properties.add(5, VALIDATION_MODE);
    
    return properties;
}
```

### 3. Relationships

```java
// Add custom relationship for validation failures
public static final Relationship REL_VALIDATION_FAILED = new Relationship.Builder()
    .name("validation_failed")
    .description("FlowFiles that fail environment validation are routed to this relationship")
    .build();

@Override
public Set<Relationship> getRelationships() {
    final Set<Relationship> relationships = new HashSet<>(super.getRelationships());
    relationships.add(REL_VALIDATION_FAILED);
    return relationships;
}
```

### 4. Main Processing Logic (onTrigger)

```java
@Override
public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    FlowFile flowFile = session.get();
    if (flowFile == null) {
        return;
    }

    // Check if validation is enabled
    boolean validationEnabled = context.getProperty(ENABLE_VALIDATION).asBoolean();
    
    if (!validationEnabled) {
        getLogger().debug("Environment validation is DISABLED - proceeding with standard ExecuteSQL");
        super.onTrigger(context, session);
        return;
    }

    // Perform validation
    try {
        ValidationResult validationResult = performEnvironmentValidation(context, flowFile);
        
        if (!validationResult.isValid()) {
            // Validation failed
            String validationMode = context.getProperty(VALIDATION_MODE).getValue();
            
            if ("STRICT".equals(validationMode)) {
                // Add error attributes and route to validation_failed
                flowFile = session.putAttribute(flowFile, "validation.passed", "false");
                flowFile = session.putAttribute(flowFile, "validation.error", validationResult.getErrorMessage());
                flowFile = session.putAttribute(flowFile, "validation.timestamp", 
                                               String.valueOf(System.currentTimeMillis()));
                
                getLogger().error("Environment validation failed for operation_id={}: {}", 
                    validationResult.getOperationId(), validationResult.getErrorMessage());
                
                session.transfer(flowFile, REL_VALIDATION_FAILED);
                return;
            } else {
                // WARNING mode - log and continue
                getLogger().warn("Environment validation warning for operation_id={}: {} - Proceeding anyway", 
                    validationResult.getOperationId(), validationResult.getErrorMessage());
            }
        } else {
            // Validation passed - add success attributes
            flowFile = session.putAttribute(flowFile, "validation.passed", "true");
            flowFile = session.putAttribute(flowFile, "validation.environment.id", 
                                           String.valueOf(validationResult.getEnvironmentId()));
            flowFile = session.putAttribute(flowFile, "validation.environment.name", 
                                           validationResult.getEnvironmentName());
            flowFile = session.putAttribute(flowFile, "validation.timestamp", 
                                           String.valueOf(System.currentTimeMillis()));
            
            getLogger().info("Environment validation passed for operation_id={}, environment={}", 
                validationResult.getOperationId(), validationResult.getEnvironmentName());
        }
        
        // Proceed with standard ExecuteSQL operation
        super.onTrigger(context, session);
        
    } catch (Exception e) {
        getLogger().error("Error during environment validation: {}", e.getMessage(), e);
        flowFile = session.putAttribute(flowFile, "validation.error", 
                                       "Validation error: " + e.getMessage());
        session.transfer(flowFile, REL_FAILURE);
    }
}
```

### 5. Validation Logic Implementation

```java
/**
 * Helper class to hold validation results
 */
private static class ValidationResult {
    private final boolean valid;
    private final String operationId;
    private final Long environmentId;
    private final String environmentName;
    private final String errorMessage;
    
    private ValidationResult(boolean valid, String operationId, Long environmentId, 
                            String environmentName, String errorMessage) {
        this.valid = valid;
        this.operationId = operationId;
        this.environmentId = environmentId;
        this.environmentName = environmentName;
        this.errorMessage = errorMessage;
    }
    
    public static ValidationResult success(String operationId, Long environmentId, String environmentName) {
        return new ValidationResult(true, operationId, environmentId, environmentName, null);
    }
    
    public static ValidationResult failure(String operationId, String errorMessage) {
        return new ValidationResult(false, operationId, null, null, errorMessage);
    }
    
    public boolean isValid() { return valid; }
    public String getOperationId() { return operationId; }
    public Long getEnvironmentId() { return environmentId; }
    public String getEnvironmentName() { return environmentName; }
    public String getErrorMessage() { return errorMessage; }
}

/**
 * Helper class to hold environment configuration
 */
private static class EnvironmentConfig {
    Long environmentId;
    String environmentName;
    String environmentStatus;
    
    String sourceDbType;
    String sourceHost;
    Integer sourcePort;
    String sourceDatabase;
    String sourceName;
    
    String targetDbType;
    String targetHost;
    Integer targetPort;
    String targetDatabase;
    String targetName;
}

/**
 * Performs environment validation by querying SSE Engine database
 */
private ValidationResult performEnvironmentValidation(ProcessContext context, FlowFile flowFile) 
        throws ProcessException {
    
    // Extract operation_id from FlowFile attributes
    String operationId = context.getProperty(OPERATION_ID)
        .evaluateAttributeExpressions(flowFile)
        .getValue();
    
    if (operationId == null || operationId.trim().isEmpty()) {
        return ValidationResult.failure(operationId, "operation_id is empty or not set");
    }
    
    getLogger().debug("Validating environment for operation_id: {}", operationId);
    
    // Get controller services
    DBCPService engineDbcp = context.getProperty(ENGINE_DBCP_SERVICE)
        .asControllerService(DBCPService.class);
    DBCPService sourceDbcp = context.getProperty(SOURCE_DBCP_SERVICE)
        .asControllerService(DBCPService.class);
    DBCPService targetDbcp = context.getProperty(TARGET_DBCP_SERVICE)
        .asControllerService(DBCPService.class);
    
    // Step 1: Query environment configuration from SSE Engine database
    EnvironmentConfig envConfig;
    try {
        envConfig = queryEnvironmentConfig(engineDbcp, operationId);
    } catch (SQLException e) {
        return ValidationResult.failure(operationId, 
            "Failed to query environment configuration: " + e.getMessage());
    }
    
    if (envConfig == null) {
        return ValidationResult.failure(operationId, 
            "No environment configuration found for operation_id: " + operationId);
    }
    
    // Step 2: Extract JDBC URLs from controller services
    String sourceJdbcUrl;
    String targetJdbcUrl;
    try {
        sourceJdbcUrl = extractJdbcUrlFromService(sourceDbcp);
        targetJdbcUrl = extractJdbcUrlFromService(targetDbcp);
    } catch (SQLException e) {
        return ValidationResult.failure(operationId, 
            "Failed to extract JDBC URLs from controller services: " + e.getMessage());
    }
    
    // Step 3: Build expected JDBC URLs from environment configuration
    String expectedSourceUrl = buildJdbcUrl(
        envConfig.sourceDbType,
        envConfig.sourceHost,
        envConfig.sourcePort,
        envConfig.sourceDatabase
    );
    
    String expectedTargetUrl = buildJdbcUrl(
        envConfig.targetDbType,
        envConfig.targetHost,
        envConfig.targetPort,
        envConfig.targetDatabase
    );
    
    getLogger().debug("Expected Source URL: {}", expectedSourceUrl);
    getLogger().debug("Actual Source URL: {}", sourceJdbcUrl);
    getLogger().debug("Expected Target URL: {}", expectedTargetUrl);
    getLogger().debug("Actual Target URL: {}", targetJdbcUrl);
    
    // Step 4: Compare URLs
    boolean sourceMatches = compareJdbcUrls(sourceJdbcUrl, expectedSourceUrl);
    boolean targetMatches = compareJdbcUrls(targetJdbcUrl, expectedTargetUrl);
    
    if (!sourceMatches || !targetMatches) {
        String errorMsg = String.format(
            "Controller service mismatch for environment '%s' (ID: %d). " +
            "Source match: %s, Target match: %s. " +
            "Expected: Source=%s, Target=%s. " +
            "Actual: Source=%s, Target=%s",
            envConfig.environmentName,
            envConfig.environmentId,
            sourceMatches,
            targetMatches,
            expectedSourceUrl,
            expectedTargetUrl,
            sourceJdbcUrl,
            targetJdbcUrl
        );
        return ValidationResult.failure(operationId, errorMsg);
    }
    
    return ValidationResult.success(operationId, envConfig.environmentId, envConfig.environmentName);
}

/**
 * Queries environment configuration from SSE Engine database using operation_id
 * 
 * Query Path: data_migration_records.operation_id -> operations.environment_id 
 *             -> environment_configurations -> database_configurations
 */
private EnvironmentConfig queryEnvironmentConfig(DBCPService engineDbcp, String operationId) 
        throws SQLException {
    
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    
    try {
        conn = engineDbcp.getConnection();
        
        // Join query to get complete environment configuration
        String sql = """
            SELECT DISTINCT
                o.id as operation_id,
                o.environment_id,
                e.name as environment_name,
                e.status as environment_status,
                -- Source configuration
                sc.id as source_config_id,
                sc.database_type as source_db_type,
                sc.host as source_host,
                sc.port as source_port,
                sc.db_name as source_database,
                sc.name as source_name,
                -- Target configuration
                tc.id as target_config_id,
                tc.database_type as target_db_type,
                tc.host as target_host,
                tc.port as target_port,
                tc.db_name as target_database,
                tc.name as target_name
            FROM data_migration_records dmr
            INNER JOIN operations o ON dmr.operation_id = o.id
            INNER JOIN environment_configurations e ON o.environment_id = e.id
            INNER JOIN database_configurations sc ON e.source_config_id = sc.id AND sc.deleted = FALSE
            INNER JOIN database_configurations tc ON e.target_config_id = tc.id AND tc.deleted = FALSE
            WHERE dmr.operation_id = ?
            LIMIT 1
            """;
        
        stmt = conn.prepareStatement(sql);
        stmt.setString(1, operationId);
        
        rs = stmt.executeQuery();
        
        if (!rs.next()) {
            getLogger().warn("No environment configuration found for operation_id: {}", operationId);
            return null;
        }
        
        // Build environment configuration object
        EnvironmentConfig config = new EnvironmentConfig();
        config.environmentId = rs.getLong("environment_id");
        config.environmentName = rs.getString("environment_name");
        config.environmentStatus = rs.getString("environment_status");
        
        // Source configuration
        config.sourceDbType = rs.getString("source_db_type");
        config.sourceHost = rs.getString("source_host");
        config.sourcePort = rs.getInt("source_port");
        config.sourceDatabase = rs.getString("source_database");
        config.sourceName = rs.getString("source_name");
        
        // Target configuration
        config.targetDbType = rs.getString("target_db_type");
        config.targetHost = rs.getString("target_host");
        config.targetPort = rs.getInt("target_port");
        config.targetDatabase = rs.getString("target_database");
        config.targetName = rs.getString("target_name");
        
        getLogger().info("Retrieved environment configuration: {} (ID: {})", 
            config.environmentName, config.environmentId);
        
        return config;
        
    } finally {
        // Close resources in reverse order
        if (rs != null) {
            try { rs.close(); } catch (SQLException e) { 
                getLogger().debug("Error closing ResultSet", e); 
            }
        }
        if (stmt != null) {
            try { stmt.close(); } catch (SQLException e) { 
                getLogger().debug("Error closing Statement", e); 
            }
        }
        if (conn != null) {
            try { conn.close(); } catch (SQLException e) { 
                getLogger().debug("Error closing Connection", e); 
            }
        }
    }
}

/**
 * Extracts JDBC URL from DBCPService controller service
 * Uses connection metadata to get the actual URL
 */
private String extractJdbcUrlFromService(DBCPService dbcpService) throws SQLException {
    Connection conn = null;
    try {
        conn = dbcpService.getConnection();
        DatabaseMetaData metadata = conn.getMetaData();
        String url = metadata.getURL();
        return url;
    } finally {
        if (conn != null) {
            try { 
                conn.close(); 
            } catch (SQLException e) { 
                getLogger().debug("Error closing connection", e); 
            }
        }
    }
}

/**
 * Builds JDBC URL using same logic as EnvironmentConnectionPoolController.java
 * Reference: sse_engine/controller/environments/EnvironmentConnectionPoolController.java:267-277
 */
private String buildJdbcUrl(String dbType, String host, int port, String database) {
    String baseUrl = switch (dbType.toLowerCase()) {
        case "oracle" -> "jdbc:oracle:thin:@%s:%d/%s";
        case "postgresql" -> "jdbc:postgresql://%s:%d/%s";
        case "mysql" -> "jdbc:mysql://%s:%d/%s";
        case "sqlserver" -> "jdbc:sqlserver://%s:%d;databaseName=%s";
        default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
    };
    
    return String.format(baseUrl, host, port, database);
}

/**
 * Compares two JDBC URLs for equivalence
 * Handles normalization and case-insensitive comparison
 */
private boolean compareJdbcUrls(String url1, String url2) {
    if (url1 == null || url2 == null) {
        return false;
    }
    
    String normalized1 = normalizeJdbcUrl(url1);
    String normalized2 = normalizeJdbcUrl(url2);
    
    return normalized1.equalsIgnoreCase(normalized2);
}

/**
 * Normalizes JDBC URL for comparison
 * - Removes query parameters
 * - Removes trailing slashes
 * - Trims whitespace
 * - Converts to lowercase
 */
private String normalizeJdbcUrl(String jdbcUrl) {
    if (jdbcUrl == null) {
        return "";
    }
    
    // Remove query parameters (everything after ?)
    int queryIndex = jdbcUrl.indexOf('?');
    if (queryIndex > 0) {
        jdbcUrl = jdbcUrl.substring(0, queryIndex);
    }
    
    // Remove trailing slashes
    jdbcUrl = jdbcUrl.replaceAll("/+$", "");
    
    // Normalize whitespace and convert to lowercase
    return jdbcUrl.trim().toLowerCase();
}
```

## Testing Strategy

### Unit Tests

```java
package com.sse.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import static org.junit.jupiter.api.Assertions.*;

class SSEExecuteSQLTest {
    
    private TestRunner runner;
    
    @Mock
    private DBCPService mockEngineDbcp;
    
    @Mock
    private DBCPService mockSourceDbcp;
    
    @Mock
    private DBCPService mockTargetDbcp;
    
    @BeforeEach
    void setup() {
        runner = TestRunners.newTestRunner(SSEExecuteSQL.class);
    }
    
    @Test
    void testValidationSuccess() {
        // Mock environment configuration query
        // Mock controller service URLs that match expected
        // Add FlowFile with operation_id attribute
        // Assert FlowFile routes to success
        // Verify validation.passed=true attribute
    }
    
    @Test
    void testValidationFailure_SourceMismatch() {
        // Mock environment configuration
        // Mock source URL mismatch
        // Assert FlowFile routes to validation_failed
        // Verify error attributes
    }
    
    @Test
    void testValidationFailure_TargetMismatch() {
        // Mock environment configuration
        // Mock target URL mismatch
        // Assert FlowFile routes to validation_failed
    }
    
    @Test
    void testValidationDisabled() {
        // Set ENABLE_VALIDATION to false
        // Assert validation is skipped
        // Assert FlowFile proceeds to ExecuteSQL
    }
    
    @Test
    void testWarningMode() {
        // Set VALIDATION_MODE to WARNING
        // Mock URL mismatch
        // Assert FlowFile proceeds despite mismatch
        // Verify warning is logged
    }
    
    @Test
    void testMissingOperationId() {
        // FlowFile without operation_id attribute
        // Assert routes to validation_failed
    }
    
    @Test
    void testJdbcUrlNormalization() {
        assertTrue(compareJdbcUrls(
            "jdbc:postgresql://host:5432/db",
            "jdbc:postgresql://host:5432/db"
        ));
        assertTrue(compareJdbcUrls(
            "jdbc:postgresql://host:5432/db?param=value",
            "jdbc:postgresql://host:5432/db"
        ));
        assertTrue(compareJdbcUrls(
            "jdbc:postgresql://HOST:5432/DB",
            "jdbc:postgresql://host:5432/db"
        ));
    }
    
    @Test
    void testBuildJdbcUrl() {
        assertEquals(
            "jdbc:oracle:thin:@host:1521/ORCL",
            buildJdbcUrl("oracle", "host", 1521, "ORCL")
        );
        assertEquals(
            "jdbc:postgresql://host:5432/mydb",
            buildJdbcUrl("postgresql", "host", 5432, "mydb")
        );
        assertEquals(
            "jdbc:mysql://host:3306/mydb",
            buildJdbcUrl("mysql", "host", 3306, "mydb")
        );
        assertEquals(
            "jdbc:sqlserver://host:1433;databaseName=mydb",
            buildJdbcUrl("sqlserver", "host", 1433, "mydb")
        );
    }
}
```

### Integration Tests

1. **Setup Test Environment**:
   - Deploy test NiFi instance
   - Create test SSE Engine database with sample data
   - Configure test controller services (ENGINE, SOURCE, TARGET)

2. **Test Scenarios**:
   
   **Scenario 1: Valid Configuration**
   ```
   - Create test operation in operations table
   - Create test records in data_migration_records
   - Configure matching controller services
   - Send FlowFile with operation_id attribute
   - Verify: Routes to success, SQL query executed
   ```
   
   **Scenario 2: Source Mismatch**
   ```
   - Configure SOURCE DBCP to wrong database
   - Send FlowFile with operation_id attribute
   - Verify: Routes to validation_failed
   ```
   
   **Scenario 3: Target Mismatch**
   ```
   - Configure TARGET DBCP to wrong database
   - Send FlowFile with operation_id attribute
   - Verify: Routes to validation_failed
   ```
   
   **Scenario 4: Missing Operation**
   ```
   - Send FlowFile with non-existent operation_id
   - Verify: Routes to validation_failed
   ```

## Maven Project Structure

### Project Layout

```
nifi-sse-execute-sql-bundle/
├── pom.xml (parent)
├── nifi-sse-execute-sql-processors/
│   ├── pom.xml
│   └── src/
│       ├── main/
│       │   ├── java/com/sse/processors/SSEExecuteSQL.java
│       │   └── resources/
│       │       └── META-INF/services/
│       │           └── org.apache.nifi.processor.Processor
│       └── test/
│           └── java/com/sse/processors/SSEExecuteSQLTest.java
└── nifi-sse-execute-sql-nar/
    ├── pom.xml
    └── src/main/resources/META-INF/NOTICE
```

### Parent POM (nifi-sensys-processors/pom.xml)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.sensys.nifi</groupId>
    <artifactId>nifi-sensys-processors-parent</artifactId>
    <version>1.0.0</version>
    <packaging>pom</packaging>

    <name>Sensys NiFi Processors</name>
    <description>Custom NiFi processors for Sensys SSE with environment validation</description>

    <modules>
        <module>nifi-sensys-processors</module>
        <module>nifi-sensys-processors-nar</module>
    </modules>

    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <maven.compiler.release>21</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <nifi.version>2.0.0</nifi.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <!-- NiFi Dependencies -->
            <dependency>
                <groupId>org.apache.nifi</groupId>
                <artifactId>nifi-api</artifactId>
                <version>${nifi.version}</version>
                <scope>provided</scope>
            </dependency>
            <dependency>
                <groupId>org.apache.nifi</groupId>
                <artifactId>nifi-utils</artifactId>
                <version>${nifi.version}</version>
                <scope>provided</scope>
            </dependency>
            <dependency>
                <groupId>org.apache.nifi</groupId>
                <artifactId>nifi-dbcp-service-api</artifactId>
                <version>${nifi.version}</version>
                <scope>provided</scope>
            </dependency>
            <dependency>
                <groupId>org.apache.nifi</groupId>
                <artifactId>nifi-standard-processors</artifactId>
                <version>${nifi.version}</version>
            </dependency>
            
            <!-- Testing -->
            <dependency>
                <groupId>org.apache.nifi</groupId>
                <artifactId>nifi-mock</artifactId>
                <version>${nifi.version}</version>
                <scope>test</scope>
            </dependency>
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>5.10.0</version>
                <scope>test</scope>
            </dependency>
            <dependency>
                <groupId>org.mockito</groupId>
                <artifactId>mockito-core</artifactId>
                <version>5.5.0</version>
                <scope>test</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.11.0</version>
                    <configuration>
                        <release>21</release>
                    </configuration>
                </plugin>
                <plugin>
                    <groupId>org.apache.nifi</groupId>
                    <artifactId>nifi-nar-maven-plugin</artifactId>
                    <version>1.5.1</version>
                    <extensions>true</extensions>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

### Processor Module POM (nifi-sensys-processors/pom.xml)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.sensys.nifi</groupId>
        <artifactId>nifi-sensys-processors-parent</artifactId>
        <version>1.0.0</version>
    </parent>

    <artifactId>nifi-sensys-processors</artifactId>
    <packaging>jar</packaging>

    <dependencies>
        <dependency>
            <groupId>org.apache.nifi</groupId>
            <artifactId>nifi-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.nifi</groupId>
            <artifactId>nifi-utils</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.nifi</groupId>
            <artifactId>nifi-dbcp-service-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.nifi</groupId>
            <artifactId>nifi-standard-processors</artifactId>
        </dependency>
        
        <!-- Testing -->
        <dependency>
            <groupId>org.apache.nifi</groupId>
            <artifactId>nifi-mock</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-core</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

### NAR Module POM (nifi-sensys-processors-nar/pom.xml)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.sensys.nifi</groupId>
        <artifactId>nifi-sensys-processors-parent</artifactId>
        <version>1.0.0</version>
    </parent>

    <artifactId>nifi-sensys-processors-nar</artifactId>
    <packaging>nar</packaging>

    <dependencies>
        <dependency>
            <groupId>com.sensys.nifi</groupId>
            <artifactId>nifi-sensys-processors</artifactId>
            <version>1.0.0</version>
        </dependency>
        
        <!-- Parent NAR dependency -->
        <dependency>
            <groupId>org.apache.nifi</groupId>
            <artifactId>nifi-standard-services-api-nar</artifactId>
            <version>${nifi.version}</version>
            <type>nar</type>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.nifi</groupId>
                <artifactId>nifi-nar-maven-plugin</artifactId>
                <extensions>true</extensions>
            </plugin>
        </plugins>
    </build>
</project>
```

### Processor Service Definition

Create file: `nifi-sensys-processors/src/main/resources/META-INF/services/org.apache.nifi.processor.Processor`

```
com.sensys.nifi.processors.ValidatedPutDatabaseRecord
```

## Build and Deployment

### Build Commands

```bash
# Clean and build
mvn clean package

# Skip tests
mvn clean package -DskipTests

# Build with specific Java version
export JAVA_HOME=/path/to/java-21
mvn clean package

# Verify NAR contents
jar -tf nifi-sensys-processors-nar/target/nifi-sensys-processors-nar-1.0.0.nar
```

### Installation

```bash
# 1. Copy NAR to NiFi lib directory
cp nifi-sensys-processors-nar/target/nifi-sensys-processors-nar-1.0.0.nar \
   /opt/nifi/nifi-current/lib/

# 2. Restart NiFi
/opt/nifi/nifi-current/bin/nifi.sh restart

# 3. Verify processor is available
# Navigate to NiFi UI → Add Processor → Search for "ValidatedPutDatabaseRecord"
```

## Configuration in NiFi

### Controller Services Setup

1. **SSE Engine DBCP (DBCP_NCBA_SSE_ENGINE)**:
   ```
   Database Connection URL: jdbc:mysql://localhost:3306/sse_engine
   Database Driver Class Name: com.mysql.cj.jdbc.Driver
   Database Driver Location(s): /path/to/mysql-connector.jar
   Database User: sse_engine_user
   Password: ********
   ```

2. **Source DBCP (DBCP_NCBA_SSE_SOURCE)**:
   ```
   Database Connection URL: jdbc:oracle:thin:@source-host:1521/SOURCEDB
   Database Driver Class Name: oracle.jdbc.OracleDriver
   Database Driver Location(s): /path/to/ojdbc10.jar
   Database User: source_user
   Password: ********
   ```

3. **Target DBCP (DBCP_NCBA_SSE_TARGET)**:
   ```
   Database Connection URL: jdbc:postgresql://target-host:5432/targetdb
   Database Driver Class Name: org.postgresql.Driver
   Database Driver Location(s): /path/to/postgresql-42.7.1.jar
   Database User: target_user
   Password: ********
   ```

### Processor Configuration

```
Record Reader: <Your Record Reader (e.g., JsonTreeReader)>
Record Writer: Not used for PutDatabaseRecord
Statement Type: INSERT or UPDATE (depending on your needs)
Database Type: PostgreSQL (or your target type)
Schema Name: public
Table Name: ${tableName}  (or hardcoded)
Translate Field Names: true
Unmatched Field Behavior: Ignore
Unmatched Column Behavior: Fail on Unmatched
Update Keys: id  (for UPDATE/UPSERT)
Field Containing SQL: Not used

--- Custom Properties ---
Enable Validation: true
Operation ID: ${operation.id}
SSE Engine Database Connection Pool: DBCP_NCBA_SSE_ENGINE
Source Database Connection Pool: DBCP_NCBA_SSE_SOURCE
Target Database Connection Pool: DBCP_NCBA_SSE_TARGET
Validation Mode: STRICT
```

### FlowFile Attribute Setup

Ensure upstream processors set the `operation.id` attribute:

```
UpdateAttribute processor:
  operation.id = <operation_id_value>
  
Or from QueryDatabaseTable/ExecuteSQL:
  Extract from query results
```

### Example Flow

```
QueryDatabaseTable (SOURCE)
  ↓
ConvertRecord (to JSON)
  ↓
UpdateAttribute (set operation.id)
  ↓
ValidatedPutDatabaseRecord
  ↓ (success)
  → LogAttribute ("Records inserted successfully")
  ↓ (validation_failed)
  → LogAttribute ("Validation failed") → PublishKafka (alert)
  ↓ (failure)
  → LogAttribute ("Processing failed") → PutEmail (alert)
```

## Monitoring and Troubleshooting

### Logging

Add to `nifi/conf/logback.xml`:

```xml
<logger name="com.sensys.nifi.processors" level="INFO"/>

<!-- For debugging -->
<logger name="com.sensys.nifi.processors.ValidatedPutDatabaseRecord" level="DEBUG"/>
```

### Key Log Messages

```
INFO  - Environment validation passed for operation_id=op123, environment=Production
WARN  - Environment validation warning for operation_id=op123: ...
ERROR - Environment validation failed for operation_id=op123: Controller service mismatch
ERROR - Failed to query environment configuration: Connection refused
```

### FlowFile Attributes for Debugging

Check these attributes in LogAttribute processor:

- `operation.id` - Operation ID
- `validation.passed` - true/false
- `validation.environment.id` - Environment ID
- `validation.environment.name` - Environment name
- `validation.error` - Error message if failed
- `validation.timestamp` - Validation timestamp

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

## Performance Considerations

### Connection Pooling

Configure DBCP services with appropriate pool sizes:

```
Max Total Connections: 10
Min Idle Connections: 2
Max Wait Time: 30 seconds
```

### Caching (Future Enhancement)

For high-throughput scenarios, consider caching environment configurations:

```java
private final LoadingCache<String, EnvironmentConfig> envConfigCache = 
    CacheBuilder.newBuilder()
        .maximumSize(100)
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build(...);
```

### Batch Processing

The processor validates once per FlowFile. For bulk operations:
- Use MergeRecord before ValidatedPutDatabaseRecord
- Ensure all records in merged FlowFile have same operation_id

## Security Considerations

1. **Sensitive Properties**: All DBCP passwords are marked sensitive
2. **SQL Injection**: Uses PreparedStatement for all queries
3. **Connection Handling**: Properly closes all JDBC resources
4. **Error Messages**: Avoid exposing sensitive details in validation errors

## References

### SSE Codebase Files

- `sse_engine/controller/environments/EnvironmentConnectionPoolController.java` (Lines 267-277)
- `sse_engine/model/operations/Operation.java`
- `sse_engine/model/operations/DataMigrationRecord.java`
- `sse_engine/model/environment/EnvironmentConfiguration.java`
- `sse_engine/model/database_models/DatabaseConfiguration.java`
- `sse_engine/src/main/resources/db/migration/mysql/V1__Init_Database_Configurations.sql`
- `sse_engine/src/main/resources/db/migration/mysql/V2__Create_Dependencies_And_Environments.sql`
- `sse_engine/src/main/resources/db/migration/mysql/V8__Create_Operation_Tracking_Tables.sql`
- `sse_engine/src/main/resources/db/migration/mysql/V13__Add_Environment_Id.sql`
- `sse_engine/src/main/resources/application.properties` (Lines 184-187)

### NiFi Documentation

- [Processor Development Guide](https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html)
- [PutDatabaseRecord Documentation](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.25.0/org.apache.nifi.processors.standard.PutDatabaseRecord/index.html)
- [DBCP Service](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-dbcp-service-nar/)
- [Property Expression Language](https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html)

### SQL Query Reference

**Query environment by operation_id via data_migration_records**:
```sql
SELECT DISTINCT
    o.id as operation_id,
    o.environment_id,
    e.name as environment_name,
    sc.database_type, sc.host, sc.port, sc.db_name,  -- source
    tc.database_type, tc.host, tc.port, tc.db_name   -- target
FROM data_migration_records dmr
INNER JOIN operations o ON dmr.operation_id = o.id
INNER JOIN environment_configurations e ON o.environment_id = e.id
INNER JOIN database_configurations sc ON e.source_config_id = sc.id AND sc.deleted = FALSE
INNER JOIN database_configurations tc ON e.target_config_id = tc.id AND tc.deleted = FALSE
WHERE dmr.operation_id = ?
LIMIT 1
```

