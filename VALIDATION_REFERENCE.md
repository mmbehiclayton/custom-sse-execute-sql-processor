# SsePutSQL Validation Reference

Quick reference for implementing environment validation in SsePutSQL, based on SseExecuteSQL patterns.

## Core Validation Pattern

### 1. Master Switch Property
```java
public static final PropertyDescriptor ENABLE_VALIDATION = new PropertyDescriptor.Builder()
    .name("enable-validation")
    .displayName("Enable Validation")
    .description("Enable or disable environment validation. Set to false only for testing or emergency bypass.")
    .required(true)
    .defaultValue("false")
    .allowableValues("true", "false")
    .build();
```

**Key Point**: When `false`, processor behaves exactly like standard PutSQL (no validation).

### 2. Required Validation Properties

All validation properties should:
- Be `required(false)` but have `.dependsOn(ENABLE_VALIDATION, "true")`
- Only appear in UI when validation is enabled

```java
// Operation ID from FlowFile attributes
OPERATION_ID = new PropertyDescriptor.Builder()
    .name("operation-id")
    .displayName("Operation ID")
    .description("The operation ID from FlowFile attributes. Used to look up environment configuration.")
    .required(false)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .defaultValue("${operation.id}")
    .dependsOn(ENABLE_VALIDATION, "true")
    .build();

// SSE Engine database connection
ENGINE_DBCP_SERVICE = new PropertyDescriptor.Builder()
    .name("engine-dbcp-service")
    .displayName("SSE Engine DBCP Service")
    .description("DBCP Controller Service for SSE Engine database (sse_engine schema).")
    .required(false)
    .identifiesControllerService(DBCPService.class)
    .dependsOn(ENABLE_VALIDATION, "true")
    .build();

// Source database connection to validate
SOURCE_DBCP_SERVICE = new PropertyDescriptor.Builder()
    .name("source-dbcp-service")
    .displayName("Source DBCP Service")
    .description("DBCP Controller Service to validate against environment's source configuration.")
    .required(false)
    .identifiesControllerService(DBCPService.class)
    .dependsOn(ENABLE_VALIDATION, "true")
    .build();

// Target database connection to validate
TARGET_DBCP_SERVICE = new PropertyDescriptor.Builder()
    .name("target-dbcp-service")
    .displayName("Target DBCP Service")
    .description("DBCP Controller Service to validate against environment's target configuration.")
    .required(false)
    .identifiesControllerService(DBCPService.class)
    .dependsOn(ENABLE_VALIDATION, "true")
    .build();

// Validation mode: STRICT or WARNING
VALIDATION_MODE = new PropertyDescriptor.Builder()
    .name("validation-mode")
    .displayName("Validation Mode")
    .description("STRICT (route to failure on mismatch) or WARNING (log warning and continue).")
    .required(false)
    .defaultValue("STRICT")
    .allowableValues("STRICT", "WARNING")
    .dependsOn(ENABLE_VALIDATION, "true")
    .build();
```

### 3. Custom Relationship

```java
public static final Relationship REL_VALIDATION_FAILED = new Relationship.Builder()
    .name("validation_failed")
    .description("FlowFiles that fail environment validation are routed to this relationship")
    .build();
```

### 4. onTrigger() Override Pattern

```java
@Override
public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    // Step 1: Check master switch
    boolean validationEnabled = context.getProperty(ENABLE_VALIDATION).asBoolean();
    
    if (!validationEnabled) {
        // No validation - proceed with standard PutSQL
        super.onTrigger(context, session);
        return;
    }
    
    // Step 2: Get FlowFile
    FlowFile flowFile = session.get();
    if (flowFile == null) {
        return;
    }
    
    // Step 3: Validate required properties
    String operationId = context.getProperty(OPERATION_ID)
        .evaluateAttributeExpressions(flowFile)
        .getValue();
    
    if (operationId == null || operationId.trim().isEmpty()) {
        flowFile = session.putAttribute(flowFile, "validation.error", 
            "Validation enabled but operation-id property is not set");
        session.transfer(flowFile, REL_FAILURE);
        return;
    }
    
    // Get DBCP services
    DBCPService engineDbcp = context.getProperty(ENGINE_DBCP_SERVICE)
        .asControllerService(DBCPService.class);
    DBCPService sourceDbcp = context.getProperty(SOURCE_DBCP_SERVICE)
        .asControllerService(DBCPService.class);
    DBCPService targetDbcp = context.getProperty(TARGET_DBCP_SERVICE)
        .asControllerService(DBCPService.class);
    
    if (engineDbcp == null || sourceDbcp == null || targetDbcp == null) {
        flowFile = session.putAttribute(flowFile, "validation.error", 
            "Validation enabled but required DBCP services are not configured");
        session.transfer(flowFile, REL_FAILURE);
        return;
    }
    
    // Step 4: Perform validation
    ValidationResult validationResult = performEnvironmentValidation(context, flowFile);
    
    if (!validationResult.isValid()) {
        String validationMode = context.getProperty(VALIDATION_MODE).getValue();
        
        if ("STRICT".equals(validationMode)) {
            // BLOCK execution - route to validation_failed
            flowFile = session.putAttribute(flowFile, "validation.passed", "false");
            flowFile = session.putAttribute(flowFile, "validation.error", 
                validationResult.getErrorMessage());
            flowFile = session.putAttribute(flowFile, "validation.timestamp", 
                String.valueOf(System.currentTimeMillis()));
            session.transfer(flowFile, REL_VALIDATION_FAILED);
            return; // CRITICAL: Don't call super.onTrigger()
        } else {
            // WARNING mode: Log but continue
            getLogger().warn("Environment validation warning: {} - Proceeding anyway", 
                validationResult.getErrorMessage());
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
    }
    
    // Step 5: Proceed with standard PutSQL (use target DBCP service)
    // Note: For PutSQL, you'll need to override DBCP_SERVICE usage to use targetDbcp
    // This may require custom implementation rather than calling super.onTrigger()
}
```

### 5. Validation Helper Classes

```java
// Validation result container
private static class ValidationResult {
    private final boolean valid;
    private final String operationId;
    private final Long environmentId;
    private final String environmentName;
    private final String errorMessage;
    
    public static ValidationResult success(String operationId, Long environmentId, String environmentName) {
        return new ValidationResult(true, operationId, environmentId, environmentName, null);
    }
    
    public static ValidationResult failure(String operationId, String errorMessage) {
        return new ValidationResult(false, operationId, null, null, errorMessage);
    }
    
    // Getters...
}

// Environment configuration container
private static class EnvironmentConfig {
    Long environmentId;
    String environmentName;
    String sourceDbType, sourceHost, sourceDatabase;
    Integer sourcePort;
    String targetDbType, targetHost, targetDatabase;
    Integer targetPort;
}
```

### 6. Core Validation Method

```java
private ValidationResult performEnvironmentValidation(ProcessContext context, FlowFile flowFile) {
    // 1. Extract operation_id
    String operationId = context.getProperty(OPERATION_ID)
        .evaluateAttributeExpressions(flowFile)
        .getValue();
    
    // 2. Get DBCP services
    DBCPService engineDbcp = context.getProperty(ENGINE_DBCP_SERVICE)
        .asControllerService(DBCPService.class);
    DBCPService sourceDbcp = context.getProperty(SOURCE_DBCP_SERVICE)
        .asControllerService(DBCPService.class);
    DBCPService targetDbcp = context.getProperty(TARGET_DBCP_SERVICE)
        .asControllerService(DBCPService.class);
    
    // 3. Query environment config from SSE Engine database
    EnvironmentConfig envConfig = queryEnvironmentConfig(engineDbcp, operationId);
    
    // 4. Extract JDBC URLs from controller services
    String sourceJdbcUrl = extractJdbcUrlFromService(sourceDbcp);
    String targetJdbcUrl = extractJdbcUrlFromService(targetDbcp);
    
    // 5. Build expected URLs from environment config
    String expectedSourceUrl = buildJdbcUrl(envConfig.sourceDbType, envConfig.sourceHost, 
        envConfig.sourcePort, envConfig.sourceDatabase);
    String expectedTargetUrl = buildJdbcUrl(envConfig.targetDbType, envConfig.targetHost, 
        envConfig.targetPort, envConfig.targetDatabase);
    
    // 6. Compare URLs
    boolean sourceMatches = compareJdbcUrls(sourceJdbcUrl, expectedSourceUrl);
    boolean targetMatches = compareJdbcUrls(targetJdbcUrl, expectedTargetUrl);
    
    if (!sourceMatches || !targetMatches) {
        String errorMsg = String.format(
            "Controller service mismatch for environment '%s' (ID: %d). " +
            "Source match: %s, Target match: %s.",
            envConfig.environmentName, envConfig.environmentId,
            sourceMatches, targetMatches);
        return ValidationResult.failure(operationId, errorMsg);
    }
    
    return ValidationResult.success(operationId, envConfig.environmentId, envConfig.environmentName);
}
```

### 7. Database Query Method

```java
private EnvironmentConfig queryEnvironmentConfig(DBCPService engineDbcp, String operationId) 
        throws SQLException {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    
    try {
        conn = engineDbcp.getConnection();
        String sql = """
            SELECT DISTINCT
                o.id as operation_id,
                o.environment_id,
                e.name as environment_name,
                sc.database_type as source_db_type,
                sc.host as source_host,
                sc.port as source_port,
                sc.db_name as source_database,
                tc.database_type as target_db_type,
                tc.host as target_host,
                tc.port as target_port,
                tc.db_name as target_database
            FROM operations o
            INNER JOIN environment_configurations e ON o.environment_id = e.id
            INNER JOIN database_configurations sc ON e.source_config_id = sc.id AND sc.deleted = FALSE
            INNER JOIN database_configurations tc ON e.target_config_id = tc.id AND tc.deleted = FALSE
            WHERE o.id = ?
            LIMIT 1
            """;
        
        stmt = conn.prepareStatement(sql);
        stmt.setString(1, operationId);
        rs = stmt.executeQuery();
        
        if (!rs.next()) {
            return null;
        }
        
        EnvironmentConfig config = new EnvironmentConfig();
        config.environmentId = rs.getLong("environment_id");
        config.environmentName = rs.getString("environment_name");
        config.sourceDbType = rs.getString("source_db_type");
        config.sourceHost = rs.getString("source_host");
        config.sourcePort = rs.getInt("source_port");
        config.sourceDatabase = rs.getString("source_database");
        config.targetDbType = rs.getString("target_db_type");
        config.targetHost = rs.getString("target_host");
        config.targetPort = rs.getInt("target_port");
        config.targetDatabase = rs.getString("target_database");
        
        return config;
    } finally {
        // Close resources: rs, stmt, conn
    }
}
```

### 8. JDBC URL Utilities

```java
// Extract URL from DBCP service
private String extractJdbcUrlFromService(DBCPService dbcpService) throws SQLException {
    Connection conn = null;
    try {
        conn = dbcpService.getConnection();
        DatabaseMetaData metadata = conn.getMetaData();
        return metadata.getURL();
    } finally {
        if (conn != null) conn.close();
    }
}

// Build expected URL
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

// Compare URLs (normalized, case-insensitive)
private boolean compareJdbcUrls(String url1, String url2) {
    if (url1 == null || url2 == null) return false;
    return normalizeJdbcUrl(url1).equalsIgnoreCase(normalizeJdbcUrl(url2));
}

// Normalize URL for comparison
private String normalizeJdbcUrl(String jdbcUrl) {
    if (jdbcUrl == null) return "";
    // Remove query parameters
    int queryIndex = jdbcUrl.indexOf('?');
    if (queryIndex > 0) {
        jdbcUrl = jdbcUrl.substring(0, queryIndex);
    }
    // Remove trailing slashes
    jdbcUrl = jdbcUrl.replaceAll("/+$", "");
    return jdbcUrl.trim().toLowerCase();
}
```

## FlowFile Attributes

Add these attributes to FlowFile:

**On Success:**
- `validation.passed` = "true"
- `validation.environment.id` = environment ID
- `validation.environment.name` = environment name
- `validation.timestamp` = current timestamp

**On Failure:**
- `validation.passed` = "false"
- `validation.error` = error message
- `validation.timestamp` = current timestamp

## Key Principles

1. **Master Switch**: When validation disabled, processor is identical to standard PutSQL
2. **STRICT Mode**: Blocks execution if validation fails (routes to `validation_failed`)
3. **WARNING Mode**: Logs warning but continues execution
4. **Property Dependencies**: All validation properties depend on `ENABLE_VALIDATION = "true"`
5. **Resource Cleanup**: Always close database connections in finally blocks
6. **Error Handling**: Catch exceptions, add attributes, route to appropriate relationship

## Differences for PutSQL

- PutSQL executes INSERT/UPDATE/DELETE statements (not SELECT)
- Use `targetDbcp` service for the actual SQL execution
- May need to override how PutSQL gets its DBCP service to use validated `targetDbcp`
- Consider validating that SQL statement type matches expected operation

