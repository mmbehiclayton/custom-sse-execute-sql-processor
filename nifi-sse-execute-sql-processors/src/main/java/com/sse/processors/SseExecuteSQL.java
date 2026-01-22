/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sse.processors;

import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.AbstractExecuteSQL;
import org.apache.nifi.processors.standard.sql.DefaultAvroSqlWriter;
import org.apache.nifi.processors.standard.sql.SqlWriter;
import org.apache.nifi.util.db.AvroUtil.CodecType;
import org.apache.nifi.util.db.JdbcCommon;

import java.util.List;
import java.util.Set;
import java.sql.*;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.apache.nifi.util.db.JdbcProperties.DEFAULT_PRECISION;
import static org.apache.nifi.util.db.JdbcProperties.DEFAULT_SCALE;
import static org.apache.nifi.util.db.JdbcProperties.NORMALIZE_NAMES_FOR_AVRO;
import static org.apache.nifi.util.db.JdbcProperties.USE_AVRO_LOGICAL_TYPES;
import static org.apache.nifi.processors.standard.AbstractExecuteSQL.*;

@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({ "sql", "select", "jdbc", "query", "database", "sse", "validation" })
@CapabilityDescription("SSE ExecuteSQL - Executes provided SQL select query with optional environment validation. " +
        "When validation is disabled, functions exactly like standard ExecuteSQL. " +
        "When validation is enabled, validates that controller services match the expected " +
        "environment configuration by querying the SSE Engine database using operation_id " +
        "from FlowFile attributes. **IMPORTANT: When validation is enabled, SQL execution " +
        "is gated by validation results - the SQL query will ONLY execute if validation passes.** " +
        "Query result will be converted to Avro format. Streaming is used so arbitrarily large result sets are supported. "
        +
        "This processor can be scheduled to run on a timer, or cron expression, using the standard scheduling methods, "
        +
        "or it can be triggered by an incoming FlowFile. If it is triggered by an incoming FlowFile, then attributes " +
        "of that FlowFile will be available when evaluating the select query, and the query may use the ? to escape parameters. "
        +
        "In this case, the parameters to use must exist as FlowFile attributes with the naming convention sql.args.N.type "
        +
        "and sql.args.N.value, where N is a positive integer. The sql.args.N.type is expected to be a number indicating "
        +
        "the JDBC Type. The content of the FlowFile is expected to be in UTF-8 format. " +
        "FlowFile attribute 'executesql.row.count' indicates how many rows were selected.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "sql.args.N.type", description = "Incoming FlowFiles are expected to be parametrized SQL statements. The type of each Parameter is specified as an integer "
                + "that represents the JDBC Type of the parameter. The following types are accepted: [LONGNVARCHAR: -16], [BIT: -7], [BOOLEAN: 16], [TINYINT: -6], [BIGINT: -5], "
                + "[LONGVARBINARY: -4], [VARBINARY: -3], [BINARY: -2], [LONGVARCHAR: -1], [CHAR: 1], [NUMERIC: 2], [DECIMAL: 3], [INTEGER: 4], [SMALLINT: 5] "
                + "[FLOAT: 6], [REAL: 7], [DOUBLE: 8], [VARCHAR: 12], [DATE: 91], [TIME: 92], [TIMESTAMP: 93], [VARCHAR: 12], [CLOB: 2005], [NCLOB: 2011]"),
        @ReadsAttribute(attribute = "sql.args.N.value", description = "Incoming FlowFiles are expected to be parametrized SQL statements. The value of the Parameters are specified as "
                + "sql.args.1.value, sql.args.2.value, sql.args.3.value, and so on. The type of the sql.args.1.value Parameter is specified by the sql.args.1.type attribute."),
        @ReadsAttribute(attribute = "sql.args.N.format", description = "This attribute is always optional, but default options may not always work for your data. "
                + "Incoming FlowFiles are expected to be parametrized SQL statements. In some cases "
                + "a format option needs to be specified, currently this is only applicable for binary data types, dates, times and timestamps. Binary Data Types (defaults to 'ascii') - "
                + "ascii: each string character in your attribute value represents a single byte. This is the format provided by Avro Processors. "
                + "base64: the string is a Base64 encoded string that can be decoded to bytes. "
                + "hex: the string is hex encoded with all letters in upper case and no '0x' at the beginning. "
                + "Dates/Times/Timestamps - "
                + "Date, Time and Timestamp formats all support both custom formats or named format ('yyyy-MM-dd','ISO_OFFSET_DATE_TIME') "
                + "as specified according to java.time.format.DateTimeFormatter. "
                + "If not specified, a long value input is expected to be an unix epoch (milli seconds from 1970/1/1), or a string value in "
                + "'yyyy-MM-dd' format for Date, 'HH:mm:ss.SSS' for Time (some database engines e.g. Derby or MySQL do not support milliseconds and will truncate milliseconds), "
                + "'yyyy-MM-dd HH:mm:ss.SSS' for Timestamp is used."),
        @ReadsAttribute(attribute = "operation_id", description = "Operation ID used for environment validation when validation is enabled.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "executesql.row.count", description = "Contains the number of rows returned by the query. "
                + "If 'Max Rows Per Flow File' is set, then this number will reflect the number of rows in the Flow File instead of the entire result set."),
        @WritesAttribute(attribute = "executesql.query.duration", description = "Combined duration of the query execution time and fetch time in milliseconds. "
                + "If 'Max Rows Per Flow File' is set, then this number will reflect only the fetch time for the rows in the Flow File instead of the entire result set."),
        @WritesAttribute(attribute = "executesql.query.executiontime", description = "Duration of the query execution time in milliseconds. "
                + "This number will reflect the query execution time regardless of the 'Max Rows Per Flow File' setting."),
        @WritesAttribute(attribute = "executesql.query.fetchtime", description = "Duration of the result set fetch time in milliseconds. "
                + "If 'Max Rows Per Flow File' is set, then this number will reflect only the fetch time for the rows in the Flow File instead of the entire result set."),
        @WritesAttribute(attribute = "executesql.resultset.index", description = "Assuming multiple result sets are returned, "
                + "the zero based index of this result set."),
        @WritesAttribute(attribute = "executesql.error.message", description = "If processing an incoming flow file causes "
                + "an Exception, the Flow File is routed to failure and this attribute is set to the exception message."),
        @WritesAttribute(attribute = "fragment.identifier", description = "If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet. If Output Batch Size is set, then this "
                + "attribute will not be populated."),
        @WritesAttribute(attribute = "fragment.index", description = "If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "input.flowfile.uuid", description = "If the processor has an incoming connection, outgoing FlowFiles will have this attribute "
                + "set to the value of the input FlowFile's UUID. If there is no incoming connection, the attribute will not be added."),
        @WritesAttribute(attribute = "validation.passed", description = "true if validation passed"),
        @WritesAttribute(attribute = "validation.environment.id", description = "Environment ID used for validation"),
        @WritesAttribute(attribute = "validation.environment.name", description = "Environment name"),
        @WritesAttribute(attribute = "validation.timestamp", description = "Validation timestamp"),
        @WritesAttribute(attribute = "validation.error", description = "Validation error message if failed")
})
@SupportsSensitiveDynamicProperties
@DynamicProperties({
        @DynamicProperty(name = "sql.args.N.type", value = "SQL type argument to be supplied", description = "Incoming FlowFiles are expected to be parametrized SQL statements. The type of each Parameter is specified as an integer "
                + "that represents the JDBC Type of the parameter. The following types are accepted: [LONGNVARCHAR: -16], [BIT: -7], [BOOLEAN: 16], [TINYINT: -6], [BIGINT: -5], "
                + "[LONGVARBINARY: -4], [VARBINARY: -3], [BINARY: -2], [LONGVARCHAR: -1], [CHAR: 1], [NUMERIC: 2], [DECIMAL: 3], [INTEGER: 4], [SMALLINT: 5] "
                + "[FLOAT: 6], [REAL: 7], [DOUBLE: 8], [VARCHAR: 12], [DATE: 91], [TIME: 92], [TIMESTAMP: 93], [VARCHAR: 12], [CLOB: 2005], [NCLOB: 2011]"),
        @DynamicProperty(name = "sql.args.N.value", value = "Argument to be supplied", description = "Incoming FlowFiles are expected to be parametrized SQL statements. The value of the Parameters are specified as "
                + "sql.args.1.value, sql.args.2.value, sql.args.3.value, and so on. The type of the sql.args.1.value Parameter is specified by the sql.args.1.type attribute."),
        @DynamicProperty(name = "sql.args.N.format", value = "SQL format argument to be supplied", description = "This attribute is always optional, but default options may not always work for your data. "
                + "Incoming FlowFiles are expected to be parametrized SQL statements. In some cases "
                + "a format option needs to be specified, currently this is only applicable for binary data types, dates, times and timestamps. Binary Data Types (defaults to 'ascii') - "
                + "ascii: each string character in your attribute value represents a single byte. This is the format provided by Avro Processors. "
                + "base64: the string is a Base64 encoded string that can be decoded to bytes. "
                + "hex: the string is hex encoded with all letters in upper case and no '0x' at the beginning. "
                + "Dates/Times/Timestamps - "
                + "Date, Time and Timestamp formats all support both custom formats or named format ('yyyy-MM-dd','ISO_OFFSET_DATE_TIME') "
                + "as specified according to java.time.format.DateTimeFormatter. "
                + "If not specified, a long value input is expected to be an unix epoch (milli seconds from 1970/1/1), or a string value in "
                + "'yyyy-MM-dd' format for Date, 'HH:mm:ss.SSS' for Time (some database engines e.g. Derby or MySQL do not support milliseconds and will truncate milliseconds), "
                + "'yyyy-MM-dd HH:mm:ss.SSS' for Timestamp is used.")
})
public class SseExecuteSQL extends AbstractExecuteSQL {

    public static final PropertyDescriptor COMPRESSION_FORMAT = new PropertyDescriptor.Builder()
            .name("compression-format")
            .displayName("Compression Format")
            .description("Compression type to use when writing Avro files. Default is None.")
            .allowableValues(CodecType.values())
            .defaultValue(CodecType.NONE.toString())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    // ============================================================================
    // VALIDATION PROPERTIES - These control the environment validation behavior
    // ============================================================================

    /**
     * Enable/Disable Validation Property
     * This is the master switch for validation functionality.
     * When set to "false", the processor behaves exactly like standard ExecuteSQL.
     * When set to "true", it performs environment validation before executing SQL.
     */
    public static final PropertyDescriptor ENABLE_VALIDATION = new PropertyDescriptor.Builder()
            .name("enable-validation")
            .displayName("Enable Validation")
            .description("Enable or disable environment validation. Set to false only for testing or emergency bypass.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    /**
     * Operation ID Property
     * This property tells the processor which FlowFile attribute contains the
     * operation ID.
     * The operation ID is used to look up environment configuration from the SSE
     * Engine database.
     * Default value is "${operation.id}" which means it reads from the
     * "operation.id" attribute.
     * Only required when validation is enabled.
     */
    public static final PropertyDescriptor OPERATION_ID = new PropertyDescriptor.Builder()
            .name("operation-id")
            .displayName("Operation ID")
            .description("The operation ID from FlowFile attributes. Used to look up environment configuration " +
                    "from sse_engine.data_migration_records table. **REQUIRED when Enable Validation is true.** " +
                    "Ignored when Enable Validation is false.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${operation_id}")
            .dependsOn(ENABLE_VALIDATION, "true")
            .build();

    /**
     * SSE Engine Database Connection Pool Property
     * This connects to the SSE Engine database (sse_engine schema) to query
     * environment configurations.
     * This is where we look up which environment the operation belongs to and what
     * the expected
     * source/target database configurations should be.
     * Typically configured as "DBCP_NCBA_SSE_ENGINE".
     * Only required when validation is enabled.
     */
    public static final PropertyDescriptor ENGINE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("engine-dbcp-service")
            .displayName("SSE Engine DBCP Service")
            .description("The DBCP Controller Service for connecting to the SSE Engine database " +
                    "(sse_engine schema) to query environment configurations. " +
                    "Typically DBCP_NCBA_SSE_ENGINE. **REQUIRED when Enable Validation is true.** " +
                    "Ignored when Enable Validation is false.")
            .required(false)
            .identifiesControllerService(DBCPService.class)
            .dependsOn(ENABLE_VALIDATION, "true")
            .build();

    /**
     * Source Database Connection Pool Property
     * This is the DBCP service that should connect to the source database according
     * to the environment configuration.
     * We validate that this service's connection URL matches what's expected for
     * the environment.
     * Typically configured as "DBCP_NCBA_SSE_SOURCE".
     * Only required when validation is enabled.
     */
    public static final PropertyDescriptor SOURCE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("source-dbcp-service")
            .displayName("Source DBCP Service")
            .description("The DBCP Controller Service to validate against the environment's source configuration. " +
                    "Typically DBCP_NCBA_SSE_SOURCE. **REQUIRED when Enable Validation is true.** " +
                    "Ignored when Enable Validation is false.")
            .required(false)
            .identifiesControllerService(DBCPService.class)
            .dependsOn(ENABLE_VALIDATION, "true")
            .build();

    /**
     * Target Database Connection Pool Property
     * This is the DBCP service that should connect to the target database according
     * to the environment configuration.
     * We validate that this service's connection URL matches what's expected for
     * the environment.
     * This service is ONLY used for validation, NOT for SQL execution.
     * SQL execution uses the main DBCP_SERVICE property (same as normal
     * ExecuteSQL).
     * Typically configured as "DBCP_NCBA_SSE_TARGET".
     * Only required when validation is enabled.
     */
    public static final PropertyDescriptor TARGET_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("target-dbcp-service")
            .displayName("Target DBCP Service")
            .description("The DBCP Controller Service to validate against the environment's target configuration. " +
                    "Typically DBCP_NCBA_SSE_TARGET. " +
                    "**REQUIRED when Enable Validation is true.** " +
                    "Ignored when Enable Validation is false.")
            .required(false)
            .identifiesControllerService(DBCPService.class)
            .dependsOn(ENABLE_VALIDATION, "true")
            .build();

    /**
     * Validation Mode Property
     * This controls what happens when validation fails:
     * - STRICT: Route FlowFile to validation_failed relationship (stops processing)
     * - WARNING: Log a warning but continue with ExecuteSQL operation
     * STRICT mode is recommended for production to prevent data corruption.
     * Only required when validation is enabled.
     */
    public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor.Builder()
            .name("validation-mode")
            .displayName("Validation Mode")
            .description("Determines validation behavior: STRICT (route to failure on mismatch) or " +
                    "WARNING (log warning and continue). **REQUIRED when Enable Validation is true.** " +
                    "Ignored when Enable Validation is false.")
            .required(false)
            .defaultValue("STRICT")
            .allowableValues("STRICT", "WARNING")
            .dependsOn(ENABLE_VALIDATION, "true")
            .build();

    // ============================================================================
    // PROPERTY DESCRIPTORS LIST - All properties available in the processor UI
    // ============================================================================
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            // Validation control property (shown first for visibility)
            ENABLE_VALIDATION, // Master switch for validation

            // Standard ExecuteSQL properties (inherited from AbstractExecuteSQL)
            DBCP_SERVICE, // Main database connection for ExecuteSQL
            SQL_PRE_QUERY, // SQL to run before main query
            SQL_SELECT_QUERY, // **CORE PROPERTY** - The SQL SELECT query to execute
            SQL_POST_QUERY, // SQL to run after main query
            QUERY_TIMEOUT, // Timeout for query execution
            NORMALIZE_NAMES_FOR_AVRO, // Convert column names for Avro format
            USE_AVRO_LOGICAL_TYPES, // Use Avro logical types
            COMPRESSION_FORMAT, // Compression for output files
            DEFAULT_PRECISION, // Default precision for numeric types
            DEFAULT_SCALE, // Default scale for numeric types
            MAX_ROWS_PER_FLOW_FILE, // Limit rows per output file
            OUTPUT_BATCH_SIZE, // Batch size for output
            FETCH_SIZE, // JDBC fetch size
            AUTO_COMMIT, // Auto-commit setting

            // Validation properties (only used when ENABLE_VALIDATION is true)
            OPERATION_ID, // FlowFile attribute containing operation ID
            ENGINE_DBCP_SERVICE, // Connection to SSE Engine database
            SOURCE_DBCP_SERVICE, // Source database connection to validate
            TARGET_DBCP_SERVICE, // Target database connection to validate
            VALIDATION_MODE // STRICT or WARNING mode
    );

    // ============================================================================
    // RELATIONSHIPS - Where FlowFiles are routed after processing
    // ============================================================================

    /**
     * Validation Failed Relationship
     * FlowFiles that fail environment validation are routed here.
     * This happens when the controller services don't match the expected
     * environment configuration.
     * Only used when validation is enabled and validation mode is STRICT.
     */
    public static final Relationship REL_VALIDATION_FAILED = new Relationship.Builder()
            .name("validation_failed")
            .description("FlowFiles that fail environment validation are routed to this relationship")
            .build();

    /**
     * All Relationships List
     * This includes the standard ExecuteSQL relationships plus our custom
     * validation relationship.
     * - REL_SUCCESS: FlowFiles that processed successfully (from ExecuteSQL)
     * - REL_FAILURE: FlowFiles that failed processing (from ExecuteSQL)
     * - REL_VALIDATION_FAILED: FlowFiles that failed validation (custom)
     */
    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS, // Standard ExecuteSQL success
            REL_FAILURE, // Standard ExecuteSQL failure
            REL_VALIDATION_FAILED // Custom validation failure
    );

    public SseExecuteSQL() {
        relationships = RELATIONSHIPS;
        propDescriptors = PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        // Note: Property migration constants may not be available in all NiFi versions
        // This method can be extended if specific property migrations are needed
    }

    @Override
    protected SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context, FlowFile fileToProcess) {
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean();
        final Boolean useAvroLogicalTypes = context.getProperty(USE_AVRO_LOGICAL_TYPES).asBoolean();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE)
                .evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer defaultPrecision = context.getProperty(DEFAULT_PRECISION)
                .evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer defaultScale = context.getProperty(DEFAULT_SCALE).evaluateAttributeExpressions(fileToProcess)
                .asInteger();
        final String codec = context.getProperty(COMPRESSION_FORMAT).getValue();

        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions.builder()
                .convertNames(convertNamesForAvro)
                .useLogicalTypes(useAvroLogicalTypes)
                .defaultPrecision(defaultPrecision)
                .defaultScale(defaultScale)
                .maxRows(maxRowsPerFlowFile)
                .codecFactory(codec)
                .build();
        return new DefaultAvroSqlWriter(options);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators
                        .createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .build();
    }

    // ============================================================================
    // MAIN PROCESSING METHOD - This is where the validation logic happens
    // ============================================================================

    /**
     * Override the onTrigger method to add environment validation before ExecuteSQL
     * processing.
     * This method is called by NiFi for each FlowFile that needs to be processed.
     * 
     * **CRITICAL BEHAVIOR**: When validation is enabled, SQL execution is GATED by
     * validation results.
     * The SQL query will ONLY execute if validation passes. This is the core safety
     * feature.
     * 
     * Processing Flow:
     * 1. Get the incoming FlowFile
     * 2. Check if validation is enabled
     * 3. If validation disabled → Call standard ExecuteSQL (super.onTrigger) - SQL
     * executes normally
     * 4. If validation enabled → Perform environment validation
     * 5. If validation passes → Call standard ExecuteSQL - SQL executes
     * 6. If validation fails → Route to validation_failed (STRICT) or continue with
     * warning (WARNING)
     * **IMPORTANT**: In STRICT mode, SQL is NEVER executed if validation fails
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        // Step 1: Check if validation is enabled first
        // This is the master switch - when false, processor behaves like standard
        // ExecuteSQL
        boolean validationEnabled = context.getProperty(ENABLE_VALIDATION).asBoolean();

        if (!validationEnabled) {
            // Validation is disabled - proceed with standard ExecuteSQL behavior
            // SQL execution proceeds normally without any validation checks
            getLogger().debug("Environment validation is DISABLED - proceeding with standard ExecuteSQL");
            super.onTrigger(context, session); // Call the parent ExecuteSQL onTrigger method
            return;
        }

        // Step 2: Validation is enabled - get the incoming FlowFile from the session
        // If no FlowFile is available, return immediately (nothing to process)
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Step 3: Validation is enabled - perform environment validation
        try {
            // First, validate that all required validation properties are provided
            String operationId = context.getProperty(OPERATION_ID)
                    .evaluateAttributeExpressions(flowFile)
                    .getValue();

            if (operationId == null || operationId.trim().isEmpty()) {
                getLogger().error("Validation is enabled but operation-id property is not set or empty");
                flowFile = session.putAttribute(flowFile, "validation.error",
                        "Validation enabled but operation-id property is not set");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            DBCPService engineDbcp = context.getProperty(ENGINE_DBCP_SERVICE)
                    .asControllerService(DBCPService.class);
            DBCPService sourceDbcp = context.getProperty(SOURCE_DBCP_SERVICE)
                    .asControllerService(DBCPService.class);
            DBCPService targetDbcp = context.getProperty(TARGET_DBCP_SERVICE)
                    .asControllerService(DBCPService.class);

            if (engineDbcp == null || sourceDbcp == null || targetDbcp == null) {
                getLogger().error("Validation is enabled but one or more DBCP services are not configured");
                flowFile = session.putAttribute(flowFile, "validation.error",
                        "Validation enabled but required DBCP services are not configured");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            // Call our custom validation method
            ValidationResult validationResult = performEnvironmentValidation(context, flowFile);

            if (!validationResult.isValid()) {
                // Step 4a: Validation failed - handle based on validation mode
                String validationMode = context.getProperty(VALIDATION_MODE).getValue();

                if ("STRICT".equals(validationMode)) {
                    // STRICT mode: Stop processing and route to validation_failed
                    // **CRITICAL**: SQL execution is BLOCKED - the query will NOT execute

                    // Add validation failure attributes to the FlowFile for debugging
                    flowFile = session.putAttribute(flowFile, "validation.passed", "false");
                    flowFile = session.putAttribute(flowFile, "validation.error", validationResult.getErrorMessage());
                    flowFile = session.putAttribute(flowFile, "validation.timestamp",
                            String.valueOf(System.currentTimeMillis()));

                    // Log the validation failure for monitoring
                    getLogger().error("Environment validation failed for operation_id={}: {} - SQL execution BLOCKED",
                            validationResult.getOperationId(), validationResult.getErrorMessage());

                    // Route FlowFile to validation_failed relationship (stops processing)
                    // **IMPORTANT**: super.onTrigger() is NEVER called, so SQL never executes
                    session.transfer(flowFile, REL_VALIDATION_FAILED);
                    return;
                } else {
                    // WARNING mode: Log warning but continue with ExecuteSQL
                    // **NOTE**: SQL execution proceeds despite validation failure
                    getLogger().warn(
                            "Environment validation warning for operation_id={}: {} - Proceeding with SQL execution anyway",
                            validationResult.getOperationId(), validationResult.getErrorMessage());
                }
            } else {
                // Step 4b: Validation passed - add success attributes and continue

                // Add validation success attributes to the FlowFile
                flowFile = session.putAttribute(flowFile, "validation.passed", "true");
                flowFile = session.putAttribute(flowFile, "validation.environment.id",
                        String.valueOf(validationResult.getEnvironmentId()));
                flowFile = session.putAttribute(flowFile, "validation.environment.name",
                        validationResult.getEnvironmentName());
                flowFile = session.putAttribute(flowFile, "validation.timestamp",
                        String.valueOf(System.currentTimeMillis()));

                // Log successful validation for monitoring
                getLogger().info("Environment validation passed for operation_id={}, environment={}",
                        validationResult.getOperationId(), validationResult.getEnvironmentName());
            }

            // Step 5: Proceed with standard ExecuteSQL operation
            // **IMPORTANT**: We can't call super.onTrigger() because we already retrieved
            // the FlowFile
            // Instead, we need to process the FlowFile ourselves and transfer it properly

            // Get the SQL query - support both scenarios:
            // 1. SQL query configured in processor property (static)
            // 2. SQL query in FlowFile content (dynamic from ExecuteScript)
            String sqlQuery = context.getProperty(SQL_SELECT_QUERY)
                    .evaluateAttributeExpressions(flowFile)
                    .getValue();

            // If SQL query is not in processor property, try reading from FlowFile content
            // This matches the behavior of normal ExecuteSQL which can read SQL from
            // content
            if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
                getLogger().debug("SQL query not found in processor property, reading from FlowFile content");

                final StringBuilder sqlBuilder = new StringBuilder();
                session.read(flowFile, inputStream -> {
                    sqlBuilder.append(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
                });

                sqlQuery = sqlBuilder.toString();
            }

            // Final validation - if still no SQL query found, fail
            if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
                getLogger().error("SQL SELECT query is not configured in processor property or FlowFile content");
                flowFile = session.putAttribute(flowFile, "executesql.error",
                        "SQL SELECT query is not configured in processor property or FlowFile content");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            // Normalize: some drivers reject trailing semicolons; remove a single trailing
            // ';' if present
            String normalizedSql = sqlQuery.trim();
            if (normalizedSql.endsWith(";")) {
                normalizedSql = normalizedSql.substring(0, normalizedSql.length() - 1).trim();
            }

            // Helpful debug logging to diagnose driver parse errors (e.g., SQL92 token
            // position)
            getLogger().debug("Executing SQL (length={}): {}", normalizedSql.length(), normalizedSql);

            // Get the main DBCP service for SQL execution (same as normal ExecuteSQL)
            // **CRITICAL FIX**: Use DBCP_SERVICE, not TARGET_DBCP_SERVICE for SQL execution
            // TARGET_DBCP_SERVICE is only for validation, not for executing queries
            DBCPService mainDbcp = context.getProperty(DBCP_SERVICE)
                    .asControllerService(DBCPService.class);

            if (mainDbcp == null) {
                getLogger().error("DBCP Service is not configured");
                flowFile = session.putAttribute(flowFile, "executesql.error", "DBCP Service is not configured");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            // Execute the SQL query using the main DBCP service
            executeSqlQuery(session, context, flowFile, mainDbcp, normalizedSql);

        } catch (Exception e) {
            // Step 6: Handle any unexpected errors during validation
            getLogger().error("Error during environment validation: {}", e.getMessage(), e);

            // Add error attribute and route to failure
            flowFile = session.putAttribute(flowFile, "validation.error",
                    "Validation error: " + e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    // ============================================================================
    // VALIDATION HELPER CLASSES AND METHODS
    // ============================================================================

    /**
     * ValidationResult Class
     * This class holds the result of environment validation.
     * It contains information about whether validation passed or failed,
     * and details about the environment and any error messages.
     */
    private static class ValidationResult {
        private final boolean valid; // true if validation passed
        private final String operationId; // the operation ID that was validated
        private final Long environmentId; // environment ID (null if validation failed)
        private final String environmentName; // environment name (null if validation failed)
        private final String errorMessage; // error message (null if validation passed)

        // Private constructor - use static factory methods instead
        private ValidationResult(boolean valid, String operationId, Long environmentId,
                String environmentName, String errorMessage) {
            this.valid = valid;
            this.operationId = operationId;
            this.environmentId = environmentId;
            this.environmentName = environmentName;
            this.errorMessage = errorMessage;
        }

        // Factory method for successful validation
        public static ValidationResult success(String operationId, Long environmentId, String environmentName) {
            return new ValidationResult(true, operationId, environmentId, environmentName, null);
        }

        // Factory method for failed validation
        public static ValidationResult failure(String operationId, String errorMessage) {
            return new ValidationResult(false, operationId, null, null, errorMessage);
        }

        // Getters
        public boolean isValid() {
            return valid;
        }

        public String getOperationId() {
            return operationId;
        }

        public Long getEnvironmentId() {
            return environmentId;
        }

        public String getEnvironmentName() {
            return environmentName;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    /**
     * EnvironmentConfig Class
     * This class holds environment configuration data retrieved from the SSE Engine
     * database.
     * It contains both source and target database configuration details.
     */
    private static class EnvironmentConfig {
        // Environment details
        Long environmentId; // Unique ID of the environment
        String environmentName; // Human-readable name of the environment

        // Source database configuration
        String sourceDbType; // Database type (mysql, postgresql, oracle, sqlserver)
        String sourceHost; // Hostname/IP of source database
        Integer sourcePort; // Port number of source database
        String sourceDatabase; // Database name of source database
        Boolean sourceIsSid; // Whether source database uses SID (Oracle only)

        // Target database configuration
        String targetDbType; // Database type (mysql, postgresql, oracle, sqlserver)
        String targetHost; // Hostname/IP of target database
        Integer targetPort; // Port number of target database
        String targetDatabase; // Database name of target database
        Boolean targetIsSid; // Whether target database uses SID (Oracle only)
    }

    /**
     * Main validation method that performs environment validation.
     * This method orchestrates the entire validation process:
     * 1. Extract operation_id from FlowFile attributes
     * 2. Query SSE Engine database for environment configuration
     * 3. Extract JDBC URLs from controller services
     * 4. Compare actual URLs with expected URLs
     * 5. Return validation result
     */
    private ValidationResult performEnvironmentValidation(ProcessContext context, FlowFile flowFile)
            throws ProcessException {

        // Step 1: Extract operation_id from FlowFile attributes
        String operationId = context.getProperty(OPERATION_ID)
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        if (operationId == null || operationId.trim().isEmpty()) {
            return ValidationResult.failure(operationId, "operation_id is empty or not set");
        }

        getLogger().debug("Validating environment for operation_id: {}", operationId);

        // Step 2: Get controller services from context
        DBCPService engineDbcp = context.getProperty(ENGINE_DBCP_SERVICE)
                .asControllerService(DBCPService.class);
        DBCPService sourceDbcp = context.getProperty(SOURCE_DBCP_SERVICE)
                .asControllerService(DBCPService.class);
        DBCPService targetDbcp = context.getProperty(TARGET_DBCP_SERVICE)
                .asControllerService(DBCPService.class);

        // Step 3: Query environment configuration from SSE Engine database
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

        // Step 4: Extract JDBC URLs from controller services
        String sourceJdbcUrl;
        String targetJdbcUrl;
        try {
            sourceJdbcUrl = extractJdbcUrlFromService(sourceDbcp);
            targetJdbcUrl = extractJdbcUrlFromService(targetDbcp);
        } catch (SQLException e) {
            return ValidationResult.failure(operationId,
                    "Failed to extract JDBC URLs from controller services: " + e.getMessage());
        }

        // Step 5: Build expected JDBC URLs from environment configuration
        String expectedSourceUrl = buildJdbcUrl(
                envConfig.sourceDbType,
                envConfig.sourceHost,
                envConfig.sourcePort,
                envConfig.sourceDatabase,
                envConfig.sourceIsSid != null ? envConfig.sourceIsSid : false);

        String expectedTargetUrl = buildJdbcUrl(
                envConfig.targetDbType,
                envConfig.targetHost,
                envConfig.targetPort,
                envConfig.targetDatabase,
                envConfig.targetIsSid != null ? envConfig.targetIsSid : false);

        // Log URLs for debugging (at debug level to avoid cluttering logs)
        getLogger().debug("Expected Source URL: {}", expectedSourceUrl);
        getLogger().debug("Actual Source URL: {}", sourceJdbcUrl);
        getLogger().debug("Expected Target URL: {}", expectedTargetUrl);
        getLogger().debug("Actual Target URL: {}", targetJdbcUrl);

        // Step 6: Compare URLs
        boolean sourceMatches = compareJdbcUrls(sourceJdbcUrl, expectedSourceUrl);
        boolean targetMatches = compareJdbcUrls(targetJdbcUrl, expectedTargetUrl);

        if (!sourceMatches || !targetMatches) {
            // Validation failed - create detailed error message
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
                    targetJdbcUrl);
            return ValidationResult.failure(operationId, errorMsg);
        }

        // Validation passed
        return ValidationResult.success(operationId, envConfig.environmentId, envConfig.environmentName);
    }

    /**
     * Queries environment configuration from SSE Engine database using
     * operation_id.
     * 
     * This method performs a complex SQL join to get all the environment details:
     * 1. Starts with data_migration_records table using operation_id
     * 2. Joins to operations table to get environment_id
     * 3. Joins to environment_configurations table to get environment details
     * 4. Joins to database_configurations table twice (for source and target)
     * 
     * @param engineDbcp  The DBCP service connected to SSE Engine database
     * @param operationId The operation ID to look up
     * @return EnvironmentConfig object with all configuration details, or null if
     *         not found
     * @throws SQLException if there's a database error
     */
    private EnvironmentConfig queryEnvironmentConfig(DBCPService engineDbcp, String operationId)
            throws SQLException {

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // Get connection from the DBCP service
            conn = engineDbcp.getConnection();

            // Updated SQL query to start from operations table directly
            // This matches your working test queries and removes dependency on
            // data_migration_records
            String sql = """
                    SELECT DISTINCT
                        o.id as operation_id,
                        o.environment_id,
                        e.name as environment_name,
                        e.status as environment_status,
                        -- Source configuration details
                        sc.id as source_config_id,
                        sc.database_type as source_db_type,
                        sc.host as source_host,
                        sc.port as source_port,
                        sc.db_name as source_database,
                        sc.name as source_name,
                        sc.is_sid as source_is_sid,
                        -- Target configuration details
                        tc.id as target_config_id,
                        tc.database_type as target_db_type,
                        tc.host as target_host,
                        tc.port as target_port,
                        tc.db_name as target_database,
                        tc.name as target_name,
                        tc.is_sid as target_is_sid
                    FROM operations o
                    INNER JOIN environment_configurations e ON o.environment_id = e.id
                    INNER JOIN database_configurations sc ON e.source_config_id = sc.id AND sc.deleted = FALSE
                    INNER JOIN database_configurations tc ON e.target_config_id = tc.id AND tc.deleted = FALSE
                    WHERE o.id = ?
                    LIMIT 1
                    """;

            // Prepare the statement with the operation_id parameter
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, operationId);

            // Execute the query
            rs = stmt.executeQuery();

            if (!rs.next()) {
                // No results found - operation_id doesn't exist or has no environment
                // configuration
                getLogger().warn("No environment configuration found for operation_id: {}", operationId);
                return null;
            }

            // Build environment configuration object from query results
            EnvironmentConfig config = new EnvironmentConfig();

            // Environment details
            config.environmentId = rs.getLong("environment_id");
            config.environmentName = rs.getString("environment_name");

            // Source database configuration
            config.sourceDbType = rs.getString("source_db_type");
            config.sourceHost = rs.getString("source_host");
            config.sourcePort = rs.getInt("source_port");
            config.sourceDatabase = rs.getString("source_database");
            // Handle is_sid - can be null, so check for null and default to false
            int sourceIsSidInt = rs.getInt("source_is_sid");
            config.sourceIsSid = rs.wasNull() ? false : (sourceIsSidInt != 0);

            // Target database configuration
            config.targetDbType = rs.getString("target_db_type");
            config.targetHost = rs.getString("target_host");
            config.targetPort = rs.getInt("target_port");
            config.targetDatabase = rs.getString("target_database");
            // Handle is_sid - can be null, so check for null and default to false
            int targetIsSidInt = rs.getInt("target_is_sid");
            config.targetIsSid = rs.wasNull() ? false : (targetIsSidInt != 0);

            getLogger().info("Retrieved environment configuration: {} (ID: {})",
                    config.environmentName, config.environmentId);

            return config;

        } finally {
            // Always close database resources in reverse order
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    getLogger().debug("Error closing ResultSet", e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    getLogger().debug("Error closing Statement", e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    getLogger().debug("Error closing Connection", e);
                }
            }
        }
    }

    /**
     * Extracts JDBC URL from a DBCP service by getting connection metadata.
     * 
     * This method:
     * 1. Gets a connection from the DBCP service
     * 2. Gets the database metadata from the connection
     * 3. Extracts the URL from the metadata
     * 4. Closes the connection
     * 
     * @param dbcpService The DBCP service to extract URL from
     * @return The JDBC URL string
     * @throws SQLException if there's a database error
     */
    private String extractJdbcUrlFromService(DBCPService dbcpService) throws SQLException {
        Connection conn = null;
        try {
            // Get a connection from the DBCP service
            conn = dbcpService.getConnection();

            // Get database metadata from the connection
            DatabaseMetaData metadata = conn.getMetaData();

            // Extract the URL from the metadata
            String url = metadata.getURL();
            return url;
        } finally {
            // Always close the connection
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
     * Builds JDBC URL using the same logic as EnvironmentConnectionPoolController.
     * 
     * This method creates JDBC URLs in the standard format for different database
     * types:
     * - Oracle: jdbc:oracle:thin:@host:port/database (service name) or
     * jdbc:oracle:thin:@host:port:database (SID)
     * - PostgreSQL: jdbc:postgresql://host:port/database
     * - MySQL: jdbc:mysql://host:port/database
     * - SQL Server: jdbc:sqlserver://host:port;databaseName=database
     * 
     * @param dbType   Database type (oracle, postgresql, mysql, sqlserver)
     * @param host     Hostname or IP address
     * @param port     Port number
     * @param database Database name
     * @param isSid    Whether the database uses SID (Oracle only, true = use :,
     *                 false = use /)
     * @return Formatted JDBC URL
     * @throws IllegalArgumentException if database type is not supported
     */
    private String buildJdbcUrl(String dbType, String host, int port, String database, boolean isSid) {
        // Use switch expression (Java 14+ feature) to build URL based on database type
        String baseUrl = switch (dbType.toLowerCase()) {
            case "oracle" -> {
                // Oracle: use ":" for SID, "/" for service name
                if (isSid) {
                    yield "jdbc:oracle:thin:@%s:%d:%s";
                } else {
                    yield "jdbc:oracle:thin:@%s:%d/%s";
                }
            }
            case "postgresql" -> "jdbc:postgresql://%s:%d/%s";
            case "mysql" -> "jdbc:mysql://%s:%d/%s";
            case "sqlserver" -> "jdbc:sqlserver://%s:%d;databaseName=%s";
            default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
        };

        // Format the URL with the provided parameters
        return String.format(baseUrl, host, port, database);
    }

    /**
     * Compares two JDBC URLs for equivalence.
     * 
     * This method handles normalization and case-insensitive comparison:
     * 1. Normalizes both URLs (removes query parameters, trailing slashes, etc.)
     * 2. Compares them case-insensitively
     * 
     * @param url1 First JDBC URL to compare
     * @param url2 Second JDBC URL to compare
     * @return true if URLs are equivalent, false otherwise
     */
    private boolean compareJdbcUrls(String url1, String url2) {
        if (url1 == null || url2 == null) {
            return false;
        }

        // Normalize both URLs before comparison
        String normalized1 = normalizeJdbcUrl(url1);
        String normalized2 = normalizeJdbcUrl(url2);

        // Compare normalized URLs case-insensitively
        return normalized1.equalsIgnoreCase(normalized2);
    }

    /**
     * Normalizes JDBC URL for comparison by:
     * 1. Removing query parameters (everything after ?)
     * 2. Removing trailing slashes
     * 3. Trimming whitespace
     * 4. Converting to lowercase
     * 
     * This ensures that URLs like:
     * - "jdbc:postgresql://host:5432/db?param=value"
     * - "jdbc:postgresql://host:5432/db/"
     * - "jdbc:postgresql://HOST:5432/DB"
     * 
     * All become: "jdbc:postgresql://host:5432/db"
     * 
     * @param jdbcUrl The JDBC URL to normalize
     * @return Normalized JDBC URL
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

        // Remove trailing slashes using regex
        jdbcUrl = jdbcUrl.replaceAll("/+$", "");

        // Normalize whitespace and convert to lowercase
        return jdbcUrl.trim().toLowerCase();
    }

    /**
     * Executes the SQL query using the provided DBCP service.
     * This method handles the actual SQL execution and result processing.
     * The FlowFile is ALWAYS transferred (to either REL_SUCCESS or REL_FAILURE)
     * before this method returns.
     * 
     * @param session     The process session
     * @param context     The process context
     * @param flowFile    The FlowFile to process
     * @param dbcpService The DBCP service to use for database connection
     * @param sqlQuery    The SQL query to execute
     */
    private void executeSqlQuery(ProcessSession session, ProcessContext context, FlowFile flowFile,
            DBCPService dbcpService, String sqlQuery) {

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // Get database connection
            conn = dbcpService.getConnection();

            // Prepare the SQL statement
            stmt = conn.prepareStatement(sqlQuery);

            // Execute the query
            boolean hasResultSet = stmt.execute(sqlQuery);

            if (hasResultSet) {
                // Scenario 1: Query returned a ResultSet (SELECT)
                rs = stmt.getResultSet();

                // Configure the SQL writer
                SqlWriter sqlWriter = configureSqlWriter(session, context, flowFile);

                // Process the result set
                // Write the result set directly to the original FlowFile
                try (OutputStream out = session.write(flowFile)) {
                    long rowCount = sqlWriter.writeResultSet(rs, out, getLogger(), null);

                    // Add row count attribute
                    flowFile = session.putAttribute(flowFile, "executesql.row.count", String.valueOf(rowCount));

                    // Transfer to success relationship
                    session.transfer(flowFile, REL_SUCCESS);

                    getLogger().info("Successfully executed SQL query, returned {} rows", rowCount);
                } catch (Exception e) {
                    getLogger().error("Error writing result set: {}", e.getMessage(), e);
                    flowFile = session.putAttribute(flowFile, "executesql.error",
                            "Error writing result set: " + e.getMessage());
                    session.transfer(flowFile, REL_FAILURE);
                }
            } else {
                // Scenario 2: Query did not return a ResultSet (INSERT, UPDATE, DELETE, DDL)
                int updateCount = stmt.getUpdateCount();

                // Clear the FlowFile content as there is no result set
                flowFile = session.write(flowFile, out -> {
                });

                // Add row count attribute (update count)
                flowFile = session.putAttribute(flowFile, "executesql.row.count", String.valueOf(updateCount));

                // Transfer to success relationship
                session.transfer(flowFile, REL_SUCCESS);

                getLogger().info("Successfully executed SQL update, affected {} rows", updateCount);
            }

        } catch (Exception e) {
            // Catch ALL exceptions (SQLException, ProcessException, RuntimeException, etc.)
            // and ensure FlowFile is transferred to failure
            getLogger().error("SQL execution failed: {}", e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, "executesql.error", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            // Close database resources
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    getLogger().debug("Error closing ResultSet", e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    getLogger().debug("Error closing Statement", e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    getLogger().debug("Error closing Connection", e);
                }
            }
        }
    }
}