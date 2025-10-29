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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for SseExecuteSQL processor.
 * 
 * Note: These are basic tests to verify the processor can be instantiated
 * and basic configuration works. Full integration tests would require
 * actual NiFi environment with database connections.
 */
public class SseExecuteSQLTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(SseExecuteSQL.class);
    }

    @Test
    public void testProcessorInstantiation() {
        // Test that the processor can be instantiated without errors
        assertNotNull(testRunner);
        assertNotNull(testRunner.getProcessor());
        assertTrue(testRunner.getProcessor() instanceof SseExecuteSQL);
    }

    @Test
    public void testValidationDisabled() {
        // Test that when validation is disabled, processor behaves like standard ExecuteSQL
        testRunner.setProperty(SseExecuteSQL.ENABLE_VALIDATION, "false");
        
        // Should not require validation-specific properties when disabled
        testRunner.assertValid();
    }

    @Test
    public void testValidationEnabledRequiresProperties() {
        // Test that when validation is enabled, required properties must be set
        testRunner.setProperty(SseExecuteSQL.ENABLE_VALIDATION, "true");
        
        // Should be invalid without required validation properties
        testRunner.assertNotValid();
        
        // Add required properties
        testRunner.setProperty(SseExecuteSQL.OPERATION_ID, "test-operation");
        testRunner.setProperty(SseExecuteSQL.VALIDATION_MODE, "STRICT");
        
        // Still invalid without DBCP services
        testRunner.assertNotValid();
    }

    @Test
    public void testValidationModeValues() {
        testRunner.setProperty(SseExecuteSQL.ENABLE_VALIDATION, "true");
        testRunner.setProperty(SseExecuteSQL.OPERATION_ID, "test-operation");
        
        // Test STRICT mode
        testRunner.setProperty(SseExecuteSQL.VALIDATION_MODE, "STRICT");
        testRunner.assertValid();
        
        // Test WARNING mode
        testRunner.setProperty(SseExecuteSQL.VALIDATION_MODE, "WARNING");
        testRunner.assertValid();
        
        // Test invalid mode
        testRunner.setProperty(SseExecuteSQL.VALIDATION_MODE, "INVALID");
        testRunner.assertNotValid();
    }

    @Test
    public void testOperationIdExpressionLanguage() {
        // Test that operation ID supports expression language
        testRunner.setProperty(SseExecuteSQL.ENABLE_VALIDATION, "true");
        testRunner.setProperty(SseExecuteSQL.OPERATION_ID, "${operation.id}");
        testRunner.setProperty(SseExecuteSQL.VALIDATION_MODE, "STRICT");
        
        // Should be valid with expression language
        testRunner.assertValid();
    }

}
