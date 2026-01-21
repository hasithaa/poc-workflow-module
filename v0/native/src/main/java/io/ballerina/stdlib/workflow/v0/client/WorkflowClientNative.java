/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.workflow.v0.client;

import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.*;
import io.ballerina.stdlib.workflow.v0.utils.TypesUtil;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;

import java.time.Duration;
import java.util.UUID;

/**
 * Native implementation for workflow client operations (V0 - simplified API).
 */
public class WorkflowClientNative {

    /**
     * Start a workflow with simple ID generation.
     */
    public static Object startWorkflowSimple(
            Object clientHandle,
            BString workflowType,
            BString workflowId,
            BString taskQueue,
            Object... args) {
        try {
            if (!(clientHandle instanceof WorkflowClient)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid client handle"));
            }

            WorkflowClient client = (WorkflowClient) clientHandle;
            String wfId = (workflowId == null || workflowId.getValue().isEmpty()) 
                    ? UUID.randomUUID().toString() 
                    : workflowId.getValue();

            // Build workflow options
            WorkflowOptions options = WorkflowOptions.newBuilder()
                    .setWorkflowId(wfId)
                    .setTaskQueue(taskQueue.getValue())
                    .setWorkflowExecutionTimeout(Duration.ofHours(1))
                    .build();

            // Create untyped workflow stub
            WorkflowStub workflow = client.newUntypedWorkflowStub(
                    workflowType.getValue(),
                    options
            );

            // Convert Ballerina args to Java
            Object[] javaArgs = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                javaArgs[i] = TypesUtil.convertBallerinaToJava(args[i]);
            }

            // Start workflow asynchronously
            workflow.start(javaArgs);

            return StringUtils.fromString(wfId);

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to start workflow: " + e.getMessage()));
        }
    }

    /**
     * Send signal to workflow by ID.
     */
    public static Object sendSignalSimple(
            Object clientHandle,
            BString workflowId,
            BString signalName,
            Object... args) {
        try {
            if (!(clientHandle instanceof WorkflowClient)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid client handle"));
            }

            WorkflowClient client = (WorkflowClient) clientHandle;

            // Get untyped workflow stub by ID
            WorkflowStub workflow = client.newUntypedWorkflowStub(workflowId.getValue());

            // Convert Ballerina args to Java
            Object[] javaArgs = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                javaArgs[i] = TypesUtil.convertBallerinaToJava(args[i]);
            }

            // Send signal
            workflow.signal(signalName.getValue(), javaArgs);

            return null;

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to send signal: " + e.getMessage()));
        }
    }

    /**
     * Query workflow state by ID.
     */
    public static Object queryWorkflowSimple(
            Object clientHandle,
            BString workflowId,
            BString queryName,
            Object... args) {
        try {
            if (!(clientHandle instanceof WorkflowClient)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid client handle"));
            }

            WorkflowClient client = (WorkflowClient) clientHandle;

            // Get untyped workflow stub by ID
            WorkflowStub workflow = client.newUntypedWorkflowStub(workflowId.getValue());

            // Convert Ballerina args to Java
            Object[] javaArgs = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                javaArgs[i] = TypesUtil.convertBallerinaToJava(args[i]);
            }

            // Execute query
            Object result = workflow.query(queryName.getValue(), Object.class, javaArgs);

            // Convert result back to Ballerina type
            return TypesUtil.convertJavaToBallerinaType(result);

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to query workflow: " + e.getMessage()));
        }
    }
}
