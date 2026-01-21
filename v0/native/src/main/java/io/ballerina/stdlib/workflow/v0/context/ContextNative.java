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

package io.ballerina.stdlib.workflow.v0.context;

import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.*;
import io.ballerina.stdlib.workflow.v0.utils.TypesUtil;
import io.ballerina.stdlib.workflow.v0.worker.WorkflowWorkerNative;
import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Native implementation for workflow Context operations (V0).
 */
public class ContextNative {

    /**
     * Execute an activity.
     */
    public static Object executeActivity(
            BString activityName,
            Object... args) {
        try {
            // Register the activity dynamically if not already registered
            WorkflowWorkerNative.registerActivity(activityName.getValue());
            
            // Configure activity options
            ActivityOptions options = ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setScheduleToCloseTimeout(Duration.ofSeconds(60))
                    .setRetryOptions(io.temporal.common.RetryOptions.newBuilder()
                            .setMaximumAttempts(3)
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .setBackoffCoefficient(2.0)
                            .build())
                    .build();

            // Convert Ballerina args to Java
            Object[] javaArgs = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                javaArgs[i] = TypesUtil.convertBallerinaToJava(args[i]);
            }

            // Execute activity
            Object result = Workflow.newUntypedActivityStub(options)
                    .execute(activityName.getValue(), Object.class, javaArgs);

            // Convert result back to Ballerina
            return TypesUtil.convertJavaToBallerinaType(result);

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Activity execution failed: " + e.getMessage()));
        }
    }

    /**
     * Execute an activity with the function pointer provided directly.
     */
    public static Object executeActivityWithFunction(
            BString activityName,
            BFunctionPointer activityFunc,
            Object... args) {
        try {
            // Register the activity directly with the provided function pointer
            WorkflowWorkerNative.registerActivityWithFunction(activityName.getValue(), activityFunc);
            
            // Configure activity options
            ActivityOptions options = ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setScheduleToCloseTimeout(Duration.ofSeconds(60))
                    .setRetryOptions(io.temporal.common.RetryOptions.newBuilder()
                            .setMaximumAttempts(3)
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .setBackoffCoefficient(2.0)
                            .build())
                    .build();

            // Convert Ballerina args to Java
            Object[] javaArgs = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                javaArgs[i] = TypesUtil.convertBallerinaToJava(args[i]);
            }

            // Execute activity
            Object result = Workflow.newUntypedActivityStub(options)
                    .execute(activityName.getValue(), Object.class, javaArgs);

            // Convert result back to Ballerina
            return TypesUtil.convertJavaToBallerinaType(result);

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Activity execution failed: " + e.getMessage()));
        }
    }

    /**
     * Wait for a signal.
     */
    public static Object awaitSignal(BString signalName, long timeoutSeconds) {
        try {
            Object signalResult = SignalAwaitWrapper.awaitSignal(
                    signalName.getValue(),
                    (int) timeoutSeconds
            );

            if (signalResult != null) {
                // If result is already a Ballerina type, return as-is
                if (signalResult instanceof io.ballerina.runtime.api.values.BValue) {
                    return signalResult;
                }

                // If result is a Java Map, convert to Ballerina map
                if (signalResult instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> data = (Map<String, Object>) signalResult;

                    @SuppressWarnings("unchecked")
                    BMap<BString, Object> ballerinaMap = ValueCreator.createMapValue(
                            TypeCreator.createMapType(PredefinedTypes.TYPE_ANYDATA));
                    for (Map.Entry<String, Object> entry : data.entrySet()) {
                        ballerinaMap.put(
                                StringUtils.fromString(entry.getKey()),
                                TypesUtil.convertJavaToBallerinaType(entry.getValue())
                        );
                    }
                    return ballerinaMap;
                }

                return TypesUtil.convertJavaToBallerinaType(signalResult);
            } else {
                return ErrorCreator.createError(
                        StringUtils.fromString("Timeout waiting for signal: " + signalName.getValue()));
            }
        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Await signal failed: " + e.getMessage()));
        }
    }

    /**
     * Sleep for specified duration.
     */
    public static Object sleep(long seconds) {
        try {
            WorkflowAwaitWrapper.sleep((int) seconds);
            return null;
        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Sleep failed: " + e.getMessage()));
        }
    }

    /**
     * Call activity with type parameter (dependently-typed).
     */
    public static Object callActivityTyped(
            BTypedesc targetType,
            BFunctionPointer activityFn,
            Object... args) {
        try {
            // Extract function name from the function pointer
            String activityName = activityFn.getType().getName();

            // Configure activity options
            ActivityOptions options = ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setScheduleToCloseTimeout(Duration.ofSeconds(60))
                    .setRetryOptions(io.temporal.common.RetryOptions.newBuilder()
                            .setMaximumAttempts(3)
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .setBackoffCoefficient(2.0)
                            .build())
                    .build();

            // Convert Ballerina args to Java
            Object[] javaArgs = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                javaArgs[i] = TypesUtil.convertBallerinaToJava(args[i]);
            }

            // Execute activity
            Object result = Workflow.newUntypedActivityStub(options)
                    .execute(activityName, Object.class, javaArgs);

            // Convert result back to Ballerina with target type
            return TypesUtil.convertJavaToBallerinaType(result);

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Activity execution failed: " + e.getMessage()));
        }
    }

    /**
     * Await signal with type parameter (dependently-typed).
     */
    public static Object awaitSignalTyped(
            BTypedesc targetType,
            BString signalName,
            long timeoutSeconds) {
        // Delegate to regular awaitSignal
        return awaitSignal(signalName, timeoutSeconds);
    }

    /**
     * Record a signal (for signal handler methods).
     */
    public static Object recordSignal(BString signalName, BMap<BString, Object> signalData) {
        Map<String, Object> javaMap = new HashMap<>();

        if (signalData != null) {
            for (BString key : signalData.getKeys()) {
                javaMap.put(key.getValue(), signalData.get(key));
            }
        }

        SignalAwaitWrapper.recordSignal(signalName.getValue(), javaMap);
        return null;
    }
}
