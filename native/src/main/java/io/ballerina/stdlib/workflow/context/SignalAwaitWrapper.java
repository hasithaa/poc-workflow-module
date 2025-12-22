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

package io.ballerina.stdlib.workflow.context;

import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Signal-based await wrapper for waiting on specific signals.
 * 
 * CRITICAL THREADING MODEL:
 * -------------------------
 * Workflow.await() does NOT block the calling thread in the traditional sense.
 * Instead, it uses Temporal's coroutine/continuation mechanism:
 * 
 * 1. When Workflow.await() is called, Temporal captures the workflow state
 * 2. The workflow execution yields back to Temporal (returns control)
 * 3. The original thread is released and can be reused for other workflows
 * 4. When the condition becomes true (signal arrives), Temporal resumes the workflow
 * 5. The workflow continues from where it left off (deterministic replay)
 * 
 * This means:
 * - No Ballerina scheduler threads are held during signal waiting
 * - Workflows can wait for hours/days without consuming resources
 * - The waiting state is persisted in Temporal's history
 * - During replay, awaits complete instantly if condition is already met
 * 
 * The workflow thread (whether from Ballerina scheduler or Temporal worker pool)
 * is only active during actual code execution, not during waits.
 */
public class SignalAwaitWrapper {

    /**
     * Signal queue - stores received signals with their results.
     * Key: signal name
     * Value: Queue of signal results (anydata - can be Map, BError, primitive, or any Ballerina value)
     * Supports multiple signals with same name in queue.
     */
    private static final Map<String, LinkedBlockingQueue<Object>> signalQueues =
        new ConcurrentHashMap<>();

    /**
     * Wait for a specific signal by name.
     * Returns the signal result (from remote method if defined, or signal data Map).
     * 
     * @param signalName The name of the signal to wait for
     * @param timeoutSeconds Timeout in seconds
     * @return Signal result (anydata), or null if timeout
     */
    public static Object awaitSignal(String signalName, int timeoutSeconds) {
        // Ensure queue exists for this signal
        signalQueues.putIfAbsent(signalName, new LinkedBlockingQueue<>());
        LinkedBlockingQueue<Object> queue = signalQueues.get(signalName);
        
        // Use Workflow.await() to wait for signal to be queued
        // MUST be called directly from workflow thread
        boolean received = Workflow.await(
            Duration.ofSeconds(timeoutSeconds),
            () -> !queue.isEmpty()
        );
        
        if (received) {
            // Signal received - get the result
            Object signalResult = queue.poll();
            return signalResult;
        } else {
            // Timeout
            return null;
        }
    }

    /**
     * Record that a signal was received with its result.
     * This should be called from the signal handler method.
     * 
     * @param signalName The name of the signal
     * @param signalResult Result from signal handler remote method (anydata) or signal data Map
     */
    public static void recordSignal(String signalName, Object signalResult) {
        signalQueues.putIfAbsent(signalName, new LinkedBlockingQueue<>());
        LinkedBlockingQueue<Object> queue = signalQueues.get(signalName);
        // Store the actual result (could be Map, BError, primitive, or any Ballerina value)
        queue.offer(signalResult != null ? signalResult : new HashMap<>());
    }

    /**
     * Wait for any of multiple signals.
     * Returns the name of the signal that was received first and its result.
     * 
     * @param signalNames Array of signal names to wait for
     * @param timeoutSeconds Timeout in seconds
     * @return SignalResult containing signal name and result data
     */
    public static SignalResult awaitAnySignal(String[] signalNames, int timeoutSeconds) {
        // Ensure queues exist
        for (String signalName : signalNames) {
            signalQueues.putIfAbsent(signalName, new LinkedBlockingQueue<>());
        }
        
        // Wait for any signal to arrive
        boolean received = Workflow.await(
            Duration.ofSeconds(timeoutSeconds),
            () -> {
                for (String signalName : signalNames) {
                    if (!signalQueues.get(signalName).isEmpty()) {
                        return true;
                    }
                }
                return false;
            }
        );
        
        if (received) {
            // Find which signal was received
            for (String signalName : signalNames) {
                LinkedBlockingQueue<Object> queue = signalQueues.get(signalName);
                if (!queue.isEmpty()) {
                    Object result = queue.poll();
                    return new SignalResult(signalName, result);
                }
            }
        }
        
        return null;
    }

    /**
     * Check if a signal has been received without waiting.
     * Non-blocking check.
     * 
     * @param signalName Signal name to check
     * @return SignalResult if signal exists, null otherwise
     */
    public static SignalResult checkSignal(String signalName) {
        LinkedBlockingQueue<Object> queue = signalQueues.get(signalName);
        if (queue != null && !queue.isEmpty()) {
            Object result = queue.poll();
            return new SignalResult(signalName, result);
        }
        return null;
    }

    /**
     * Clear all pending signals for a workflow instance.
     * Call this at workflow start to ensure clean state.
     */
    public static void clearAllSignals() {
        signalQueues.clear();
    }

    /**
     * Result of a signal await operation.
     */
    public static class SignalResult {
        private final String signalName;
        private final Object data;
        
        public SignalResult(String signalName, Object data) {
            this.signalName = signalName;
            this.data = data;
        }
        
        public String getSignalName() {
            return signalName;
        }
        
        public Object getData() {
            return data;
        }
        
        public Object get(String key) {
            // Support map-like access if data is a Map
            if (data instanceof Map) {
                return ((Map<?, ?>) data).get(key);
            }
            return null;
        }
        
        @Override
        public String toString() {
            return "SignalResult{signal='" + signalName + "', data=" + data + "}";
        }
    }
}
