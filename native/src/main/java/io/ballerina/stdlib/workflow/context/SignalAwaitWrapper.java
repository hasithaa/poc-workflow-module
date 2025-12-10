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
 * CRITICAL: Workflow.await() must be called directly from workflow thread.
 */
public class SignalAwaitWrapper {

    /**
     * Signal queue - stores received signals with their data.
     * Key: signal name
     * Value: Queue of signal data (supports multiple signals with same name)
     */
    private static final Map<String, LinkedBlockingQueue<Map<String, String>>> signalQueues = 
        new ConcurrentHashMap<>();

    /**
     * Wait for a specific signal by name.
     * 
     * @param signalName The name of the signal to wait for
     * @param timeoutSeconds Timeout in seconds
     * @return Map containing signal data, or null if timeout
     */
    public static Map<String, String> awaitSignal(String signalName, int timeoutSeconds) {
        // Ensure queue exists for this signal
        signalQueues.putIfAbsent(signalName, new LinkedBlockingQueue<>());
        LinkedBlockingQueue<Map<String, String>> queue = signalQueues.get(signalName);
        
        // Use Workflow.await() to wait for signal to be queued
        // MUST be called directly from workflow thread
        boolean received = Workflow.await(
            Duration.ofSeconds(timeoutSeconds),
            () -> !queue.isEmpty()
        );
        
        if (received) {
            // Signal received - get the data
            Map<String, String> signalData = queue.poll();
            return signalData;
        } else {
            // Timeout
            return null;
        }
    }

    /**
     * Record that a signal was received.
     * This should be called from the signal handler method.
     * 
     * @param signalName The name of the signal
     * @param signalData Data associated with the signal
     */
    public static void recordSignal(String signalName, Map<String, String> signalData) {
        signalQueues.putIfAbsent(signalName, new LinkedBlockingQueue<>());
        LinkedBlockingQueue<Map<String, String>> queue = signalQueues.get(signalName);
        queue.offer(signalData != null ? signalData : new HashMap<>());
    }

    /**
     * Wait for any of multiple signals.
     * Returns the name of the signal that was received first.
     * 
     * @param signalNames Array of signal names to wait for
     * @param timeoutSeconds Timeout in seconds
     * @return SignalResult containing signal name and data
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
                LinkedBlockingQueue<Map<String, String>> queue = signalQueues.get(signalName);
                if (!queue.isEmpty()) {
                    Map<String, String> data = queue.poll();
                    return new SignalResult(signalName, data);
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
        LinkedBlockingQueue<Map<String, String>> queue = signalQueues.get(signalName);
        if (queue != null && !queue.isEmpty()) {
            Map<String, String> data = queue.poll();
            return new SignalResult(signalName, data);
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
        private final Map<String, String> data;
        
        public SignalResult(String signalName, Map<String, String> data) {
            this.signalName = signalName;
            this.data = data;
        }
        
        public String getSignalName() {
            return signalName;
        }
        
        public Map<String, String> getData() {
            return data;
        }
        
        public String get(String key) {
            return data != null ? data.get(key) : null;
        }
        
        @Override
        public String toString() {
            return "SignalResult{signal='" + signalName + "', data=" + data + "}";
        }
    }
}
