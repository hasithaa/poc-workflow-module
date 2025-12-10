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

package io.ballerina.stdlib.workflow.utils;

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for computing correlation-based workflow IDs.
 */
public class CorrelationUtils {

    /**
     * Compute workflow ID from workflow type and correlation data.
     * Format: WorkflowType-value1-value2-...
     *
     * @param workflowType Workflow type name
     * @param correlationData Map of correlation keys and values
     * @param correlationKeys Ordered list of correlation keys
     * @param separator Separator character (default: "-")
     * @return Computed workflow ID
     */
    public static String computeWorkflowId(
            String workflowType,
            BMap<BString, BString> correlationData,
            String[] correlationKeys,
            String separator) {
        
        if (separator == null || separator.isEmpty()) {
            separator = "-";
        }

        StringBuilder workflowId = new StringBuilder(workflowType);

        for (String key : correlationKeys) {
            BString value = correlationData.get(StringUtils.fromString(key));
            if (value != null) {
                workflowId.append(separator).append(value.getValue());
            } else {
                throw new IllegalArgumentException("Missing correlation key: " + key);
            }
        }

        return workflowId.toString();
    }

    /**
     * Compute workflow ID with default separator.
     *
     * @param workflowType Workflow type name
     * @param correlationData Map of correlation keys and values
     * @param correlationKeys Ordered list of correlation keys
     * @return Computed workflow ID
     */
    public static String computeWorkflowId(
            String workflowType,
            BMap<BString, BString> correlationData,
            String[] correlationKeys) {
        return computeWorkflowId(workflowType, correlationData, correlationKeys, "-");
    }

    /**
     * Generate workflow ID from workflow type and correlation data (Java Map version).
     * Uses all correlation data values in sorted key order.
     *
     * @param workflowType Workflow type name
     * @param correlationData Java Map of correlation data
     * @return Generated workflow ID
     */
    public static String generateWorkflowId(
            String workflowType,
            java.util.Map<String, String> correlationData) {
        
        StringBuilder workflowId = new StringBuilder(workflowType);
        
        // Sort keys for consistent ordering
        List<String> sortedKeys = new ArrayList<>(correlationData.keySet());
        java.util.Collections.sort(sortedKeys);
        
        for (String key : sortedKeys) {
            String value = correlationData.get(key);
            if (value != null) {
                workflowId.append("-").append(value);
            }
        }
        
        return workflowId.toString();
    }

    /**
     * Resolve workflow ID from correlation data.
     * Currently returns the first value found, but could be enhanced
     * to use a stored mapping or Temporal query.
     *
     * @param correlationData Java Map of correlation data
     * @return Workflow ID or null if not found
     */
    public static String resolveWorkflowId(java.util.Map<String, String> correlationData) {
        // Simple implementation: use "workflowId" key if present
        if (correlationData.containsKey("workflowId")) {
            return correlationData.get("workflowId");
        }
        
        // Alternative: construct from available correlation data
        // This is a placeholder - in production, you might:
        // 1. Query a correlation mapping service
        // 2. Use Temporal's list/query APIs
        // 3. Maintain a correlation cache
        
        // For now, return null to indicate ID must be provided explicitly
        return null;
    }
}
