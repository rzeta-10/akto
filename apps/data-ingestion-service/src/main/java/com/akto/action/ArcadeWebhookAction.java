package com.akto.action;

import com.akto.gateway.Gateway;
import com.akto.log.LoggerMaker;
import com.akto.publisher.KafkaDataPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opensymphony.xwork2.Action;
import com.opensymphony.xwork2.ActionSupport;
import org.apache.struts2.ServletActionContext;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


@lombok.Getter
@lombok.Setter
public class ArcadeWebhookAction extends ActionSupport {

    private static final LoggerMaker loggerMaker =
            new LoggerMaker(ArcadeWebhookAction.class, LoggerMaker.LogDb.DATA_INGESTION);
    private static final Gateway gateway = Gateway.getInstance();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String ARCADE_HOST = "arcade.dev";
    private static final String AKTO_CONNECTOR = "arcade";
    private static final String HEADER_EXECUTION_ID = "x-arcade-execution-id";

    // Arcade API response codes
    private static final String CODE_OK = "OK";
    private static final String CODE_CHECK_FAILED = "CHECK_FAILED";

    static {
        gateway.setDataPublisher(new KafkaDataPublisher());
        loggerMaker.info("ArcadeWebhookAction initialized with KafkaDataPublisher");
    }


    private String execution_id;
    private Map<String, Object> tool;
    private Map<String, Object> inputs;
    private Map<String, Object> context;
    private String user_id;
    private Object toolkits;
    private Boolean success;
    private Object output;
    private String execution_code;
    private String execution_error;

    private String status;

    private String code;

    private String error_message;

    private Map<String, Object> override;

    private Map<String, Object> only;

    private Map<String, Object> deny;


    public String getExecution_id()              { return execution_id; }
    public void   setExecution_id(String v)      { this.execution_id = v; }
    public String getError_message()             { return error_message; }
    public void   setError_message(String v)     { this.error_message = v; }
    public String getExecution_code()            { return execution_code; }
    public void   setExecution_code(String v)    { this.execution_code = v; }
    public String getExecution_error()           { return execution_error; }
    public void   setExecution_error(String v)   { this.execution_error = v; }
    public String getUser_id()                   { return user_id; }
    public void   setUser_id(String v)           { this.user_id = v; }


    public String health() {
        status = "healthy";
        return Action.SUCCESS.toUpperCase();
    }

    public String access() {
        loggerMaker.info("Arcade /access hook called");

        this.only = null;
        this.deny = null;
        return Action.SUCCESS.toUpperCase();
    }

   
    public String preExecution() {
        try {
            HttpServletRequest request = ServletActionContext.getRequest();
            Map<String, Object> bodyMap = parseJsonBody(readRequestBody(request));
            populateFieldsFromRequest(bodyMap, request);

            Map<String, Object> proxyData = buildPreProxyData();
            Map<String, Object> gatewayResponse = gateway.processHttpProxy(proxyData);

            if (isRequestBlocked(gatewayResponse)) {
                String reason = extractBlockReason(gatewayResponse);
                String blockedMessage = "Blocked by Akto Guardrails: " + reason;
                this.code = CODE_CHECK_FAILED;
                this.error_message = blockedMessage;
                loggerMaker.info("Pre-execution blocked: " + reason + ", execution_id: " + execution_id);

                final Map<String, Object> capturedProxyData = proxyData;
                final String capturedMessage = blockedMessage;
                final String capturedExecutionId = execution_id;
                CompletableFuture.runAsync(() ->
                        ingestBlockedRequest(capturedProxyData, capturedMessage, capturedExecutionId));
            } else {
                this.code = CODE_OK;
                loggerMaker.info("Pre-execution allowed, execution_id: " + execution_id);
            }

        } catch (Exception e) {
            loggerMaker.errorAndAddToDb("Error in pre hook: " + e.getMessage(),
                    LoggerMaker.LogDb.DATA_INGESTION);
            this.code = CODE_CHECK_FAILED;
            this.error_message = "Internal error";
        }
        return Action.SUCCESS.toUpperCase();
    }

    public String postExecution() {
        try {
            HttpServletRequest request = ServletActionContext.getRequest();
            Map<String, Object> bodyMap = parseJsonBody(readRequestBody(request));
            populateFieldsFromRequest(bodyMap, request);

            final Map<String, Object> proxyData = buildPostProxyData();
            final String capturedExecutionId = execution_id;
            CompletableFuture.runAsync(() -> {
                try {
                    gateway.processHttpProxy(proxyData);
                    loggerMaker.info("Post-execution ingested, execution_id: " + capturedExecutionId);
                } catch (Exception e) {
                    loggerMaker.errorAndAddToDb("Error ingesting post-execution data: " + e.getMessage(),
                            LoggerMaker.LogDb.DATA_INGESTION);
                }
            });

            this.code = CODE_OK;
            loggerMaker.info("Post-execution accepted, execution_id: " + execution_id);

        } catch (Exception e) {
            loggerMaker.errorAndAddToDb("Error in post hook: " + e.getMessage(),
                    LoggerMaker.LogDb.DATA_INGESTION);
            this.code = CODE_OK;
        }
        return Action.SUCCESS.toUpperCase();
    }

    private Map<String, Object> buildPreProxyData() throws Exception {
        String toolName = extractToolName();
        String path = "/tools/" + toolName;

        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("guardrails", "true");
        queryParams.put("akto_connector", AKTO_CONNECTOR);

        return buildProxyDataMap(path,
                buildBaseRequestMap("pre_tool_execution", toolName),
                null,
                queryParams);
    }

    private Map<String, Object> buildPostProxyData() throws Exception {
        String toolName = extractToolName();
        String path = "/tools/" + toolName;

        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("statusCode", Boolean.TRUE.equals(success) ? 200 : 500);
        responseMap.put("status", Boolean.TRUE.equals(success) ? "SUCCESS" : "ERROR");
        responseMap.put("body", output != null ? objectMapper.writeValueAsString(output) : "{}");
        responseMap.put("headers", new HashMap<>());

        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("akto_connector", AKTO_CONNECTOR);
        queryParams.put("ingest_data", "true");

        return buildProxyDataMap(path,
                buildBaseRequestMap("post_tool_execution", toolName),
                responseMap,
                queryParams);
    }

    private Map<String, Object> buildBaseRequestMap(String hookType, String toolName) {
        Map<String, Object> body = new HashMap<>();
        if (tool != null)   body.put("tool", tool);
        if (inputs != null) body.put("inputs", inputs);

        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("method", "POST");
        requestMap.put("headers", buildRequestHeaders(hookType, toolName));
        requestMap.put("body", body);
        requestMap.put("metadata", buildMetadata());
        return requestMap;
    }

    private Map<String, Object> buildProxyDataMap(String path,
            Map<String, Object> request,
            Map<String, Object> response,
            Map<String, Object> queryParams) {
        Map<String, Object> proxyData = new HashMap<>();
        proxyData.put("url", "https://" + ARCADE_HOST + path);
        proxyData.put("path", path);
        proxyData.put("request", request);
        if (response != null) proxyData.put("response", response);
        proxyData.put("urlQueryParams", queryParams);
        return proxyData;
    }

    private Map<String, Object> buildRequestHeaders(String hookType, String toolName) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("host", ARCADE_HOST);
        headers.put("content-type", "application/json");
        headers.put("x-arcade-event", hookType);

        if (execution_id != null && !execution_id.isEmpty()) {
            headers.put("x-arcade-execution-id", execution_id);
        }
        if (toolName != null) {
            headers.put("x-arcade-tool", toolName);
        }
        if (tool != null) {
            Object toolkit = tool.get("toolkit");
            if (toolkit != null) headers.put("x-arcade-toolkit", toolkit.toString());
            Object version = tool.get("version");
            if (version != null) headers.put("x-arcade-tool-version", version.toString());
        }
        if (context != null) {
            for (Map.Entry<String, Object> entry : context.entrySet()) {
                Object value = entry.getValue();
                if (value == null) continue;
                String headerKey = "x-arcade-context-" + entry.getKey();
                if (value instanceof String) {
                    headers.put(headerKey, value);
                } else {
                    try {
                        headers.put(headerKey, objectMapper.writeValueAsString(value));
                    } catch (Exception e) {
                        headers.put(headerKey, value.toString());
                    }
                }
            }
        }
        return headers;
    }

    private Map<String, Object> buildMetadata() {
        Map<String, String> tag = new HashMap<>();
        tag.put("gen-ai", "Gen AI");
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("tag", tag);
        return metadata;
    }


    @SuppressWarnings("unchecked")
    private boolean isRequestBlocked(Map<String, Object> gatewayResponse) {
        if (gatewayResponse == null) return false;
        Map<String, Object> guardrailsResult = extractMap(gatewayResponse.get("guardrailsResult"));
        if (guardrailsResult == null || guardrailsResult.isEmpty()) return false;

        Object allowed = guardrailsResult.containsKey("Allowed")
                ? guardrailsResult.get("Allowed")
                : guardrailsResult.get("allowed");
        if (allowed == null) return false;
        if (allowed instanceof Boolean) return !Boolean.TRUE.equals(allowed);
        return !"true".equalsIgnoreCase(String.valueOf(allowed));
    }

    private String extractBlockReason(Map<String, Object> gatewayResponse) {
        if (gatewayResponse == null) return "Request blocked by security policy";
        Map<String, Object> guardrailsResult = extractMap(gatewayResponse.get("guardrailsResult"));
        if (guardrailsResult == null) return "Request blocked by security policy";

        Object reason = guardrailsResult.containsKey("Reason")
                ? guardrailsResult.get("Reason")
                : guardrailsResult.get("reason");
        String reasonStr = extractString(reason);
        return (reasonStr != null && !reasonStr.isEmpty()) ? reasonStr : "Request blocked by security policy";
    }

    private void ingestBlockedRequest(Map<String, Object> proxyData,
            String blockedMessage,
            String executionId) {
        try {
            Map<String, Object> responseBody = new HashMap<>();
            if (executionId != null && !executionId.isEmpty()) {
                responseBody.put("execution_id", executionId);
            }
            responseBody.put("code", CODE_CHECK_FAILED);
            responseBody.put("error_message", blockedMessage);

            Map<String, Object> blockedResponse = new HashMap<>();
            blockedResponse.put("statusCode", 403);
            blockedResponse.put("status", "BLOCKED");
            blockedResponse.put("headers", new HashMap<>());
            blockedResponse.put("body", objectMapper.writeValueAsString(responseBody));

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("ingest_data", "true");
            queryParams.put("akto_connector", AKTO_CONNECTOR);

            Map<String, Object> ingestData = new HashMap<>(proxyData);
            ingestData.put("response", blockedResponse);
            ingestData.put("urlQueryParams", queryParams);

            gateway.processHttpProxy(ingestData);
            loggerMaker.info("Blocked request ingested, execution_id: " + executionId);
        } catch (Exception e) {
            loggerMaker.errorAndAddToDb("Error ingesting blocked request: " + e.getMessage(),
                    LoggerMaker.LogDb.DATA_INGESTION);
        }
    }

    private void populateFieldsFromRequest(Map<String, Object> bodyMap, HttpServletRequest request) {
        this.tool           = extractMap(bodyMap.get("tool"));
        this.inputs         = extractMap(bodyMap.get("inputs"));
        this.context        = extractMap(bodyMap.get("context"));
        this.execution_id   = extractString(bodyMap.get("execution_id"));
        if (this.execution_id == null || this.execution_id.isEmpty()) {
            this.execution_id = request.getHeader(HEADER_EXECUTION_ID);
        }
        this.toolkits       = bodyMap.get("toolkits");
        this.user_id        = extractString(bodyMap.get("user_id"));
        this.success        = extractBoolean(bodyMap.get("success"));
        this.output         = bodyMap.get("output");
        this.execution_code = extractString(bodyMap.get("execution_code"));
        this.execution_error = extractString(bodyMap.get("execution_error"));
    }

    private String readRequestBody(HttpServletRequest request) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(request.getInputStream()))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            loggerMaker.errorAndAddToDb("Error reading request body: " + e.getMessage(),
                    LoggerMaker.LogDb.DATA_INGESTION);
            return "";
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonBody(String jsonBody) {
        if (jsonBody == null || jsonBody.isEmpty()) return new HashMap<>();
        try {
            return objectMapper.readValue(jsonBody, Map.class);
        } catch (Exception e) {
            loggerMaker.errorAndAddToDb("Error parsing JSON body: " + e.getMessage(),
                    LoggerMaker.LogDb.DATA_INGESTION);
            return new HashMap<>();
        }
    }

    private String extractToolName() {
        if (tool == null) return "unknown";
        Object name = tool.get("name");
        return name != null ? name.toString() : "unknown";
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractMap(Object value) {
        return (value instanceof Map) ? (Map<String, Object>) value : null;
    }

    private String extractString(Object value) {
        return value != null ? value.toString() : null;
    }

    private Boolean extractBoolean(Object value) {
        if (value == null) return null;
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof String) return "true".equalsIgnoreCase((String) value);
        return null;
    }
}
