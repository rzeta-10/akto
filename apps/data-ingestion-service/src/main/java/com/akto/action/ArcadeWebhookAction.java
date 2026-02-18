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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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
    private static final String PATH_PREFIX_TOOLS = "/tools/";
    private static final String RESULT_SUCCESS = Action.SUCCESS.toUpperCase();

    // Arcade API response codes
    private static final String CODE_OK = "OK";
    private static final String CODE_CHECK_FAILED = "CHECK_FAILED";
    private static final String BLOCKED_MESSAGE_PREFIX = "Blocked by Akto Guardrails: ";
    private static final String ERROR_INTERNAL = "Internal error";
    private static final String UNKNOWN_TOOL = "unknown";

    static {
        gateway.setDataPublisher(new KafkaDataPublisher());
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

    public String health() {
        status = "healthy";
        return RESULT_SUCCESS;
    }

    public String access() {
        this.only = null;
        this.deny = null;
        return RESULT_SUCCESS;
    }

    public String preExecution() {
        try {
            parseAndPopulateFromRequest(ServletActionContext.getRequest());
            Map<String, Object> proxyData = buildPreProxyData();
            Map<String, Object> gatewayResponse = gateway.processHttpProxy(proxyData);

            if (isRequestBlocked(gatewayResponse)) {
                String blockedMessage = formatBlockedMessage(extractBlockReason(gatewayResponse));
                setCheckFailed(blockedMessage);
                final String executionIdForIngest = execution_id;
                CompletableFuture.runAsync(() ->
                        ingestBlockedRequest(proxyData, blockedMessage, executionIdForIngest));
            } else {
                this.code = CODE_OK;
            }
        } catch (Exception e) {
            logError("Error in pre hook", e);
            setCheckFailed(ERROR_INTERNAL);
        }
        return RESULT_SUCCESS;
    }

    public String postExecution() {
        try {
            parseAndPopulateFromRequest(ServletActionContext.getRequest());
            Map<String, Object> proxyData = buildPostProxyData();
            CompletableFuture.runAsync(() -> processHttpProxySafe(proxyData, "Error ingesting post-execution data"));

            this.code = CODE_OK;
        } catch (Exception e) {
            logError("Error in post hook", e);
            this.code = CODE_OK;
        }
        return RESULT_SUCCESS;
    }

    private Map<String, Object> buildPreProxyData() throws Exception {
        String toolName = extractToolName();
        Map<String, Object> queryParams = baseQueryParams();
        queryParams.put("guardrails", "true");
        return buildProxyDataMap(pathForTool(toolName),
                buildBaseRequestMap("pre_tool_execution", toolName),
                null,
                queryParams);
    }

    private Map<String, Object> buildPostProxyData() throws Exception {
        String toolName = extractToolName();
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("statusCode", Boolean.TRUE.equals(success) ? 200 : 500);
        responseMap.put("status", Boolean.TRUE.equals(success) ? "SUCCESS" : "ERROR");
        responseMap.put("body", output != null ? objectMapper.writeValueAsString(output) : "{}");
        responseMap.put("headers", Collections.emptyMap());

        Map<String, Object> queryParams = baseQueryParams();
        queryParams.put("ingest_data", "true");

        return buildProxyDataMap(pathForTool(toolName),
                buildBaseRequestMap("post_tool_execution", toolName),
                responseMap,
                queryParams);
    }

    private static String pathForTool(String toolName) {
        return PATH_PREFIX_TOOLS + (toolName != null ? toolName : UNKNOWN_TOOL);
    }

    private static Map<String, Object> baseQueryParams() {
        Map<String, Object> q = new HashMap<>();
        q.put("akto_connector", AKTO_CONNECTOR);
        return q;
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

    private void parseAndPopulateFromRequest(HttpServletRequest request) {
        if (request == null) return;
        String rawBody = readRequestBody(request);
        Map<String, Object> bodyMap = parseJsonBody(rawBody);
        populateFieldsFromRequest(bodyMap, request);
    }

    private static String formatBlockedMessage(String reason) {
        return isNullOrEmpty(reason) ? "" : BLOCKED_MESSAGE_PREFIX + reason;
    }

    private void setCheckFailed(String message) {
        this.code = CODE_CHECK_FAILED;
        this.error_message = message;
    }

    private void processHttpProxySafe(Map<String, Object> proxyData, String errorContext) {
        try {
            gateway.processHttpProxy(proxyData);
        } catch (Exception e) {
            logError(errorContext, e);
        }
    }

    private void logError(String context, Exception e) {
        String message = e != null && e.getMessage() != null ? e.getMessage() : (e != null ? e.getClass().getSimpleName() : "unknown");
        loggerMaker.errorAndAddToDb(context + ": " + message, LoggerMaker.LogDb.DATA_INGESTION);
    }

    private static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    private Map<String, Object> buildRequestHeaders(String hookType, String toolName) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("host", ARCADE_HOST);
        headers.put("content-type", "application/json");
        headers.put("x-arcade-event", hookType);

        if (!isNullOrEmpty(execution_id)) {
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
                String key = entry.getKey();
                Object value = entry.getValue();
                if (key == null || value == null) continue;
                String headerKey = "x-arcade-context-" + key;
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
    private Map<String, Object> getGuardrailsResult(Map<String, Object> gatewayResponse) {
        if (gatewayResponse == null) return null;
        Map<String, Object> result = extractMap(gatewayResponse.get("guardrailsResult"));
        return (result != null && !result.isEmpty()) ? result : null;
    }

    private static Object getFromMap(Map<String, Object> map, String firstKey, String secondKey) {
        if (map == null) return null;
        if (map.containsKey(firstKey)) return map.get(firstKey);
        return map.get(secondKey);
    }

    @SuppressWarnings("unchecked")
    private boolean isRequestBlocked(Map<String, Object> gatewayResponse) {
        Map<String, Object> guardrails = getGuardrailsResult(gatewayResponse);
        if (guardrails == null) return false;

        Object allowed = getFromMap(guardrails, "Allowed", "allowed");
        if (allowed == null) return false;
        if (allowed instanceof Boolean) return !Boolean.TRUE.equals(allowed);
        return !"true".equalsIgnoreCase(String.valueOf(allowed));
    }

    private String extractBlockReason(Map<String, Object> gatewayResponse) {
        Map<String, Object> guardrails = getGuardrailsResult(gatewayResponse);
        if (guardrails == null) return null;

        Object reason = getFromMap(guardrails, "Reason", "reason");
        String reasonStr = extractString(reason);
        return isNullOrEmpty(reasonStr) ? null : reasonStr;
    }

    private void ingestBlockedRequest(Map<String, Object> proxyData,
            String blockedMessage,
            String executionId) {
        try {
            Map<String, Object> responseBody = new HashMap<>();
            if (!isNullOrEmpty(executionId)) {
                responseBody.put("execution_id", executionId);
            }
            responseBody.put("code", CODE_CHECK_FAILED);
            responseBody.put("error_message", blockedMessage);

            Map<String, Object> blockedResponse = new HashMap<>();
            blockedResponse.put("statusCode", 403);
            blockedResponse.put("status", "BLOCKED");
            blockedResponse.put("headers", Collections.emptyMap());
            blockedResponse.put("body", objectMapper.writeValueAsString(responseBody));

            Map<String, Object> queryParams = baseQueryParams();
            queryParams.put("ingest_data", "true");

            Map<String, Object> ingestData = new HashMap<>(proxyData);
            ingestData.put("response", blockedResponse);
            ingestData.put("urlQueryParams", queryParams);

            gateway.processHttpProxy(ingestData);
        } catch (Exception e) {
            logError("Error ingesting blocked request", e);
        }
    }

    private void populateFieldsFromRequest(Map<String, Object> bodyMap, HttpServletRequest request) {
        if (bodyMap == null) bodyMap = new HashMap<>();
        this.tool = extractMap(bodyMap.get("tool"));
        this.inputs = extractMap(bodyMap.get("inputs"));
        this.context = extractMap(bodyMap.get("context"));
        this.execution_id = extractString(bodyMap.get("execution_id"));
        if (isNullOrEmpty(this.execution_id)) {
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
                new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            logError("Error reading request body", e);
            return "";
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonBody(String jsonBody) {
        if (isNullOrEmpty(jsonBody)) return new HashMap<>();
        try {
            return objectMapper.readValue(jsonBody, Map.class);
        } catch (Exception e) {
            logError("Error parsing JSON body", e);
            return new HashMap<>();
        }
    }

    private String extractToolName() {
        if (tool == null) return UNKNOWN_TOOL;
        Object name = tool.get("name");
        return name != null ? name.toString() : UNKNOWN_TOOL;
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
