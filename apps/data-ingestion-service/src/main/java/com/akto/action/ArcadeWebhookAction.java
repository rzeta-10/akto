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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@lombok.Getter
@lombok.Setter
public class ArcadeWebhookAction extends ActionSupport {

    private static final LoggerMaker loggerMaker = new LoggerMaker(ArcadeWebhookAction.class, LoggerMaker.LogDb.DATA_INGESTION);
    private static final Gateway gateway = Gateway.getInstance();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String ARCADE_HOST = "arcade.dev";
    private static final String HOOK_TYPE_ACCESS = "tool_access";
    private static final String HOOK_TYPE_PRE = "pre_tool_execution";
    private static final String HOOK_TYPE_POST = "post_tool_execution";
    private static final String AKTO_CONNECTOR = "arcade";

    private static final String HEADER_ARCADE_EVENT = "x-arcade-event";
    private static final String HEADER_EXECUTION_ID = "x-arcade-execution-id";

    static {
        gateway.setDataPublisher(new KafkaDataPublisher());
        loggerMaker.info("ArcadeWebhookAction: Gateway configured with KafkaDataPublisher");
    }

    private String execution_id;
    private Map<String, Object> tool;
    private Map<String, Object> inputs;
    private Map<String, Object> context;
    
    // Explicit getter to ensure Struts JSON serialization includes execution_id
    public String getExecution_id() {
        return execution_id;
    }
    
    public void setExecution_id(String execution_id) {
        this.execution_id = execution_id;
    }

    private Boolean success;
    private Object output;
    private String execution_code;
    private String execution_error;

    private String user_id;
    private Object toolkits;

    // Arcade API response fields (per https://docs.arcade.dev/en/guides/contextual-access/build-your-own)
    private String code;  // Required: "OK", "CHECK_FAILED", or "RATE_LIMIT_EXCEEDED"
    private String error_message;  // Optional: shown to agent when denying
    private Map<String, Object> override;  // Optional: inputs/secrets/output to modify
    
    // For access hook only
    private Map<String, Object> only;  // Allow list (only these tools)
    private Map<String, Object> deny;  // Deny list (ignore if 'only' is present)
    
    private Map<String, Object> result;  // Legacy field, keeping for backward compatibility
    private String status;
    
    // Getters for Arcade API response fields
    public String getCode() {
        return code;
    }
    
    public void setCode(String code) {
        this.code = code;
    }
    
    public String getError_message() {
        return error_message;
    }
    
    public void setError_message(String error_message) {
        this.error_message = error_message;
    }
    
    public Map<String, Object> getOverride() {
        return override;
    }
    
    public void setOverride(Map<String, Object> override) {
        this.override = override;
    }
    
    public Map<String, Object> getOnly() {
        return only;
    }
    
    public void setOnly(Map<String, Object> only) {
        this.only = only;
    }
    
    public Map<String, Object> getDeny() {
        return deny;
    }
    
    public void setDeny(Map<String, Object> deny) {
        this.deny = deny;
    }

    public String arcade() {
        String hookType = HOOK_TYPE_PRE;

        try {
            loggerMaker.info("Arcade webhook received");

            HttpServletRequest request = ServletActionContext.getRequest();
            String jsonBody = readRequestBody(request);
            Map<String, Object> bodyMap = parseJsonBody(jsonBody);

            populateFieldsFromRequest(bodyMap, request);

            hookType = detectHookType(request);
            loggerMaker.info("Detected Arcade hook type: " + hookType + " (URI: " + request.getRequestURI() + ")");

            if (HOOK_TYPE_ACCESS.equals(hookType)) {
                setToolAccessAllowAllResult();
                loggerMaker.info("Arcade tool_access hook processed with allow=all");
                return Action.SUCCESS.toUpperCase();
            }

            Map<String, Object> proxyData = buildProxyData(hookType);
            Map<String, Object> gatewayResponse = gateway.processHttpProxy(proxyData);

            if (HOOK_TYPE_PRE.equals(hookType) && isRequestBlocked(gatewayResponse)) {
                String reason = extractBlockReason(gatewayResponse);
                String blockedMessage = "Blocked by Akto Guardrails: " + reason;
                setBlockedResult(blockedMessage);
                ingestBlockedRequest(proxyData, blockedMessage);
                loggerMaker.info("Arcade webhook blocked by guardrails - reason: " + reason);
                return Action.SUCCESS.toUpperCase();
            }

            setContinueResult(true);
            loggerMaker.info("Arcade webhook processed successfully - hookType: " + hookType + ", execution_id: " + execution_id);
            return Action.SUCCESS.toUpperCase();

        } catch (Exception e) {
            loggerMaker.errorAndAddToDb("Error processing Arcade webhook: " + e.getMessage(), LoggerMaker.LogDb.DATA_INGESTION);
            setInternalErrorResult(hookType, "Internal processing error: " + e.getMessage());
            return Action.SUCCESS.toUpperCase();
        }
    }

    public String health() {
        status = "healthy";
        return Action.SUCCESS.toUpperCase();
    }

    /** Arcade pre_tool_execution webhook; same handling as arcade() which detects hook type from request. */
    public String preExecution() {
        return arcade();
    }

    /** Arcade post_tool_execution webhook; same handling as arcade() which detects hook type from request. */
    public String postExecution() {
        return arcade();
    }

    private void populateFieldsFromRequest(Map<String, Object> bodyMap, HttpServletRequest request) {
        this.tool = extractMap(bodyMap.get("tool"));
        this.inputs = extractMap(bodyMap.get("inputs"));
        this.context = extractMap(bodyMap.get("context"));

        this.execution_id = extractString(bodyMap.get("execution_id"));
        if (this.execution_id == null || this.execution_id.isEmpty()) {
            this.execution_id = request.getHeader(HEADER_EXECUTION_ID);
        }

        this.toolkits = bodyMap.get("toolkits");
        this.user_id = extractString(bodyMap.get("user_id"));

        this.success = extractBoolean(bodyMap.get("success"));
        this.output = bodyMap.get("output");
        this.execution_code = extractString(bodyMap.get("execution_code"));
        this.execution_error = extractString(bodyMap.get("execution_error"));

        loggerMaker.info("Parsed inputs from webhook: " + (this.inputs != null ? this.inputs.toString() : "null"));
        loggerMaker.info("Inputs size: " + (this.inputs != null ? this.inputs.size() : 0));
    }

    private String readRequestBody(HttpServletRequest request) {
        try {
            InputStream inputStream = request.getInputStream();
            InputStreamReader reader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            return bufferedReader.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            loggerMaker.errorAndAddToDb("Error reading request body: " + e.getMessage(), LoggerMaker.LogDb.DATA_INGESTION);
            return "";
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonBody(String jsonBody) {
        if (jsonBody == null || jsonBody.isEmpty()) {
            return new HashMap<>();
        }

        try {
            return objectMapper.readValue(jsonBody, Map.class);
        } catch (Exception e) {
            loggerMaker.errorAndAddToDb("Error parsing JSON body: " + e.getMessage(), LoggerMaker.LogDb.DATA_INGESTION);
            return new HashMap<>();
        }
    }

    private String detectHookType(HttpServletRequest request) {
        String headerHookType = request.getHeader(HEADER_ARCADE_EVENT);
        if (isSupportedHookType(headerHookType)) {
            return headerHookType;
        }

        if (headerHookType != null && !headerHookType.isEmpty()) {
            loggerMaker.info("Unsupported x-arcade-event header value: " + headerHookType + ", falling back to path detection");
        }

        return detectHookTypeFromPath(request.getRequestURI());
    }

    private boolean isSupportedHookType(String hookType) {
        if (hookType == null || hookType.isEmpty()) {
            return false;
        }

        return HOOK_TYPE_ACCESS.equals(hookType)
                || HOOK_TYPE_PRE.equals(hookType)
                || HOOK_TYPE_POST.equals(hookType);
    }

    private String detectHookTypeFromPath(String requestURI) {
        if (requestURI == null) {
            return HOOK_TYPE_PRE;
        }

        if (requestURI.contains("/api/arcade/access") || requestURI.endsWith("/access") || requestURI.equals("/access")) {
            return HOOK_TYPE_ACCESS;
        }

        if (requestURI.contains("/api/arcade/pre") || requestURI.endsWith("/pre") || requestURI.equals("/pre")) {
            return HOOK_TYPE_PRE;
        }

        if (requestURI.contains("/api/arcade/post") || requestURI.endsWith("/post") || requestURI.equals("/post")) {
            return HOOK_TYPE_POST;
        }

        return HOOK_TYPE_PRE;
    }

    @SuppressWarnings("unchecked")
    private boolean isRequestBlocked(Map<String, Object> gatewayResponse) {
        if (gatewayResponse == null) {
            return false;
        }

        Map<String, Object> guardrailsResult = extractMap(gatewayResponse.get("guardrailsResult"));
        if (guardrailsResult == null || guardrailsResult.isEmpty()) {
            return false;
        }

        Object allowedObj = guardrailsResult.get("Allowed");
        if (allowedObj == null) {
            allowedObj = guardrailsResult.get("allowed");
        }

        if (allowedObj == null) {
            return false;
        }

        if (allowedObj instanceof Boolean) {
            return !Boolean.TRUE.equals(allowedObj);
        }

        return !"true".equalsIgnoreCase(String.valueOf(allowedObj));
    }

    private String extractBlockReason(Map<String, Object> gatewayResponse) {
        if (gatewayResponse == null) {
            return "Request blocked by security policy";
        }

        Map<String, Object> guardrailsResult = extractMap(gatewayResponse.get("guardrailsResult"));
        if (guardrailsResult == null || guardrailsResult.isEmpty()) {
            return "Request blocked by security policy";
        }

        Object reasonObj = guardrailsResult.get("Reason");
        if (reasonObj == null) {
            reasonObj = guardrailsResult.get("reason");
        }

        String reason = extractString(reasonObj);
        if (reason == null || reason.isEmpty()) {
            return "Request blocked by security policy";
        }

        return reason;
    }

    /**
     * Set response for pre/post hooks when allowing execution to proceed.
     * Per Arcade API: code="OK" means proceed, optional override can modify inputs/output.
     */
    private void setContinueResult(boolean shouldContinue) {
        this.code = "OK";
        this.error_message = null;
        this.override = null;
        // Ensure execution_id is preserved for response matching
        if (this.execution_id == null || this.execution_id.isEmpty()) {
            loggerMaker.info("Warning: execution_id is null or empty when setting continue result");
        }
    }

    /**
     * Set response for pre/post hooks when blocking execution.
     * Per Arcade API: code="CHECK_FAILED" means deny, error_message shown to agent.
     */
    private void setBlockedResult(String message) {
        this.code = "CHECK_FAILED";
        this.error_message = message;
        this.override = null;
    }

    /**
     * Set response for access hook - allow all tools.
     * Per Arcade API: return neither 'only' nor 'deny' means "no change" (all tools remain allowed).
     */
    private void setToolAccessAllowAllResult() {
        this.code = null;  // Access hook doesn't use 'code'
        this.only = null;
        this.deny = null;
        // Note: execution_id is not part of access hook response
    }

    /**
     * Set response when an internal error occurs.
     * For access hook: allow all (fail open).
     * For post hook: allow to proceed (fail open).
     * For pre hook: block (fail closed for security).
     */
    private void setInternalErrorResult(String hookType, String errorMessage) {
        if (HOOK_TYPE_ACCESS.equals(hookType)) {
            setToolAccessAllowAllResult();
            return;
        }

        if (HOOK_TYPE_POST.equals(hookType)) {
            // Fail open for post hook - allow execution to complete
            setContinueResult(true);
            return;
        }

        // Fail closed for pre hook - block on error
        setBlockedResult(errorMessage);
    }

    private void ingestBlockedRequest(Map<String, Object> proxyData, String blockedMessage) {
        try {
            Map<String, Object> blockedResponse = new HashMap<>();
            blockedResponse.put("statusCode", 403);
            blockedResponse.put("status", "BLOCKED");
            blockedResponse.put("headers", new HashMap<>());
            blockedResponse.put("body", objectMapper.writeValueAsString(buildBlockedResponsePayload(blockedMessage)));
            proxyData.put("response", blockedResponse);

            Map<String, Object> blockedQueryParams = new HashMap<>();
            blockedQueryParams.put("ingest_data", "true");
            blockedQueryParams.put("akto_connector", AKTO_CONNECTOR);
            proxyData.put("urlQueryParams", blockedQueryParams);

            gateway.processHttpProxy(proxyData);
            loggerMaker.info("Blocked request ingested into Kafka");
        } catch (Exception ingestEx) {
            loggerMaker.errorAndAddToDb("Error ingesting blocked request: " + ingestEx.getMessage(), LoggerMaker.LogDb.DATA_INGESTION);
        }
    }

    private Map<String, Object> buildBlockedResponsePayload(String blockedMessage) {
        Map<String, Object> blockedBody = new HashMap<>();
        if (execution_id != null && !execution_id.isEmpty()) {
            blockedBody.put("execution_id", execution_id);
        }

        Map<String, Object> blockedResult = new HashMap<>();
        blockedResult.put("continue", false);

        Map<String, Object> error = new HashMap<>();
        error.put("message", blockedMessage);
        blockedResult.put("error", error);

        blockedBody.put("result", blockedResult);
        return blockedBody;
    }

    private Map<String, Object> buildProxyData(String hookType) throws Exception {
        Map<String, Object> proxyData = new HashMap<>();

        String toolName = extractToolName();
        String path;
        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> responseMap = null;

        switch (hookType) {
            case HOOK_TYPE_ACCESS:
                path = "/access";
                requestMap.put("method", "POST");
                requestMap.put("headers", buildRequestHeaders(hookType, null));
                Map<String, Object> accessBody = new HashMap<>();
                accessBody.put("user_id", user_id);
                if (toolkits != null) {
                    accessBody.put("toolkits", toolkits);
                }
                requestMap.put("body", accessBody);
                break;

            case HOOK_TYPE_POST:
                path = "/tools/" + toolName;
                requestMap.put("method", "POST");
                requestMap.put("headers", buildRequestHeaders(hookType, toolName));
                Map<String, Object> postRequestBody = new HashMap<>();
                if (tool != null) {
                    postRequestBody.put("tool", tool);
                }
                if (inputs != null) {
                    postRequestBody.put("inputs", inputs);
                }
                requestMap.put("body", postRequestBody);
                loggerMaker.info("POST hook - inputs: " + (inputs != null ? inputs.toString() : "null"));

                responseMap = new HashMap<>();
                responseMap.put("statusCode", success != null && success ? 200 : 500);
                responseMap.put("status", success != null && success ? "SUCCESS" : "ERROR");
                if (output != null) {
                    responseMap.put("body", objectMapper.writeValueAsString(output));
                } else {
                    responseMap.put("body", "{}");
                }
                responseMap.put("headers", new HashMap<>());
                break;

            case HOOK_TYPE_PRE:
            default:
                path = "/tools/" + toolName;
                requestMap.put("method", "POST");
                requestMap.put("headers", buildRequestHeaders(hookType, toolName));
                Map<String, Object> preRequestBody = new HashMap<>();
                if (tool != null) {
                    preRequestBody.put("tool", tool);
                }
                if (inputs != null) {
                    preRequestBody.put("inputs", inputs);
                }
                requestMap.put("body", preRequestBody);
                loggerMaker.info("PRE hook - inputs: " + (inputs != null ? inputs.toString() : "null"));
                break;
        }

        Map<String, Object> metadata = new HashMap<>();
        Map<String, String> tag = new HashMap<>();
        tag.put("gen-ai", "Gen AI");
        metadata.put("tag", tag);
        requestMap.put("metadata", metadata);

        String url = "https://" + ARCADE_HOST + path;
        proxyData.put("url", url);
        proxyData.put("path", path);
        proxyData.put("request", requestMap);
        if (responseMap != null) {
            proxyData.put("response", responseMap);
        }

        Map<String, Object> urlQueryParams = new HashMap<>();

        if (HOOK_TYPE_PRE.equals(hookType)) {
            urlQueryParams.put("guardrails", "true");
            urlQueryParams.put("akto_connector", AKTO_CONNECTOR);
            loggerMaker.info("Guardrails enabled for pre-execution hook");
        } else if (HOOK_TYPE_POST.equals(hookType)) {
            urlQueryParams.put("akto_connector", AKTO_CONNECTOR);
            urlQueryParams.put("ingest_data", "true");
            loggerMaker.info("Data ingestion enabled for post-execution hook");
        }

        proxyData.put("urlQueryParams", urlQueryParams);
        return proxyData;
    }

    private Map<String, Object> buildRequestHeaders(String hookType, String toolName) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("host", ARCADE_HOST);
        headers.put("content-type", "application/json");
        headers.put("x-arcade-event", hookType);
        if (execution_id != null) {
            headers.put("x-arcade-execution-id", execution_id);
        }
        if (toolName != null) {
            headers.put("x-arcade-tool", toolName);
        }

        if (context != null) {
            for (Map.Entry<String, Object> entry : context.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value == null) {
                    continue;
                }

                String headerKey = "x-arcade-context-" + key;
                if (value instanceof String) {
                    headers.put(headerKey, value);
                } else {
                    try {
                        headers.put(headerKey, objectMapper.writeValueAsString(value));
                    } catch (Exception e) {
                        loggerMaker.errorAndAddToDb("Error serializing context field " + key + ": " + e.getMessage(), LoggerMaker.LogDb.DATA_INGESTION);
                        headers.put(headerKey, value.toString());
                    }
                }
            }
        }

        if (tool != null) {
            Object toolkit = tool.get("toolkit");
            if (toolkit != null) {
                headers.put("x-arcade-toolkit", toolkit.toString());
            }
            Object version = tool.get("version");
            if (version != null) {
                headers.put("x-arcade-tool-version", version.toString());
            }
        }

        return headers;
    }

    private String extractToolName() {
        if (tool == null) {
            return "unknown";
        }

        Object name = tool.get("name");
        return name != null ? name.toString() : "unknown";
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractMap(Object value) {
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }

    private String extractString(Object value) {
        if (value == null) {
            return null;
        }

        return value.toString();
    }

    private Boolean extractBoolean(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        }

        if (value instanceof String) {
            return "true".equalsIgnoreCase((String) value);
        }

        return null;
    }
}
