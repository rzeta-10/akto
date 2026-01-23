#!/usr/bin/env python3

import os
import json
import urllib.request
import urllib.error
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

GUARDRAILS_URL = os.getenv("AKTO_GUARDRAILS_URL", "http://localhost:80")
if GUARDRAILS_URL and not GUARDRAILS_URL.lower().startswith(("http://", "https://")):
    print("[Akto Guardrails] WARNING: URL uses unsafe scheme; ignoring")
    GUARDRAILS_URL = ""

AUTH_TOKEN = os.getenv("DATABASE_ABSTRACTOR_SERVICE_TOKEN", "")
TIMEOUT = float(os.getenv("AKTO_GUARDRAILS_TIMEOUT", "5"))
FAIL_OPEN = os.getenv("AKTO_GUARDRAILS_FAIL_OPEN", "true").lower() == "true"


def _call_guardrails_api(query: str) -> tuple[bool, str]:
    if not GUARDRAILS_URL or not query.strip():
        return True, ""
    
    try:
        payload = {"query": query.strip(), "model": "vertex-ai-adk"}
        request_body = {"payload": json.dumps(payload), "call_type": "completion"}
        
        headers = {"Content-Type": "application/json"}
        if AUTH_TOKEN:
            headers["authorization"] = AUTH_TOKEN
        
        req = urllib.request.Request(
            f"{GUARDRAILS_URL}/api/validate/request",
            data=json.dumps(request_body).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        
        with urllib.request.urlopen(req, timeout=TIMEOUT) as response:
            result = json.loads(response.read().decode("utf-8"))
            allowed = result.get("Allowed", result.get("allowed", True))
            reason = result.get("Reason", result.get("reason", ""))
            return allowed, reason
            
    except urllib.error.URLError as e:
        print(f"[Akto Guardrails] Network error: {e}")
        return FAIL_OPEN, f"Network error: {e}"
    except Exception as e:
        print(f"[Akto Guardrails] Error: {e}")
        return FAIL_OPEN, str(e)


def akto_guardrails_callback(callback_context, llm_request) -> Optional[object]:
    from google.adk.models import LlmResponse
    from google.genai import types
    
    agent_name = callback_context.agent_name
    print(f"[Akto Guardrails] Validating request for agent: {agent_name}")
    
    last_user_message = ""
    if llm_request.contents and len(llm_request.contents) > 0:
        last_content = llm_request.contents[-1]
        if hasattr(last_content, 'role') and last_content.role == 'user':
            if hasattr(last_content, 'parts') and len(last_content.parts) > 0:
                first_part = last_content.parts[0]
                if hasattr(first_part, 'text'):
                    last_user_message = first_part.text
    
    if not last_user_message.strip():
        print("[Akto Guardrails] Empty prompt, allowing")
        return None
    
    display_msg = last_user_message[:100] + "..." if len(last_user_message) > 100 else last_user_message
    print(f"[Akto Guardrails] Checking prompt: '{display_msg}'")
    
    allowed, reason = _call_guardrails_api(last_user_message)
    
    if allowed:
        print("[Akto Guardrails] ✅ Request ALLOWED")
        return None
    else:
        print(f"[Akto Guardrails] 🚫 Request BLOCKED: {reason}")
        return LlmResponse(
            content=types.Content(
                role="model",
                parts=[types.Part(text=f"🚫 Blocked by Akto Guardrails: {reason or 'Security policy violation detected'}")],
            )
        )


async def akto_guardrails_callback_async(callback_context, llm_request) -> Optional[object]:
    return akto_guardrails_callback(callback_context, llm_request)
