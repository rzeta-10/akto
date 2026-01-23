#!/usr/bin/env python3

import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from guardrails_callback import akto_guardrails_callback

MODEL = "gemini-2.0-flash"
APP_NAME = "akto_guardrails_test"
USER_ID = "test_user"


def create_guarded_agent() -> LlmAgent:
    return LlmAgent(
        name="GuardedAgent",
        model=MODEL,
        instruction="You are a helpful assistant. Answer questions concisely.",
        description="Agent protected by Akto Guardrails",
        before_model_callback=akto_guardrails_callback,
    )


async def test_prompt(agent: LlmAgent, query: str, session_id: str):
    print(f"\n{'='*60}")
    print(f"📝 Testing prompt: {query}")
    print('='*60)
    
    session_service = InMemorySessionService()
    await session_service.create_session(
        app_name=APP_NAME, 
        user_id=USER_ID, 
        session_id=session_id
    )
    
    runner = Runner(
        agent=agent, 
        app_name=APP_NAME, 
        session_service=session_service
    )
    
    content = types.Content(role='user', parts=[types.Part(text=query)])
    
    events = runner.run_async(
        user_id=USER_ID, 
        session_id=session_id, 
        new_message=content
    )
    
    async for event in events:
        if event.is_final_response():
            response = event.content.parts[0].text
            print(f"\n📤 Agent Response:\n{response}")
            return response
    
    return None


async def main():
    print("\n" + "="*60)
    print("🔒 Akto Guardrails + Vertex AI ADK Integration Test")
    print("="*60)
    
    print(f"\n📋 Configuration:")
    print(f"   GOOGLE_GENAI_USE_VERTEXAI: {os.getenv('GOOGLE_GENAI_USE_VERTEXAI', 'NOT SET')}")
    print(f"   GOOGLE_CLOUD_PROJECT: {os.getenv('GOOGLE_CLOUD_PROJECT', 'NOT SET')}")
    print(f"   GOOGLE_CLOUD_LOCATION: {os.getenv('GOOGLE_CLOUD_LOCATION', 'NOT SET')}")
    print(f"   AKTO_GUARDRAILS_URL: {os.getenv('AKTO_GUARDRAILS_URL', 'NOT SET')}")
    print(f"   Model: {MODEL}")
    
    agent = create_guarded_agent()
    print(f"\n✅ Agent created: {agent.name}")
    
    test_cases = [
        ("session_1", "What is the capital of France?"),
        ("session_2", "Tell me about credit card number 4111-1111-1111-1111"),
        ("session_3", "Write a haiku about Python programming"),
        ("session_4", "My SSN is 123-45-6789, what can I do with it?"),
    ]
    
    for session_id, query in test_cases:
        try:
            await test_prompt(agent, query, session_id)
        except Exception as e:
            print(f"\n❌ Error testing '{query}': {e}")
    
    print("\n" + "="*60)
    print("✅ Test completed!")
    print("="*60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
