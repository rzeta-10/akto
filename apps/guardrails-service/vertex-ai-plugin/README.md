# Akto Guardrails for Vertex AI

Add Akto Guardrails protection to your Vertex AI agents. This validates prompts before they reach the LLM.

## Setup

Install dependencies:

```bash
pip install google-adk google-genai python-dotenv
```

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

## Usage

Import the callback and add it to your agent:

```python
from google.adk.agents import LlmAgent
from guardrails_callback import akto_guardrails_callback

agent = LlmAgent(
    name="MyAgent",
    model="gemini-2.0-flash",
    before_model_callback=akto_guardrails_callback,
)
```

That's it. The callback will validate each prompt against Akto Guardrails before sending it to the model.

## Testing

Run the test script to see it in action:

```bash
python test_agent.py
```

This runs a few test prompts including some with sensitive data (credit card numbers, SSNs) to show how guardrails blocks them.

## Configuration

All config is done through environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `AKTO_GUARDRAILS_URL` | Your Akto Guardrails service URL | `http://localhost:80` |
| `DATABASE_ABSTRACTOR_SERVICE_TOKEN` | Auth token for the service | - |
| `AKTO_GUARDRAILS_TIMEOUT` | Request timeout in seconds | `5` |
| `AKTO_GUARDRAILS_FAIL_OPEN` | Allow requests if guardrails is down | `true` |

## Files

- `guardrails_callback.py` - The callback function you import
- `test_agent.py` - Test script
- `requirements.txt` - Python dependencies
- `.env.example` - Example environment config
