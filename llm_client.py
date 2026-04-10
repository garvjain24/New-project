import os
import json
import urllib.error
import urllib.request
from datetime import datetime
from config import BASE_DIR, LOGS_DIR

class OpenRouterClient:
    def __init__(self):
        env_file = BASE_DIR / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

        self.api_key = os.environ.get("OPENROUTER_API_KEY", "")
        self.model = os.environ.get("OPENROUTER_MODEL", "google/gemma-4-26b-a4b-it:free")
        self.site_url = os.environ.get("OPENROUTER_SITE_URL", "http://localhost")
        self.site_name = os.environ.get("OPENROUTER_APP_NAME", "Self-Healing Agentic Data Pipeline")

    @property
    def enabled(self):
        return bool(self.api_key)

    def _call_openrouter(self, model, prompt, include_reasoning=True, response_format=None):
        payload = {
            "model": model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": "You are a careful data reliability agent."},
                {"role": "user", "content": json.dumps(prompt)},
            ],
        }
        if response_format:
            payload["response_format"] = response_format
        if include_reasoning:
            payload["reasoning"] = {"enabled": True}
            
        request = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": self.site_url,
                "X-OpenRouter-Title": self.site_name,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                resp_payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            error_msg = f"[{datetime.now().isoformat()}] [{model}] HTTPError {e.code}: {error_body}\n"
            (LOGS_DIR / "openrouter_errors.log").open("a", encoding="utf-8").write(error_msg)
            print(error_msg)
            return None
        except Exception as e:
            error_msg = f"[{datetime.now().isoformat()}] [{model}] Exception: {str(e)}\n"
            (LOGS_DIR / "openrouter_errors.log").open("a", encoding="utf-8").write(error_msg)
            print(error_msg)
            return None
            
        try:
            content = resp_payload["choices"][0]["message"]["content"]
            # some reasoning models return reasoning wrapper or markdown json blocks
            # clean markdown json blocks just in case
            if content.startswith("```json"):
                content = content[7:-3].strip()
            elif content.startswith("```"):
                content = content[3:-3].strip()
            return json.loads(content)
        except Exception as e:
            error_msg = f"[{datetime.now().isoformat()}] [{model}] Parse Error: {str(e)} | Payload: {json.dumps(resp_payload)}\n"
            (LOGS_DIR / "openrouter_errors.log").open("a", encoding="utf-8").write(error_msg)
            print(error_msg)
            return None

    def suggest(self, issue, retrieved_context):
        if not self.enabled:
            return None
        prompt = {
            "task": "Suggest a safe data-healing action for this pipeline issue and provide Python code to execute the fix.",
            "rules": [
                "Prefer local trusted data over invention.",
                "If the issue can be fixed deterministically from local trusted data, provide Python code.",
                f"The python code must be a complete function named `heal_{issue.get('error_type')}`.",
                f"Signature MUST be exactly: `def heal_{issue.get('error_type')}(issue, healed, clean_lookup):`",
                "The `healed` argument is a dictionary: {'orders': [...], 'payments': [...], 'delivery': [...]}. Modify it in place using row_index.",
                "The `clean_lookup` argument is a dict mapping dataset names to a dict mapping order_id to row dicts.",
                "If you cannot solve it or confidence is low, say escalate and leave python_code empty.",
                "Return compact JSON only.",
            ],
            "issue": {
                "error_type": issue.get("error_type"),
                "dataset": issue.get("dataset"),
                "finding": issue.get("finding"),
                "dirty_value": issue.get("dirty_value"),
                "original_value": issue.get("original_value"),
                "order_id": issue.get("order_id"),
                "row_index": issue.get("row_index"),
                "column": issue.get("column"),
            },
            "retrieved_context": retrieved_context,
            "output_schema": {
                "route": "generate_local | escalate",
                "reason": "Explain your reasoning",
                "recommended_action": "short string",
                "confidence": "0-1 float",
                "python_code": "String containing the exact Python function definition. Empty if escalating. Do not wrap in markdown tags like ```python inside this JSON field.",
            },
        }

        # primary model attempt
        result = self._call_openrouter(self.model, prompt, include_reasoning=True, response_format={"type": "json_object"})
        if result:
            return result
            
        # fallback model attempt 1
        fallback_model_1 = "qwen/qwen3-coder:free"
        print(f"[{datetime.now().isoformat()}] Primary model {self.model} failed, falling back to {fallback_model_1}")
        result = self._call_openrouter(fallback_model_1, prompt, include_reasoning=False, response_format=None)
        if result:
            return result
            
        # fallback model attempt 2
        fallback_model_2 = "meta-llama/llama-3.3-70b-instruct:free"
        print(f"[{datetime.now().isoformat()}] Fallback model {fallback_model_1} failed, falling back to {fallback_model_2}")
        result = self._call_openrouter(fallback_model_2, prompt, include_reasoning=False, response_format=None)
        if result:
            return result
            
        # fallback model attempt 3
        fallback_model_3 = "liquid/lfm-2.5-1.2b-instruct:free"
        print(f"[{datetime.now().isoformat()}] Fallback model {fallback_model_2} failed, falling back to {fallback_model_3}")
        return self._call_openrouter(fallback_model_3, prompt, include_reasoning=False, response_format=None)
