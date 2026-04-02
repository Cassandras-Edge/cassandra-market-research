"""LLM analysis tool — available inside Code Mode sandbox for data interpretation."""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from fastmcp import FastMCP

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.environ.get("OPENAI_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_ANALYSIS_MODEL", "gpt-4.1-mini")


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        tags={"analysis"},
        annotations={
            "title": "Analyze Data",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        },
    )
    async def analyze(
        prompt: str,
        data: str | dict | list | None = None,
        schema: dict | None = None,
    ) -> dict:
        """Analyze financial data using an LLM. Use inside Code Mode scripts to
        interpret, summarize, or make determinations from tool results.

        Typical uses:
        - Interpret COT positioning shifts and identify sentiment signals
        - Compare financial metrics across peers and rank them
        - Synthesize earnings data into a bull/bear thesis
        - Score or grade data against criteria you define in the prompt
        - Identify anomalies or trends in time series data

        The prompt should describe what analysis you want. Pass tool results
        as the data parameter — the LLM sees both and returns its analysis.

        When schema is provided, the response is guaranteed to conform to it
        via OpenAI structured outputs. Example schema:

            {"signal": "string", "confidence": "number", "reasoning": "string"}

        Shorthand types (string, number, integer, boolean, array) are expanded
        automatically. For nested objects, use dicts. For arrays of objects,
        use [{"field": "type"}].

        Args:
            prompt: What to analyze or determine.
            data: The data to analyze. Can be a dict/list from tool results,
                  or a string. Omit if the prompt is self-contained.
            schema: Optional output schema. When provided, OpenAI structured
                    outputs guarantees the response matches this shape exactly.
        """
        if not OPENAI_API_KEY:
            return {"error": "OPENAI_KEY not configured — analyze tool unavailable"}

        # Serialize data for the LLM
        if data is not None:
            if isinstance(data, (dict, list)):
                data_str = json.dumps(data, indent=2, default=str)
            else:
                data_str = str(data)
        else:
            data_str = None

        system_msg = (
            "You are a financial data analyst. Analyze the provided data and "
            "respond to the user's prompt. Be concise and quantitative. "
            "When asked for a signal or determination, always include your "
            "confidence level and reasoning."
        )

        user_msg = prompt
        if data_str:
            user_msg = f"{prompt}\n\n--- DATA ---\n{data_str}"

        request_body: dict = {
            "model": OPENAI_MODEL,
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            "max_tokens": 2048,
        }

        # Build response_format for structured outputs
        if schema is not None:
            json_schema = _shorthand_to_json_schema(schema)
            request_body["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "analysis",
                    "strict": True,
                    "schema": json_schema,
                },
            }

        try:
            async with httpx.AsyncClient(timeout=30) as http:
                resp = await http.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENAI_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json=request_body,
                )
                resp.raise_for_status()
                result = resp.json()
                content = result["choices"][0]["message"]["content"]

                # When using structured outputs, response is always valid JSON
                if schema is not None:
                    return {"analysis": json.loads(content)}

                # Without schema, try to parse as JSON if it looks like it
                stripped = content.strip()
                if stripped.startswith(("{", "[")):
                    try:
                        return {"analysis": json.loads(stripped)}
                    except json.JSONDecodeError:
                        pass

                return {"analysis": content}

        except httpx.HTTPError as e:
            logger.exception("LLM analysis call failed")
            return {"error": f"LLM call failed: {e}"}


# Shorthand type names → JSON Schema types
_TYPE_MAP = {
    "string": {"type": "string"},
    "str": {"type": "string"},
    "number": {"type": "number"},
    "float": {"type": "number"},
    "integer": {"type": "integer"},
    "int": {"type": "integer"},
    "boolean": {"type": "boolean"},
    "bool": {"type": "boolean"},
}


def _shorthand_to_json_schema(shorthand: dict | list | str) -> dict:
    """Convert a shorthand schema to a full JSON Schema for OpenAI structured outputs.

    Shorthand format:
        {"signal": "string", "confidence": "number"}
        → full JSON Schema object with required fields, additionalProperties: false

    Supports nested objects, arrays of primitives, and arrays of objects:
        {"items": [{"name": "string", "value": "number"}]}
        → array of objects

        {"tags": ["string"]}
        → array of strings
    """
    if isinstance(shorthand, str):
        return _TYPE_MAP.get(shorthand, {"type": shorthand})

    if isinstance(shorthand, list):
        if not shorthand:
            return {"type": "array", "items": {"type": "string"}}
        return {"type": "array", "items": _shorthand_to_json_schema(shorthand[0])}

    if isinstance(shorthand, dict):
        # Check if it's already a valid JSON Schema (has "type" key)
        if "type" in shorthand and isinstance(shorthand.get("type"), str):
            return shorthand

        properties = {}
        for key, value in shorthand.items():
            if isinstance(value, str):
                properties[key] = _TYPE_MAP.get(value, {"type": value})
            elif isinstance(value, dict):
                properties[key] = _shorthand_to_json_schema(value)
            elif isinstance(value, list):
                properties[key] = _shorthand_to_json_schema(value)
            else:
                properties[key] = {"type": "string"}

        return {
            "type": "object",
            "properties": properties,
            "required": list(properties.keys()),
            "additionalProperties": False,
        }

    return {"type": "string"}
