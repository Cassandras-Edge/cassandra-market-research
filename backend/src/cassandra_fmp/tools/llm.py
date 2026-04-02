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

        Args:
            prompt: What to analyze or determine. Be specific about the output
                    format you want (e.g. "return a JSON with signal, confidence,
                    and reasoning fields").
            data: The data to analyze. Can be a dict/list from tool results,
                  or a string. Omit if the prompt is self-contained.
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
            "confidence level and reasoning. Respond in valid JSON when the "
            "prompt asks for structured output."
        )

        user_msg = prompt
        if data_str:
            user_msg = f"{prompt}\n\n--- DATA ---\n{data_str}"

        try:
            async with httpx.AsyncClient(timeout=30) as http:
                resp = await http.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENAI_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": OPENAI_MODEL,
                        "messages": [
                            {"role": "system", "content": system_msg},
                            {"role": "user", "content": user_msg},
                        ],
                        "max_tokens": 2048,
                    },
                )
                resp.raise_for_status()
                result = resp.json()
                content = result["choices"][0]["message"]["content"]

                # Try to parse as JSON if it looks like JSON
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
