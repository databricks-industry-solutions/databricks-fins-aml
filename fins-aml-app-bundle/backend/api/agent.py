"""
MAS Agent API endpoints
"""

import os
import logging
import re
import requests
import json
import asyncio
import httpx
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from typing import List, Dict, Any, AsyncGenerator

from backend.models.schemas import ChatRequestModel, ChatResponseModel, ChatMessageModel
from backend import config

router = APIRouter()
logger = logging.getLogger(__name__)

# MAS Agent endpoint configuration
MAS_ENDPOINT_URL = config.MAS_ENDPOINT_URL

# Chunk size for simulated streaming (in characters)
STREAM_CHUNK_SIZE = 80
STREAM_CHUNK_DELAY = 0.03  # seconds between chunks

# ---------------------------------------------------------------------------
# Real-time streaming constants
# ---------------------------------------------------------------------------
STREAM_TIMEOUT = 180.0  # seconds for the full streaming connection

# Friendly display names for MAS sub-agents / tools
AGENT_DISPLAY_NAMES = {
    "FIN-AML-case-details": "Case Details Search",
    "FIN-AML-policies": "Policies & Regulations Search",
    "FIN-AML-media": "Adverse Media Search",
    "agent-aml-case360-executive-view": "Case360 — querying cases database",
    "agent-aml-alert360-executive-view": "Alert360 — querying alerts database",
    "you-search": "You.com — searching the web",
    "you-contents": "You.com — extracting page content",
    "you-research": "You.com — deep research",
    "FIN-AML-mas": "Synthesizing results",
}

_NAME_TAG_RE = re.compile(r"<name>(.*?)</name>")


def _friendly_agent_name(raw_name: str) -> str:
    """Resolve a raw MAS agent/tool name to a user-friendly display name."""
    return AGENT_DISPLAY_NAMES.get(raw_name, raw_name.replace("-", " ").title())


def _extract_text_from_item(item: dict) -> str:
    """Pull concatenated text from a MAS output item's content array."""
    content = item.get("content", [])
    if isinstance(content, list):
        return " ".join(
            c.get("text", "") for c in content if isinstance(c, dict)
        )
    return str(content)


def _sse_event(data: dict) -> str:
    """Format a dict as an SSE data line."""
    return f"data: {json.dumps(data)}\n\n"


def _extract_final_answer(result: dict) -> str:
    """Extract only the final assistant message from MAS response.

    The MAS output array contains multiple items: function_call,
    function_call_output, and message types.  We only want the last
    message-type item which holds the human-readable answer.
    """
    if "output" in result and isinstance(result["output"], list):
        items = result["output"]
        logger.info(f"🤖 Processing output array with {len(items)} items")

        # Walk backwards to find the last 'message' type item
        for item in reversed(items):
            if not isinstance(item, dict):
                continue
            item_type = item.get("type", "")
            role = item.get("role", "")

            # Skip tool calls and tool outputs
            if item_type in ("function_call", "function_call_output"):
                continue

            # Accept message-type items from the assistant
            if item_type == "message" and role == "assistant":
                content = item.get("content", [])
                if isinstance(content, list):
                    texts = []
                    for c in content:
                        if isinstance(c, dict) and "text" in c:
                            texts.append(c["text"])
                    if texts:
                        return "\n".join(texts).strip()
                elif isinstance(content, str) and content.strip():
                    return content.strip()

        # Fallback: grab any text from any output item
        all_texts = []
        for item in items:
            if not isinstance(item, dict):
                continue
            # Skip function calls entirely
            if item.get("type") in ("function_call", "function_call_output"):
                continue
            content = item.get("content", [])
            if isinstance(content, list):
                for c in content:
                    if isinstance(c, dict) and "text" in c:
                        all_texts.append(c["text"])
            elif isinstance(content, str):
                all_texts.append(content)
        if all_texts:
            return "\n".join(all_texts).strip()

    # Handle simple string output
    if "output" in result and isinstance(result["output"], str):
        return result["output"].strip()

    # Other formats
    for key in ("response", "text"):
        if key in result and isinstance(result[key], str):
            return result[key].strip()

    logger.warning(f"⚠️ Could not extract answer. Keys: {list(result.keys())}")
    return ""


def _format_response(text: str) -> str:
    """Clean up and format the response text for chat display.

    - Collapse very large markdown tables to a summary
    - Trim excessive whitespace
    """
    if not text:
        return text

    # Collapse very large markdown tables (>20 rows) into a summary
    def _collapse_table(match):
        table_text = match.group(0)
        rows = [r for r in table_text.strip().split("\n") if r.strip()]
        # rows[0] = header, rows[1] = separator, rows[2:] = data
        if len(rows) <= 22:  # header + sep + 20 data rows
            return table_text

        header = rows[0]
        separator = rows[1]
        data_rows = rows[2:]
        shown = data_rows[:10]
        remaining = len(data_rows) - 10

        collapsed = "\n".join([header, separator] + shown)
        collapsed += f"\n\n*... and {remaining} more rows (showing top 10 of {len(data_rows)})*\n"
        return collapsed

    # Match markdown tables: lines starting with |
    text = re.sub(
        r'(?:^\|.+\|$\n?){4,}',
        _collapse_table,
        text,
        flags=re.MULTILINE,
    )

    return text.strip()


async def get_agent_service():
    """Get agent service with authentication (OAuth M2M preferred)"""
    try:
        token = config.get_oauth_token()
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Agent authentication not configured")
    return AgentService(token)


class AgentService:
    """Service for interacting with MAS Agent endpoint"""

    def __init__(self, token: str):
        self.token = token
        self.endpoint_url = MAS_ENDPOINT_URL

    def _build_payload(self, message: str, context: Dict[str, Any],
                       chat_history: List[ChatMessageModel] = None) -> dict:
        """Build the request payload for the MAS endpoint."""
        conversation = []
        if chat_history:
            logger.info(f"🤖 Including {len(chat_history)} messages from chat history")
            for msg in chat_history[-5:]:
                conversation.append({"role": msg.role, "content": msg.content})
        conversation.append({"role": "user", "content": message})

        conversation_id = f"alert_{context.get('alert_id', 'unknown')}_{context.get('customer_id', 'unknown')}"
        logger.info(f"🤖 Conversation ID: {conversation_id}")

        return {
            "input": conversation,
            "databricks_options": {
                "conversation_id": conversation_id,
                "return_trace": True,
            },
            "context": {
                "conversation_id": conversation_id,
                "user_id": "analyst",
            },
        }

    def _call_endpoint(self, payload: dict) -> dict:
        """Make the synchronous HTTP call to the MAS endpoint."""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        logger.info(f"🤖 Sending request to MAS endpoint: {self.endpoint_url}")
        logger.info(f"🤖 Payload size: {len(json.dumps(payload))} bytes")

        response = requests.post(
            self.endpoint_url, json=payload, headers=headers, timeout=120,
        )

        if response.status_code != 200:
            logger.error(f"❌ MAS Agent error: {response.status_code} - {response.text[:500]}")
            raise HTTPException(status_code=503, detail="Agent service error")

        logger.info("✅ MAS Agent Response received - Status: 200")
        return response.json()

    async def send_message(self, message: str, context: Dict[str, Any],
                           chat_history: List[ChatMessageModel] = None) -> Dict[str, Any]:
        """Send message to MAS agent endpoint and return final answer."""
        try:
            logger.info(f"🤖 MAS Agent Request - Message: {message[:100]}...")
            logger.info(f"🤖 Context: alert_id={context.get('alert_id')}, customer_id={context.get('customer_id')}")

            payload = self._build_payload(message, context, chat_history)

            # Run blocking HTTP call in a separate thread so the event loop
            # stays free for asyncio.sleep() in the streaming generator.
            result = await asyncio.to_thread(self._call_endpoint, payload)

            logger.info(f"🤖 Response structure keys: {list(result.keys())}")
            response_text = _extract_final_answer(result)
            response_text = _format_response(response_text)

            logger.info(f"✅ Final response length: {len(response_text)} characters")

            return {
                "response": response_text or "I received your request but couldn't generate a response.",
                "confidence_score": 0.9,
                "recommendation": None,
                "evidence": [],
            }

        except requests.exceptions.Timeout:
            logger.error(f"❌ MAS Agent request timeout after 120 seconds")
            raise HTTPException(
                status_code=504,
                detail="Agent response timeout - The request took too long. Please try a simpler question or try again later.",
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ MAS Agent connection error: {e}")
            raise HTTPException(status_code=503, detail="Unable to connect to agent service")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error calling MAS agent: {e}")
            logger.error(f"❌ Error type: {type(e).__name__}")
            raise HTTPException(status_code=500, detail=f"Internal agent service error: {str(e)[:100]}")

    async def _stream_endpoint(self, payload: dict) -> AsyncGenerator[str, None]:
        """Open a real SSE connection to the MAS endpoint with stream=true.

        Yields frontend-compatible SSE ``data:`` lines by mapping MAS events:
          response.output_text.delta        -> {"content": delta}
          response.output_item.done (func)  -> {"activity": {...}}
          response.output_item.done (<name>)-> {"activity": {...}}
          response.output_item.done (other) -> skipped (raw sub-agent data)
          mcp_approval_request              -> auto-approve and continue
          [DONE]                            -> {"done": true}
        """
        stream_payload = {**payload, "stream": True}
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        timeout = httpx.Timeout(STREAM_TIMEOUT, connect=15.0)

        # State tracked across the stream (and potential approval continuation)
        current_step = 0
        has_content = False
        active_agent = "MAS Supervisor"
        last_tool_agents = []

        # Conversation history accumulated for MCP approval follow-ups
        history_items = list(stream_payload.get("input", []))
        pending_approval = None  # set when mcp_approval_request is received

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:

                # --- Inner helper: stream one request and process events ---
                async def _process_stream(req_payload):
                    nonlocal current_step, has_content, active_agent
                    nonlocal last_tool_agents, history_items, pending_approval

                    async with client.stream(
                        "POST", self.endpoint_url,
                        json=req_payload, headers=headers,
                    ) as response:
                        if response.status_code != 200:
                            body = await response.aread()
                            logger.error(f"❌ MAS stream error: {response.status_code} - {body[:500]}")
                            yield _sse_event({"error": f"Agent service error (HTTP {response.status_code})"})
                            return

                        logger.info("✅ MAS SSE stream connected")
                        assistant_text_parts = []  # accumulate assistant text for history

                        async for line in response.aiter_lines():
                            if not line or not line.startswith("data: "):
                                continue

                            raw = line[6:]

                            if raw.strip() == "[DONE]":
                                # If an MCP approval is pending, don't yield done —
                                # the caller will send the approval and continue.
                                if pending_approval:
                                    # Save accumulated assistant text to history
                                    if assistant_text_parts:
                                        history_items.append({
                                            "type": "message", "role": "assistant",
                                            "content": [{"type": "output_text", "text": "".join(assistant_text_parts)}],
                                        })
                                        assistant_text_parts.clear()
                                    return
                                yield _sse_event({"done": True})
                                return

                            try:
                                event = json.loads(raw)
                            except json.JSONDecodeError:
                                logger.warning(f"⚠️ Skipping malformed SSE line: {raw[:120]}")
                                continue

                            event_type = event.get("type", "")

                            # --- Text delta ---
                            if event_type == "response.output_text.delta":
                                step = event.get("step", current_step)
                                delta = event.get("delta", "")
                                if delta:
                                    if step > current_step and has_content:
                                        if last_tool_agents:
                                            sources = ", ".join(last_tool_agents)
                                            header = f"\n\n---\n\n**Source: {sources}**\n\n"
                                        else:
                                            header = f"\n\n---\n\n**{active_agent}**\n\n"
                                        yield _sse_event({"content": header})
                                        last_tool_agents = []
                                    current_step = max(current_step, step)
                                    has_content = True
                                    assistant_text_parts.append(delta)
                                    yield _sse_event({"content": delta})

                            # --- Item completed ---
                            elif event_type == "response.output_item.done":
                                item = event.get("item", {})
                                item_type = item.get("type", "")

                                if item_type == "function_call":
                                    tool_name = item.get("name", "unknown")
                                    friendly = _friendly_agent_name(tool_name)
                                    last_tool_agents.append(friendly)
                                    logger.info(f"🔧 Tool call: {tool_name} -> {friendly}")
                                    yield _sse_event({
                                        "activity": {
                                            "agent": friendly,
                                            "action": f"Retrieving data from {friendly}",
                                            "tool": tool_name,
                                            "status": "running",
                                        }
                                    })

                                elif item_type == "mcp_approval_request":
                                    # MCP tool needs approval — store it and
                                    # show activity; auto-approve after [DONE]
                                    tool_name = item.get("name", "unknown")
                                    friendly = _friendly_agent_name(tool_name)
                                    last_tool_agents.append(friendly)
                                    pending_approval = item
                                    logger.info(f"🔐 MCP approval request: {tool_name} — auto-approving")
                                    yield _sse_event({
                                        "activity": {
                                            "agent": friendly,
                                            "action": f"Searching via {friendly}",
                                            "tool": tool_name,
                                            "status": "running",
                                        }
                                    })

                                elif item_type == "message":
                                    text = _extract_text_from_item(item)
                                    name_match = _NAME_TAG_RE.search(text)
                                    if name_match:
                                        agent_name = name_match.group(1)
                                        friendly = _friendly_agent_name(agent_name)
                                        active_agent = friendly
                                        logger.info(f"🔀 Agent handoff: {agent_name} -> {friendly}")
                                        yield _sse_event({
                                            "activity": {
                                                "agent": friendly,
                                                "action": AGENT_DISPLAY_NAMES.get(agent_name, "Processing"),
                                                "status": "active",
                                            }
                                        })

                        # Stream ended without [DONE]
                        if not pending_approval:
                            yield _sse_event({"done": True})

                # ---- Main flow: first stream ----
                async for event_line in _process_stream(stream_payload):
                    yield event_line

                # ---- MCP approval continuation (if needed) ----
                if pending_approval:
                    approval_id = pending_approval.get("id", "")
                    logger.info(f"🔐 Sending auto-approval for {approval_id}")

                    # Build the continuation payload with full history
                    history_items.append({
                        "type": "mcp_approval_request",
                        "id": approval_id,
                        "arguments": pending_approval.get("arguments", "{}"),
                        "name": pending_approval.get("name", ""),
                        "server_label": pending_approval.get("server_label", ""),
                    })
                    history_items.append({
                        "type": "mcp_approval_response",
                        "approval_request_id": approval_id,
                        "approve": True,
                        "id": f"auto_approve_{approval_id}",
                    })

                    continuation_payload = {
                        **stream_payload,
                        "input": history_items,
                    }
                    pending_approval = None  # reset for the continuation

                    async for event_line in _process_stream(continuation_payload):
                        yield event_line

        except httpx.ReadTimeout:
            logger.error("❌ MAS stream read timeout")
            yield _sse_event({"error": "Response timeout -- the agent took too long. Please try again."})
        except httpx.ConnectError as e:
            logger.error(f"❌ MAS stream connect error: {e}")
            yield _sse_event({"error": "Unable to connect to agent service."})
        except Exception as e:
            logger.error(f"❌ Unexpected streaming error: {type(e).__name__}: {e}")
            yield _sse_event({"error": f"Streaming error: {str(e)[:100]}"})

    async def stream_message(self, message: str, context: Dict[str, Any],
                             chat_history: List[ChatMessageModel] = None) -> AsyncGenerator[str, None]:
        """Stream real-time SSE events from the MAS endpoint."""
        try:
            logger.info(f"🔄 Starting real-time stream for: {message[:100]}...")
            payload = self._build_payload(message, context, chat_history)
            async for event_line in self._stream_endpoint(payload):
                yield event_line
        except Exception as e:
            logger.error(f"❌ Error in stream_message: {e}")
            yield _sse_event({"error": f"Failed to stream response: {str(e)[:100]}"})

@router.post("/chat", response_model=ChatResponseModel)
async def agent_chat(
    request: ChatRequestModel,
    agent_service: AgentService = Depends(get_agent_service)
):
    """Chat with MAS agent"""
    try:
        result = await agent_service.send_message(
            message=request.message,
            context=request.context,
            chat_history=request.chat_history
        )

        return ChatResponseModel(
            response=result["response"],
            confidence_score=result.get("confidence_score"),
            recommendation=result.get("recommendation"),
            evidence=result.get("evidence")
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in agent chat endpoint: {e}")
        raise HTTPException(status_code=500, detail="Failed to process agent chat")

@router.post("/chat/stream")
async def agent_chat_stream(
    request: ChatRequestModel,
    agent_service: AgentService = Depends(get_agent_service)
):
    """Simple streaming chat"""
    try:
        async def generate_stream():
            try:
                async for chunk in agent_service.stream_message(
                    message=request.message,
                    context=request.context,
                    chat_history=request.chat_history
                ):
                    yield chunk
            except Exception as e:
                logger.error(f"Error in streaming: {e}")
                yield f"data: {json.dumps({'error': 'Streaming error'})}\n\n"

        return StreamingResponse(
            generate_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "Transfer-Encoding": "chunked",
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in streaming endpoint: {e}")
        raise HTTPException(status_code=500, detail="Failed to process streaming chat")

@router.post("/warmup")
async def agent_warmup():
    """Fire-and-forget warmup ping to the MAS endpoint.

    Called automatically when a user visits the app.  The request wakes
    the serverless serving endpoint so it is ready by the time the user
    starts chatting.  Returns immediately — the actual HTTP call to
    MAS runs in the background.
    """
    async def _ping():
        try:
            token = config.get_oauth_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            payload = {
                "input": [{"role": "user", "content": "hello"}],
                "stream": False,
            }
            timeout = httpx.Timeout(90.0, connect=15.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    MAS_ENDPOINT_URL, json=payload, headers=headers,
                )
                logger.info(f"🔥 MAS warmup ping: HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ MAS warmup ping failed (non-blocking): {e}")

    # Launch the ping in the background — don't block the response
    asyncio.create_task(_ping())
    logger.info("🔥 MAS warmup triggered")
    return {"status": "warming", "message": "MAS endpoint warmup initiated"}


@router.get("/test")
async def test_agent_endpoint(agent_service: AgentService = Depends(get_agent_service)):
    """Test agent endpoint"""
    try:
        test_context = {
            "alert_id": "TEST-001",
            "customer_id": "test_customer"
        }

        result = await agent_service.send_message(
            message="Hello, this is a test.",
            context=test_context,
            chat_history=[]
        )

        return {
            "status": "success",
            "response": result["response"][:200] + "..." if len(result["response"]) > 200 else result["response"]
        }

    except HTTPException as e:
        return {"status": "error", "error": e.detail}
    except Exception as e:
        return {"status": "error", "error": str(e)}