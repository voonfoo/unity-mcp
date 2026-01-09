"""MCP tools package - auto-discovery and Unity routing helpers."""

import functools
import inspect
import logging
import os
from pathlib import Path
from typing import TypeVar

from fastmcp import Context, FastMCP
from core.telemetry_decorator import telemetry_tool
from core.logging_decorator import log_execution
from utils.module_discovery import discover_modules
from services.registry import get_registered_tools, is_tool_enabled

logger = logging.getLogger("mcp-for-unity-server")

# Export decorator and helpers for easy imports within tools
__all__ = [
    "register_all_tools",
    "get_unity_instance_from_context",
]


def _create_guarded_tool(func, tool_name: str):
    """Create a wrapper that checks if the tool is enabled before execution.

    If the tool is disabled by Unity's selection, returns an error
    instead of executing the tool.
    """
    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def async_guarded(*args, **kwargs):
            if not is_tool_enabled(tool_name):
                return {
                    "error": f"Tool '{tool_name}' is currently disabled. "
                             "Enable it in Unity Editor's MCP window (Tools tab).",
                    "disabled": True
                }
            return await func(*args, **kwargs)
        return async_guarded
    else:
        @functools.wraps(func)
        def sync_guarded(*args, **kwargs):
            if not is_tool_enabled(tool_name):
                return {
                    "error": f"Tool '{tool_name}' is currently disabled. "
                             "Enable it in Unity Editor's MCP window (Tools tab).",
                    "disabled": True
                }
            return func(*args, **kwargs)
        return sync_guarded


def register_all_tools(mcp: FastMCP):
    """
    Auto-discover and register all tools in the tools/ directory.

    Any .py file in this directory or subdirectories with @mcp_for_unity_tool decorated
    functions will be automatically registered.

    All tools are registered with FastMCP, but each tool has a guard that checks
    is_tool_enabled() at runtime. This allows Unity to dynamically enable/disable
    tools without restarting the server.
    """
    logger.info("Auto-discovering MCP for Unity Server tools...")
    # Dynamic import of all modules in this directory
    tools_dir = Path(__file__).parent

    # Discover and import all modules
    list(discover_modules(tools_dir, __package__))

    tools = get_registered_tools()

    if not tools:
        logger.warning("No MCP tools registered!")
        return

    for tool_info in tools:
        func = tool_info['func']
        tool_name = tool_info['name']
        description = tool_info['description']
        kwargs = tool_info['kwargs']

        # Wrap with guard that checks is_tool_enabled() at runtime
        guarded = _create_guarded_tool(func, tool_name)

        # Apply the @mcp.tool decorator, telemetry, and logging
        wrapped = log_execution(tool_name, "Tool")(guarded)
        wrapped = telemetry_tool(tool_name)(wrapped)
        wrapped = mcp.tool(
            name=tool_name, description=description, **kwargs)(wrapped)
        tool_info['func'] = wrapped
        logger.debug(f"Registered tool: {tool_name} - {description}")

    logger.info(f"Registered {len(tools)} MCP tools (runtime filtering enabled)")


def get_unity_instance_from_context(
    ctx: Context,
    key: str = "unity_instance",
) -> str | None:
    """Extract the unity_instance value from middleware state.

    The instance is set via the set_active_instance tool and injected into
    request state by UnityInstanceMiddleware.
    """
    get_state_fn = getattr(ctx, "get_state", None)
    if callable(get_state_fn):
        try:
            return get_state_fn(key)
        except Exception:  # pragma: no cover - defensive
            pass

    return None
