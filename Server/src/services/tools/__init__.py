"""MCP tools package - auto-discovery and Unity routing helpers."""

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


def register_all_tools(mcp: FastMCP):
    """
    Auto-discover and register all tools in the tools/ directory.

    Any .py file in this directory or subdirectories with @mcp_for_unity_tool decorated
    functions will be automatically registered.
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

    registered_count = 0
    skipped_count = 0

    for tool_info in tools:
        func = tool_info['func']
        tool_name = tool_info['name']
        description = tool_info['description']
        kwargs = tool_info['kwargs']

        # Apply filter
        if not is_tool_enabled(tool_name):
            logger.debug(f"Skipping disabled tool: {tool_name}")
            skipped_count += 1
            continue

        # Apply the @mcp.tool decorator, telemetry, and logging
        wrapped = log_execution(tool_name, "Tool")(func)
        wrapped = telemetry_tool(tool_name)(wrapped)
        wrapped = mcp.tool(
            name=tool_name, description=description, **kwargs)(wrapped)
        tool_info['func'] = wrapped
        logger.debug(f"Registered tool: {tool_name} - {description}")
        registered_count += 1

    if skipped_count > 0:
        logger.info(f"Registered {registered_count} MCP tools ({skipped_count} filtered out)")
    else:
        logger.info(f"Registered {registered_count} MCP tools")


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
