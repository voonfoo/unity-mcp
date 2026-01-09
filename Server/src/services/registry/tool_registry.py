"""
Tool registry for auto-discovery of MCP tools.
"""
from typing import Callable, Any

# Global registry to collect decorated tools
_tool_registry: list[dict[str, Any]] = []

# Runtime tool filter state
_enabled_tools: set[str] | None = None  # None = no filter, all tools enabled
_disabled_tools: set[str] = set()  # Explicitly disabled tools


def set_enabled_tools(tools: set[str] | None) -> None:
    """Set the enabled tools filter.

    Args:
        tools: Set of tool names to enable. If None, all tools are enabled.
    """
    global _enabled_tools
    _enabled_tools = tools


def set_disabled_tools(tools: set[str]) -> None:
    """Set the disabled tools filter.

    Args:
        tools: Set of tool names to disable.
    """
    global _disabled_tools
    _disabled_tools = tools


def get_enabled_tools() -> set[str] | None:
    """Get the current enabled tools set."""
    return _enabled_tools


def get_disabled_tools() -> set[str]:
    """Get the current disabled tools set."""
    return _disabled_tools


def clear_tool_filters() -> None:
    """Clear all tool filters (called when Unity disconnects)."""
    global _enabled_tools, _disabled_tools
    _enabled_tools = None
    _disabled_tools = set()


def is_tool_enabled(tool_name: str) -> bool:
    """Check if a tool should be enabled based on current filters.

    Returns True if:
    - Tool is not in the disabled set, AND
    - Either no enabled filter is set, OR the tool is in the enabled set
    """
    if tool_name in _disabled_tools:
        return False
    if _enabled_tools is not None:
        return tool_name in _enabled_tools
    return True


def get_all_tool_names() -> list[str]:
    """Get names of all registered (unfiltered) tools."""
    return [t['name'] for t in _tool_registry]


def mcp_for_unity_tool(
    name: str | None = None,
    description: str | None = None,
    **kwargs
) -> Callable:
    """
    Decorator for registering MCP tools in the server's tools directory.

    Tools are registered in the global tool registry.

    Args:
        name: Tool name (defaults to function name)
        description: Tool description
        **kwargs: Additional arguments passed to @mcp.tool()

    Example:
        @mcp_for_unity_tool(description="Does something cool")
        async def my_custom_tool(ctx: Context, ...):
            pass
    """
    def decorator(func: Callable) -> Callable:
        tool_name = name if name is not None else func.__name__
        _tool_registry.append({
            'func': func,
            'name': tool_name,
            'description': description,
            'kwargs': kwargs
        })

        return func

    return decorator


def get_registered_tools() -> list[dict[str, Any]]:
    """Get all registered tools"""
    return _tool_registry.copy()


def clear_tool_registry():
    """Clear the tool registry (useful for testing)"""
    _tool_registry.clear()
