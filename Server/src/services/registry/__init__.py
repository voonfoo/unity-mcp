"""
Registry package for MCP tool auto-discovery.
"""
from .tool_registry import (
    mcp_for_unity_tool,
    get_registered_tools,
    clear_tool_registry,
    set_enabled_tools,
    set_disabled_tools,
    get_enabled_tools,
    get_disabled_tools,
    clear_tool_filters,
    is_tool_enabled,
    get_all_tool_names,
)
from .resource_registry import (
    mcp_for_unity_resource,
    get_registered_resources,
    clear_resource_registry,
)

__all__ = [
    'mcp_for_unity_tool',
    'get_registered_tools',
    'clear_tool_registry',
    'set_enabled_tools',
    'set_disabled_tools',
    'get_enabled_tools',
    'get_disabled_tools',
    'clear_tool_filters',
    'is_tool_enabled',
    'get_all_tool_names',
    'mcp_for_unity_resource',
    'get_registered_resources',
    'clear_resource_registry'
]
