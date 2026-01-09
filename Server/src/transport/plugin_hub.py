"""WebSocket hub for Unity plugin communication."""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from typing import Any, TYPE_CHECKING

from starlette.endpoints import WebSocketEndpoint
from starlette.websockets import WebSocket

from core.config import config

if TYPE_CHECKING:
    from fastmcp import FastMCP
from models.models import MCPResponse
from transport.plugin_registry import PluginRegistry
from transport.models import (
    WelcomeMessage,
    RegisteredMessage,
    ExecuteCommandMessage,
    RegisterMessage,
    RegisterToolsMessage,
    PongMessage,
    CommandResultMessage,
    SessionList,
    SessionDetails,
)

logger = logging.getLogger("mcp-for-unity-server")


class PluginDisconnectedError(RuntimeError):
    """Raised when a plugin WebSocket disconnects while commands are in flight."""


class NoUnitySessionError(RuntimeError):
    """Raised when no Unity plugins are available."""


class PluginHub(WebSocketEndpoint):
    """Manages persistent WebSocket connections to Unity plugins."""

    encoding = "json"
    KEEP_ALIVE_INTERVAL = 15
    SERVER_TIMEOUT = 30
    COMMAND_TIMEOUT = 30
    # Timeout (seconds) for fast-fail commands like ping/read_console/get_editor_state.
    # Keep short so MCP clients aren't blocked during Unity compilation/reload/unfocused throttling.
    FAST_FAIL_TIMEOUT = 2.0
    # Fast-path commands should never block the client for long; return a retry hint instead.
    # This helps avoid the Cursor-side ~30s tool-call timeout when Unity is compiling/reloading
    # or is throttled while unfocused.
    _FAST_FAIL_COMMANDS: set[str] = {
        "read_console", "get_editor_state", "ping"}

    _registry: PluginRegistry | None = None
    _connections: dict[str, WebSocket] = {}
    # command_id -> {"future": Future, "session_id": str}
    _pending: dict[str, dict[str, Any]] = {}
    _lock: asyncio.Lock | None = None
    _loop: asyncio.AbstractEventLoop | None = None
    _mcp: "FastMCP | None" = None

    @classmethod
    def configure(
        cls,
        registry: PluginRegistry,
        loop: asyncio.AbstractEventLoop | None = None,
        mcp: "FastMCP | None" = None,
    ) -> None:
        cls._registry = registry
        cls._loop = loop or asyncio.get_running_loop()
        cls._mcp = mcp
        # Ensure coordination primitives are bound to the configured loop
        cls._lock = asyncio.Lock()

    @classmethod
    def is_configured(cls) -> bool:
        return cls._registry is not None and cls._lock is not None

    async def on_connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        logger.info("[WebSocket] New connection accepted, sending welcome message")
        msg = WelcomeMessage(
            serverTimeout=self.SERVER_TIMEOUT,
            keepAliveInterval=self.KEEP_ALIVE_INTERVAL,
        )
        await websocket.send_json(msg.model_dump())

    async def on_receive(self, websocket: WebSocket, data: Any) -> None:
        if not isinstance(data, dict):
            logger.warning(f"Received non-object payload from plugin: {data}")
            return

        message_type = data.get("type")
        logger.info(f"[WebSocket] Received message type: {message_type}")
        try:
            if message_type == "register":
                await self._handle_register(websocket, RegisterMessage(**data))
            elif message_type == "register_tools":
                await self._handle_register_tools(websocket, RegisterToolsMessage(**data))
            elif message_type == "pong":
                await self._handle_pong(PongMessage(**data))
            elif message_type == "command_result":
                await self._handle_command_result(CommandResultMessage(**data))
            else:
                logger.debug(f"Ignoring plugin message: {data}")
        except Exception as e:
            logger.error(f"Error handling message type {message_type}: {e}")

    async def on_disconnect(self, websocket: WebSocket, close_code: int) -> None:
        cls = type(self)
        lock = cls._lock
        if lock is None:
            return
        async with lock:
            session_id = next(
                (sid for sid, ws in cls._connections.items() if ws is websocket), None)
            if session_id:
                cls._connections.pop(session_id, None)
                # Fail-fast any in-flight commands for this session to avoid waiting for COMMAND_TIMEOUT.
                pending_ids = [
                    command_id
                    for command_id, entry in cls._pending.items()
                    if entry.get("session_id") == session_id
                ]
                for command_id in pending_ids:
                    entry = cls._pending.get(command_id)
                    future = entry.get("future") if isinstance(
                        entry, dict) else None
                    if future and not future.done():
                        future.set_exception(
                            PluginDisconnectedError(
                                f"Unity plugin session {session_id} disconnected while awaiting command_result"
                            )
                        )
                if cls._registry:
                    await cls._registry.unregister(session_id)

                    # If no more sessions, clear tool filters and restore full tool list
                    remaining = await cls._registry.list_sessions()
                    if len(remaining) == 0:
                        from services.registry import clear_tool_filters
                        clear_tool_filters()
                        await cls._restore_all_tools()
                        await cls._send_tools_list_changed()
                        logger.info("All Unity sessions disconnected, restored full tool list")

                logger.info(
                    f"Plugin session {session_id} disconnected ({close_code})")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @classmethod
    async def send_command(cls, session_id: str, command_type: str, params: dict[str, Any]) -> dict[str, Any]:
        websocket = await cls._get_connection(session_id)
        command_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        # Compute a per-command timeout:
        # - fast-path commands: short timeout (encourage retry)
        # - long-running commands: allow caller to request a longer timeout via params
        unity_timeout_s = float(cls.COMMAND_TIMEOUT)
        server_wait_s = float(cls.COMMAND_TIMEOUT)
        if command_type in cls._FAST_FAIL_COMMANDS:
            fast_timeout = float(cls.FAST_FAIL_TIMEOUT)
            unity_timeout_s = fast_timeout
            server_wait_s = fast_timeout
        else:
            # Common tools pass a requested timeout in seconds (e.g., timeout_seconds=900).
            requested = None
            try:
                if isinstance(params, dict):
                    requested = params.get("timeout_seconds", None)
                    if requested is None:
                        requested = params.get("timeoutSeconds", None)
            except Exception:
                requested = None

            if requested is not None:
                try:
                    requested_s = float(requested)
                    # Clamp to a sane upper bound to avoid accidental infinite hangs.
                    requested_s = max(1.0, min(requested_s, 60.0 * 60.0))
                    unity_timeout_s = max(unity_timeout_s, requested_s)
                    # Give the server a small cushion beyond the Unity-side timeout to account for transport overhead.
                    server_wait_s = max(server_wait_s, requested_s + 5.0)
                except Exception:
                    pass

        lock = cls._lock
        if lock is None:
            raise RuntimeError("PluginHub not configured")

        async with lock:
            if command_id in cls._pending:
                raise RuntimeError(
                    f"Duplicate command id generated: {command_id}")
            cls._pending[command_id] = {
                "future": future, "session_id": session_id}

        try:
            msg = ExecuteCommandMessage(
                id=command_id,
                name=command_type,
                params=params,
                timeout=unity_timeout_s,
            )
            try:
                await websocket.send_json(msg.model_dump())
            except Exception as exc:
                # If send fails (socket already closing), fail the future so callers don't hang.
                if not future.done():
                    future.set_exception(exc)
                raise
            try:
                result = await asyncio.wait_for(future, timeout=server_wait_s)
                return result
            except PluginDisconnectedError as exc:
                return MCPResponse(success=False, error=str(exc), hint="retry").model_dump()
            except asyncio.TimeoutError:
                if command_type in cls._FAST_FAIL_COMMANDS:
                    return MCPResponse(
                        success=False,
                        error=f"Unity did not respond to '{command_type}' within {server_wait_s:.1f}s; please retry",
                        hint="retry",
                    ).model_dump()
                raise
        finally:
            async with lock:
                cls._pending.pop(command_id, None)

    @classmethod
    async def get_sessions(cls) -> SessionList:
        if cls._registry is None:
            return SessionList(sessions={})
        sessions = await cls._registry.list_sessions()
        return SessionList(
            sessions={
                session_id: SessionDetails(
                    project=session.project_name,
                    hash=session.project_hash,
                    unity_version=session.unity_version,
                    connected_at=session.connected_at.isoformat(),
                )
                for session_id, session in sessions.items()
            }
        )

    @classmethod
    async def get_tools_for_project(cls, project_hash: str) -> list[Any]:
        """Retrieve tools registered for a active project hash."""
        if cls._registry is None:
            return []

        session_id = await cls._registry.get_session_id_by_hash(project_hash)
        if not session_id:
            return []

        session = await cls._registry.get_session(session_id)
        if not session:
            return []

        return list(session.tools.values())

    @classmethod
    async def get_tool_definition(cls, project_hash: str, tool_name: str) -> Any | None:
        """Retrieve a specific tool definition for an active project hash."""
        if cls._registry is None:
            return None

        session_id = await cls._registry.get_session_id_by_hash(project_hash)
        if not session_id:
            return None

        session = await cls._registry.get_session(session_id)
        if not session:
            return None

        return session.tools.get(tool_name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _handle_register(self, websocket: WebSocket, payload: RegisterMessage) -> None:
        cls = type(self)
        registry = cls._registry
        lock = cls._lock
        if registry is None or lock is None:
            await websocket.close(code=1011)
            raise RuntimeError("PluginHub not configured")

        project_name = payload.project_name
        project_hash = payload.project_hash
        unity_version = payload.unity_version

        if not project_hash:
            await websocket.close(code=4400)
            raise ValueError(
                "Plugin registration missing project_hash")

        session_id = str(uuid.uuid4())
        # Inform the plugin of its assigned session ID
        response = RegisteredMessage(session_id=session_id)
        await websocket.send_json(response.model_dump())

        session = await registry.register(session_id, project_name, project_hash, unity_version)
        async with lock:
            cls._connections[session.session_id] = websocket
        logger.info(f"Plugin registered: {project_name} ({project_hash})")

    async def _handle_register_tools(self, websocket: WebSocket, payload: RegisterToolsMessage) -> None:
        logger.info(f"[RegisterTools] Starting _handle_register_tools with {len(payload.tools)} tools")
        cls = type(self)
        registry = cls._registry
        lock = cls._lock
        if registry is None or lock is None:
            logger.warning("[RegisterTools] Registry or lock is None, returning early")
            return

        # Find session_id for this websocket
        async with lock:
            session_id = next(
                (sid for sid, ws in cls._connections.items() if ws is websocket), None)

        if not session_id:
            logger.warning("Received register_tools from unknown connection")
            return

        logger.info(f"[RegisterTools] Found session_id: {session_id}")

        # Extract enabled tool names from Unity's selection
        enabled_tool_names = {tool.name for tool in payload.tools}
        logger.info(f"[RegisterTools] Unity enabled tools: {enabled_tool_names}")

        # Update the tool filters
        from services.registry import set_enabled_tools, set_disabled_tools, get_all_tool_names
        all_tools = set(get_all_tool_names())
        disabled_tool_names = all_tools - enabled_tool_names

        set_enabled_tools(enabled_tool_names)
        set_disabled_tools(disabled_tool_names)
        logger.info("[RegisterTools] Set tool filters")

        # Store tools in registry (existing behavior)
        await registry.register_tools_for_session(session_id, payload.tools)
        logger.info("[RegisterTools] Stored tools in registry")

        # Update FastMCP's tool manager to hide disabled tools from MCP clients
        logger.info(f"[RegisterTools] All tools: {len(all_tools)}, Disabled: {len(disabled_tool_names)}")
        logger.info(f"[RegisterTools] About to call _update_fastmcp_tools...")
        await cls._update_fastmcp_tools(enabled_tool_names, disabled_tool_names)
        logger.info("[RegisterTools] Finished _update_fastmcp_tools")

        # Notify MCP clients that the tool list has changed
        await cls._send_tools_list_changed()

        # Log enabled/disabled tools
        logger.info(
            f"Unity session {session_id}: {len(enabled_tool_names)}/{len(all_tools)} tools enabled")
        if enabled_tool_names:
            logger.info(f"  Enabled tools: {sorted(enabled_tool_names)}")
        if disabled_tool_names:
            logger.info(f"  Disabled tools: {sorted(disabled_tool_names)}")

    async def _handle_command_result(self, payload: CommandResultMessage) -> None:
        cls = type(self)
        lock = cls._lock
        if lock is None:
            return
        command_id = payload.id
        result = payload.result

        if not command_id:
            logger.warning(f"Command result missing id: {payload}")
            return

        async with lock:
            entry = cls._pending.get(command_id)
        future = entry.get("future") if isinstance(entry, dict) else None
        if future and not future.done():
            future.set_result(result)

    async def _handle_pong(self, payload: PongMessage) -> None:
        cls = type(self)
        registry = cls._registry
        if registry is None:
            return
        session_id = payload.session_id
        if session_id:
            await registry.touch(session_id)

    @classmethod
    async def _get_connection(cls, session_id: str) -> WebSocket:
        lock = cls._lock
        if lock is None:
            raise RuntimeError("PluginHub not configured")
        async with lock:
            websocket = cls._connections.get(session_id)
        if websocket is None:
            raise RuntimeError(f"Plugin session {session_id} not connected")
        return websocket

    # Storage for disabled tools so we can restore them later
    _disabled_tool_backup: dict[str, Any] = {}

    @classmethod
    async def _update_fastmcp_tools(cls, enabled_tools: set[str], disabled_tools: set[str]) -> None:
        """Update FastMCP's tool manager to hide/show tools based on Unity's selection.

        This directly modifies FastMCP's internal tool registry so disabled tools
        don't appear in the MCP tools/list response.
        """
        try:
            mcp = cls._mcp
            if mcp is None:
                logger.warning("[ToolFilter] FastMCP instance not configured")
                return

            logger.info(f"[ToolFilter] Attempting to update FastMCP tools...")
            logger.info(f"[ToolFilter] mcp type: {type(mcp)}, has _tool_manager: {hasattr(mcp, '_tool_manager')}")

            if not hasattr(mcp, '_tool_manager') or mcp._tool_manager is None:
                logger.warning("[ToolFilter] FastMCP tool manager not available")
                return

            tool_manager = mcp._tool_manager
            logger.info(f"[ToolFilter] tool_manager type: {type(tool_manager)}, has _tools: {hasattr(tool_manager, '_tools')}")

            if not hasattr(tool_manager, '_tools'):
                logger.warning("[ToolFilter] FastMCP tool manager has no _tools attribute")
                return

            logger.info(f"[ToolFilter] Current tools in manager: {list(tool_manager._tools.keys())}")

            # Restore any previously disabled tools first
            for tool_name, tool in cls._disabled_tool_backup.items():
                if tool_name not in tool_manager._tools:
                    tool_manager._tools[tool_name] = tool
                    logger.debug(f"[ToolFilter] Restored tool: {tool_name}")

            cls._disabled_tool_backup.clear()

            # Now remove the newly disabled tools
            removed_count = 0
            for tool_name in disabled_tools:
                if tool_name in tool_manager._tools:
                    cls._disabled_tool_backup[tool_name] = tool_manager._tools.pop(tool_name)
                    removed_count += 1
                    logger.debug(f"[ToolFilter] Removed tool from MCP list: {tool_name}")

            logger.info(f"[ToolFilter] Removed {removed_count} tools, {len(tool_manager._tools)} tools now visible")
            logger.info(f"[ToolFilter] Visible tools: {list(tool_manager._tools.keys())}")

        except Exception as e:
            logger.warning(f"[ToolFilter] Could not update FastMCP tools: {e}", exc_info=True)

    @classmethod
    async def _restore_all_tools(cls) -> None:
        """Restore all disabled tools when Unity disconnects."""
        try:
            mcp = cls._mcp
            if mcp is None:
                return

            if not hasattr(mcp, '_tool_manager') or mcp._tool_manager is None:
                return

            tool_manager = mcp._tool_manager
            if not hasattr(tool_manager, '_tools'):
                return

            # Restore all backed up tools
            for tool_name, tool in cls._disabled_tool_backup.items():
                if tool_name not in tool_manager._tools:
                    tool_manager._tools[tool_name] = tool
                    logger.debug(f"Restored tool: {tool_name}")

            cls._disabled_tool_backup.clear()
            logger.info(f"Restored all tools: {len(tool_manager._tools)} tools visible")

        except Exception as e:
            logger.warning(f"Could not restore FastMCP tools: {e}")

    @classmethod
    async def _send_tools_list_changed(cls) -> None:
        """Send tools/list_changed notification to MCP clients.

        This notifies clients that the available tool list has changed
        so they can re-fetch the tool list.
        """
        try:
            mcp = cls._mcp
            if mcp is None:
                return
            # FastMCP exposes the low-level MCP server for notifications
            if hasattr(mcp, '_mcp_server') and mcp._mcp_server is not None:
                await mcp._mcp_server.request_context.session.send_notification(
                    "notifications/tools/list_changed", {}
                )
                logger.debug("Sent tools/list_changed notification")
        except Exception as e:
            # Notification is best-effort; don't fail if it doesn't work
            logger.debug(f"Could not send tools/list_changed notification: {e}")

    # ------------------------------------------------------------------
    # Session resolution helpers
    # ------------------------------------------------------------------
    @classmethod
    async def _resolve_session_id(cls, unity_instance: str | None) -> str:
        """Resolve a project hash (Unity instance id) to an active plugin session.

        During Unity domain reloads the plugin's WebSocket session is torn down
        and reconnected shortly afterwards. Instead of failing immediately when
        no sessions are available, we wait for a bounded period for a plugin
        to reconnect so in-flight MCP calls can succeed transparently.
        """
        if cls._registry is None:
            raise RuntimeError("Plugin registry not configured")

        # Bound waiting for Unity sessions so calls fail fast when editors are not ready.
        try:
            max_wait_s = float(
                os.environ.get("UNITY_MCP_SESSION_RESOLVE_MAX_WAIT_S", "2.0"))
        except ValueError as e:
            raw_val = os.environ.get(
                "UNITY_MCP_SESSION_RESOLVE_MAX_WAIT_S", "2.0")
            logger.warning(
                "Invalid UNITY_MCP_SESSION_RESOLVE_MAX_WAIT_S=%r, using default 2.0: %s",
                raw_val, e)
            max_wait_s = 2.0
        # Clamp to [0, 30] to prevent misconfiguration from causing excessive waits
        max_wait_s = max(0.0, min(max_wait_s, 30.0))
        retry_ms = float(getattr(config, "reload_retry_ms", 250))
        sleep_seconds = max(0.05, min(0.25, retry_ms / 1000.0))

        # Allow callers to provide either just the hash or Name@hash
        target_hash: str | None = None
        if unity_instance:
            if "@" in unity_instance:
                _, _, suffix = unity_instance.rpartition("@")
                target_hash = suffix or None
            else:
                target_hash = unity_instance

        async def _try_once() -> tuple[str | None, int]:
            # Prefer a specific Unity instance if one was requested
            if target_hash:
                session_id = await cls._registry.get_session_id_by_hash(target_hash)
                sessions = await cls._registry.list_sessions()
                return session_id, len(sessions)

            # No target provided: determine if we can auto-select
            sessions = await cls._registry.list_sessions()
            count = len(sessions)
            if count == 0:
                return None, count
            if count == 1:
                return next(iter(sessions.keys())), count
            # Multiple sessions but no explicit target is ambiguous
            return None, count

        session_id, session_count = await _try_once()
        deadline = time.monotonic() + max_wait_s
        wait_started = None

        # If there is no active plugin yet (e.g., Unity starting up or reloading),
        # wait politely for a session to appear before surfacing an error.
        while session_id is None and time.monotonic() < deadline:
            if not target_hash and session_count > 1:
                raise RuntimeError(
                    "Multiple Unity instances are connected. "
                    "Call set_active_instance with Name@hash from mcpforunity://instances."
                )
            if wait_started is None:
                wait_started = time.monotonic()
                logger.debug(
                    "No plugin session available (instance=%s); waiting up to %.2fs",
                    unity_instance or "default",
                    max_wait_s,
                )
            await asyncio.sleep(sleep_seconds)
            session_id, session_count = await _try_once()

        if session_id is not None and wait_started is not None:
            logger.debug(
                "Plugin session restored after %.3fs (instance=%s)",
                time.monotonic() - wait_started,
                unity_instance or "default",
            )
        if session_id is None and not target_hash and session_count > 1:
            raise RuntimeError(
                "Multiple Unity instances are connected. "
                "Call set_active_instance with Name@hash from mcpforunity://instances."
            )

        if session_id is None:
            logger.warning(
                "No Unity plugin reconnected within %.2fs (instance=%s)",
                max_wait_s,
                unity_instance or "default",
            )
            # At this point we've given the plugin ample time to reconnect; surface
            # a clear error so the client can prompt the user to open Unity.
            raise NoUnitySessionError(
                "No Unity plugins are currently connected")

        return session_id

    @classmethod
    async def send_command_for_instance(
        cls,
        unity_instance: str | None,
        command_type: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            session_id = await cls._resolve_session_id(unity_instance)
        except NoUnitySessionError:
            logger.debug(
                "Unity session unavailable; returning retry: command=%s instance=%s",
                command_type,
                unity_instance or "default",
            )
            return MCPResponse(
                success=False,
                error="Unity session not available; please retry",
                hint="retry",
                data={"reason": "no_unity_session", "retry_after_ms": 250},
            ).model_dump()

        # During domain reload / immediate reconnect windows, the plugin may be connected but not yet
        # ready to process execute commands on the Unity main thread (which can be further delayed when
        # the Unity Editor is unfocused). For fast-path commands, we do a bounded readiness probe using
        # a main-thread ping command (handled by TransportCommandDispatcher) rather than waiting on
        # register_tools (which can be delayed by EditorApplication.delayCall).
        if command_type in cls._FAST_FAIL_COMMANDS and command_type != "ping":
            try:
                max_wait_s = float(os.environ.get(
                    "UNITY_MCP_SESSION_READY_WAIT_SECONDS", "6"))
            except ValueError as e:
                raw_val = os.environ.get(
                    "UNITY_MCP_SESSION_READY_WAIT_SECONDS", "6")
                logger.warning(
                    "Invalid UNITY_MCP_SESSION_READY_WAIT_SECONDS=%r, using default 6.0: %s",
                    raw_val, e)
                max_wait_s = 6.0
            max_wait_s = max(0.0, min(max_wait_s, 30.0))
            if max_wait_s > 0:
                deadline = time.monotonic() + max_wait_s
                while time.monotonic() < deadline:
                    try:
                        probe = await cls.send_command(session_id, "ping", {})
                    except Exception:
                        probe = None

                    # The Unity-side dispatcher responds with {status:"success", result:{message:"pong"}}
                    if isinstance(probe, dict) and probe.get("status") == "success":
                        result = probe.get("result") if isinstance(
                            probe.get("result"), dict) else {}
                        if result.get("message") == "pong":
                            break
                    await asyncio.sleep(0.1)
                else:
                    # Not ready within the bounded window: return retry hint without sending.
                    return MCPResponse(
                        success=False,
                        error=f"Unity session not ready for '{command_type}' (ping not answered); please retry",
                        hint="retry",
                    ).model_dump()

        return await cls.send_command(session_id, command_type, params)

    # ------------------------------------------------------------------
    # Blocking helpers for synchronous tool code
    # ------------------------------------------------------------------
    @classmethod
    def _run_coroutine_sync(cls, coro: "asyncio.Future[Any]") -> Any:
        if cls._loop is None:
            raise RuntimeError("PluginHub event loop not configured")
        loop = cls._loop
        if loop.is_running():
            try:
                running_loop = asyncio.get_running_loop()
            except RuntimeError:
                running_loop = None
            else:
                if running_loop is loop:
                    raise RuntimeError(
                        "Cannot wait synchronously for PluginHub coroutine from within the event loop"
                    )
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()

    @classmethod
    def send_command_blocking(
        cls,
        unity_instance: str | None,
        command_type: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._run_coroutine_sync(
            cls.send_command_for_instance(unity_instance, command_type, params)
        )

    @classmethod
    def list_sessions_sync(cls) -> SessionList:
        return cls._run_coroutine_sync(cls.get_sessions())


def send_command_to_plugin(
    *,
    unity_instance: str | None,
    command_type: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    return PluginHub.send_command_blocking(unity_instance, command_type, params)
