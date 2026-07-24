"""Tests for Slack's deterministic ``Read aloud`` message shortcut."""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import SendResult
from plugins.platforms.slack import adapter as slack_module
from plugins.platforms.slack.adapter import (
    HERMES_READ_ALOUD_SHORTCUT_ID,
    SlackAdapter,
)


def _adapter() -> SlackAdapter:
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-fake"))
    adapter._app = MagicMock()
    adapter._app.client = AsyncMock()
    adapter._team_clients["T_WORKSPACE"] = adapter._app.client
    adapter._running = True
    return adapter


def _shortcut_body(text: str | None = "Read this exactly.") -> dict:
    message = {} if text is None else {"text": text}
    return {
        "callback_id": HERMES_READ_ALOUD_SHORTCUT_ID,
        "trigger_id": "trigger-1",
        "team": {"id": "T_WORKSPACE"},
        "user": {"id": "U_INVOKER"},
        "channel": {"id": "C_SOURCE"},
        "message": message,
    }


@pytest.fixture(autouse=True)
def _stable_tts_limit():
    with (
        patch("tools.tts_tool._load_tts_config", return_value={}),
        patch("tools.tts_tool._get_provider", return_value="edge"),
        patch("tools.tts_tool._resolve_max_text_length", return_value=5000),
    ):
        yield


@pytest.mark.asyncio
async def test_shortcut_acks_before_starting_background_work():
    adapter = _adapter()
    events: list[str] = []
    release = asyncio.Event()

    async def ack():
        events.append("ack")

    async def process(body):
        del body
        events.append("work")
        await release.wait()

    adapter._process_read_aloud_shortcut = process

    await adapter._handle_read_aloud_shortcut(ack, _shortcut_body())
    await asyncio.sleep(0)

    assert events == ["ack", "work"]
    tasks = list(adapter._background_tasks)
    assert len(tasks) == 1
    release.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_duplicate_shortcut_trigger_is_acked_without_duplicate_tts_work():
    adapter = _adapter()
    release = asyncio.Event()
    started = 0

    async def ack():
        return None

    async def process(body):
        nonlocal started
        del body
        started += 1
        await release.wait()

    adapter._process_read_aloud_shortcut = process
    body = _shortcut_body()

    await adapter._handle_read_aloud_shortcut(ack, body)
    await adapter._handle_read_aloud_shortcut(ack, body)
    await asyncio.sleep(0)

    assert started == 1
    tasks = list(adapter._background_tasks)
    release.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_authorized_shortcut_synthesizes_exact_text_and_uploads_only_to_invoker_dm():
    adapter = _adapter()
    adapter.set_authorization_check(
        lambda user_id, chat_type, chat_id: (
            user_id,
            chat_type,
            chat_id,
        )
        == ("U_INVOKER", "group", "C_SOURCE")
    )
    uploaded_paths: list[Path] = []

    def synthesize(
        text: str, output_path: str, *, _strict_max_length: bool = False
    ) -> str:
        assert _strict_max_length is True
        assert text == "Read this exactly."
        path = Path(output_path)
        path.write_bytes(b"fake mp3")
        return json.dumps({"success": True, "file_path": str(path)})

    async def upload(**kwargs):
        path = Path(kwargs["file"])
        assert path.exists()
        uploaded_paths.append(path)
        assert kwargs["channel"] == "D_PRIVATE"
        return {"ok": True}

    client = AsyncMock()

    async def open_dm(**kwargs):
        assert kwargs == {"users": "U_INVOKER"}
        adapter._team_clients.clear()
        return {"channel": {"id": "D_PRIVATE"}}

    client.conversations_open = AsyncMock(side_effect=open_dm)
    client.files_upload_v2 = AsyncMock(side_effect=upload)
    primary_client = AsyncMock()
    adapter._app = MagicMock(client=primary_client)
    adapter._team_clients["T_WORKSPACE"] = client
    adapter.send = AsyncMock()

    with patch("tools.tts_tool.text_to_speech_tool", side_effect=synthesize) as tts:
        await adapter._process_read_aloud_shortcut(_shortcut_body())

    assert tts.call_count == 1
    client.conversations_open.assert_awaited_once_with(users="U_INVOKER")
    client.files_upload_v2.assert_awaited_once()
    primary_client.conversations_open.assert_not_awaited()
    primary_client.files_upload_v2.assert_not_awaited()
    adapter.send.assert_not_awaited()
    assert uploaded_paths and not uploaded_paths[0].exists()
    assert client.files_upload_v2.await_args.kwargs["channel"] != "C_SOURCE"


@pytest.mark.asyncio
async def test_unauthorized_shortcut_is_denied_before_tts_and_notifies_in_dm():
    adapter = _adapter()
    adapter.set_authorization_check(lambda *_args: False)
    adapter.send = AsyncMock(return_value=SendResult(success=True))

    with patch("tools.tts_tool.text_to_speech_tool") as tts:
        await adapter._process_read_aloud_shortcut(_shortcut_body())

    tts.assert_not_called()
    adapter.send.assert_awaited_once()
    args, kwargs = adapter.send.await_args
    assert args[0] == "U_INVOKER"
    assert "not authorized" in args[1].lower()
    assert kwargs["metadata"] == {"slack_team_id": "T_WORKSPACE"}


@pytest.mark.asyncio
async def test_private_error_keeps_exact_client_if_workspace_routes_change_mid_send():
    adapter = _adapter()
    exact_client = AsyncMock()
    primary_client = AsyncMock()

    async def open_dm(**kwargs):
        assert kwargs == {"users": "U_INVOKER"}
        adapter._team_clients.clear()
        return {"channel": {"id": "D_PRIVATE"}}

    exact_client.conversations_open = AsyncMock(side_effect=open_dm)
    exact_client.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "1.0"})
    adapter._app = MagicMock(client=primary_client)
    adapter._team_clients["T_WORKSPACE"] = exact_client

    await adapter._send_read_aloud_error(
        "U_INVOKER", "T_WORKSPACE", "Private failure"
    )

    exact_client.conversations_open.assert_awaited_once_with(users="U_INVOKER")
    exact_client.chat_postMessage.assert_awaited_once()
    post_call = exact_client.chat_postMessage.await_args
    assert post_call is not None
    assert post_call.kwargs["channel"] == "D_PRIVATE"
    primary_client.conversations_open.assert_not_awaited()
    primary_client.chat_postMessage.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("team_id", ["", "T_UNKNOWN"])
async def test_missing_or_unknown_workspace_fails_closed_without_primary_fallback(team_id):
    adapter = _adapter()
    adapter.set_authorization_check(lambda *_args: True)
    adapter.send = AsyncMock(return_value=SendResult(success=True))
    primary_client = adapter._team_clients["T_WORKSPACE"]
    body = _shortcut_body()
    body["team"] = {"id": team_id} if team_id else {}

    with patch("tools.tts_tool.text_to_speech_tool") as tts:
        await adapter._process_read_aloud_shortcut(body)

    tts.assert_not_called()
    adapter.send.assert_not_awaited()
    primary_client.conversations_open.assert_not_awaited()
    primary_client.files_upload_v2.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("text", [None, "", "   "])
async def test_missing_or_empty_selected_text_gets_private_dm_error(text):
    adapter = _adapter()
    adapter.set_authorization_check(lambda *_args: True)
    adapter.send = AsyncMock(return_value=SendResult(success=True))

    with patch("tools.tts_tool.text_to_speech_tool") as tts:
        await adapter._process_read_aloud_shortcut(_shortcut_body(text))

    tts.assert_not_called()
    adapter.send.assert_awaited_once()
    assert adapter.send.await_args.args[0] == "U_INVOKER"
    assert "readable text" in adapter.send.await_args.args[1].lower()


@pytest.mark.asyncio
async def test_overlong_message_gets_private_error_instead_of_truncated_audio():
    adapter = _adapter()
    adapter.set_authorization_check(lambda *_args: True)
    adapter.send = AsyncMock(return_value=SendResult(success=True))

    with (
        patch("tools.tts_tool._resolve_max_text_length", return_value=5),
        patch("tools.tts_tool.text_to_speech_tool") as tts,
    ):
        await adapter._process_read_aloud_shortcut(_shortcut_body("longer than five"))

    tts.assert_not_called()
    adapter.send.assert_awaited_once()
    error_call = adapter.send.await_args
    assert error_call is not None
    assert error_call.args[0] == "U_INVOKER"
    assert "too long" in error_call.args[1].lower()


@pytest.mark.asyncio
async def test_provider_limit_change_cannot_upload_silently_truncated_audio():
    adapter = _adapter()
    adapter.set_authorization_check(lambda *_args: True)
    adapter.send = AsyncMock(return_value=SendResult(success=True))
    adapter.send_voice = AsyncMock(return_value=SendResult(success=True))

    with (
        patch("tools.tts_tool._load_tts_config", return_value={"provider": "openai"}),
        patch("tools.tts_tool._get_provider", return_value="openai"),
        patch("tools.tts_tool._resolve_max_text_length", side_effect=[10000, 5]),
        patch("tools.tts_tool._generate_openai_tts") as generate,
    ):
        await adapter._process_read_aloud_shortcut(_shortcut_body("123456"))

    generate.assert_not_called()
    adapter.send_voice.assert_not_awaited()
    adapter.send.assert_awaited_once()
    error_call = adapter.send.await_args
    assert error_call is not None
    assert "couldn't synthesize" in error_call.args[1].lower()


@pytest.mark.asyncio
async def test_read_aloud_burst_is_bounded_before_tts():
    adapter = _adapter()
    adapter.set_authorization_check(lambda *_args: True)
    adapter._READ_ALOUD_MAX_IN_FLIGHT = 1
    adapter._read_aloud_in_flight.add(("T_WORKSPACE", "U_OTHER"))
    adapter.send = AsyncMock(return_value=SendResult(success=True))

    with patch("tools.tts_tool.text_to_speech_tool") as tts:
        await adapter._process_read_aloud_shortcut(_shortcut_body())

    tts.assert_not_called()
    adapter.send.assert_awaited_once()
    error_call = adapter.send.await_args
    assert error_call is not None
    assert "busy" in error_call.args[1].lower()


@pytest.mark.asyncio
async def test_cancellation_waits_for_tts_worker_then_skips_upload_and_cleans_temp():
    adapter = _adapter()
    adapter.set_authorization_check(lambda *_args: True)
    adapter.send_voice = AsyncMock(return_value=SendResult(success=True))
    started = threading.Event()
    release = threading.Event()
    generated_paths: list[Path] = []

    def synthesize(
        text: str, output_path: str, *, _strict_max_length: bool = False
    ) -> str:
        assert _strict_max_length is True
        del text
        path = Path(output_path)
        generated_paths.append(path)
        started.set()
        release.wait(timeout=2)
        path.write_bytes(b"fake mp3")
        return json.dumps({"success": True, "file_path": str(path)})

    with patch("tools.tts_tool.text_to_speech_tool", side_effect=synthesize):
        task = asyncio.create_task(
            adapter._process_read_aloud_shortcut(_shortcut_body())
        )
        assert await asyncio.to_thread(started.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    adapter.send_voice.assert_not_awaited()
    assert not adapter._read_aloud_in_flight
    assert generated_paths and not generated_paths[0].exists()


@pytest.mark.asyncio
async def test_tts_failure_gets_private_dm_error_and_cleans_temp_file():
    adapter = _adapter()
    adapter.set_authorization_check(lambda *_args: True)
    adapter.send = AsyncMock(return_value=SendResult(success=True))
    generated_paths: list[Path] = []

    def fail_tts(
        text: str, output_path: str, *, _strict_max_length: bool = False
    ) -> str:
        assert _strict_max_length is True
        assert text == "Read this exactly."
        path = Path(output_path)
        path.write_bytes(b"partial")
        generated_paths.append(path)
        return json.dumps({"success": False, "error": "provider unavailable"})

    with patch("tools.tts_tool.text_to_speech_tool", side_effect=fail_tts):
        await adapter._process_read_aloud_shortcut(_shortcut_body())

    adapter.send.assert_awaited_once()
    assert adapter.send.await_args.args[0] == "U_INVOKER"
    assert "couldn't synthesize" in adapter.send.await_args.args[1].lower()
    assert generated_paths and not generated_paths[0].exists()


@pytest.mark.asyncio
async def test_upload_failure_gets_private_dm_error_and_cleans_temp_file():
    adapter = _adapter()
    adapter.set_authorization_check(lambda *_args: True)
    adapter.send = AsyncMock(return_value=SendResult(success=True))
    generated_paths: list[Path] = []

    def synthesize(
        text: str, output_path: str, *, _strict_max_length: bool = False
    ) -> str:
        assert _strict_max_length is True
        assert text == "Read this exactly."
        path = Path(output_path)
        path.write_bytes(b"fake mp3")
        generated_paths.append(path)
        return json.dumps({"success": True, "file_path": str(path)})

    adapter.send_voice = AsyncMock(
        return_value=SendResult(success=False, error="upload failed")
    )

    with patch("tools.tts_tool.text_to_speech_tool", side_effect=synthesize):
        await adapter._process_read_aloud_shortcut(_shortcut_body())

    adapter.send_voice.assert_awaited_once()
    adapter.send.assert_awaited_once()
    assert adapter.send.await_args.args[0] == "U_INVOKER"
    assert "couldn't upload" in adapter.send.await_args.args[1].lower()
    assert generated_paths and not generated_paths[0].exists()


@pytest.mark.asyncio
async def test_slack_control_tokens_are_cleaned_before_tts():
    adapter = _adapter()
    adapter.set_authorization_check(lambda *_args: True)
    adapter._resolve_user_name = AsyncMock(return_value="Cristiano")
    adapter.send_voice = AsyncMock(return_value=SendResult(success=True))
    spoken: list[str] = []

    def synthesize(
        text: str, output_path: str, *, _strict_max_length: bool = False
    ) -> str:
        assert _strict_max_length is True
        spoken.append(text)
        path = Path(output_path)
        path.write_bytes(b"fake mp3")
        return json.dumps({"success": True, "file_path": str(path)})

    body = _shortcut_body(
        "Hi &lt;@U123ABC&gt; — see <https://example.com|the link> in <#C456DEF|general>."
    )
    with patch("tools.tts_tool.text_to_speech_tool", side_effect=synthesize):
        await adapter._process_read_aloud_shortcut(body)

    assert spoken == ["Hi @Cristiano — see the link in #general."]
    assert "<@" not in spoken[0]
    assert "<#" not in spoken[0]


@pytest.mark.asyncio
async def test_connect_registers_read_aloud_message_shortcut():
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-fake"))
    shortcuts: dict[str, object] = {}

    def decorator(_matcher):
        def register(fn):
            return fn

        return register

    def shortcut_decorator(callback_id):
        def register(fn):
            shortcuts[callback_id] = fn
            return fn

        return register

    mock_app = MagicMock()
    mock_app.event = decorator
    mock_app.command = decorator
    mock_app.action = decorator
    mock_app.shortcut = shortcut_decorator
    mock_app.client = AsyncMock()

    web_client = AsyncMock()
    web_client.auth_test = AsyncMock(
        return_value={
            "user_id": "U_BOT",
            "user": "hermes",
            "team_id": "T_WORKSPACE",
            "team": "Workspace",
        }
    )

    with (
        patch.object(slack_module, "AsyncApp", return_value=mock_app),
        patch.object(slack_module, "AsyncWebClient", return_value=web_client),
        patch.dict("os.environ", {"SLACK_APP_TOKEN": "xapp-fake"}),
        patch("gateway.status.acquire_scoped_lock", return_value=(True, None)),
        patch.object(adapter, "_start_socket_mode_handler"),
        patch.object(adapter, "_ensure_socket_watchdog"),
    ):
        assert await adapter.connect() is True

    assert HERMES_READ_ALOUD_SHORTCUT_ID in shortcuts
    assert shortcuts[HERMES_READ_ALOUD_SHORTCUT_ID] is not None
