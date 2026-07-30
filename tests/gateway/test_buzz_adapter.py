"""Tests for the Buzz platform adapter plugin."""

import asyncio
import json

import pytest
from unittest.mock import AsyncMock, MagicMock

from tests.gateway._plugin_adapter_loader import load_plugin_adapter

# Load plugins/platforms/buzz/adapter.py under a unique module name
# (plugin_adapter_buzz) so it cannot collide with other plugin adapters
# loaded by sibling tests in the same xdist worker.
_buzz_mod = load_plugin_adapter("buzz")

BuzzAdapter = _buzz_mod.BuzzAdapter
hex_to_npub = _buzz_mod.hex_to_npub
npub_to_hex = _buzz_mod.npub_to_hex
_normalize_user_ref = _buzz_mod._normalize_user_ref
_cli_error_message = _buzz_mod._cli_error_message
_resolve_private_key = _buzz_mod._resolve_private_key
_resolve_auth_tag = _buzz_mod._resolve_auth_tag
_exec_buzz = _buzz_mod._exec_buzz
check_requirements = _buzz_mod.check_requirements
validate_config = _buzz_mod.validate_config
register = _buzz_mod.register
_env_enablement = _buzz_mod._env_enablement
_standalone_send = _buzz_mod._standalone_send

# Real key pair (Chip's public identity — public information, not a secret)
SELF_PUBKEY = "9fd5c7ba6d3ef224da78f541e0fcb9c50f72cc63edb19aae76ac6a0474dfa860"
SELF_NPUB = "npub1nl2u0wnd8mezfknc74q7pl9ec58h9nrrakce4tnk434qgaxl4psqe5twr6"
OTHER_PUBKEY = "a" * 64
CHANNEL = "ccc2bc1a-7a82-5a8f-8c4e-57a070cbe7cd"
# Real DM conversation as materialized by a hosted relay: `dms list` returns
# [] for it (#68871) while `channels list` shows it as name "DM", empty
# description, indistinguishable from a channel except via message p-tags.
DM_CHANNEL = "6468cc16-a114-4f23-8b8c-02c1655cbf6b"
# A second hosted-relay DM: same empty `dms list`, but its kind-9 messages
# carry only ["h", <dm id>] — no p-tag for the latch to key off.
HOSTED_DM = "4e645536-f4af-4d6d-8123-caebeeadd7ec"

_ENV_VARS = (
    "BUZZ_RELAY_URL",
    "BUZZ_PRIVATE_KEY",
    "BUZZ_CHANNELS",
    "BUZZ_HOME_CHANNEL",
    "BUZZ_ALLOWED_USERS",
    "BUZZ_ALLOW_ALL_USERS",
    "BUZZ_POLL_INTERVAL",
    "BUZZ_CLI_PATH",
    "BUZZ_CREDENTIALS_FILE",
    "BUZZ_AUTH_TAG",
)

# Obviously-fake NIP-OA auth tags (four-string ["auth", …] shape). Never use
# real credentials in tests.
FAKE_FILE_AUTH_TAG = json.dumps(["auth", "f" * 64, "", "1" * 128])
FAKE_ENV_AUTH_TAG = json.dumps(["auth", "e" * 64, "", "2" * 128])


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    """Keep tests hermetic: no ambient Buzz env vars or real credentials."""
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(_buzz_mod, "_DEFAULT_CREDENTIALS_DIR", tmp_path / "no-creds")
    yield


def _event(event_id, pubkey=OTHER_PUBKEY, content="hello", created_at=1000, kind=9):
    return {
        "id": event_id,
        "pubkey": pubkey,
        "content": content,
        "created_at": created_at,
        "kind": kind,
        "tags": [["h", CHANNEL]],
    }


def _make_adapter(extra=None):
    from gateway.config import PlatformConfig

    cfg = PlatformConfig(enabled=True, extra={"relay_url": "https://test.relay", **(extra or {})})
    adapter = BuzzAdapter(cfg)
    adapter._self_pubkey = SELF_PUBKEY
    adapter._self_npub = SELF_NPUB
    adapter._display_name = "Chip"
    adapter._private_key = "nsec1test"
    return adapter


class _ScriptedCli:
    """Fake ``_run_cli`` that routes on the buzz subcommand and records calls."""

    def __init__(self):
        self.responses = {}  # (group, cmd) -> list of (code, stdout, stderr)
        self.calls = []

    def script(self, group, cmd, payload, code=0, stderr=""):
        stdout = payload if isinstance(payload, str) else json.dumps(payload)
        self.responses.setdefault((group, cmd), []).append((code, stdout, stderr))

    async def __call__(self, args, *, input_text=None):
        self.calls.append((list(args), input_text))
        queue = self.responses.get((args[0], args[1]), [])
        if len(queue) > 1:
            return queue.pop(0)
        if queue:
            return queue[0]
        return 0, "[]", ""


# ── bech32 / identity helpers ─────────────────────────────────────────────


class TestBech32Helpers:

    def test_hex_to_npub_known_pair(self):
        assert hex_to_npub(SELF_PUBKEY) == SELF_NPUB

    def test_npub_to_hex_known_pair(self):
        assert npub_to_hex(SELF_NPUB) == SELF_PUBKEY


# ── Adapter init / config precedence ──────────────────────────────────────


class TestBuzzAdapterInit:


    def test_init_from_config_extra(self):
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(
            enabled=True,
            extra={
                "relay_url": "https://cfg.relay",
                "channels": ["ccc"],
                "poll_interval": 2,
                "home_channel": "ccc",
            },
        )
        adapter = BuzzAdapter(cfg)
        assert adapter.relay_url == "https://cfg.relay"
        assert adapter.channels == ["ccc"]
        assert adapter.poll_interval == 2.0
        assert adapter.home_channel == "ccc"

    def test_env_overrides_config(self, monkeypatch):
        monkeypatch.setenv("BUZZ_RELAY_URL", "https://env.relay")
        from gateway.config import PlatformConfig
        adapter = BuzzAdapter(PlatformConfig(enabled=True, extra={"relay_url": "https://cfg.relay"}))
        assert adapter.relay_url == "https://env.relay"


# ── CLI error contract ────────────────────────────────────────────────────


class TestCliErrorContract:

    def test_parses_json_error(self):
        msg = _cli_error_message('{"error":"relay_error","message":"boom","retryable":false}', 2)
        assert "relay_error" in msg and "boom" in msg and "exit 2" in msg


# ── Seeding / high-water mark / de-dupe ───────────────────────────────────


class TestPollingDedupe:

    @pytest.fixture
    def adapter(self):
        a = _make_adapter()
        a._dispatched = []

        async def capture(**kwargs):
            a._dispatched.append(kwargs)

        a._dispatch_message = capture
        a._message_handler = AsyncMock()
        return a

    @pytest.mark.asyncio
    async def test_seed_sets_high_water_mark_without_dispatch(self, adapter):
        cli = _ScriptedCli()
        cli.script("messages", "get", [
            _event("e1", content="@Chip old history", created_at=100),
            _event("e2", content="@Chip newer history", created_at=200),
        ])
        adapter._run_cli = cli
        await adapter._seed_channel(CHANNEL, chat_type="group")

        state = adapter._channel_state[CHANNEL]
        assert state["last_ts"] == 200
        assert set(state["seen"]) == {"e1", "e2"}
        # Seeding must never replay history into the agent
        assert adapter._dispatched == []

    @pytest.mark.asyncio
    async def test_new_event_dispatched_once(self, adapter):
        cli = _ScriptedCli()
        cli.script("messages", "get", [_event("e1", content="@Chip hi", created_at=100)])
        adapter._run_cli = cli
        await adapter._seed_channel(CHANNEL, chat_type="group")

        # Poll 1: seeded event + a genuinely new mention
        cli.responses.clear()
        cli.script("messages", "get", [
            _event("e1", content="@Chip hi", created_at=100),
            _event("e2", content="hey @Chip, ping", created_at=150),
        ])
        await adapter._poll_channel(CHANNEL)
        assert [d["message_id"] for d in adapter._dispatched] == ["e2"]
        assert adapter._dispatched[0]["text"] == "hey @Chip, ping"
        assert adapter._channel_state[CHANNEL]["last_ts"] == 150

        # Poll 2: identical response — the seen-id set must de-dupe
        await adapter._poll_channel(CHANNEL)
        assert len(adapter._dispatched) == 1


# ── Mention gating / DMs / authorization ──────────────────────────────────


class TestMentionGating:

    @pytest.fixture
    def adapter(self):
        a = _make_adapter()
        a._dispatched = []

        async def capture(**kwargs):
            a._dispatched.append(kwargs)

        a._dispatch_message = capture
        a._message_handler = AsyncMock()
        a._channel_state[CHANNEL] = {"chat_type": "group", "last_ts": 0, "seen": {}}
        return a

    async def _poll_with(self, adapter, *events):
        cli = _ScriptedCli()
        cli.script("messages", "get", list(events))
        adapter._run_cli = cli
        await adapter._poll_channel(CHANNEL)

    @pytest.mark.asyncio
    async def test_unaddressed_channel_message_ignored(self, adapter):
        await self._poll_with(adapter, _event("e1", content="just chatting", created_at=10))
        assert adapter._dispatched == []

    @pytest.mark.asyncio
    async def test_name_mention_dispatched(self, adapter):
        await self._poll_with(adapter, _event("e1", content="hey @Chip can you help?", created_at=10))
        assert len(adapter._dispatched) == 1


    @pytest.mark.asyncio
    async def test_allowlist_blocks_unauthorized(self, adapter):
        adapter._allowed_pubkeys = {"b" * 64}
        await self._poll_with(adapter, _event("e1", content="@Chip hello", created_at=10))
        assert adapter._dispatched == []


# ── DM classification via p-tags (issue #68871) ──────────────────────────
#
# `buzz dms list` returns [] on some hosted relays, so DM conversations leak
# in via `channels list` and get seeded chat_type="group".  The adapter must
# reclassify them from the Nostr tags of real traffic: DM messages are
# p-tagged to our own pubkey WITHOUT the text mentioning us, while channel
# messages only ever p-tag us when the text visibly @mentions us.


def _tagged_event(event_id, channel, *, content, pubkey=OTHER_PUBKEY,
                  created_at=1000, kind=9, p=None, reply_to=None):
    """Event with the tag shapes observed on a live relay (h/p/e tags)."""
    tags = [["h", channel]]
    if reply_to:
        tags.append(["e", reply_to, "", "reply"])
    if p:
        tags.append(["p", p])
    return {
        "id": event_id,
        "pubkey": pubkey,
        "content": content,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
    }


class TestDmClassification:

    @pytest.fixture
    def adapter(self):
        a = _make_adapter()
        a._dispatched = []

        async def capture(**kwargs):
            a._dispatched.append(kwargs)

        a._dispatch_message = capture
        a._message_handler = AsyncMock()
        # Metadata exactly as `channels list` returns it on the hosted relay.
        a._channel_meta = {
            DM_CHANNEL: {"channel_id": DM_CHANNEL, "name": "DM", "description": ""},
            CHANNEL: {
                "channel_id": CHANNEL,
                "name": "general",
                "description": "General conversation and community updates.",
            },
        }
        a._channel_names = {DM_CHANNEL: "DM", CHANNEL: "general"}
        # Both leaked in as group — the bug under test.
        a._channel_state[DM_CHANNEL] = {"chat_type": "group", "last_ts": 0, "seen": {}}
        a._channel_state[CHANNEL] = {"chat_type": "group", "last_ts": 0, "seen": {}}
        return a

    async def _poll_with(self, adapter, channel, *events):
        cli = _ScriptedCli()
        cli.script("messages", "get", list(events))
        adapter._run_cli = cli
        await adapter._poll_channel(channel)

    @pytest.mark.asyncio
    async def test_unmentioned_ptagged_dm_latches_and_dispatches(self, adapter):
        """The reported bug: a DM without an @mention must dispatch."""
        await self._poll_with(
            adapter, DM_CHANNEL,
            _tagged_event("e1", DM_CHANNEL, content="here's a test message", p=SELF_PUBKEY),
        )
        assert adapter._channel_state[DM_CHANNEL]["chat_type"] == "dm"
        assert [d["message_id"] for d in adapter._dispatched] == ["e1"]
        assert adapter._dispatched[0]["chat_type"] == "dm"


    @pytest.mark.asyncio
    async def test_general_reply_ptagging_self_stays_channel(self, adapter):
        """A #general reply to us p-tags our pubkey (observed live) — that
        must NOT reclassify the channel; mention gating still applies."""
        await self._poll_with(
            adapter, CHANNEL,
            _tagged_event("e1", CHANNEL, content="@chip what's up?",
                          p=SELF_PUBKEY, reply_to="root-event"),
        )
        assert adapter._channel_state[CHANNEL]["chat_type"] == "group"
        # It carried a mention, so it dispatches — but as a group message.
        assert [d["chat_type"] for d in adapter._dispatched] == ["group"]

        # And once the mention is absent, the channel gate drops the message
        # even though the earlier reply p-tagged us.
        await self._poll_with(
            adapter, CHANNEL,
            _tagged_event("e2", CHANNEL, content="thanks everyone", created_at=1001),
        )
        assert len(adapter._dispatched) == 1


    @pytest.mark.asyncio
    async def test_channel_like_metadata_blocks_latch_even_without_mention(self, adapter):
        """Second guard on its own: even a p-tagged, un-mentioned message
        cannot reclassify a conversation whose metadata says real channel."""
        adapter._channel_meta[CHANNEL]["description"] = ""
        adapter._channel_meta[CHANNEL]["name"] = "announcements"
        await self._poll_with(
            adapter, CHANNEL,
            _tagged_event("e1", CHANNEL, content="fyi everyone", p=SELF_PUBKEY),
        )
        assert adapter._channel_state[CHANNEL]["chat_type"] == "group"
        assert adapter._dispatched == []


    @pytest.mark.asyncio
    async def test_dm_shaped_channel_discovered_when_dms_list_empty(self):
        """Fallback discovery: with `dms list` broken (returns []), a
        DM-shaped `channels list` entry gets watched; real channels not
        already watched are left alone."""
        a = _make_adapter()
        cli = _ScriptedCli()
        cli.script("dms", "list", [])
        cli.script("channels", "list", [
            {"channel_id": DM_CHANNEL, "name": "DM", "description": "", "created_at": 1},
            {"channel_id": CHANNEL, "name": "general",
             "description": "General conversation and community updates.", "created_at": 2},
        ])
        a._run_cli = cli
        await a._discover_dms(seed=False)
        # Watched as group; the p-tag latch flips it on the first real DM.
        assert a._channel_state[DM_CHANNEL]["chat_type"] == "group"
        assert a._may_reclassify_as_dm(DM_CHANNEL) is True
        assert CHANNEL not in a._channel_state
        assert a._may_reclassify_as_dm(CHANNEL) is False


# ── Hosted-relay DMs discovered by kind-44100 membership events ──────────
#
# Second flavour of #68871, seen on https://fckall.communities.buzz.xyz:
# `dms list` is empty, the DM only shows up in `channels list` as
# name "DM"/empty description/no channel_type, AND the owner's kind-9
# messages carry nothing but ["h", <dm id>] — no p-tag at all.  The p-tag
# latch can never fire for those, so classification has to come from the
# authenticated kind-44100 membership event that put the conversation in
# front of us in the first place.


class _RecordingWebSocket:
    """Captures the frames the adapter sends; no relay round-trip."""

    def __init__(self):
        self.sent = []

    async def send(self, raw):
        self.sent.append(json.loads(raw))


def _membership_event(*, conversation=None, created_at=5000, pubkey=OTHER_PUBKEY):
    """A kind-44100 channel-membership event p-tagged to us, as delivered on
    the authenticated (NIP-42) membership subscription."""
    tags = [["p", SELF_PUBKEY]]
    if conversation:
        tags.append(["h", conversation])
    return {
        "id": "m1",
        "pubkey": pubkey,
        "kind": 44100,
        "created_at": created_at,
        "content": "",
        "tags": tags,
    }


def _h_only_event(event_id, channel, *, content="here's a test message",
                  pubkey=OTHER_PUBKEY, created_at=5001):
    """Owner message exactly as the hosted relay emits it inside a DM:
    kind 9 with a single ["h", <dm id>] tag and no p-tag."""
    return {
        "id": event_id,
        "pubkey": pubkey,
        "content": content,
        "created_at": created_at,
        "kind": 9,
        "tags": [["h", channel]],
    }


class TestHostedDmMembershipDiscovery:

    def _adapter(self, extra=None):
        a = _make_adapter(extra)
        a._dispatched = []

        async def capture(**kwargs):
            a._dispatched.append(kwargs)

        a._dispatch_message = capture
        a._message_handler = AsyncMock()
        return a

    def _cli(self, *channel_entries):
        cli = _ScriptedCli()
        cli.script("dms", "list", [])          # hosted relay: always empty
        cli.script("channels", "list", list(channel_entries))
        return cli

    @pytest.mark.asyncio
    async def test_membership_discovered_dm_dispatches_h_only_message(self):
        """The reported bug end to end: `dms list` empty, DM-shaped fallback
        metadata, discovery driven by an authenticated membership event, then
        a kind-9 owner message with no p-tag — it must route as a DM."""
        a = self._adapter()
        a._run_cli = self._cli(
            {"channel_id": HOSTED_DM, "name": "DM", "description": ""},
            {"channel_id": CHANNEL, "name": "general",
             "description": "General conversation and community updates."},
        )
        websocket = _RecordingWebSocket()
        subscriptions = {}

        await a._handle_membership_event(
            websocket, subscriptions, _membership_event(conversation=HOSTED_DM)
        )

        # The new conversation is watched and subscribed to over the WS.
        assert a._channel_state[HOSTED_DM]["chat_type"] == "dm"
        assert HOSTED_DM in subscriptions.values()
        assert [f[2]["#h"] for f in websocket.sent if f[0] == "REQ"] == [[HOSTED_DM]]
        # The real channel is neither watched nor reclassified.
        assert CHANNEL not in a._channel_state

        await a._handle_event(
            HOSTED_DM, a._channel_state[HOSTED_DM], _h_only_event("e1", HOSTED_DM)
        )
        assert [d["message_id"] for d in a._dispatched] == ["e1"]
        assert a._dispatched[0]["chat_type"] == "dm"

    @pytest.mark.asyncio
    async def test_real_channel_named_dm_is_not_reclassified(self):
        """Negative: a real channel that merely calls itself "DM" must not
        become a DM or escape the mention gate, even when it shows up in the
        same authenticated membership pass."""
        a = self._adapter()
        a._run_cli = self._cli(
            # Real channels carry a channel_type; relay-materialized DMs don't.
            {"channel_id": CHANNEL, "name": "DM", "description": "",
             "channel_type": "channel"},
        )
        await a._handle_membership_event(
            _RecordingWebSocket(), {}, _membership_event(conversation=CHANNEL)
        )

        assert a._channel_state[CHANNEL]["chat_type"] == "group"
        await a._handle_event(
            CHANNEL, a._channel_state[CHANNEL], _h_only_event("e1", CHANNEL)
        )
        assert a._dispatched == []
        # Still reachable the normal way — the mention gate, not a mute.
        await a._handle_event(
            CHANNEL, a._channel_state[CHANNEL],
            _h_only_event("e2", CHANNEL, content="@Chip ping", created_at=5002),
        )
        assert [d["chat_type"] for d in a._dispatched] == ["group"]

    @pytest.mark.asyncio
    async def test_configured_channel_named_dm_is_not_reclassified(self):
        """A conversation the operator explicitly configured as a watched
        channel stays a channel, whatever it is named."""
        a = self._adapter({"channels": [CHANNEL]})
        a._run_cli = self._cli({"channel_id": CHANNEL, "name": "DM", "description": ""})
        await a._handle_membership_event(
            _RecordingWebSocket(), {}, _membership_event(conversation=CHANNEL)
        )
        assert a._channel_state[CHANNEL]["chat_type"] == "group"

    @pytest.mark.asyncio
    async def test_membership_event_only_promotes_the_conversation_it_names(self):
        """A membership event naming one conversation must not promote some
        other DM-shaped entry that happens to appear in the same listing."""
        other_dm = "11111111-2222-3333-4444-555555555555"
        a = self._adapter()
        a._run_cli = self._cli(
            {"channel_id": HOSTED_DM, "name": "DM", "description": ""},
            {"channel_id": other_dm, "name": "DM", "description": ""},
        )
        await a._handle_membership_event(
            _RecordingWebSocket(), {}, _membership_event(conversation=HOSTED_DM)
        )
        assert a._channel_state[HOSTED_DM]["chat_type"] == "dm"
        assert a._channel_state[other_dm]["chat_type"] == "group"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("conversations", [[], [HOSTED_DM, CHANNEL]])
    async def test_membership_event_that_names_no_single_conversation_promotes_nothing(
        self, conversations
    ):
        """Fail closed on shapes the relay is not known to emit: an event
        naming no conversation — or several — promotes none of them, however
        DM-shaped the listing looks."""
        a = self._adapter()
        a._run_cli = self._cli(
            {"channel_id": HOSTED_DM, "name": "DM", "description": ""},
            {"channel_id": CHANNEL, "name": "DM", "description": ""},
        )
        event = _membership_event()
        event["tags"] += [["h", c] for c in conversations]

        await a._handle_membership_event(_RecordingWebSocket(), {}, event)

        assert a._channel_state[HOSTED_DM]["chat_type"] == "group"
        assert a._channel_state[CHANNEL]["chat_type"] == "group"
        await a._handle_event(
            HOSTED_DM, a._channel_state[HOSTED_DM], _h_only_event("e1", HOSTED_DM)
        )
        assert a._dispatched == []

    @pytest.mark.asyncio
    async def test_typed_conversation_named_dm_is_not_reclassified(self):
        """A ``channels list`` entry that carries any channel_type at all is
        not the untyped hosted-DM shape, so it stays a channel."""
        a = self._adapter()
        a._run_cli = self._cli(
            {"channel_id": HOSTED_DM, "name": "DM", "description": "", "channel_type": "dm"},
        )
        await a._handle_membership_event(
            _RecordingWebSocket(), {}, _membership_event(conversation=HOSTED_DM)
        )
        assert a._channel_state[HOSTED_DM]["chat_type"] == "group"

    @pytest.mark.asyncio
    async def test_unauthenticated_discovery_keeps_group_classification(self):
        """Without a membership event (startup seeding / poll sweeps), the
        fallback listing alone never unlocks the DM path — name "DM" is not
        evidence on its own."""
        a = self._adapter()
        a._run_cli = self._cli({"channel_id": HOSTED_DM, "name": "DM", "description": ""})
        await a._discover_dms(seed=False)
        assert a._channel_state[HOSTED_DM]["chat_type"] == "group"
        await a._handle_event(
            HOSTED_DM, a._channel_state[HOSTED_DM], _h_only_event("e1", HOSTED_DM)
        )
        assert a._dispatched == []

    @pytest.mark.asyncio
    async def test_allowlist_still_gates_membership_discovered_dm(self):
        """DM classification must not bypass the adapter allow-list."""
        a = self._adapter()
        a._allowed_pubkeys = {"b" * 64}
        a._run_cli = self._cli({"channel_id": HOSTED_DM, "name": "DM", "description": ""})
        await a._handle_membership_event(
            _RecordingWebSocket(), {}, _membership_event(conversation=HOSTED_DM)
        )
        assert a._channel_state[HOSTED_DM]["chat_type"] == "dm"
        await a._handle_event(
            HOSTED_DM, a._channel_state[HOSTED_DM], _h_only_event("e1", HOSTED_DM)
        )
        assert a._dispatched == []


# ── Sending ───────────────────────────────────────────────────────────────


class TestBuzzAdapterSend:

    @pytest.mark.asyncio
    async def test_send_success_via_stdin(self):
        adapter = _make_adapter()
        adapter._channel_state[CHANNEL] = {"chat_type": "group", "last_ts": 0, "seen": {}}
        cli = _ScriptedCli()
        cli.script("messages", "send", {"accepted": True, "event_id": "evt123", "message": ""})
        adapter._run_cli = cli

        result = await adapter.send(CHANNEL, "hello **markdown**")
        assert result.success is True
        assert result.message_id == "evt123"

        args, stdin_text = cli.calls[0]
        assert args[:2] == ["messages", "send"]
        assert args[args.index("--channel") + 1] == CHANNEL
        # Content travels via stdin (--content -), never argv
        assert args[args.index("--content") + 1] == "-"
        assert stdin_text == "hello **markdown**"
        # Our own event id is marked seen for echo suppression
        assert "evt123" in adapter._channel_state[CHANNEL]["seen"]


    @pytest.mark.asyncio
    async def test_send_image_local_file_uses_file_flag(self, tmp_path):
        img = tmp_path / "shot.png"
        img.write_bytes(b"\x89PNG fake")
        adapter = _make_adapter()
        cli = _ScriptedCli()
        cli.script("messages", "send", {"accepted": True, "event_id": "evt126", "message": ""})
        adapter._run_cli = cli
        result = await adapter.send_image(CHANNEL, str(img), caption="screenshot")
        assert result.success is True
        args, _stdin = cli.calls[0]
        assert args[args.index("--file") + 1] == str(img)


# ── Lifecycle ─────────────────────────────────────────────────────────────


class TestBuzzAdapterLifecycle:


    @pytest.mark.asyncio
    async def test_disconnect_releases_scoped_lock(self, monkeypatch):
        """The identity lock taken in connect() must be released on disconnect."""
        import gateway.status as gateway_status

        released = []
        monkeypatch.setattr(
            gateway_status,
            "release_scoped_lock",
            lambda platform, key: released.append((platform, key)),
        )
        adapter = _make_adapter()
        adapter._lock_key = "wss://relay.example:" + SELF_PUBKEY
        await adapter.disconnect()
        assert released == [("buzz", "wss://relay.example:" + SELF_PUBKEY)]
        assert adapter._lock_key is None

    @pytest.mark.asyncio
    async def test_connect_fails_when_identity_lock_held(self, monkeypatch):
        """A second profile using the same relay+pubkey must fail fast."""
        import gateway.status as gateway_status

        monkeypatch.setattr(
            gateway_status, "acquire_scoped_lock", lambda platform, key: False
        )
        adapter = _make_adapter()
        adapter.cli_path = "/fake/buzz"
        monkeypatch.setattr(_buzz_mod, "_resolve_private_key", lambda extra=None: "nsec1test")
        cli = _ScriptedCli()
        cli.script(
            "users", "get",
            [{"pubkey": SELF_PUBKEY, "display_name": "Chip"}],
        )
        adapter._run_cli = cli
        assert await adapter.connect() is False
        assert adapter._lock_key is None


# ── Credentials / requirements ────────────────────────────────────────────


class TestCredentialResolution:

    def test_env_key_wins(self, monkeypatch):
        monkeypatch.setenv("BUZZ_PRIVATE_KEY", "nsec1fromenv")
        assert _resolve_private_key() == "nsec1fromenv"

    def test_credentials_file_fallback(self, monkeypatch, tmp_path):
        creds = tmp_path / "agent_credentials.json"
        creds.write_text(json.dumps({"nsec": "nsec1fromfile", "npub": "npub1x"}), encoding="utf-8")
        monkeypatch.setenv("BUZZ_CREDENTIALS_FILE", str(creds))
        assert _resolve_private_key() == "nsec1fromfile"


class TestAuthTagResolution:
    """The NIP-OA auth tag resolves like the private key: env, then the same
    credentials JSON (configured or auto-discovered).  Hosted Builderlab
    relays reject NIP-42 auth with 403 relay_membership_required when the tag
    sitting in the credentials file never reaches the auth event."""

    def test_env_auth_tag_wins_over_credentials_file(self, monkeypatch, tmp_path):
        creds = tmp_path / "agent_credentials.json"
        creds.write_text(
            json.dumps({"nsec": "nsec1fromfile", "auth_tag": FAKE_FILE_AUTH_TAG}),
            encoding="utf-8",
        )
        monkeypatch.setenv("BUZZ_CREDENTIALS_FILE", str(creds))
        monkeypatch.setenv("BUZZ_AUTH_TAG", FAKE_ENV_AUTH_TAG)
        assert _resolve_auth_tag() == FAKE_ENV_AUTH_TAG

    def test_blank_env_auth_tag_falls_back_to_file(self, monkeypatch, tmp_path):
        creds = tmp_path / "agent_credentials.json"
        creds.write_text(
            json.dumps({"nsec": "nsec1fromfile", "auth_tag": f"  {FAKE_FILE_AUTH_TAG}  "}),
            encoding="utf-8",
        )
        monkeypatch.setenv("BUZZ_CREDENTIALS_FILE", str(creds))
        monkeypatch.setenv("BUZZ_AUTH_TAG", "   ")
        assert _resolve_auth_tag() == FAKE_FILE_AUTH_TAG

    def test_one_credentials_file_serves_key_and_tag(self, monkeypatch, tmp_path):
        """The documented credentials_file config selects a single source for
        both the nsec and the auth tag."""
        creds = tmp_path / "agent_credentials.json"
        creds.write_text(
            json.dumps({"nsec": "nsec1fromfile", "auth_tag": FAKE_FILE_AUTH_TAG}),
            encoding="utf-8",
        )
        extra = {"credentials_file": str(creds)}
        assert _resolve_private_key(extra) == "nsec1fromfile"
        assert _resolve_auth_tag(extra) == FAKE_FILE_AUTH_TAG

    def test_auto_discovered_credentials_file(self, monkeypatch, tmp_path):
        creds_dir = tmp_path / "buzz-config"
        creds_dir.mkdir()
        (creds_dir / "hermes_credentials.json").write_text(
            json.dumps({"nsec": "nsec1discovered", "auth_tag": FAKE_FILE_AUTH_TAG}),
            encoding="utf-8",
        )
        monkeypatch.setattr(_buzz_mod, "_DEFAULT_CREDENTIALS_DIR", creds_dir)
        assert _resolve_private_key() == "nsec1discovered"
        assert _resolve_auth_tag() == FAKE_FILE_AUTH_TAG

    @pytest.mark.parametrize(
        "payload",
        [
            json.dumps({"nsec": "nsec1fromfile"}),          # field absent
            json.dumps({"auth_tag": ["auth", "f" * 64]}),   # non-string value
            json.dumps({"auth_tag": "   "}),                # blank string
            json.dumps(["not", "a", "dict"]),               # non-dict document
            "{not json at all",                             # malformed
        ],
    )
    def test_unusable_auth_tag_resolves_empty(self, monkeypatch, tmp_path, payload):
        creds = tmp_path / "agent_credentials.json"
        creds.write_text(payload, encoding="utf-8")
        monkeypatch.setenv("BUZZ_CREDENTIALS_FILE", str(creds))
        assert _resolve_auth_tag() == ""

    def test_unreadable_credentials_file_resolves_empty(self, monkeypatch, tmp_path):
        monkeypatch.setenv("BUZZ_CREDENTIALS_FILE", str(tmp_path / "does-not-exist.json"))
        assert _resolve_auth_tag() == ""


class TestAuthTagReachesCli:
    """Every buzz CLI subprocess must inherit the resolved auth tag, so hosted
    relays accept the CLI's own NIP-42 handshake."""

    @pytest.fixture
    def spawned(self, monkeypatch):
        """Capture the env of the subprocess ``_exec_buzz`` would spawn."""
        captured = {}

        class _FakeProc:
            returncode = 0

            async def communicate(self, data=None):
                return b"[]", b""

        async def fake_spawn(cli_path, *args, **kwargs):
            captured.update(cli_path=cli_path, args=list(args), env=kwargs.get("env") or {})
            return _FakeProc()

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
        return captured

    @pytest.mark.asyncio
    async def test_exec_buzz_exports_auth_tag(self, spawned):
        await _exec_buzz(
            "/fake/buzz", ["users", "get"],
            relay_url="https://r", private_key="nsec1x", auth_tag=FAKE_FILE_AUTH_TAG,
        )
        assert spawned["env"]["BUZZ_AUTH_TAG"] == FAKE_FILE_AUTH_TAG
        assert spawned["env"]["BUZZ_RELAY_URL"] == "https://r"
        assert spawned["env"]["BUZZ_PRIVATE_KEY"] == "nsec1x"
        # Credentials travel by env only — never argv.
        assert all(FAKE_FILE_AUTH_TAG not in str(a) for a in spawned["args"])
        assert all("nsec1x" not in str(a) for a in spawned["args"])

    @pytest.mark.asyncio
    async def test_exec_buzz_leaves_auth_tag_unset_when_empty(self, monkeypatch, spawned):
        monkeypatch.delenv("BUZZ_AUTH_TAG", raising=False)
        await _exec_buzz(
            "/fake/buzz", ["users", "get"], relay_url="https://r", private_key="nsec1x",
        )
        assert "BUZZ_AUTH_TAG" not in spawned["env"]

    @pytest.mark.asyncio
    async def test_exec_buzz_never_clobbers_inherited_auth_tag(self, monkeypatch, spawned):
        """An unresolved tag must leave whatever the parent process exports
        alone rather than blanking it out."""
        monkeypatch.setenv("BUZZ_AUTH_TAG", FAKE_ENV_AUTH_TAG)
        await _exec_buzz(
            "/fake/buzz", ["users", "get"], relay_url="https://r", private_key="nsec1x",
        )
        assert spawned["env"]["BUZZ_AUTH_TAG"] == FAKE_ENV_AUTH_TAG

    @pytest.mark.asyncio
    async def test_run_cli_forwards_credentials_file_auth_tag(self, monkeypatch, tmp_path):
        creds = tmp_path / "agent_credentials.json"
        creds.write_text(
            json.dumps({"nsec": "nsec1fromfile", "auth_tag": FAKE_FILE_AUTH_TAG}),
            encoding="utf-8",
        )
        from gateway.config import PlatformConfig

        adapter = BuzzAdapter(PlatformConfig(
            enabled=True,
            extra={"relay_url": "https://test.relay", "credentials_file": str(creds)},
        ))
        captured = {}

        async def fake_exec(cli_path, args, *, relay_url, private_key, auth_tag="",
                            input_text=None, timeout=30.0):
            captured.update(private_key=private_key, auth_tag=auth_tag)
            return 0, "[]", ""

        monkeypatch.setattr(_buzz_mod, "_exec_buzz", fake_exec)
        await adapter._run_cli(["users", "get"])
        assert captured == {"private_key": "nsec1fromfile", "auth_tag": FAKE_FILE_AUTH_TAG}


class TestAuthTagReachesWebSocket:
    """The NIP-42 handshake must sign the resolved auth tag — reading only the
    parent environment loses a tag that lives in the credentials file, which
    hosted relays answer with 403 relay_membership_required."""

    class _FakeWebSocket:
        """Replays a NIP-42 handshake: AUTH challenge, then OK for the reply."""

        def __init__(self):
            self.sent = []

        async def recv(self):
            if self.sent:
                return json.dumps(["OK", self.sent[0][1]["id"], True, "authenticated"])
            return json.dumps(["AUTH", "relay-challenge"])

        async def send(self, raw):
            self.sent.append(json.loads(raw))

    @pytest.mark.asyncio
    async def test_auth_event_carries_credentials_file_auth_tag(self, monkeypatch, tmp_path):
        creds = tmp_path / "agent_credentials.json"
        creds.write_text(
            json.dumps({"nsec": "nsec1fromfile", "auth_tag": FAKE_FILE_AUTH_TAG}),
            encoding="utf-8",
        )
        from gateway.config import PlatformConfig

        adapter = BuzzAdapter(PlatformConfig(
            enabled=True,
            extra={"relay_url": "https://test.relay", "credentials_file": str(creds)},
        ))

        signed = {}

        def fake_build_auth_event(*, private_key, challenge, relay_url, auth_tag_json=""):
            signed.update(private_key=private_key, auth_tag_json=auth_tag_json)
            return {"id": "evt-auth", "kind": 22242}

        monkeypatch.setattr(
            _buzz_mod, "_load_nostr_auth",
            lambda: MagicMock(build_auth_event=fake_build_auth_event),
        )

        async def fake_exec(cli_path, args, **kwargs):
            return 0, "[]", ""

        monkeypatch.setattr(_buzz_mod, "_exec_buzz", fake_exec)
        # The adapter lifecycle resolves credentials before the WS starts.
        await adapter._run_cli(["users", "get"])

        await adapter._authenticate_websocket(self._FakeWebSocket())
        assert signed == {"private_key": "nsec1fromfile", "auth_tag_json": FAKE_FILE_AUTH_TAG}


# ── Env enablement / registration / standalone send ──────────────────────


class TestEnvEnablement:

    def test_returns_none_when_unconfigured(self):
        assert _env_enablement() is None


class TestBuzzPluginRegistration:

    def test_register_platform_contract(self):
        from gateway.platform_registry import platform_registry

        platform_registry.unregister("buzz")
        ctx = MagicMock()
        register(ctx)
        ctx.register_platform.assert_called_once()
        kwargs = ctx.register_platform.call_args.kwargs
        assert kwargs["name"] == "buzz"
        assert kwargs["cron_deliver_env_var"] == "BUZZ_HOME_CHANNEL"
        assert kwargs["allowed_users_env"] == "BUZZ_ALLOWED_USERS"
        assert kwargs["allow_all_env"] == "BUZZ_ALLOW_ALL_USERS"
        assert callable(kwargs["standalone_sender_fn"])
        assert callable(kwargs["env_enablement_fn"])
        assert set(kwargs["required_env"]) == {"BUZZ_RELAY_URL", "BUZZ_PRIVATE_KEY"}


class TestStandaloneSend:

    @pytest.mark.asyncio
    async def test_standalone_send_success(self, monkeypatch, tmp_path):
        from gateway.config import PlatformConfig

        fake_cli = tmp_path / "buzz"
        fake_cli.write_text("#!/bin/sh\n", encoding="utf-8")
        monkeypatch.setenv("BUZZ_RELAY_URL", "https://r")
        monkeypatch.setenv("BUZZ_PRIVATE_KEY", "nsec1x")
        monkeypatch.setenv("BUZZ_CLI_PATH", str(fake_cli))

        captured = {}

        async def fake_exec(cli_path, args, *, relay_url, private_key, auth_tag="",
                            input_text=None, timeout=30.0):
            captured.update(cli_path=cli_path, args=args, relay_url=relay_url, input_text=input_text)
            return 0, json.dumps({"accepted": True, "event_id": "evt-cron", "message": ""}), ""

        monkeypatch.setattr(_buzz_mod, "_exec_buzz", fake_exec)

        result = await _standalone_send(PlatformConfig(enabled=True, extra={}), CHANNEL, "cron says hi")
        assert result == {"success": True, "message_id": "evt-cron"}
        assert captured["args"][:2] == ["messages", "send"]
        assert captured["input_text"] == "cron says hi"
        # The private key must never be part of argv
        assert all("nsec1x" not in str(a) for a in captured["args"])

    @pytest.mark.asyncio
    async def test_standalone_send_forwards_credentials_file_auth_tag(self, monkeypatch, tmp_path):
        """Out-of-process cron sends resolve the auth tag the same way the
        live adapter does."""
        from gateway.config import PlatformConfig

        fake_cli = tmp_path / "buzz"
        fake_cli.write_text("#!/bin/sh\n", encoding="utf-8")
        creds = tmp_path / "agent_credentials.json"
        creds.write_text(
            json.dumps({"nsec": "nsec1fromfile", "auth_tag": FAKE_FILE_AUTH_TAG}),
            encoding="utf-8",
        )
        monkeypatch.setenv("BUZZ_RELAY_URL", "https://r")
        monkeypatch.setenv("BUZZ_CLI_PATH", str(fake_cli))
        monkeypatch.setenv("BUZZ_CREDENTIALS_FILE", str(creds))

        captured = {}

        async def fake_exec(cli_path, args, *, relay_url, private_key, auth_tag="",
                            input_text=None, timeout=30.0):
            captured.update(private_key=private_key, auth_tag=auth_tag, args=args)
            return 0, json.dumps({"accepted": True, "event_id": "evt-cron"}), ""

        monkeypatch.setattr(_buzz_mod, "_exec_buzz", fake_exec)

        result = await _standalone_send(PlatformConfig(enabled=True, extra={}), CHANNEL, "cron says hi")
        assert result == {"success": True, "message_id": "evt-cron"}
        assert captured["private_key"] == "nsec1fromfile"
        assert captured["auth_tag"] == FAKE_FILE_AUTH_TAG
        assert all(FAKE_FILE_AUTH_TAG not in str(a) for a in captured["args"])


