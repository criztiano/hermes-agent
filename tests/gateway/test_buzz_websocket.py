"""Tests for the Buzz WebSocket transport (NIP-42) and Nostr signing module.

The signing module and WS transport were contributed in PR #73636 by
@ScaleLeanChris and consolidated onto the merged poll-based adapter; these
tests cover the crypto (against the official BIP-340 vector) and the WS
lifecycle as wired into BuzzAdapter.
"""

import asyncio
import json
import time

import pytest

from tests.gateway._plugin_adapter_loader import load_plugin_adapter

_buzz_mod = load_plugin_adapter("buzz")
BuzzAdapter = _buzz_mod.BuzzAdapter

import importlib.util as _ilu
from pathlib import Path as _Path

_auth_path = _Path(_buzz_mod.__file__).with_name("nostr_auth.py")
_spec = _ilu.spec_from_file_location("plugin_adapter_buzz_nostr_auth", _auth_path)
nostr_auth = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(nostr_auth)

SELF_PUBKEY = "9fd5c7ba6d3ef224da78f541e0fcb9c50f72cc63edb19aae76ac6a0474dfa860"
# BIP-340 test vector 0 private key
TEST_PRIVATE_KEY = "00" * 31 + "03"
CHANNEL = "ccc2bc1a-7a82-5a8f-8c4e-57a070cbe7cd"


def _make_adapter(extra=None):
    from gateway.config import PlatformConfig

    cfg = PlatformConfig(enabled=True, extra={"relay_url": "https://test.relay", **(extra or {})})
    adapter = BuzzAdapter(cfg)
    adapter._self_pubkey = SELF_PUBKEY
    adapter._private_key = TEST_PRIVATE_KEY
    adapter._display_name = "Chip"
    return adapter


# ── nostr_auth: BIP-340 / NIP-42 ──────────────────────────────────────────


def test_schnorr_sign_matches_official_bip340_vector_zero():
    signature = nostr_auth.schnorr_sign(
        bytes(32), TEST_PRIVATE_KEY, auxiliary_randomness=bytes(32)
    )
    assert nostr_auth.public_key_hex(TEST_PRIVATE_KEY).upper() == (
        "F9308A019258C31049344F85F89D5229B531C845836F99B08601F113BCE036F9"
    )
    assert signature.hex().upper() == (
        "E907831F80848D1069A5371B402410364BDF1C5F8307B0084C55F1CE2DCA8215"
        "25F66A4A85EA8B71E482A74F382D2CE5EBEEE8FDB2172F477DF4900D310536C0"
    )


def test_decode_private_key_rejects_bad_input():
    with pytest.raises(ValueError):
        nostr_auth.decode_private_key("not-a-key")
    with pytest.raises(ValueError):
        nostr_auth.decode_private_key("00" * 32)  # zero — outside range
    with pytest.raises(ValueError):
        nostr_auth.decode_private_key("nsec1qqqqqqqq")  # bad checksum/length


def test_build_auth_event_shape_and_owner_tag():
    tag = json.dumps(["auth", "b" * 64, "", "c" * 128])
    event = nostr_auth.build_auth_event(
        private_key=TEST_PRIVATE_KEY,
        challenge="challenge-1",
        relay_url="wss://relay.example",
        auth_tag_json=tag,
        created_at=1_700_000_000,
        auxiliary_randomness=bytes(32),
    )
    assert event["kind"] == 22242
    assert ["relay", "wss://relay.example"] in event["tags"]
    assert ["challenge", "challenge-1"] in event["tags"]
    assert ["auth", "b" * 64, "", "c" * 128] in event["tags"]
    assert len(bytes.fromhex(event["sig"])) == 64
    assert event["pubkey"] == nostr_auth.public_key_hex(TEST_PRIVATE_KEY)


# ── Adapter WS wiring ─────────────────────────────────────────────────────


class _FakeWebSocket:
    """Replays a NIP-42 handshake: AUTH challenge, then OK for the reply."""

    def __init__(self):
        self.sent = []

    async def recv(self):
        if self.sent:
            auth_event = self.sent[0][1]
            return json.dumps(["OK", auth_event["id"], True, "authenticated"])
        return json.dumps(["AUTH", "relay-challenge"])

    async def send(self, raw):
        self.sent.append(json.loads(raw))


@pytest.mark.asyncio
async def test_membership_subscription_is_authenticated_and_self_addressed():
    """Live DM discovery rides a kind-44100 subscription opened after NIP-42
    auth and filtered to events p-tagged to our own pubkey — the
    self-addressed half of hosted-DM classification, which only takes effect
    together with a DM-shaped `channels list` entry (see
    `_is_hosted_dm_metadata`)."""
    adapter = _make_adapter()
    adapter._channel_state[CHANNEL] = {"chat_type": "group", "last_ts": 100, "seen": {}}
    websocket = _FakeWebSocket()

    subscriptions = await adapter._subscribe_websocket(websocket)

    membership_sub = _buzz_mod._WS_MEMBERSHIP_SUB_ID
    assert subscriptions[membership_sub] is None  # not a per-channel sub
    assert subscriptions["hermes-buzz-0"] == CHANNEL
    frames = {f[1]: f[2] for f in websocket.sent if f[0] == "REQ"}
    assert frames[membership_sub]["kinds"] == [_buzz_mod._WS_MEMBERSHIP_KIND]
    assert frames[membership_sub]["#p"] == [SELF_PUBKEY]
    assert frames["hermes-buzz-0"]["#h"] == [CHANNEL]


@pytest.mark.asyncio
async def test_membership_event_routes_hosted_dm_through_websocket_flow():
    """End to end on the WS transport: `dms list` empty, DM-shaped fallback
    metadata, a kind-44100 membership event, then a kind-9 owner message
    carrying only an h-tag — it dispatches as a DM."""
    hosted_dm = "4e645536-f4af-4d6d-8123-caebeeadd7ec"
    adapter = _make_adapter()
    dispatched = []

    async def capture(**kwargs):
        dispatched.append(kwargs)

    adapter._dispatch_message = capture

    async def fake_cli(args, *, input_text=None):
        if args[:2] == ["channels", "list"]:
            return 0, json.dumps([{"channel_id": hosted_dm, "name": "DM", "description": ""}]), ""
        return 0, "[]", ""

    adapter._run_cli = fake_cli

    websocket = _FakeWebSocket()
    subscriptions = {}
    await adapter._handle_membership_event(websocket, subscriptions, {
        "id": "m1", "kind": _buzz_mod._WS_MEMBERSHIP_KIND, "created_at": int(time.time()),
        "pubkey": "a" * 64, "content": "",
        "tags": [["p", SELF_PUBKEY], ["h", hosted_dm]],
    })
    assert hosted_dm in subscriptions.values()

    state = adapter._channel_state[hosted_dm]
    await adapter._handle_event(hosted_dm, state, {
        "id": "e1", "kind": 9, "pubkey": "a" * 64, "created_at": int(time.time()),
        "content": "here's a test message", "tags": [["h", hosted_dm]],
    })
    assert [d["chat_type"] for d in dispatched] == ["dm"]


@pytest.mark.asyncio
async def test_websocket_auth_raises_on_rejection():
    adapter = _make_adapter()

    class RejectingWs(_FakeWebSocket):
        async def recv(self):
            if self.sent:
                auth_event = self.sent[0][1]
                return json.dumps(["OK", auth_event["id"], False, "denied"])
            return json.dumps(["AUTH", "relay-challenge"])

    with pytest.raises(ConnectionError):
        await adapter._authenticate_websocket(RejectingWs())


