"""Tests for the per-container egress firewall (``egress_firewall``)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.agent_loop_lib.sandbox.coding.egress_firewall import (
    BLOCKED_CIDRS,
    FIREWALL_UNAVAILABLE_EXIT_CODE,
    build_firewall_script,
    ensure_egress_network_sync,
    firewall_unavailable,
    firewalled_container_kwargs,
    parse_cidrs,
)

_ICC = "com.docker.network.bridge.enable_icc"


def _index(script: str, needle: str) -> int:
    lines = script.splitlines()
    return next(i for i, line in enumerate(lines) if needle in line)


class TestBuildFirewallScript:
    def test_rejects_every_non_public_range_metadata_and_the_bridge_subnet(self) -> None:
        script = build_firewall_script(["203.0.200.0/24"])
        for cidr in (*BLOCKED_CIDRS, "169.254.0.0/16", "168.63.129.16/32", "203.0.200.0/24"):
            assert f"r -A OUTPUT -d {cidr} -j REJECT" in script

    def test_loopback_is_accepted_first_so_embedded_dns_works(self) -> None:
        script = build_firewall_script(["172.30.0.0/16"])
        assert _index(script, "-o lo -j ACCEPT") < _index(script, "-j REJECT")

    def test_allowance_cannot_open_metadata_but_precedes_private_ranges(self) -> None:
        script = build_firewall_script([], allowed_cidrs=["0.0.0.0/0", "10.20.0.0/16"])
        assert _index(script, "169.254.0.0/16 -j REJECT") < _index(script, "0.0.0.0/0 -j ACCEPT")
        assert _index(script, "10.20.0.0/16 -j ACCEPT") < _index(script, "10.0.0.0/8 -j REJECT")

    def test_drops_to_unprivileged_user_after_the_rules(self) -> None:
        script = build_firewall_script(["172.30.0.0/16"], run_as="runner")
        last = script.strip().splitlines()[-1]
        assert last.startswith("exec setpriv --reuid=runner --regid=runner --init-groups")
        assert "--bounding-set=-all" in last and "--inh-caps=-all" in last and "--no-new-privs" in last
        assert last.endswith('-- "$@"')
        assert 'h="$(getent passwd runner | cut -d: -f6)"' in script

    def test_run_as_is_shell_quoted(self) -> None:
        assert "--reuid='a b;x'" in build_firewall_script([], run_as="a b;x")


def test_container_kwargs_wrap_command_and_drop_everything_else() -> None:
    kwargs = firewalled_container_kwargs(["python3", "main.py"], network_cidrs=["172.30.0.0/16"])
    assert kwargs["command"][:2] == ["sh", "-c"]
    assert kwargs["command"][4:] == ["python3", "main.py"]
    assert kwargs["user"] == "0"
    assert kwargs["cap_drop"] == ["ALL"]
    assert set(kwargs["cap_add"]) == {"NET_ADMIN", "SETUID", "SETGID", "SETPCAP"}
    assert "NET_RAW" not in kwargs["cap_add"]
    assert kwargs["security_opt"] == ["no-new-privileges:true"]


@pytest.mark.parametrize(
    ("exit_code", "stderr", "expected"),
    [
        (FIREWALL_UNAVAILABLE_EXIT_CODE, "[sandbox-egress] firewall unavailable: no iptables", True),
        (FIREWALL_UNAVAILABLE_EXIT_CODE, "user program chose this exit code", False),
        (1, "[sandbox-egress] firewall unavailable: no iptables", False),
    ],
)
def test_firewall_unavailable_needs_code_and_marker(exit_code, stderr, expected) -> None:
    assert firewall_unavailable(exit_code, stderr) is expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, ()),
        ("", ()),
        ("10.1.2.3/16, 192.168.5.0/24", ("10.1.0.0/16", "192.168.5.0/24")),
        ("not-a-cidr,10.0.0.0/8", ("10.0.0.0/8",)),
        ("fd00::/8", ()),
    ],
)
def test_parse_cidrs(raw, expected) -> None:
    assert parse_cidrs(raw) == expected


def _network(name: str, *, icc_off: bool = True, containers: dict | None = None, subnet: str = "172.30.0.0/16") -> MagicMock:
    network = MagicMock()
    network.name = name
    network.id = "a" * 64
    network.attrs = {
        "Options": {_ICC: "false"} if icc_off else {},
        "IPAM": {"Config": [{"Subnet": subnet}, {"Subnet": "fd00::/64"}]},
        "Containers": containers or {},
    }
    return network


class TestEnsureEgressNetwork:
    def test_creates_with_icc_off_and_returns_ipv4_subnets(self) -> None:
        client = MagicMock()
        created = _network("egress")
        client.networks.list.side_effect = [[], [created]]
        client.networks.get.return_value = created

        assert ensure_egress_network_sync(client, "egress", {"l": "v"}) == ["172.30.0.0/16"]
        kwargs = client.networks.create.call_args.kwargs
        assert kwargs["options"] == {_ICC: "false"}
        assert kwargs["enable_ipv6"] is False

    def test_idle_network_with_icc_on_is_recreated(self) -> None:
        client = MagicMock()
        legacy = _network("egress", icc_off=False)
        fresh = _network("egress", subnet="172.31.0.0/16")
        client.networks.list.side_effect = [[legacy], [fresh]]
        client.networks.get.side_effect = [legacy, fresh]

        assert ensure_egress_network_sync(client, "egress", {}) == ["172.31.0.0/16"]
        legacy.remove.assert_called_once()
        client.networks.create.assert_called_once()

    def test_in_use_network_with_icc_on_is_kept(self) -> None:
        client = MagicMock()
        legacy = _network("egress", icc_off=False, containers={"c": {}})
        client.networks.list.return_value = [legacy]
        client.networks.get.return_value = legacy

        assert ensure_egress_network_sync(client, "egress", {}) == ["172.30.0.0/16"]
        legacy.remove.assert_not_called()
        client.networks.create.assert_not_called()

    def test_substring_match_is_not_the_network(self) -> None:
        client = MagicMock()
        near_miss = _network("pipeshub_sandbox_egress")
        created = _network("sandbox_egress")
        client.networks.list.side_effect = [[near_miss], [near_miss, created]]
        client.networks.get.return_value = created

        ensure_egress_network_sync(client, "sandbox_egress", {})
        assert client.networks.create.call_args.kwargs["name"] == "sandbox_egress"

    def test_original_error_survives_a_failing_recheck(self) -> None:
        client = MagicMock()
        client.networks.list.side_effect = [[], RuntimeError("daemon gone")]
        client.networks.create.side_effect = RuntimeError("create failed")
        with pytest.raises(RuntimeError, match="create failed"):
            ensure_egress_network_sync(client, "egress", {})
