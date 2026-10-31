"""/qa command parsing, history and culprit search.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import tempfile
import unittest
from pathlib import Path

from qa import command, config, culprit, history, testing


FULL = "e4f5a6b" + "0" * 33


class TestCommand(unittest.TestCase):
    def test_range_and_since(self) -> None:
        self.assertEqual(command.parse("/qa range abc1234..def5678", "write")["head"], "def5678")
        self.assertEqual(command.parse("/qa since abc1234", "write")["base"], "abc1234")

    def test_trusted_needs_a_sha(self) -> None:
        with self.assertRaises(command.CommandError):
            command.parse("/qa run --trusted", "admin")
        self.assertEqual(command.parse(f"/qa run --trusted {FULL}", "admin")["trusted_head"], FULL)
        with self.assertRaises(command.CommandError):
            command.parse("/qa run --trusted abc1234", "admin")
        with self.assertRaises(command.CommandError):
            command.parse("/qa run --trusted xyz", "admin")

    def test_trusted_does_not_replace_the_range_head(self) -> None:
        out = command.parse(f"/qa range a1b2c3d..e4f5a6b --trusted {FULL}", "admin")
        self.assertEqual((out["head"], out["trusted_head"]), ("e4f5a6b", FULL))

    def test_since_is_its_own_mode(self) -> None:
        self.assertEqual(command.parse("/qa since abc1234", "write")["mode"], "since")

    def test_missing_only_plans(self) -> None:
        self.assertIs(command.parse("/qa missing", "write")["plan_only"], True)
        self.assertIs(command.parse("/qa run", "write")["plan_only"], False)

    def test_full_needs_admin(self) -> None:
        with self.assertRaises(command.CommandError):
            command.parse("/qa full", "write")
        self.assertEqual(command.parse("/qa full", "admin")["mode"], "full")
        self.assertEqual(command.parse("/qa full", "write", is_admin_team=True)["mode"], "full")

    def test_untrusted_arguments_are_rejected(self) -> None:
        for body in ("/qa range $(curl evil)", "/qa since main; rm -rf /", "/qa deploy", "/qa range abc"):
            with self.assertRaises(command.CommandError, msg=body):
                command.parse(body, "admin")

    def test_read_permission_cannot_run_anything(self) -> None:
        with self.assertRaises(command.CommandError):
            command.parse("/qa", "read")

    def test_profile_flag(self) -> None:
        self.assertEqual(command.parse("/qa run --profile conservative", "write")["profile"], "conservative")


class TestHistory(unittest.TestCase):
    def test_failing_on_main_and_flaky_rates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "h.jsonl"
            for outcome in ("fail", "flaky", "pass"):
                plan = {"head": "x", "branch": "main", "tests": [{"id": "a", "source": "floor"}], "planner": {}}
                history.record(store, plan, {"tests": [{"id": "a", "outcome": outcome, "seconds": 2.0}]})
            entries = history.load(store)
            self.assertEqual(history.failing_on_main(entries), ["a"])
            self.assertEqual(history.flaky_rates(entries), {"a": 0.333})
            self.assertEqual(history.durations(entries), {"a": 2.0})

    def test_miss_is_a_failing_test_the_culprit_run_did_not_select(self) -> None:
        entries = [{"head": "c1", "selected": {"a": "floor"}}]
        self.assertEqual(history.misses(entries, ["a", "b"], "c1"), ["b"])


class TestCulpritSearch(unittest.TestCase):
    def test_finds_the_breaking_commit_with_a_fake_probe(self) -> None:
        root, tmp = testing.make_repo()
        self.addCleanup(tmp.cleanup)
        base = testing.git(root, "rev-parse", "HEAD")
        shas = []
        for i in range(4):
            testing.write(root, "backend/python/app/services/retrieval.py", f"def search():\n    return [{i}]\n")
            shas.append(testing.commit(root, f"c{i}"))
        broken_from = shas[2]
        order = testing.git(root, "rev-list", "--reverse", f"{base}..HEAD").split()

        def probe(_root, args):
            sha, ids = args[0], args[1:]
            bad = order.index(sha) >= order.index(broken_from)
            return {i: ("fail" if bad else "pass") for i in ids}

        cfg = config.load(root)
        result = culprit.search(cfg, base, "HEAD", ["it:retrieval/test_search.py::test_acl_filter"], probe)
        self.assertEqual(result["result"], "exact")
        self.assertEqual(result["culprit"], broken_from)

    def test_flaky_when_head_passes_on_recheck(self) -> None:
        root, tmp = testing.make_repo()
        self.addCleanup(tmp.cleanup)
        base = testing.git(root, "rev-parse", "HEAD")
        testing.commit(root, "empty")
        result = culprit.search(config.load(root), base, "HEAD", ["x"], lambda _r, a: {i: "pass" for i in a[1:]})
        self.assertEqual(result["result"], "flaky")


if __name__ == "__main__":
    unittest.main()
