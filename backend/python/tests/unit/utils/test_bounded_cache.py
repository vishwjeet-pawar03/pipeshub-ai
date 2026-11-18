"""Unit tests for app.utils.bounded_cache.SizeBoundedCache."""

from app.utils.bounded_cache import SizeBoundedCache


def _size_of(value: str) -> int:
    return len(value)


class TestSizeBoundedCacheEviction:
    """Tests that eviction keeps the tracked size in sync with the entries kept.

    Entry size must stay within a tenth of max_size (see put()'s own guard),
    so these use size-1 values against a max_size of 10 to stay under that
    cap while still being able to force eviction.
    """

    def test_put_under_budget_keeps_all_entries(self):
        cache = SizeBoundedCache[str](max_size=10, size_of=_size_of)
        cache.put("a", "x")
        cache.put("b", "x")
        assert cache.get("a") == "x"
        assert cache.get("b") == "x"

    def test_cache_keeps_a_full_window_of_entries_after_churn(self):
        # Regression: evicting an entry must subtract its size from the running
        # total, or the tracked size keeps climbing and every later put thinks
        # the cache is over budget, evicting far more than necessary.
        cache = SizeBoundedCache[str](max_size=10, size_of=_size_of)
        for i in range(15):
            cache.put(f"key{i}", "x")
        # Budget fits 10 one-unit entries; a correctly accounted cache keeps
        # a 10-entry sliding window, not just the single latest put.
        assert len(cache._entries) == 10
        assert cache.get("key14") == "x"
        assert cache.get("key5") == "x"
        assert cache.get("key4") is None

    def test_max_entries_limit_is_respected_after_many_puts(self):
        cache = SizeBoundedCache[str](max_size=10_000, size_of=_size_of, max_entries=3)
        for i in range(10):
            cache.put(f"key{i}", "x")
        assert len(cache._entries) <= 3
        assert cache.get("key9") == "x"
