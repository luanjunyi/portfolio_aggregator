import os
import sys
import unittest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.fetch_all_positions as fap
from backend.models.portfolio import CrawlerResult


class _FakeCrawler:
    """Stand-in for a crawler class; __name__ is what the retry logic logs."""
    pass


def _ok() -> CrawlerResult:
    return CrawlerResult(broker="fake", success=True, holdings=[])


class CrawlerRetryTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._orig = fap._run_crawler

    def tearDown(self):
        fap._run_crawler = self._orig

    async def test_succeeds_first_try(self):
        calls = {"n": 0}

        async def run(cls):
            calls["n"] += 1
            return _ok()

        fap._run_crawler = run
        result = await fap._run_crawler_with_retries(_FakeCrawler, attempts=3, backoff=0)
        self.assertTrue(result.success)
        self.assertEqual(calls["n"], 1)  # no retries when it works

    async def test_retries_then_succeeds(self):
        calls = {"n": 0}

        async def run(cls):
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("transient boom")
            return _ok()

        fap._run_crawler = run
        result = await fap._run_crawler_with_retries(_FakeCrawler, attempts=3, backoff=0)
        self.assertTrue(result.success)
        self.assertEqual(calls["n"], 3)

    async def test_raises_after_all_attempts(self):
        calls = {"n": 0}

        async def run(cls):
            calls["n"] += 1
            raise RuntimeError("always down")

        fap._run_crawler = run
        with self.assertRaises(RuntimeError) as ctx:
            await fap._run_crawler_with_retries(_FakeCrawler, attempts=3, backoff=0)
        self.assertEqual(calls["n"], 3)
        self.assertIn("after 3 attempts", str(ctx.exception))

    async def test_success_false_is_treated_as_failure(self):
        calls = {"n": 0}

        async def run(cls):
            calls["n"] += 1
            return CrawlerResult(broker="fake", success=False, error_message="login failed")

        fap._run_crawler = run
        with self.assertRaises(RuntimeError):
            await fap._run_crawler_with_retries(_FakeCrawler, attempts=3, backoff=0)
        self.assertEqual(calls["n"], 3)  # retried, no partial acceptance


if __name__ == "__main__":
    unittest.main()
