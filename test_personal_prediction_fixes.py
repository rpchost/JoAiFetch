"""Regression tests for expiry enforcement and fallback prediction rows
in the JoAiFetch personal predictions generator.

Run with:  python -m unittest test_personal_prediction_fixes -v
(System python: .venv in this repo is broken and must not be used.)
"""

import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import generate_personal_daily_predictions as gen


class RecordingCursor:
    """Records executed SQL and serves queued fetchone/fetchall results."""

    def __init__(self, fetchone_results=None, fetchall_results=None, fail_when=None):
        self.executed = []
        self._fetchone = list(fetchone_results or [])
        self._fetchall = list(fetchall_results or [])
        self._fail_when = fail_when
        self.query = b""
        self.closed = False

    def execute(self, sql, params=None):
        if self._fail_when and self._fail_when(sql):
            raise RuntimeError("simulated database failure")
        self.executed.append((sql, params))
        self.query = sql.encode("utf-8") if isinstance(sql, str) else sql

    def fetchone(self):
        return self._fetchone.pop(0) if self._fetchone else None

    def fetchall(self):
        return self._fetchall.pop(0) if self._fetchall else []

    def close(self):
        self.closed = True


def fake_connection(cur):
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


class FakeResponse:
    def __init__(self, payload=None, ok=True, status_code=200, text=""):
        self._payload = payload
        self.ok = ok
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._payload


EXPIRY_PREDICATE = "expires_at IS NULL OR expires_at > NOW()"


class UserListFlipTests(unittest.TestCase):
    def test_query_enforces_expiry_and_flips_expired_rows(self):
        cur = RecordingCursor(fetchall_results=[[(10,), (20,)]])
        with patch("generate_personal_daily_predictions.psycopg2.connect",
                   return_value=fake_connection(cur)):
            users = gen.get_users_with_live_adapter()

        self.assertEqual(users, [10, 20])
        sqls = [sql for sql, _ in cur.executed]
        self.assertEqual(len(sqls), 2)

        flip_sql = sqls[0]
        self.assertIn("UPDATE user_custom_indicators", flip_sql)
        self.assertIn("SET status = 'expired'", flip_sql)
        self.assertIn("expires_at IS NOT NULL", flip_sql)
        self.assertIn("expires_at <= NOW()", flip_sql)

        select_sql = sqls[1]
        self.assertIn("SELECT DISTINCT user_id", select_sql)
        self.assertIn("status = 'live'", select_sql)
        self.assertIn(EXPIRY_PREDICATE, select_sql)

    def test_flip_failure_does_not_block_the_users_query(self):
        cur = RecordingCursor(
            fetchall_results=[[(7,)]],
            fail_when=lambda sql: "UPDATE user_custom_indicators" in sql,
        )
        conn = fake_connection(cur)
        with patch("generate_personal_daily_predictions.psycopg2.connect",
                   return_value=conn):
            users = gen.get_users_with_live_adapter()

        self.assertEqual(users, [7])
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
        select_sqls = [sql for sql, _ in cur.executed if "SELECT DISTINCT" in sql]
        self.assertEqual(len(select_sqls), 1)
        self.assertIn(EXPIRY_PREDICATE, select_sqls[0])

    def test_connection_failure_returns_empty_list(self):
        with patch("generate_personal_daily_predictions.psycopg2.connect",
                   side_effect=RuntimeError("down")):
            self.assertEqual(gen.get_users_with_live_adapter(), [])


class CustomIndicatorIdTests(unittest.TestCase):
    def test_query_enforces_expiry(self):
        cur = RecordingCursor(fetchone_results=[(55,)])
        indicator_id = gen.get_custom_indicator_id_for_user(cur, 3)

        self.assertEqual(indicator_id, 55)
        sql, params = cur.executed[0]
        self.assertIn("status = 'live'", sql)
        self.assertIn(EXPIRY_PREDICATE, sql)
        self.assertEqual(params, (3,))

    def test_returns_none_when_only_expired_indicator_exists(self):
        cur = RecordingCursor(fetchone_results=[None])
        self.assertIsNone(gen.get_custom_indicator_id_for_user(cur, 3))


class FallbackRowTests(unittest.TestCase):
    PAYLOAD = {
        "prediction": {"open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0},
    }

    def _call(self, response):
        with patch("generate_personal_daily_predictions.requests.post",
                   return_value=response):
            return gen.get_personal_prediction(
                3, "BTCUSD", "1 day", date(2026, 1, 1), 9
            )

    def test_fallback_prediction_is_kept_and_flagged(self):
        pred = self._call(FakeResponse({**self.PAYLOAD, "personalized": False}))

        self.assertIsNotNone(pred)
        self.assertIs(pred["is_personalized"], False)
        self.assertEqual(pred["user_id"], 3)
        self.assertEqual(pred["custom_indicator_id"], 9)
        self.assertEqual(pred["predicted_close"], 105.0)
        self.assertEqual(pred["for_date"], date(2026, 1, 1))

    def test_personalized_prediction_is_kept_and_flagged(self):
        pred = self._call(FakeResponse({**self.PAYLOAD, "personalized": True}))

        self.assertIsNotNone(pred)
        self.assertIs(pred["is_personalized"], True)

    def test_missing_personalized_flag_defaults_to_personalized(self):
        pred = self._call(FakeResponse(dict(self.PAYLOAD)))

        self.assertIsNotNone(pred)
        self.assertIs(pred["is_personalized"], True)

    def test_missing_prediction_still_skips(self):
        pred = self._call(FakeResponse({"prediction": None, "personalized": True}))
        self.assertIsNone(pred)

    def test_http_error_still_skips(self):
        pred = self._call(FakeResponse({"error": "boom"}, ok=False, status_code=500))
        self.assertIsNone(pred)

    def test_timeout_still_skips(self):
        with patch("generate_personal_daily_predictions.requests.post",
                   side_effect=gen.requests.exceptions.Timeout()):
            pred = gen.get_personal_prediction(
                3, "BTCUSD", "1 day", date(2026, 1, 1), 9
            )

        self.assertIsNone(pred)


class SavePredictionTests(unittest.TestCase):
    PERSONAL_PRED = {
        "user_id": 3,
        "symbol": "BTCUSD",
        "timeframe": "1 day",
        "predicted_open": 100.0,
        "predicted_high": 110.0,
        "predicted_low": 90.0,
        "predicted_close": 105.0,
        "for_date": date(2026, 1, 1),
        "is_personalized": False,
        "custom_indicator_id": 9,
    }

    def test_upsert_parametrizes_is_personalized(self):
        cur = RecordingCursor(fetchone_results=[(123,)])
        gen.save_personal_prediction(cur, dict(self.PERSONAL_PRED))

        upsert_sql = cur.executed[0][0]
        self.assertIn("personal_daily_predictions", upsert_sql)
        self.assertIn("%(is_personalized)s", upsert_sql)
        self.assertIn("is_personalized = EXCLUDED.is_personalized", upsert_sql)
        self.assertIn("EXCLUDED.custom_indicator_id", upsert_sql)

    def test_upsert_can_heal_a_fallback_row_back_to_personalized(self):
        pred = dict(self.PERSONAL_PRED)
        pred["is_personalized"] = True

        cur = RecordingCursor(fetchone_results=[(123,)])
        gen.save_personal_prediction(cur, pred)

        upsert_sql, params = cur.executed[0]
        self.assertIn("is_personalized = EXCLUDED.is_personalized", upsert_sql)
        self.assertIs(params["is_personalized"], True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
