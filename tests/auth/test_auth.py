"""
Test suite for the auth package.

Covers:
    - PasswordHandler
    - PasswordPolicyValidator
    - UserAuthenticator
    - UserRepository
    - UserManager
    - AccountLockoutManager / LockoutInfo
    - TwoFactorAuth
    - SessionManager
    - PermissionManager / Role / Permission

All DB interactions are mocked via unittest.mock — no live database required.
Run with:
    pytest test_auth.py -v
"""

import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pyotp

# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------

def _make_cursor(rows=None, rowcount=1):
    """Return a mock cursor that fetchone/fetchall returns rows."""
    cur = MagicMock()
    if rows is None:
        cur.fetchone.return_value = None
        cur.fetchall.return_value = []
    elif isinstance(rows, dict):
        cur.fetchone.return_value = rows
        cur.fetchall.return_value = [rows]
    elif isinstance(rows, list):
        cur.fetchone.return_value = rows[0] if rows else None
        cur.fetchall.return_value = rows
    else:
        cur.fetchone.return_value = rows
    cur.rowcount = rowcount
    return cur


def _make_conn(cursor=None):
    """Return a mock DB connection."""
    conn = MagicMock()
    if cursor is not None:
        conn.cursor.return_value = cursor
    return conn


# ===========================================================================
# PasswordHandler
# ===========================================================================

class TestPasswordHandler(unittest.TestCase):

    def _make_handler(self, user_row=None, hash_fn=None, verify_fn=None):
        from auth.password_handler import PasswordHandler

        cur = _make_cursor(user_row)
        # Make cursor() work both with and without kwargs (dict cursor compat)
        conn = MagicMock()
        conn.cursor.return_value = cur
        self._cursor = cur
        self._conn = conn

        hash_fn = hash_fn or (lambda p: "hashed_" + p)
        verify_fn = verify_fn or (lambda p, h: p == h.replace("hashed_", ""))
        return PasswordHandler(conn, hash_fn, verify_fn)

    # --- default_hash_password / default_verify_password ---

    def test_default_hash_and_verify(self):
        from auth.password_handler import default_hash_password, default_verify_password
        hashed = default_hash_password("Secret1!")
        self.assertNotEqual(hashed, "Secret1!")
        self.assertTrue(default_verify_password("Secret1!", hashed))
        self.assertFalse(default_verify_password("wrong", hashed))

    def test_default_verify_bad_hash_returns_false(self):
        from auth.password_handler import default_verify_password
        self.assertFalse(default_verify_password("pass", "not_a_bcrypt_hash"))

    # --- change_password ---

    def test_change_password_success(self):
        handler = self._make_handler({"password_hash": "hashed_oldpass"})
        result = handler.change_password(1, "oldpass", "newpass")
        self.assertTrue(result)
        self._conn.commit.assert_called_once()

    def test_change_password_wrong_old_password(self):
        handler = self._make_handler({"password_hash": "hashed_oldpass"})
        result = handler.change_password(1, "wrongpass", "newpass")
        self.assertFalse(result)
        self._conn.commit.assert_not_called()

    def test_change_password_user_not_found(self):
        handler = self._make_handler(None)
        result = handler.change_password(99, "old", "new")
        self.assertFalse(result)

    def test_change_password_empty_new_password(self):
        handler = self._make_handler({"password_hash": "hashed_old"})
        result = handler.change_password(1, "old", "")
        self.assertFalse(result)

    def test_change_password_db_error_triggers_rollback(self):
        from auth.password_handler import PasswordHandler
        conn = MagicMock()
        conn.cursor.side_effect = Exception("DB down")
        handler = PasswordHandler(conn, lambda p: p, lambda p, h: True)
        result = handler.change_password(1, "old", "new")
        self.assertFalse(result)
        conn.rollback.assert_called_once()


# ===========================================================================
# PasswordPolicyValidator
# ===========================================================================

class TestPasswordPolicyValidator(unittest.TestCase):

    def setUp(self):
        from auth.password_policy import PasswordPolicyValidator
        self.validator = PasswordPolicyValidator()

    # --- validate ---

    def test_strong_password_passes(self):
        valid, errors = self.validator.validate("StrongP@ss1!")
        self.assertTrue(valid)
        self.assertEqual(errors, [])

    def test_too_short_fails(self):
        valid, errors = self.validator.validate("Ab1!")
        self.assertFalse(valid)
        self.assertTrue(any("characters" in e for e in errors))

    def test_no_uppercase_fails(self):
        valid, errors = self.validator.validate("weakpass1!")
        self.assertFalse(valid)
        self.assertTrue(any("uppercase" in e for e in errors))

    def test_no_lowercase_fails(self):
        valid, errors = self.validator.validate("WEAKPASS1!")
        self.assertFalse(valid)
        self.assertTrue(any("lowercase" in e for e in errors))

    def test_no_digit_fails(self):
        valid, errors = self.validator.validate("WeakPass!!")
        self.assertFalse(valid)
        self.assertTrue(any("number" in e for e in errors))

    def test_no_special_char_fails(self):
        valid, errors = self.validator.validate("WeakPass12")
        self.assertFalse(valid)
        self.assertTrue(any("special" in e for e in errors))

    def test_common_password_fails(self):
        valid, errors = self.validator.validate("password")
        self.assertFalse(valid)
        self.assertTrue(any("common" in e for e in errors))

    def test_common_password_case_insensitive(self):
        valid, errors = self.validator.validate("PASSWORD123")
        # Should fail on uppercase/special/common checks
        self.assertFalse(valid)

    # --- calculate_strength ---

    def test_very_weak_strength(self):
        label, score = self.validator.calculate_strength("abc")
        self.assertEqual(label, "Very Weak")

    def test_very_strong_strength(self):
        label, score = self.validator.calculate_strength("Tr0ub4dor&3XyZ!!")
        self.assertIn(label, ("Strong", "Very Strong"))

    def test_strength_score_clamped(self):
        _, score = self.validator.calculate_strength("Tr0ub4dor&3XyZ!!")
        self.assertLessEqual(score, 100)
        self.assertGreaterEqual(score, 0)

    # --- get_requirements_text ---

    def test_requirements_text_contains_bullets(self):
        text = self.validator.get_requirements_text()
        self.assertIn("•", text)
        self.assertIn("8", text)

    # --- custom requirements ---

    def test_custom_requirements(self):
        from auth.password_policy import PasswordPolicyValidator, PasswordRequirements
        req = PasswordRequirements(min_length=4, require_uppercase=False,
                                   require_special=False, require_digit=False)
        v = PasswordPolicyValidator(req)
        valid, errors = v.validate("abcd")
        self.assertTrue(valid)


# ===========================================================================
# UserAuthenticator
# ===========================================================================

class TestUserAuthenticator(unittest.TestCase):

    def _make_auth(self, user_row=None, verify_result=True):
        from auth.user_authenticator import UserAuthenticator

        cur = MagicMock()
        cur.fetchone.return_value = user_row
        conn = MagicMock()
        conn.cursor.return_value = cur

        ph = MagicMock()
        ph.verify_password.return_value = verify_result
        return UserAuthenticator(conn, ph)

    def _active_user(self):
        return {
            "user_id": 1, "username": "alice", "password_hash": "hashed",
            "role": "Employee", "staff_id": None, "active": True,
            "name": "Alice", "last_name": "Smith", "email": "alice@example.com",
        }

    def test_authenticate_success(self):
        auth = self._make_auth(self._active_user(), verify_result=True)
        result = auth.authenticate("alice", "correct")
        self.assertIsNotNone(result)
        self.assertNotIn("password_hash", result)
        self.assertEqual(result["username"], "alice")

    def test_authenticate_user_not_found(self):
        auth = self._make_auth(None)
        self.assertIsNone(auth.authenticate("ghost", "pass"))

    def test_authenticate_inactive_user(self):
        user = self._active_user()
        user["active"] = False
        auth = self._make_auth(user)
        self.assertIsNone(auth.authenticate("alice", "pass"))

    def test_authenticate_wrong_password(self):
        auth = self._make_auth(self._active_user(), verify_result=False)
        self.assertIsNone(auth.authenticate("alice", "wrong"))

    def test_authenticate_db_error_returns_none(self):
        from auth.user_authenticator import UserAuthenticator
        conn = MagicMock()
        conn.cursor.side_effect = Exception("DB error")
        ph = MagicMock()
        auth = UserAuthenticator(conn, ph)
        self.assertIsNone(auth.authenticate("alice", "pass"))

    def test_password_hash_not_in_result(self):
        auth = self._make_auth(self._active_user(), verify_result=True)
        result = auth.authenticate("alice", "correct")
        self.assertNotIn("password_hash", result)


# ===========================================================================
# UserRepository
# ===========================================================================

class TestUserRepository(unittest.TestCase):

    def _make_repo(self, cursor=None):
        from auth.user_repository import UserRepository
        conn = MagicMock()
        if cursor:
            conn.cursor.return_value = cursor
        ph = MagicMock()
        ph.hash_password.return_value = "hashed_pw"
        return UserRepository(conn, ph), conn, ph

    # --- create_user ---

    def test_create_user_success(self):
        repo, conn, _ = self._make_repo()
        result = repo.create_user("bob", "pass", "Employee")
        self.assertTrue(result)
        conn.commit.assert_called_once()

    def test_create_user_duplicate_returns_false(self):
        from auth.user_repository import UserRepository
        conn = MagicMock()
        exc = Exception("Duplicate entry 'bob' for key 'username'")
        conn.cursor.return_value.execute.side_effect = exc
        ph = MagicMock()
        ph.hash_password.return_value = "h"
        repo = UserRepository(conn, ph)
        self.assertFalse(repo.create_user("bob", "pass", "Employee"))

    def test_create_user_with_staff_id(self):
        repo, conn, _ = self._make_repo()
        result = repo.create_user("bob", "pass", "Manager", staff_id=5)
        self.assertTrue(result)

    # --- get_user_by_id ---

    def test_get_user_by_id_found(self):
        cur = _make_cursor({"user_id": 1, "username": "alice"})
        repo, _, _ = self._make_repo(cur)
        user = repo.get_user_by_id(1)
        self.assertEqual(user["username"], "alice")

    def test_get_user_by_id_not_found(self):
        cur = _make_cursor(None)
        repo, _, _ = self._make_repo(cur)
        self.assertIsNone(repo.get_user_by_id(99))

    def test_get_user_by_id_db_error(self):
        from auth.user_repository import UserRepository
        conn = MagicMock()
        conn.cursor.side_effect = Exception("DB error")
        repo = UserRepository(conn, MagicMock())
        self.assertIsNone(repo.get_user_by_id(1))

    # --- get_all_users ---

    def test_get_all_users_returns_list(self):
        rows = [{"user_id": 1}, {"user_id": 2}]
        cur = _make_cursor(rows)
        repo, _, _ = self._make_repo(cur)
        result = repo.get_all_users()
        self.assertEqual(len(result), 2)

    def test_get_all_users_db_error_returns_empty(self):
        from auth.user_repository import UserRepository
        conn = MagicMock()
        conn.cursor.side_effect = Exception("DB error")
        repo = UserRepository(conn, MagicMock())
        self.assertEqual(repo.get_all_users(), [])

    # --- update_user_role ---

    def test_update_user_role_success(self):
        repo, conn, _ = self._make_repo()
        self.assertTrue(repo.update_user_role(1, "Manager"))
        conn.commit.assert_called()

    def test_update_user_role_db_error(self):
        from auth.user_repository import UserRepository
        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = Exception("fail")
        repo = UserRepository(conn, MagicMock())
        self.assertFalse(repo.update_user_role(1, "Manager"))

    # --- activate / deactivate ---

    def test_activate_user(self):
        repo, conn, _ = self._make_repo()
        self.assertTrue(repo.activate_user(1))

    def test_deactivate_user(self):
        repo, conn, _ = self._make_repo()
        self.assertTrue(repo.deactivate_user(1))

    # --- delete_user ---

    def test_delete_user_success(self):
        cur = MagicMock()
        cur.rowcount = 1
        repo, conn, _ = self._make_repo(cur)
        self.assertTrue(repo.delete_user(1))

    def test_delete_user_not_found(self):
        cur = MagicMock()
        cur.rowcount = 0
        repo, conn, _ = self._make_repo(cur)
        self.assertFalse(repo.delete_user(99))

    def test_delete_user_db_error(self):
        from auth.user_repository import UserRepository
        conn = MagicMock()
        conn.cursor.side_effect = Exception("fail")
        repo = UserRepository(conn, MagicMock())
        self.assertFalse(repo.delete_user(1))


# ===========================================================================
# UserManager (facade)
# ===========================================================================

class TestUserManager(unittest.TestCase):

    def _make_manager(self):
        from auth.user_manager import UserManager
        conn = MagicMock()
        with patch("auth.user_manager.PasswordHandler") as MockPH, \
             patch("auth.user_manager.UserAuthenticator") as MockUA, \
             patch("auth.user_manager.UserRepository") as MockUR:
            mgr = UserManager(conn)
            mgr._password_handler = MockPH.return_value
            mgr._authenticator    = MockUA.return_value
            mgr._repository       = MockUR.return_value
        return mgr

    def test_authenticate_delegates(self):
        mgr = self._make_manager()
        mgr._authenticator.authenticate.return_value = {"user_id": 1}
        result = mgr.authenticate("alice", "pass")
        mgr._authenticator.authenticate.assert_called_once_with("alice", "pass")
        self.assertEqual(result["user_id"], 1)

    def test_change_password_delegates(self):
        mgr = self._make_manager()
        mgr._password_handler.change_password.return_value = True
        self.assertTrue(mgr.change_password(1, "old", "new"))

    def test_create_user_delegates(self):
        mgr = self._make_manager()
        mgr._repository.create_user.return_value = True
        self.assertTrue(mgr.create_user("bob", "pass", "Employee"))

    def test_get_user_by_id_delegates(self):
        mgr = self._make_manager()
        mgr._repository.get_user_by_id.return_value = {"user_id": 1}
        self.assertEqual(mgr.get_user_by_id(1)["user_id"], 1)

    def test_get_all_users_delegates(self):
        mgr = self._make_manager()
        mgr._repository.get_all_users.return_value = [{"user_id": 1}]
        self.assertEqual(len(mgr.get_all_users()), 1)

    def test_update_user_role_delegates(self):
        mgr = self._make_manager()
        mgr._repository.update_user_role.return_value = True
        self.assertTrue(mgr.update_user_role(1, "Manager"))

    def test_activate_user_delegates(self):
        mgr = self._make_manager()
        mgr._repository.activate_user.return_value = True
        self.assertTrue(mgr.activate_user(1))

    def test_deactivate_user_delegates(self):
        mgr = self._make_manager()
        mgr._repository.deactivate_user.return_value = True
        self.assertTrue(mgr.deactivate_user(1))

    def test_delete_user_delegates(self):
        mgr = self._make_manager()
        mgr._repository.delete_user.return_value = True
        self.assertTrue(mgr.delete_user(1))


# ===========================================================================
# LockoutInfo
# ===========================================================================

class TestLockoutInfo(unittest.TestCase):

    def _locked(self, seconds_remaining=60):
        from auth.account_lockout import LockoutInfo
        return LockoutInfo(
            is_locked=True,
            failed_attempts=5,
            locked_until=datetime.now() + timedelta(seconds=seconds_remaining),
            last_failed_attempt=datetime.now(),
        )

    def _unlocked(self):
        from auth.account_lockout import LockoutInfo
        return LockoutInfo(
            is_locked=False, failed_attempts=0,
            locked_until=None, last_failed_attempt=None,
        )

    def test_time_remaining_when_locked(self):
        info = self._locked(60)
        self.assertGreater(info.time_remaining(), 0)
        self.assertLessEqual(info.time_remaining(), 60)

    def test_time_remaining_when_not_locked(self):
        self.assertEqual(self._unlocked().time_remaining(), 0)

    def test_is_lockout_expired_false_when_active(self):
        self.assertFalse(self._locked(60).is_lockout_expired())

    def test_is_lockout_expired_true_when_past(self):
        from auth.account_lockout import LockoutInfo
        info = LockoutInfo(
            is_locked=True, failed_attempts=5,
            locked_until=datetime.now() - timedelta(seconds=1),
            last_failed_attempt=datetime.now(),
        )
        self.assertTrue(info.is_lockout_expired())

    def test_is_lockout_expired_false_when_not_locked(self):
        self.assertFalse(self._unlocked().is_lockout_expired())


# ===========================================================================
# AccountLockoutManager
# ===========================================================================

class TestAccountLockoutManager(unittest.TestCase):

    def _make_mgr(self, max_attempts=3, lockout_minutes=15):
        from auth.account_lockout import AccountLockoutManager
        return AccountLockoutManager(
            max_attempts=max_attempts,
            lockout_duration_minutes=lockout_minutes,
        )

    def test_initial_state_not_locked(self):
        mgr = self._make_mgr()
        self.assertFalse(mgr.is_account_locked("alice"))

    def test_attempts_remaining_initial(self):
        mgr = self._make_mgr(max_attempts=3)
        self.assertEqual(mgr.get_attempts_remaining("alice"), 3)

    def test_failed_attempts_increment(self):
        mgr = self._make_mgr(max_attempts=3)
        mgr.record_failed_attempt("alice")
        self.assertEqual(mgr.get_attempts_remaining("alice"), 2)

    def test_account_locks_at_max_attempts(self):
        mgr = self._make_mgr(max_attempts=3)
        for _ in range(3):
            mgr.record_failed_attempt("alice")
        self.assertTrue(mgr.is_account_locked("alice"))

    def test_no_increment_when_already_locked(self):
        mgr = self._make_mgr(max_attempts=3)
        for _ in range(3):
            mgr.record_failed_attempt("alice")
        # Further attempts should not increment
        mgr.record_failed_attempt("alice")
        info = mgr.get_lockout_info("alice")
        self.assertEqual(info.failed_attempts, 3)

    def test_attempts_remaining_zero_when_locked(self):
        mgr = self._make_mgr(max_attempts=3)
        for _ in range(3):
            mgr.record_failed_attempt("alice")
        self.assertEqual(mgr.get_attempts_remaining("alice"), 0)

    def test_successful_login_clears_state(self):
        mgr = self._make_mgr(max_attempts=3)
        mgr.record_failed_attempt("alice")
        mgr.record_successful_login("alice")
        self.assertFalse(mgr.is_account_locked("alice"))
        self.assertEqual(mgr.get_attempts_remaining("alice"), 3)

    def test_manual_unlock(self):
        mgr = self._make_mgr(max_attempts=3)
        for _ in range(3):
            mgr.record_failed_attempt("alice")
        self.assertTrue(mgr.is_account_locked("alice"))
        mgr.unlock_account("alice")
        self.assertFalse(mgr.is_account_locked("alice"))

    def test_expired_lockout_auto_clears(self):
        from auth.account_lockout import AccountLockoutManager, LockoutInfo
        mgr = AccountLockoutManager(max_attempts=3, lockout_duration_minutes=0)
        # Inject an already-expired lockout into the cache
        mgr._cache["alice"] = LockoutInfo(
            is_locked=True,
            failed_attempts=3,
            locked_until=datetime.now() - timedelta(seconds=1),
            last_failed_attempt=datetime.now(),
        )
        self.assertFalse(mgr.is_account_locked("alice"))

    def test_separate_users_independent(self):
        mgr = self._make_mgr(max_attempts=3)
        for _ in range(3):
            mgr.record_failed_attempt("alice")
        self.assertTrue(mgr.is_account_locked("alice"))
        self.assertFalse(mgr.is_account_locked("bob"))

    def test_db_persist_called_on_update(self):
        from auth.account_lockout import AccountLockoutManager
        db = MagicMock()
        conn = MagicMock()
        db.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
        db.get_connection.return_value.__exit__ = MagicMock(return_value=False)
        mgr = AccountLockoutManager(max_attempts=3, db_manager=db)
        mgr.record_failed_attempt("alice")
        db.get_connection.assert_called()

    def test_db_load_on_cache_miss(self):
        from auth.account_lockout import AccountLockoutManager
        db = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = (2, None, datetime.now())
        conn = MagicMock()
        conn.cursor.return_value = cur
        db.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
        db.get_connection.return_value.__exit__ = MagicMock(return_value=False)
        mgr = AccountLockoutManager(max_attempts=5, db_manager=db)
        info = mgr.get_lockout_info("alice")
        self.assertEqual(info.failed_attempts, 2)


# ===========================================================================
# TwoFactorAuth
# ===========================================================================

class TestTwoFactorAuth(unittest.TestCase):

    def _make_tfa(self, fetch_result=None):
        from auth.two_factor_auth import TwoFactorAuth
        cur = MagicMock()
        cur.fetchone.return_value = fetch_result
        conn = MagicMock()
        conn.cursor.return_value = cur
        self._cursor = cur
        self._conn = conn
        return TwoFactorAuth(conn)

    # --- generate_secret ---

    def test_generate_secret_is_base32(self):
        from auth.two_factor_auth import TwoFactorAuth
        secret = TwoFactorAuth.generate_secret()

        # pyotp secrets are valid base32
        self.assertIsInstance(secret, str)
        self.assertGreater(len(secret), 0)

    # --- generate_backup_codes ---

    def test_generate_backup_codes_default_count(self):
        from auth.two_factor_auth import TwoFactorAuth
        codes = TwoFactorAuth.generate_backup_codes()
        self.assertEqual(len(codes), 8)

    def test_generate_backup_codes_custom_count(self):
        from auth.two_factor_auth import TwoFactorAuth
        codes = TwoFactorAuth.generate_backup_codes(count=4)
        self.assertEqual(len(codes), 4)

    def test_backup_codes_are_uppercase_hex(self):
        from auth.two_factor_auth import TwoFactorAuth
        codes = TwoFactorAuth.generate_backup_codes()
        for code in codes:
            self.assertEqual(code, code.upper())
            int(code, 16)  # raises ValueError if not valid hex

    # --- verify_code ---

    def test_verify_code_valid(self):
        from auth.two_factor_auth import TwoFactorAuth
        secret = TwoFactorAuth.generate_secret()
        code = pyotp.TOTP(secret).now()
        self.assertTrue(TwoFactorAuth.verify_code(secret, code))

    def test_verify_code_invalid(self):
        from auth.two_factor_auth import TwoFactorAuth
        secret = TwoFactorAuth.generate_secret()
        self.assertFalse(TwoFactorAuth.verify_code(secret, "000000"))

    def test_verify_code_strips_whitespace(self):
        from auth.two_factor_auth import TwoFactorAuth
        secret = TwoFactorAuth.generate_secret()
        code = "  " + pyotp.TOTP(secret).now() + "  "
        self.assertTrue(TwoFactorAuth.verify_code(secret, code))

    def test_verify_code_bad_secret_returns_false(self):
        from auth.two_factor_auth import TwoFactorAuth
        self.assertFalse(TwoFactorAuth.verify_code("!!!bad!!!", "123456"))

    # --- generate_qr_code ---

    def test_generate_qr_code_returns_png_bytes(self):
        tfa = self._make_tfa()
        secret = pyotp.random_base32()
        data = tfa.generate_qr_code("alice", secret)
        self.assertIsInstance(data, bytes)
        self.assertTrue(data.startswith(b"\x89PNG"))

    # --- enable_2fa / disable_2fa ---

    def test_enable_2fa_success(self):
        tfa = self._make_tfa()
        result = tfa.enable_2fa(1, "SECRET", ["CODE1", "CODE2"])
        self.assertTrue(result)
        self._conn.commit.assert_called_once()

    def test_enable_2fa_db_error(self):
        from auth.two_factor_auth import TwoFactorAuth
        conn = MagicMock()
        conn.cursor.side_effect = Exception("DB error")
        tfa = TwoFactorAuth(conn)
        self.assertFalse(tfa.enable_2fa(1, "SECRET", []))

    def test_disable_2fa_success(self):
        tfa = self._make_tfa()
        self.assertTrue(tfa.disable_2fa(1))
        self._conn.commit.assert_called_once()

    # --- is_2fa_enabled ---

    def test_is_2fa_enabled_true(self):
        tfa = self._make_tfa(fetch_result=(True,))
        self.assertTrue(tfa.is_2fa_enabled(1))

    def test_is_2fa_enabled_false(self):
        tfa = self._make_tfa(fetch_result=(False,))
        self.assertFalse(tfa.is_2fa_enabled(1))

    def test_is_2fa_enabled_not_found(self):
        tfa = self._make_tfa(fetch_result=None)
        self.assertFalse(tfa.is_2fa_enabled(99))

    # --- get_user_secret ---

    def test_get_user_secret_found(self):
        tfa = self._make_tfa(fetch_result=("MYSECRET",))
        self.assertEqual(tfa.get_user_secret(1), "MYSECRET")

    def test_get_user_secret_not_found(self):
        tfa = self._make_tfa(fetch_result=None)
        self.assertIsNone(tfa.get_user_secret(99))

    # --- verify_backup_code ---

    def test_verify_backup_code_valid_and_consumed(self):
        from auth.two_factor_auth import TwoFactorAuth
        codes = ["AABB1122", "CCDD3344"]
        cur = MagicMock()
        cur.fetchone.return_value = (json.dumps(codes),)
        conn = MagicMock()
        conn.cursor.return_value = cur
        tfa = TwoFactorAuth(conn)
        self.assertTrue(tfa.verify_backup_code(1, "AABB1122"))
        conn.commit.assert_called_once()

    def test_verify_backup_code_case_insensitive(self):
        from auth.two_factor_auth import TwoFactorAuth
        codes = ["AABB1122"]
        cur = MagicMock()
        cur.fetchone.return_value = (json.dumps(codes),)
        conn = MagicMock()
        conn.cursor.return_value = cur
        tfa = TwoFactorAuth(conn)
        self.assertTrue(tfa.verify_backup_code(1, "aabb1122"))

    def test_verify_backup_code_invalid(self):
        from auth.two_factor_auth import TwoFactorAuth
        codes = ["AABB1122"]
        cur = MagicMock()
        cur.fetchone.return_value = (json.dumps(codes),)
        conn = MagicMock()
        conn.cursor.return_value = cur
        tfa = TwoFactorAuth(conn)
        self.assertFalse(tfa.verify_backup_code(1, "ZZZZZZZZ"))

    def test_verify_backup_code_no_codes(self):
        tfa = self._make_tfa(fetch_result=None)
        self.assertFalse(tfa.verify_backup_code(1, "AABB1122"))

    # --- get_remaining_backup_codes ---

    def test_get_remaining_backup_codes(self):
        from auth.two_factor_auth import TwoFactorAuth
        codes = ["AABB1122", "CCDD3344"]
        cur = MagicMock()
        cur.fetchone.return_value = (json.dumps(codes),)
        conn = MagicMock()
        conn.cursor.return_value = cur
        tfa = TwoFactorAuth(conn)
        result = tfa.get_remaining_backup_codes(1)
        self.assertEqual(result, codes)

    def test_get_remaining_backup_codes_empty(self):
        tfa = self._make_tfa(fetch_result=None)
        self.assertEqual(tfa.get_remaining_backup_codes(99), [])


# ===========================================================================
# SessionManager
# ===========================================================================

class TestSessionManager(unittest.TestCase):

    def setUp(self):
        # Reset singleton between tests
        from auth.session import SessionManager
        SessionManager._instance = None
        self.sm = SessionManager()

    def _user(self, username="alice", role="Employee"):
        return {"user_id": 1, "username": username, "role": role, "staff_id": None}

    def test_singleton(self):
        from auth.session import SessionManager
        sm2 = SessionManager()
        self.assertIs(self.sm, sm2)

    def test_not_logged_in_initially(self):
        self.assertFalse(self.sm.is_logged_in())

    def test_login_sets_user(self):
        self.sm.login(self._user())
        self.assertTrue(self.sm.is_logged_in())
        self.assertEqual(self.sm.get_username(), "alice")

    def test_logout_clears_user(self):
        self.sm.login(self._user())
        self.sm.logout()
        self.assertFalse(self.sm.is_logged_in())
        self.assertIsNone(self.sm.get_current_user())

    def test_get_role(self):
        self.sm.login(self._user(role="Manager"))
        self.assertEqual(self.sm.get_role(), "Manager")

    def test_get_user_id(self):
        self.sm.login(self._user())
        self.assertEqual(self.sm.get_user_id(), 1)

    def test_get_current_user_returns_copy(self):
        self.sm.login(self._user())
        u1 = self.sm.get_current_user()
        u1["username"] = "hacked"
        self.assertEqual(self.sm.get_username(), "alice")

    def test_get_login_time_set_on_login(self):
        self.sm.login(self._user())
        self.assertIsInstance(self.sm.get_login_time(), datetime)

    def test_update_user_data(self):
        self.sm.login(self._user())
        self.sm.update_user_data({"role": "Manager"})
        self.assertEqual(self.sm.get_role(), "Manager")

    def test_update_user_data_raises_when_not_logged_in(self):
        with self.assertRaises(RuntimeError):
            self.sm.update_user_data({"role": "Manager"})

    def test_accessors_return_none_when_logged_out(self):
        self.assertIsNone(self.sm.get_user_id())
        self.assertIsNone(self.sm.get_username())
        self.assertIsNone(self.sm.get_role())
        self.assertIsNone(self.sm.get_current_user())

    def test_repr(self):
        self.assertIn("logged_in=False", repr(self.sm))
        self.sm.login(self._user())
        self.assertIn("logged_in=True", repr(self.sm))


# ===========================================================================
# PermissionManager
# ===========================================================================

class TestPermissionManager(unittest.TestCase):

    def setUp(self):
        from auth.permissions import PermissionManager, Permission
        self.pm = PermissionManager
        self.Permission = Permission

    def test_employee_can_view_dashboard(self):
        self.assertTrue(self.pm.can_view_dashboard("Employee"))

    def test_employee_cannot_manage_database(self):
        self.assertFalse(self.pm.can_manage_database("Employee"))

    def test_employee_cannot_delete_data(self):
        self.assertFalse(self.pm.can_delete_data("Employee"))

    def test_manager_can_modify_data(self):
        self.assertTrue(self.pm.can_modify_data("Manager"))

    def test_manager_cannot_manage_database(self):
        self.assertFalse(self.pm.can_manage_database("Manager"))

    def test_manager_cannot_create_users(self):
        self.assertFalse(self.pm.can_manage_users("Manager"))

    def test_administrator_has_all_permissions(self):
        checks = [
            self.pm.can_view_dashboard,
            self.pm.can_manage_database,
            self.pm.can_modify_data,
            self.pm.can_delete_data,
            self.pm.can_manage_users,
            self.pm.can_modify_users,
            self.pm.can_delete_users,
            self.pm.can_import_data,
            self.pm.can_export_data,
            self.pm.can_view_logs,
            self.pm.can_access_system_settings,
        ]
        for check in checks:
            self.assertTrue(check("Administrator"), f"{check.__name__} should be True")

    def test_invalid_role_returns_empty_frozenset(self):
        perms = self.pm.get_role_permissions("Overlord")
        self.assertEqual(perms, frozenset())

    def test_has_permission(self):
        self.assertTrue(self.pm.has_permission("Employee", self.Permission.VIEW_DASHBOARD))
        self.assertFalse(self.pm.has_permission("Employee", self.Permission.DELETE_DATA))

    def test_role_permissions_immutable(self):
        perms = self.pm.get_role_permissions("Employee")
        with self.assertRaises((AttributeError, TypeError)):
            perms.add(self.Permission.DELETE_DATA)  # frozenset raises TypeError

    def test_employee_can_export_data(self):
        self.assertTrue(self.pm.can_export_data("Employee"))

    def test_manager_can_view_logs(self):
        self.assertTrue(self.pm.can_view_logs("Manager"))

    def test_employee_cannot_import_data(self):
        self.assertFalse(self.pm.can_import_data("Employee"))


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)