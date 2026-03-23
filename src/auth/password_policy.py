"""
Password Policy Validator
Enforces password strength requirements.
"""

import re
from typing import List, Tuple
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_SPECIAL_CHARS = "!@#$%^&*()_+-=[]{}|;:,.<>?"

_WEAK_PASSWORDS = frozenset({
    'password', 'password1', 'password123', 'admin', 'admin123',
    '12345678', 'qwerty123', 'letmein', 'welcome', 'monkey123',
})

# Strength thresholds
_STRENGTH_LABELS = [
    (80, "Very Strong"),
    (60, "Strong"),
    (40, "Medium"),
    (20, "Weak"),
    (0,  "Very Weak"),
]

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PasswordRequirements:
    """Password policy requirements."""
    min_length:        int  = 8
    require_uppercase: bool = True
    require_lowercase: bool = True
    require_digit:     bool = True
    require_special:   bool = True
    special_chars:     str  = field(default=_DEFAULT_SPECIAL_CHARS)


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

class PasswordPolicyValidator:
    """Validates passwords against a configurable security policy."""

    def __init__(self, requirements: PasswordRequirements = None):
        self._req = requirements or PasswordRequirements()
        self._special_pattern = re.compile(f"[{re.escape(self._req.special_chars)}]")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def validate(self, password: str) -> Tuple[bool, List[str]]:
        """
        Validate password against policy.

        Returns:
            (is_valid, list_of_errors)
        """
        errors = []

        if len(password) < self._req.min_length:
            errors.append(f"Password must be at least {self._req.min_length} characters long")

        if self._req.require_uppercase and not re.search(r'[A-Z]', password):
            errors.append("Password must contain at least one uppercase letter")

        if self._req.require_lowercase and not re.search(r'[a-z]', password):
            errors.append("Password must contain at least one lowercase letter")

        if self._req.require_digit and not re.search(r'\d', password):
            errors.append("Password must contain at least one number")

        if self._req.require_special and not self._special_pattern.search(password):
            errors.append(
                f"Password must contain at least one special character ({self._req.special_chars})"
            )

        if password.lower() in _WEAK_PASSWORDS:
            errors.append("Password is too common and easily guessable")

        return not errors, errors

    def get_requirements_text(self) -> str:
        """Return human-readable password requirements."""
        lines = [f"At least {self._req.min_length} characters long"]

        if self._req.require_uppercase:
            lines.append("At least one uppercase letter (A-Z)")
        if self._req.require_lowercase:
            lines.append("At least one lowercase letter (a-z)")
        if self._req.require_digit:
            lines.append("At least one number (0-9)")
        if self._req.require_special:
            lines.append(f"At least one special character ({self._req.special_chars})")

        return "\n".join(f"• {line}" for line in lines)

    def calculate_strength(self, password: str) -> Tuple[str, int]:
        """
        Calculate password strength.

        Returns:
            (strength_label, strength_percentage)
        """
        score = (
            self._length_score(password)
            + self._variety_score(password)
            + self._complexity_score(password)
        )

        # Clamp score to 0–100
        score = max(0, min(score, 100))

        label = next(label for threshold, label in _STRENGTH_LABELS if score >= threshold)
        return label, score

    # ------------------------------------------------------------------
    # Private scoring helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _length_score(password: str) -> int:
        """Up to 30 points for length."""
        n = len(password)
        if n >= 16:
            return 30
        if n >= 12:
            return 25
        if n >= 10:
            return 20
        if n >= 8:
            return 10
        return 0

    def _variety_score(self, password: str) -> int:
        """Up to 40 points for character variety."""
        score = 0
        if re.search(r'[a-z]', password):
            score += 10
        if re.search(r'[A-Z]', password):
            score += 10
        if re.search(r'\d', password):
            score += 10
        if self._special_pattern.search(password):
            score += 10
        return score

    @staticmethod
    def _complexity_score(password: str) -> int:
        """Up to 30 points for additional complexity."""
        score = 0
        lower = password.lower()

        # Character diversity
        if len(set(lower)) > len(lower) * 0.7:
            score += 10

        # No triple repeated characters
        if not any(c * 3 in lower for c in set(lower)):
            score += 10

        # Extra length bonus
        if len(password) >= 16:
            score += 10

        return score


# ---------------------------------------------------------------------------
# Module-level default instance
# ---------------------------------------------------------------------------

default_validator = PasswordPolicyValidator()