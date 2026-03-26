"""
Permission management for role-based access control.
Defines role permissions and provides permission-checking helpers.
"""

import logging
from enum import Enum
from typing import FrozenSet

_logger = logging.getLogger(__name__)


class Role(Enum):
    """User role enumeration."""

    EMPLOYEE      = "Employee"
    MANAGER       = "Manager"
    ADMINISTRATOR = "Administrator"


class Permission(Enum):
    """Permission enumeration."""

    # Dashboard
    VIEW_DASHBOARD = "view_dashboard"

    # Database management
    MANAGE_DATABASE = "manage_database"

    # Data
    VIEW_DATA   = "view_data"
    EXPORT_DATA = "export_data"
    IMPORT_DATA = "import_data"
    MODIFY_DATA = "modify_data"
    DELETE_DATA = "delete_data"

    # User management
    VIEW_USERS   = "view_users"
    CREATE_USERS = "create_users"
    MODIFY_USERS = "modify_users"
    DELETE_USERS = "delete_users"

    # System
    VIEW_LOGS       = "view_logs"
    SYSTEM_SETTINGS = "system_settings"


# ---------------------------------------------------------------------------
# Role → permission mappings
# Stored as frozensets so no caller can mutate them.
# Private: access only via PermissionManager.get_role_permissions()
# ---------------------------------------------------------------------------

_ROLE_PERMISSIONS: dict[Role, FrozenSet[Permission]] = {
    Role.EMPLOYEE: frozenset({
        Permission.VIEW_DASHBOARD,
        Permission.VIEW_DATA,
        Permission.EXPORT_DATA,
    }),
    Role.MANAGER: frozenset({
        Permission.VIEW_DASHBOARD,
        Permission.VIEW_DATA,
        Permission.EXPORT_DATA,
        Permission.IMPORT_DATA,
        Permission.MODIFY_DATA,
        Permission.VIEW_USERS,
        Permission.VIEW_LOGS,
    }),
    Role.ADMINISTRATOR: frozenset({
        Permission.VIEW_DASHBOARD,
        Permission.MANAGE_DATABASE,
        Permission.VIEW_DATA,
        Permission.EXPORT_DATA,
        Permission.IMPORT_DATA,
        Permission.MODIFY_DATA,
        Permission.DELETE_DATA,
        Permission.VIEW_USERS,
        Permission.CREATE_USERS,
        Permission.MODIFY_USERS,
        Permission.DELETE_USERS,
        Permission.VIEW_LOGS,
        Permission.SYSTEM_SETTINGS,
    }),
}


class PermissionManager:
    """Manages role-based permissions."""

    @staticmethod
    def get_role_permissions(role: str) -> FrozenSet[Permission]:
        """
        Return all permissions granted to a role.

        Args:
            role: Role name as string (e.g. "Employee").

        Returns:
            FrozenSet of Permission values; empty frozenset for unknown roles.
        """
        try:
            return _ROLE_PERMISSIONS.get(Role(role), frozenset())
        except ValueError:
            _logger.error("Invalid role: '%s'", role)
            return frozenset()

    @staticmethod
    def has_permission(role: str, permission: Permission) -> bool:
        """
        Return True if the role has the given permission.

        Args:
            role: Role name as string.
            permission: Permission to check.
        """
        return permission in PermissionManager.get_role_permissions(role)

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    @staticmethod
    def can_manage_database(role: str) -> bool:
        """Return True if the role can access database management."""
        return PermissionManager.has_permission(role, Permission.MANAGE_DATABASE)

    @staticmethod
    def can_view_dashboard(role: str) -> bool:
        """Return True if the role can view the dashboard."""
        return PermissionManager.has_permission(role, Permission.VIEW_DASHBOARD)

    @staticmethod
    def can_modify_data(role: str) -> bool:
        """Return True if the role can modify data."""
        return PermissionManager.has_permission(role, Permission.MODIFY_DATA)

    @staticmethod
    def can_delete_data(role: str) -> bool:
        """Return True if the role can delete data."""
        return PermissionManager.has_permission(role, Permission.DELETE_DATA)

    @staticmethod
    def can_manage_users(role: str) -> bool:
        """
        Return True if the role can create users.

        Note: checks CREATE_USERS only. Use can_modify_users() or
        can_delete_users() to check those permissions individually.
        """
        return PermissionManager.has_permission(role, Permission.CREATE_USERS)

    @staticmethod
    def can_modify_users(role: str) -> bool:
        """Return True if the role can modify existing users."""
        return PermissionManager.has_permission(role, Permission.MODIFY_USERS)

    @staticmethod
    def can_delete_users(role: str) -> bool:
        """Return True if the role can delete users."""
        return PermissionManager.has_permission(role, Permission.DELETE_USERS)

    @staticmethod
    def can_import_data(role: str) -> bool:
        """Return True if the role can import data."""
        return PermissionManager.has_permission(role, Permission.IMPORT_DATA)

    @staticmethod
    def can_export_data(role: str) -> bool:
        """Return True if the role can export data."""
        return PermissionManager.has_permission(role, Permission.EXPORT_DATA)

    @staticmethod
    def can_view_logs(role: str) -> bool:
        """Return True if the role can view system logs."""
        return PermissionManager.has_permission(role, Permission.VIEW_LOGS)

    @staticmethod
    def can_access_system_settings(role: str) -> bool:
        """Return True if the role can access system settings."""
        return PermissionManager.has_permission(role, Permission.SYSTEM_SETTINGS)
