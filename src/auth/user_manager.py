r"""
C:\\Economy\Invest\TrendMaster\rc\auth\user_manager.py
User management for authentication system.
Facade that coordinates authentication, password, and repository operations.
"""
import logging
from typing import Optional, Dict, Any, List
from .password_handler import PasswordHandler
from .user_authenticator import UserAuthenticator
from .user_repository import UserRepository
from .session import UserData

_logger = logging.getLogger(__name__)


class UserManager:
    """
    Facade for user management operations.
    Delegates to specialized handlers for authentication, passwords, and data access.
    """

    def __init__(self, db_connection):
        """
        Args:
            db_connection: Database connection object (mysql.connector or PyMySQL).
        """
        self._password_handler = PasswordHandler(db_connection)
        self._authenticator    = UserAuthenticator(db_connection, self._password_handler)
        self._repository       = UserRepository(db_connection, self._password_handler)

    def authenticate(self, username: str, password: str) -> Optional[UserData]:
        return self._authenticator.authenticate(username, password)

    def change_password(self, user_id: int, old_password: str, new_password: str) -> bool:
        return self._password_handler.change_password(user_id, old_password, new_password)

    def create_user(self, username: str, password: str, role: str, staff_id: Optional[int] = None) -> bool:
        return self._repository.create_user(username, password, role, staff_id)

    def get_user_by_id(self, user_id: int) -> Optional[UserData]:
        return self._repository.get_user_by_id(user_id)

    def get_all_users(self) -> List[UserData]:
        return self._repository.get_all_users()

    def update_user_role(self, user_id: int, new_role: str) -> bool:
        return self._repository.update_user_role(user_id, new_role)

    def activate_user(self, user_id: int) -> bool:
        return self._repository.activate_user(user_id)

    def deactivate_user(self, user_id: int) -> bool:
        return self._repository.deactivate_user(user_id)

    def delete_user(self, user_id: int) -> bool:
        return self._repository.delete_user(user_id)
