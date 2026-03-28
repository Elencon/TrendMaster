import pytest
from unittest.mock import MagicMock, patch

from src.gui.two_factor_setup_dialog import TwoFactorSetupDialog
from src.gui.two_factor_verify_dialog import TwoFactorVerifyDialog

@patch('src.gui.two_factor_setup_dialog.AuthManager')
def test_setup_dialog_init(mock_auth, qapp_instance):
    """Test setup dialog initializes properly."""
    # Mocking AuthManager logic
    mock_instance = MagicMock()
    mock_instance.generate_totp_secret.return_value = "JBSWY3DPEHPK3PXP"
    mock_instance.generate_totp_uri.return_value = "otpauth://totp/TrendMaster:user?secret=JBSWY3DPEHPK3PXP"
    mock_auth.get_instance.return_value = mock_instance
    
    dialog = TwoFactorSetupDialog("test_user")
    
    assert dialog.windowTitle() == "Set Up Two-Factor Authentication"
    mock_instance.generate_totp_secret.assert_called_once()
    
@patch('src.gui.two_factor_verify_dialog.AuthManager')
def test_verify_dialog_init(mock_auth, qapp_instance):
    """Test verify dialog initializes properly."""
    dialog = TwoFactorVerifyDialog("test_user_id")
    assert dialog.windowTitle() == "Verify Context"
    
@patch('src.gui.two_factor_verify_dialog.AuthManager')
def test_verify_dialog_logic(mock_auth, qapp_instance):
    """Test verification success flow."""
    mock_instance = MagicMock()
    mock_instance.verify_totp.return_value = True
    mock_auth.get_instance.return_value = mock_instance
    
    dialog = TwoFactorVerifyDialog("test_user_id")
    dialog.code_input.setText("123456")
    
    # We mock accept so it doesn't close the non-existent window
    with patch.object(dialog, 'accept') as mock_accept:
        dialog._on_verify_clicked()
        mock_instance.verify_totp.assert_called_once_with("test_user_id", "123456")
        mock_accept.assert_called_once()
