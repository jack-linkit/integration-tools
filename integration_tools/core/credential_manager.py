"""
Unified credential management for database and SFTP connections.
"""

import getpass
import os
from typing import Optional, Tuple

import keyring


class CredentialManager:
    """Manages credentials for database and SFTP connections."""
    
    def __init__(self, service_prefix: str = "integration_tools"):
        self.service_prefix = service_prefix
        self.db_service = f"{service_prefix}_db"
        self.sftp_service = f"{service_prefix}_sftp"
    
    def get_db_credentials(self, interactive_save: bool = True) -> Tuple[str, str]:
        """
        Get database credentials with fallback chain: env vars -> keyring -> prompt.
        
        Args:
            interactive_save: Whether to offer saving credentials interactively
            
        Returns:
            Tuple of (username, password)
            
        Raises:
            RuntimeError: If credentials cannot be obtained
        """
        # Try environment first
        uid_env = os.getenv("DB_UID")
        pwd_env = os.getenv("DB_PWD")
        if uid_env and pwd_env:
            return uid_env, pwd_env

        # Try keyring
        try:
            saved_uid = keyring.get_password(self.db_service, "username")
            saved_pwd = keyring.get_password(self.db_service, "password")
            if saved_uid and saved_pwd:
                print(f"Using saved DB credentials for user: {saved_uid}")
                return saved_uid, saved_pwd
        except Exception:
            # Keyring may not be available or configured
            pass

        # Prompt for new credentials
        print("Database Authentication Required")
        uid = input("DB Username: ").strip()
        pwd = getpass.getpass("DB Password: ")
        if not uid or not pwd:
            raise RuntimeError("DB credentials are required")

        # Offer to save in keychain
        if interactive_save:
            try:
                save = input("Save DB credentials to Keychain for future use? (y/n): ").strip().lower()
            except EOFError:
                save = "n"
            if save == "y":
                try:
                    keyring.set_password(self.db_service, "username", uid)
                    keyring.set_password(self.db_service, "password", pwd)
                    print("DB credentials saved to Keychain.")
                except Exception as e:
                    print(f"Warning: failed to save credentials to Keychain: {e}")
        
        return uid, pwd

    def get_sftp_credentials(
        self, 
        default_host: str = "ftp.linkit.com", 
        force_prompt: bool = False
    ) -> Tuple[str, str, str]:
        """
        Get SFTP credentials (host, username, password).
        
        Args:
            default_host: Default SFTP host
            force_prompt: Force prompting even if saved credentials exist
            
        Returns:
            Tuple of (host, username, password)
            
        Raises:
            RuntimeError: If credentials cannot be obtained
        """
        saved_host = None
        saved_user = None
        saved_pwd = None
        
        if not force_prompt:
            try:
                saved_host = keyring.get_password(self.sftp_service, "host")
                saved_user = keyring.get_password(self.sftp_service, "username")
                saved_pwd = keyring.get_password(self.sftp_service, "password")
            except Exception:
                pass

            if saved_user and saved_pwd and saved_host:
                print(f"Found saved SFTP credentials for user: {saved_user}@{saved_host}")
                use_saved = input("Use saved SFTP credentials? (y/n): ").strip().lower()
                if use_saved == "y":
                    return saved_host, saved_user, saved_pwd

        # Prompt for new/updated credentials
        host_default = saved_host or default_host
        host = input(f"SFTP Host [{host_default}]: ").strip() or host_default
        username = input("SFTP Username: ").strip()
        password = getpass.getpass("SFTP Password: ")
        
        if not host or not username or not password:
            raise RuntimeError("SFTP host/username/password are required")

        # Offer to save in keychain
        try:
            save = input("Save SFTP credentials to Keychain for future use? (y/n): ").strip().lower()
        except EOFError:
            save = "n"
        
        if save == "y":
            try:
                keyring.set_password(self.sftp_service, "host", host)
                keyring.set_password(self.sftp_service, "username", username)
                keyring.set_password(self.sftp_service, "password", password)
                print("SFTP credentials saved to Keychain.")
            except Exception as e:
                print(f"Warning: failed to save SFTP credentials to Keychain: {e}")

        return host, username, password

    def clear_saved_credentials(self, credential_type: Optional[str] = None) -> None:
        """
        Clear saved credentials from keychain.
        
        Args:
            credential_type: "db", "sftp", or None for both
        """
        services_to_clear = []
        
        if credential_type is None or credential_type == "db":
            services_to_clear.extend([
                (self.db_service, "username"),
                (self.db_service, "password")
            ])
        
        if credential_type is None or credential_type == "sftp":
            services_to_clear.extend([
                (self.sftp_service, "host"),
                (self.sftp_service, "username"), 
                (self.sftp_service, "password")
            ])
        
        for service, account in services_to_clear:
            try:
                keyring.delete_password(service, account)
            except Exception:
                pass  # Credential may not exist
        
        print(f"Saved {credential_type or 'DB and SFTP'} credentials removed from Keychain.")