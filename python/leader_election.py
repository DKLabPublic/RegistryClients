#   ---------------------------------------------------------------------------------
#   Copyright (c) 2024 DK Lab, LLC. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   ---------------------------------------------------------------------------------

import threading
import time

from registry_client import RegistryClient, RegistryError

class Role:
    """
    `Role` is a tool for leader election in a distributed system.  Leader election happens among
    participants who want to play the role.  A configurable number of leaders are elected from the
    participants.  The tool leverages DK Lab's Registry Service (dklab.us/registry-service).

    Args:
        name (str):  The name of this client.  It could be the host name, IP address, or any name
            unique among the participants.
        service_instance (str):  The name of the Registry Service instance.
        org_name (str):  The org where the role is.
        org_key (str):  The authorization credential for the org.
        role_path (str):  The role path without the "/<org>" prefix and the ".role" suffix.
        max_players (int, optional):  The number of clients that can assume the role.
            Default is 1.
        playtime_secs (int, optional):  The duration in which the client holds the role on success.
            Default is 10 seconds.

    Attributes:
        name (str): This stores `name` arg.
        client (RegistryClient): This client talks to the Registry Service instance.
        role_path (str): This stores `role_path` arg.
        max_players (int): This stores `max_players` arg.
        playtime_secs (int): This stores `playtime_secs` arg.
        expiration_time_msecs (int): The timestamp when the playtime expires in milliseconds.
            It is initially set to 0.

    Example:
        # Create a Role instance
        writer = Role(
            "my_name", "useast2", "my_org", "my_org_key", "/partition/writer", 1, 30)
        
        # Attempt to become the writer
        if writer.take():
            print("I am the writer now")
        else:
            print("Failed to become the writer")

        # Sleep for some time
        time.sleep(5)

        # Check if still being the writer for the next 200 milliseconds
        if writer.is_holding(0.200):
            print("Still be the writer.")
        else:
            print("No longer be the writer.")
    """
    def __init__(self, name, service_instance, org_name, org_key, role_path, max_players, playtime_secs=10):
        self.name = name
        self.client = RegistryClient(service_instance, org_name, org_key)
        self.role_path = role_path
        self.max_players = max_players
        self.playtime_secs = playtime_secs
        self.expiration_time_msecs = 0
        self._lock = threading.RLock()
        self._to_maintain_role = False
        self._maintenance_thread = None

    def take(self):
        """
        Assume the role.  Thread safe.

        Returns:
            bool: True on success, False otherwise.  Note that in rare cases the client has become
                a player, but the server or network fails, leading to a False outcome.  The client
                could read the role to confirm, or retry, or just let the requested playtime expire.
        """
        with self._lock:
            try:
                resp = self.client.take_role(
                    self.role_path, self.name, self.playtime_secs, self.max_players)
                if resp.client_expiration_time_in_msecs > self.expiration_time_msecs:
                    self.expiration_time_msecs = resp.client_expiration_time_in_msecs
                self._to_maintain_role = True
                # Maintain the role
                if self._maintenance_thread is None:
                    self._maintenance_thread = threading.Thread(target=self._maintain_role)
                    self._maintenance_thread.daemon = True
                    self._maintenance_thread.start()
                return True
            except RegistryError as e:
                return False

    def release(self):
        """
        Stop being a player of the role.  Thread safe.

        Returns:
            bool: True on success, False otherwise.  Note that in rare cases the client been removed
                from the role, but the server or network fails, leading to a False outcome.  The client
                could read the role to confirm, or retry, or just let the requested playtime expire.
        """
        with self._lock:
            self._to_maintain_role = False
            if self._maintenance_thread:
                # Don't wait for the maintenance thread to finish, because 
                # the thread is waiting to take the lock.
                self._maintenance_thread = None
            try:
                self.client.release_role(self.role_path, self.name)
                return True
            except Exception:
                num_attempts -= 1

    def is_holding(self, num_secs=0.0):
        """
        Check if the client is holding the role.

        Args:
            num_secs (float, optional):  When provided, the method checks if the client will be
                holding the role for `num_secs` seconds.  When not provided, it checks if the client
                is currently holing the role (but note, it might be incorrect right after checking).

        Returns:
            bool: True if the client is holding the role, False otherwise.
        """
        target_time_msecs = int((time.time() + num_secs) * 1000)
        with self._lock:
            return target_time_msecs < self.expiration_time_msecs

    def active_players(self):
        """
        Get the list of active players.  It is fresh from the service, but due to the nature of
        distributed systems, there's no guarantee that the players are still active.

        Returns:
            a set of names of players whose playtime is unexpired, or None if there is an error.
        """
        try:
            resp = self.client.read_role(self.role_path)
            return {player_name for (player_name, remaining_playtime) in resp.players_remaining_milliseconds.items() if remaining_playtime > 0}
        except RegistryError as e:
            return None

    def remaining_playtime(self):
        """
        Returns:
            float: a number of remaining seconds.
        """
        return float(self.expiration_time_msecs)/1000.0 - time.time()

    def _maintain_role(self):
        """
        Internal method to periodically re-take the role.
        """
        # Allow to retry before the playtime ends.  For simplicity, retries are 1 second apart.
        # And to incur fewest requests in the normal case, retries happen at the end of the playtime.
        num_retries = 1

        retake_interval = self.playtime_secs - 1 - num_retries
        time.sleep(retake_interval)
        while self._to_maintain_role:
            with self._lock:
                # Checking the flag `_to_maintain_role` again below to ensure that after release()
                # is called, the maintenance thread will not attempt to take the role again, which
                # otherwise could happen as follows:
                # - Here the flag is check in the while condition above,
                # - Then release() gets the lock and calls client.release_role().
                # - Then here it gets the lock and (without checking the flag) calls client.take_role().
                retry_remaining = num_retries
                while self._to_maintain_role and retry_remaining > 0:
                    try:
                        resp = self.client.take_role(
                            self.role_path, self.instance_name, self.playtime_secs, self.max_players)
                        if resp.client_expiration_time_in_msecs > self.expiration_time_msecs:
                            self.expiration_time_msecs = resp.client_expiration_time_in_msecs
                        retry_remaining = 0
                    except Exception:
                        time.sleep(1)
                        retry_remaining -= 1
            time.sleep(retake_interval)
