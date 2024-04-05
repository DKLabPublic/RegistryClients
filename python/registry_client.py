#   ---------------------------------------------------------------------------------
#   Copyright (c) 2024 DK Lab, LLC. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   ---------------------------------------------------------------------------------
"""
This is a Python client of Registry Service, leveraging its HTTP API.
Please refer to the HTTP API specification for detail.
"""

import requests
import time


class TakeRoleResult:
    """Represents the result of a successful take-role operation in RegistryService.

    Args:
        org_time (int): The org time right after the operation has taken place.
        is_new_role_created (bool): A boolean indicating whether a new role was created as a result of the operation.
        client_expiration_time_in_msecs (int): The timestamp (per client's clock) when the client must stop playing the role.

    Attributes:
        org_time (int): This stores `org_time` arg.
        is_new_role_created (bool): This stores `is_new_role_created` arg.
        client_expiration_time_in_msecs (int): This stores `client_expiration_time_in_msecs` arg.
    """
    def __init__(self, org_time, is_new_role_created, client_expiration_time_in_msecs):
        self.org_time = org_time
        self.is_new_role_created = is_new_role_created
        self.client_expiration_time_in_msecs = client_expiration_time_in_msecs
    def __repr__(self) -> str:
        return (
            "TakeRoleResult {"
            f"'org_time':{self.org_time},"
            f"'is_new_role_created':{self.is_new_role_created},"
            f"'client_expiration_time_in_msecs':{self.client_expiration_time_in_msecs}"
            "}"
        )

class ReleaseRoleResult:
    """Represents the result of a successful release-role operation in RegistryService.

    Args:
        org_time (int): The org time right after the operation has taken place.

    Attributes:
        org_time (int): This stores `org_time` arg.
    """
    def __init__(self, org_time):
        self.org_time = org_time
    def __repr__(self) -> str:
        return (
            "ReleaseRoleResult {"
            f"'org_time':{self.org_time}"
            "}"
        )

class ReadRoleResult:
    """Represents the result of a successful read-role operation in RegistryService.

    Args:
        org_time (int): The org time right after the operation has taken place.
        default_playtime_secs (int): A system-wide value the service grants the player if no playtime was requested.
        max_players (int): The role allows this many players to play at the same time.
        players_remaining_milliseconds (dict): All players (key) playing the role and their remaining playtime (value).

    Attributes:
        org_time (int): This stores `org_time` arg.
        default_playtime_secs (int): This stores `default_playtime_secs` arg.
        max_players (int): This stores `max_players` arg.
        players_remaining_milliseconds (dict): This stores `players_remaining_milliseconds` arg.
    """
    def __init__(self, org_time, default_playtime_secs, max_players, players_remaining_milliseconds):
        self.org_time = org_time
        self.default_playtime_secs = default_playtime_secs
        self.max_players = max_players
        self.players_remaining_milliseconds = players_remaining_milliseconds
    def __repr__(self) -> str:
        return (
            "ReadRoleResult {"
            f"'org_time':{self.org_time},"
            f"'default_playtime_secs':{self.default_playtime_secs},"
            f"'max_players':{self.max_players},"
            f"'player_remaining_milliseconds':{self.players_remaining_milliseconds}"
            "}"
        )

class WriteDataResult:
    """Represents the result of a successful write-data operation in RegistryService.

    Args:
        org_time (int): The org time right after the operation has taken place.
        is_new_data_item_created (bool): A boolean indicating whether a new data item was created during the operation.
        create_org_time (int): The org time right after the data item was created.
        update_org_time (int): The org time right after the data item was updated most recently.
        number_of_bytes_written (int): The size of the data item written.

    Attributes:
        org_time (int): This stores `org_time` arg.
        is_new_data_item_created (bool): This stores `is_new_data_item_created` arg.
        create_org_time (int): This stores `create_org_time` arg.
        update_org_time (int): This stores `update_org_time` arg.
        number_of_bytes_written (int): This stores `number_of_bytes_written` arg.
    """
    def __init__(self, org_time, is_new_data_item_created, create_org_time, update_org_time, number_of_bytes_written):
        self.org_time = org_time
        self.is_new_data_item_created = is_new_data_item_created
        self.create_org_time = create_org_time
        self.update_org_time = update_org_time
        self.number_of_bytes_written = number_of_bytes_written
    def __repr__(self) -> str:
        return (
            "WriteDataResult {"
            f"'org_time':{self.org_time},"
            f"'is_new_data_item_created':{self.is_new_data_item_created},"
            f"'create_org_time':{self.create_org_time},"
            f"'update_org_time':{self.update_org_time},"
            f"'number_of_bytes_written':{self.number_of_bytes_written}"
            "}"
        )

class DeleteDataResult:
    """Represents the result of a successful delete-data operation in RegistryService.

    Args:
        org_time (int): The org time right after the operation has taken place.

    Attributes:
        org_time (int): This stores `org_time` arg.
    """
    def __init__(self, org_time):
        self.org_time = org_time
    def __repr__(self) -> str:
        return (
            "DeleteDataResult {"
            f"'org_time':{self.org_time}"
            "}"
        )

class ReadDataResult:
    """Represents the result of a successful read-data operation in RegistryService.

    Args:
        org_time (int): The org time right after the operation has taken place.
        create_org_time (int): The org time right after the data item was created.
        update_org_time (int): The org time right after the data item was updated most recently.
        content_type (str): The Content-Type header that was saved along with the data in the last write.
        content (bytes): The data.

    Attributes:
        org_time (int): This stores `org_time` arg.
        create_org_time (int): This stores `create_org_time` arg.
        update_org_time (int): This stores `update_org_time` arg.
        content_type (str): This stores `content_type` arg.
        content (bytes): This stores `content` arg.
    """
    def __init__(self, org_time, create_org_time, update_org_time, content_type, content):
        self.org_time = org_time
        self.create_org_time = create_org_time
        self.update_org_time = update_org_time
        self.content_type = content_type
        self.content = content
    def __repr__(self) -> str:
        return (
            "ReadDataResult {"
            f"'org_time':{self.org_time},"
            f"'create_org_time':{self.create_org_time},"
            f"'update_org_time':{self.update_org_time},"
            f"'content_type':{self.content_type},"
            f"'content':{self.content}"
            "}"
        )

class ListStats:
    """The statistics of a directory.

    Args:
        role_count (int): The number of roles under the directory.
        data_item_count (int): The number of data items under the directory.

    Attributes:
        role_count (int): This stores `role_count` arg.
        data_item_count (int): This stores `data_item_count` arg.
    """
    def __init__(self, role_count, data_item_count):
        self.role_count = role_count
        self.data_item_count = data_item_count
    def __repr__(self) -> str:
        return (
            "ListStats {"
            f"'role_count':{self.role_count},"
            f"'data_item_count':{self.data_item_count}"
            "}"
        )
    
class ListItemsResult:
    """Represents the result of a successful list-items operation in RegistryService.

    Args:
        org_time (int): The org time right after the operation has taken place.
        files (list of str): The full-path roles and data items directly and indirectly under this directory.
        stats (ListStats): The statistics of the directory.

    Attributes:
        org_time (int): This stores `org_time` arg.
        files (list of str): This stores `files` arg.
        stats (ListStats): This stores `stats` arg.
    """
    def __init__(self, org_time, files, stats):
        self.org_time = org_time
        self.files = files
        self.stats = stats
    def __repr__(self) -> str:
        return (
            "ListItemsResult {"
            f"'org_time':{self.org_time},"
            f"'files':{self.files},"
            f"'stats':{self.stats}"
            "}"
        )

class RotateOrgKeyResult:
    """Represents the result of a successful rotate-org-key operation in RegistryService.

    Args:
        org_time (int): The org time right after the operation has taken place.
        org_key (str): The resulting org key.

    Attributes:
        org_time (int): This stores `org_time` arg.
        org_key (str): This stores `org_key` arg.
    """
    def __init__(self, org_time, org_key):
        self.org_time = org_time
        self.org_key = org_key
    def __repr__(self) -> str:
        return (
            "RotateOrgKeyResult {"
            f"'org_time':{self.org_time},"
            f"'org_key':'{self.org_key}'"
            "}"
        )

class OrgStats:
    """The statistics of an org.

    Args:
        total_role_count (int): The number of roles in the org.
        total_data_item_count (int): The number of data items in the org.
        total_data_size (int): The number of bytes of the data items in the org.

    Attributes:
        total_role_count (int): This stores `total_role_count` arg.
        total_data_item_count (int): This stores `total_data_item_count` arg.
        total_data_size (int): This stores `total_data_size` arg.
    """
    def __init__(self, total_role_count, total_data_item_count, total_data_size):
        self.total_role_count = total_role_count
        self.total_data_item_count = total_data_item_count
        self.total_data_size = total_data_size
    def __repr__(self) -> str:
        return (
            "OrgStats {"
            f"'total_role_count':{self.total_role_count},"
            f"'total_data_item_count':{self.total_data_item_count}"
            f"'total_data_size':{self.total_data_size}"
            "}"
        )
    
class DescribeOrgResult:
    """Represents the result of a successful describe-org operation in RegistryService.

    Args:
        org_name (str): The name of the org.
        org_key (str): The authorization key for operations in the org.
        org_time (int): The org time right after the operation has taken place.
        is_deleted (bool): The flag that tells whether the org has been deleted.
        stats (OrgStats): The statistics of the org.

    Attributes:
        org_name (str): This stores `org_name` arg.
        org_key (str): This stores `org_key` arg.
        org_time (int): This stores `org_time` arg.
        is_deleted (bool): This stores `is_deleted` arg.
        stats (OrgStats): This stores `stats` arg.
    """
    def __init__(self, org_name, org_key, org_time, is_deleted, stats):
        self.org_name = org_name
        self.org_key = org_key
        self.org_time = org_time
        self.is_deleted = is_deleted
        self.stats = stats
    def __repr__(self) -> str:
        return (
            "DescribeOrgResult {"
            f"'org_name':'{self.org_name}',"
            f"'org_key':'{self.org_key}',"
            f"'org_time':{self.org_time},"
            f"'is_deleted':{self.is_deleted},"
            f"'stats':{self.stats}"
            "}"
        )

class RegistryError(Exception):
    """Custom exception for errors that RegistryService returns.

    Args:
        http_code (int): HTTP status code in the response from RegistryService.
        message (str): The error message.
        extra_json (dict): The JSON providing extra information on the error. Refer to the HTTP API Document for the details.
 
    Attributes:
        http_code (int): This stores `http_code` arg.
        message (str): This stores `message` arg.
        extra_json (dict): This stores `extra_json` arg.
    """
    def __init__(self, http_code, message, extra_json=None):
        super().__init__(message)
        self.http_code = http_code
        self.message = message
        self.extra_json = extra_json
    def __repr__(self) -> str:
        return (
            "RegistryError {"
            f"'http_code':{self.http_code},"
            f"'message':'{self.message}',"
            f"'extra_json':{self.extra_json}"
            "}"
        )

class RegistryClient:
    """
    A client for interacting with the Registry Service.

    Args:
        instance (str): The name of the Registry Service instance, e.g., "useast2", or "beta.useast2".
        org_name (str): The org this client works with.
        org_key (str): The authorization credential for the org.

    Attributes:
        service_url (str): This URL is formed from the instance name.
        org_name (str): This stores `org_name` arg.
        org_key (str): This stores `org_key` arg.
    """
    def __init__(self, instance, org_name, org_key):
        self.service_url = f"https://{instance}.registry.dkplatform.io/svc/"
        self.org_name = org_name
        self.org_key = org_key

    GenericErrorMessages = {
        400: "Bad request",
        403: "Forbidden",
        404: "Not found",
        409: "Conflict",
        413: "Payload too large",
        500: "Internal server error",
        503: "Request collision",
        507: "Insufficient storage"
    }

    """Header names."""
    CONTENT_TYPE = "Content-Type"
    X_DK_ORG_TIME = "x-dk-org-time"
    X_DK_CREATE_ORG_TIME = "x-dk-create-org-time"
    X_DK_UPDATE_ORG_TIME = "x-dk-update-org-time"

    def _extract_json_from_response(self, response):
        """
        Extract JSON data from the response if the response content is in JSON format.

        Args:
            response (requests.models.Response): The response object from the HTTP request.

        Returns:
            dict or None: JSON data if the response is in JSON format, otherwise None.
        """
        if "application/json" in response.headers.get("content-type", ""):
            try:
                return response.json()
            except:
                return None

    def take_role(self, role_path, player_name, playtime_secs=None, max_players=None):
        """
        Refer to the HTTP API Document for the details.

        Args:
            role_path (str): The combined path and role name (without the ".role" suffix).
            player_name (str): The virtual identity of the client at the service.
            playtime_secs (int, optional): How many seconds the player wants the role. Default is 10 seconds.
            max_players (int, optional): Maximum number of players allowed to play the role. Default is 1.

        Returns:
            TakeRoleResult

        Raises:
            RegistryError
        """
        url = f"{self.service_url}/{self.org_name}/{role_path}.role"
        headers = {
            "Authorization": self.org_key,
            "Content-Type": "application/json"
        }
        
        client_unix_time_in_msecs = int(time.time() * 1000)  # Current local epoch time in milliseconds
        
        data = {
            "player_name": player_name,
            "client_unix_time_in_msecs": client_unix_time_in_msecs
        }
        if playtime_secs is not None:
            data["playtime_secs"] = playtime_secs
        if max_players is not None:
            data["max_players"] = max_players

        response = requests.put(url, headers=headers, json=data)

        if response.status_code == 200:
            json = self._extract_json_from_response(response)
            return TakeRoleResult(
                int(response.headers.get(self.X_DK_ORG_TIME)),
                False,
                json.get("client_expiration_time_in_msecs"))
        elif response.status_code == 201:
            json = self._extract_json_from_response(response)
            return TakeRoleResult(
                int(response.headers.get(self.X_DK_ORG_TIME)),
                True,
                json.get("client_expiration_time_in_msecs"))
        else:
            error_message = self.GenericErrorMessages.get(response.status_code, "Unknown error")
            extra_json = self._extract_json_from_response(response)
            raise RegistryError(response.status_code, error_message, extra_json)

    def release_role(self, role_path, player_name):
        """
        Refer to the HTTP API Document for the details.

        Args:
            role_path (str): The combined path and role name (without the ".role" suffix).
            player_name (str): The virtual identity of the client at the service.

        Returns:
            ReleaseRoleResult

        Raises:
            RegistryError
        """
        url = f"{self.service_url}/{self.org_name}/{role_path}.role"
        headers = {
            "Authorization": self.org_key,
            "Content-Type": "application/json"
        }

        data = {
            "player_name": player_name
        }

        response = requests.delete(url, headers=headers, json=data)

        if response.status_code == 200:
            return ReleaseRoleResult(int(response.headers.get(self.X_DK_ORG_TIME)))
        else:
            error_message = self.GenericErrorMessages.get(response.status_code, "Unknown error")
            extra_json = self._extract_json_from_response(response)
            raise RegistryError(response.status_code, error_message, extra_json)

    def read_role(self, role_path):
        """
        Refer to the HTTP API Document for the details.

        Args:
            role_path (str): The combined path and role name (without the ".role" suffix).

        Returns:
            ReadRoleResult

        Raises:
            RegistryError
        """
        url = f"{self.service_url}/{self.org_name}/{role_path}.role"
        headers = {
            "Authorization": self.org_key
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            json = self._extract_json_from_response(response)
            return ReadRoleResult(
                int(response.headers.get(self.X_DK_ORG_TIME)),
                json.get("default_playtime_secs"),
                json.get("max_players"),
                json.get("players_remaining_milliseconds"))
        else:
            error_message = self.GenericErrorMessages.get(response.status_code, "Unknown error")
            extra_json = self._extract_json_from_response(response)
            raise RegistryError(response.status_code, error_message, extra_json)

    def write_data(self, data_item_path, data, content_type="application/json", update_org_time=None):
        """
        Refer to the HTTP API Document for the details.

        Args:
            data_item_path (str): The combined path and data item name (without the ".data" suffix).
            data: The data to be stored.
            content_type (str, optional): The content type of the data. Default is "application/json".
            update_org_time (int, optional): A conditional write only happens if `update_org_time` matched.

        Returns:
            WriteDataResult

        Raises:
            RegistryError
        """
        url = f"{self.service_url}/{self.org_name}/{data_item_path}.data"
        headers = {
            "Authorization": self.org_key,
            "Content-Type": content_type,
        }

        if update_org_time is not None:
            headers["x-dk-update-org-time"] = str(update_org_time)

        response = requests.put(url, headers=headers, data=data)
        json = self._extract_json_from_response(response)

        if response.status_code == 200:
            return WriteDataResult(
                int(response.headers.get(self.X_DK_ORG_TIME)),
                False,
                json.get("create_org_time"),
                json.get("update_org_time"),
                json.get("number_of_bytes_written"))
        elif response.status_code == 201:
            return WriteDataResult(
                int(response.headers.get(self.X_DK_ORG_TIME)),
                True,
                json.get("create_org_time"),
                json.get("update_org_time"),
                json.get("number_of_bytes_written"))
        else:
            error_message = self.GenericErrorMessages.get(response.status_code, "Unknown error")
            raise RegistryError(response.status_code, error_message, json)

    def delete_data(self, data_item_path, update_org_time=None):
        """
        Refer to the HTTP API Document for the details.

        Args:
            data_item_path (str): The combined path and data item name (without the ".data" suffix).
            update_org_time (int, optional):  A conditional delete only happens if `update_org_time` matched.

        Returns:
            DeleteDataResult

        Raises:
            RegistryError
        """
        url = f"{self.service_url}/{self.org_name}/{data_item_path}.data"
        headers = {
            "Authorization": self.org_key
        }

        if update_org_time is not None:
            headers["x-dk-update-org-time"] = str(update_org_time)

        response = requests.delete(url, headers=headers)

        if response.status_code == 200:
            return DeleteDataResult(int(response.headers.get(self.X_DK_ORG_TIME)))
        else:
            error_message = self.GenericErrorMessages.get(response.status_code, "Unknown error")
            extra_json = self._extract_json_from_response(response)
            raise RegistryError(response.status_code, error_message, extra_json)

    def read_data(self, data_item_path):
        """
        Refer to the HTTP API Document for the details.

        Args:
            data_item_path (str): The combined path and data item name (without the ".data" suffix).

        Returns:
            ReadDataResult

        Raises:
            RegistryError
        """
        url = f"{self.service_url}/{self.org_name}/{data_item_path}.data"
        headers = {
            "Authorization": self.org_key
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return ReadDataResult(
                int(response.headers.get(self.X_DK_ORG_TIME)),
                int(response.headers.get(self.X_DK_CREATE_ORG_TIME)),
                int(response.headers.get(self.X_DK_UPDATE_ORG_TIME)),
                response.headers.get(self.CONTENT_TYPE),
                response.content)
        else:
            error_message = self.GenericErrorMessages.get(response.status_code, "Unknown error")
            extra_json = self._extract_json_from_response(response)
            raise RegistryError(response.status_code, error_message, extra_json)

    def list_items(self, directory_path):
        """
         Refer to the HTTP API Document for the details.

        Args:
            directory_path (str): The path of the directory, must end with a slash ("/").

        Returns:
            ReadDirectoryResult

        Raises:
            RegistryError
        """
        url = f"{self.service_url}/{self.org_name}/{directory_path}/"
        headers = {
            "Authorization": self.org_key
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            json = self._extract_json_from_response(response)
            return ListItemsResult(
                int(response.headers.get(self.X_DK_ORG_TIME)),
                json.get("files"),
                ListStats(
                    int(json.get("stats").get("role_count")),
                    int(json.get("stats").get("data_item_count"))))
        else:
            error_message = self.GenericErrorMessages.get(response.status_code, "Unknown error")
            extra_json = self._extract_json_from_response(response)
            raise RegistryError(response.status_code, error_message, extra_json)

    def rotate_org_key(self, new_org_key=None):
        """
        Refer to the HTTP API Document for the details.

        Args:
            new_org_key (str, optional): The new org key. If not provided, one will be generated.

        Returns:
            RotateOrgKeyResult

        Raises:
            RegistryError
        """
        url = f"{self.service_url}/{self.org_name}"
        headers = {
            "Authorization": self.org_key,
            "Content-Type": "application/json"
        }

        data = {}
        if new_org_key is not None:
            data["org_key"] = new_org_key

        response = requests.put(url, headers=headers, json=data)

        if response.status_code == 200:
            json = self._extract_json_from_response(response)
            self.org_key = json.get("org_key")
            return RotateOrgKeyResult(
                int(response.headers.get(self.X_DK_ORG_TIME)),
                self.org_key)
        else:
            error_message = self.GenericErrorMessages.get(response.status_code, "Unknown error")
            extra_json = self._extract_json_from_response(response)
            raise RegistryError(response.status_code, error_message, extra_json)

    def describe_org(self):
        """
        Refer to the HTTP API Document for the details.

        Returns:
            dict: A dictionary containing the org's information.

        Raises:
            RegistryError
        """
        url = f"{self.service_url}/{self.org_name}"
        headers = {
            "Authorization": self.org_key
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            json = self._extract_json_from_response(response)
            return DescribeOrgResult(
                json.get("org_name"),
                json.get("org_key"),
                int(response.headers.get(self.X_DK_ORG_TIME)),
                json.get("is_deleted") == True,
                OrgStats(
                    json.get("stats").get("total_role_count"),
                    json.get("stats").get("total_data_item_count"),
                    json.get("stats").get("total_data_size")))
        else:
            error_message = self.GenericErrorMessages.get(response.status_code, "Unknown error")
            extra_json = self._extract_json_from_response(response)
            raise RegistryError(response.status_code, error_message, extra_json)
