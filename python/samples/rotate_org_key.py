#   ---------------------------------------------------------------------------------
#   Copyright (c) 2024 DK Lab, LLC. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   ---------------------------------------------------------------------------------

# To run, need PYTHONPATH to point to registry_client.py.
# Example: % PYTHONPATH=/Users/dklab/RegistryClients/python/ python3 rotate_org_key.py

from registry_client import RegistryClient, RegistryError

ORG_NAME = "sample_org"
ORG_KEY = "randomAlphaNumericString"
INSTANCE = "beta.useast2"

client = RegistryClient(INSTANCE, ORG_NAME, ORG_KEY)

try:
    original_key = client.describe_org().org_key
    print(f"Current org_key: '{original_key}'\n")

    print("Rotating to a randomly generated key:")
    print(client.rotate_org_key())

    new_key = client.describe_org().org_key
    print(f"Current org_key: '{new_key}'\n")

    print("Restoring the original key:")
    print(client.rotate_org_key(original_key))

    final_key = client.describe_org().org_key
    print(f"Current org_key: '{final_key}'")
except RegistryError as e:
    print(f"ErrorCode: {e.http_code}, ErrorMessage: {e.message}")
