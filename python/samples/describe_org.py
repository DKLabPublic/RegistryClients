#   ---------------------------------------------------------------------------------
#   Copyright (c) 2024 DK Lab, LLC. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   ---------------------------------------------------------------------------------

# To run, need PYTHONPATH to point to registry_client.py.
# Example: % PYTHONPATH=/Users/dklab/RegistryClients/python/ python3 describe_org.py

from registry_client import RegistryClient, RegistryError

ORG_NAME = "sample_org"
ORG_KEY = "randomAlphaNumericString"
INSTANCE = "beta.useast2"

client = RegistryClient(INSTANCE, ORG_NAME, ORG_KEY)

try:
    org_info = client.describe_org()
    print(f"Organization Information: {org_info}")
except RegistryError as e:
    print(f"ErrorCode: {e.http_code}, ErrorMessage: {e.message}")
