#   ---------------------------------------------------------------------------------
#   Copyright (c) 2024 DK Lab, LLC. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   ---------------------------------------------------------------------------------

# To run, need PYTHONPATH to point to registry_client.py.
# Example: % PYTHONPATH=/Users/dklab/RegistryClients/python/ python3 role_operations.py

from registry_client import RegistryClient, RegistryError

ORG_NAME = "sample_org"
ORG_KEY = "randomAlphaNumericString"
INSTANCE = "beta.useast2"

client = RegistryClient(INSTANCE, ORG_NAME, ORG_KEY)

try:
    before = client.list_items("/nemmies/")
    print(f"Before:\n{before}\n")

    print("Ten cooks take the role '/nemmies/rolls/cooks':")
    for i in range(1,11):
        take = client.take_role("/nemmies/rolls/cooks", f"cook_{i}", 30, 10)
        print(f"cook_{i}: {take}")

    after = client.list_items("/nemmies/")
    print(f"\nAfter:\n{after}\n")
    role = client.read_role("/nemmies/rolls/cooks")
    print(f"Role:\n{role}\n")

    print("The 11th cook is unable to take the role:")
    try:
        print(client.take_role("/nemmies/rolls/cooks", "cook_11", max_players=10))
    except RegistryError as ex:
        print(f"ErrorCode: {ex.http_code}, ErrorMessage: {ex.message}\n")

    print("The ten cooks release the role:")
    for i in range(1,11):
        release = client.release_role("/nemmies/rolls/cooks", f"cook_{i}")
        print(f"cook_{i}: {release}")

    final = client.list_items("/nemmies/")
    print(f"\nFinal:\n{final}\n")

except RegistryError as e:
    print(f"ErrorCode: {e.http_code}, ErrorMessage: {e.message}")
