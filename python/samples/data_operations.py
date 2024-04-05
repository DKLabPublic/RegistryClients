#   ---------------------------------------------------------------------------------
#   Copyright (c) 2024 DK Lab, LLC. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   ---------------------------------------------------------------------------------

# To run, need PYTHONPATH to point to registry_client.py.
# Example: % PYTHONPATH=/Users/dklab/RegistryClients/python/ python3 data_operations.py

from registry_client import RegistryClient, RegistryError

ORG_NAME = "sample_org"
ORG_KEY = "randomAlphaNumericString"
INSTANCE = "beta.useast2"

client = RegistryClient(INSTANCE, ORG_NAME, ORG_KEY)

try:
    before = client.list_items("/nemmies/")
    print(f"Before:\n{before}\n")

    print("Write '/nemmies/rolls/recipe.data':")
    write = client.write_data("/nemmies/rolls/recipe", "The secret ingredient is passion!!", "text/plain")
    print(f"{write}\n")

    after = client.list_items("/nemmies/")
    print(f"After writing:\n{after}\n")

    print("Read '/nemmies/rolls/recipe.data':")
    read = client.read_data("/nemmies/rolls/recipe")
    print(f"{read}\n")

    print("Delete '/nemmies/rolls/recipe.data':")
    delete = client.delete_data("/nemmies/rolls/recipe")
    print(f"{delete}\n")

    final = client.list_items("/nemmies/")
    print(f"After deleting:\n{final}\n")

except RegistryError as e:
    print(f"ErrorCode: {e.http_code}, ErrorMessage: {e.message}")
