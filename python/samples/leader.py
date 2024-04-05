#   ---------------------------------------------------------------------------------
#   Copyright (c) 2024 DK Lab, LLC. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   ---------------------------------------------------------------------------------

# To run, need PYTHONPATH to point to registry_client.py.
# Example: % PYTHONPATH=/Users/dklab/RegistryClients/python/ python3 leader.py

from random import randint
import threading
import time

from registry_client import RegistryClient, RegistryError

ORG_NAME = "sample_org"
ORG_KEY = "randomAlphaNumericString"
INSTANCE = "beta.useast2"

RUN_TIME_SECS = 30

def elect_leader(name):
    client = RegistryClient(INSTANCE, ORG_NAME, ORG_KEY)
    is_leader = False
    expiration = 0
    t_end = time.time() + 30
    while time.time() < t_end:
        try:
            # Usually `playtime_secs` is greater than 3. Use a small number here to
            # demonstrate the leader election more effectively.
            expiration = client.take_role(
                "/nemmies/rolls/chef", name, playtime_secs=3, max_players=1,
                ).client_expiration_time_in_msecs
            if not is_leader:
                is_leader = True
                timestamp = int(time.time() * 1000)
                print(f"{name} becomes the leader at UNIX time {timestamp}")
        except RegistryError as e:
            if e.http_code == 409 and is_leader:
                is_leader = False
                print(f"{name}'s leadership ended at UNIX time {expiration}")
        # Doing some leader work as long as `expiration` timestamp isn't passed.
        time.sleep(randint(2,5))

if __name__ == "__main__":
    print("Two chefs compete to be the leader. Note that there's only one leader at any time.\n"
          "One leadership must end before a new leader is elected--as the timestamps show.\n")
    print(f"The demo runs for {RUN_TIME_SECS} seconds.\n")
    t = threading.Thread(target=elect_leader, args=("chef_1",))
    t.start()
    elect_leader("chef_2")
