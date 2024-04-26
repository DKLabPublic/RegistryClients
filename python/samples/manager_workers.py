#   ---------------------------------------------------------------------------------
#   Copyright (c) 2024 DK Lab, LLC. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   ---------------------------------------------------------------------------------

"""
This code demonstrates the manager-worker pattern.  Our hypothetical problem is iterating
over the natural numbers.  The manager's job is to assign ranges of numbers to the workers
and collect their results.  Each worker, upon receiving a range from the manager, iterates
over the numbers and returns the count.

In this demonstration:
- The problem state is small, and it is stored at the Registry Service.
- Each machine is modeled as a thread.  A thread may terminate early to model a machine failure.
- For the manager-worker pattern, we use a manager role that allows only one player, and a
  worker role that allows a configurable number of players.  A machine in the manager role
  is the manager, and a machine in the worker role is a worker.
- For fault tolerance, one may run a pool of machines for the manager and a separate one for
  the workers.  It is arguably more economical, however, to run a single pool for both the
  manger and the workers.
  . A machine can play either the manager or a worker, with priority given to the manager role,
    because there is only one manager and its missing stalls the system.
  . When a machine boots up, it tries to take the manager role first, then upon un-success
    tries to take the worker role.
  . Our pool includes two extra machines.  When a machine fails to take neither the manager
    nor the worker roles, it stays idle and retries to take the roles every now and then
    to replace a failed manger or worker.
- Each machine has a UDP port, which is used for the manager to send assignment to the workers,
  and for the workers to send the results to the manager.  Manager and workers use their port
  number as name, so the manager can learn about the workers (and hence their ports) by reading
  the worker role.  Similarly, workers learn about the manager by reading the manager role.
"""

# To run, need PYTHONPATH to point to leader_election.py.
# Example: % PYTHONPATH=/Users/dklab/RegistryClients/python/ python3 manager_workers.py

import json
import random
import socket
import threading
import time
import uuid
from contextlib import closing

import registry_client
from leader_election import Role

# Constants for using the Registry Service instance.  Should not need to change.
ORG_NAME = "sample_org"
ORG_KEY  = "randomAlphaNumericString"
SVC_INSTANCE = "beta.useast2"

# Constants for the demonstration.  Adjust them to see different results.
NUM_WORKERS  = 5
ASSIGNMENT_SIZE = 1000     # Each assignment has this many numbers.
RESULT_WAIT_TIME_SECS = 1  # The manager waits for this many seconds for a worker to send back the result.
RUN_TIME_SECS = 20         # The sample will run this many seconds.

class ProblemState:
    """
    Represents the problem being solved by the manager-worker demonstration.

    Args:
        saving_path (str):  Where the problem state is stored.

    Attributes:
        next (int): The next number to assign to workers.
        pending (dict): A dictionary mapping assignment ranges to their last assigned time.
        busy_workers (dict): A dictionary of workers currently processing assignments.
        finished (int): The total count of processed numbers.
    """
    def __init__(self, saving_path):
        self.next = 0
        self.pending = {}
        self.busy_workers = {}
        self.finished = 0
        self._saving_path = saving_path
        self._client = registry_client.RegistryClient(SVC_INSTANCE, ORG_NAME, ORG_KEY)
    def durable_json(self):
        # Use double quotes around the fields, because it is what json.loads() needs.
        return (
            '{'
            f'"next":{json.dumps(self.next)},'
            f'"pending":{json.dumps(self.pending)},'
            f'"busy_workers":{json.dumps(self.busy_workers)},'
            f'"finished":{json.dumps(self.finished)}'
            '}'
        )
    def __repr__(self) -> str:
        return f"ProblemState {self.durable_json()}"

    def save(self):
        """
        Saves the object to Registry Service at the `_saving_path`.

        Returns:
            bool:  True on success, False otherwise.
        """
        try:
            self._client.write_data(
                self._saving_path,
                self.durable_json(),
                "application/json")
            return True
        except registry_client.RegistryError as e:
            print(f"ProblemState.store: ErrorCode: {e.http_code}, ErrorMessage: {e.message}")
            return False

    def load(self):
        """
        Load the object at the `_saving_path` from Registry Service.

        Returns:
            bool:  True on success, False otherwise.
        """
        try:
            read = self._client.read_data(self._saving_path)
            assert read.content_type == "application/json"
            obj = json.loads(read.content.decode())
            self.next = obj['next']
            self.pending = obj['pending']
            self.busy_workers = obj['busy_workers']
            self.finished = obj['finished']
        except registry_client.RegistryError as e:
            print(f"ProblemState.load: ErrorCode: {e.http_code}, ErrorMessage: {e.message}")
        return self

class Manager:
    """
    Manages assignments and aggregates results in a manager-worker framework.

    Args:
        name (str): The name of the manager machine.
        problem (Problem): The problem state object.
        role (leader_election.Role): The `Role` object that must be held to do the manager work.
        workers (leader_election.Role): The `Role` object to read about the current workers.
        sock (socket.socket): The UDP socket for communication with workers.
    """
    def __init__(self, name, problem_state_path, role, workers, sock):
        self._name = name
        self._role = role
        self._workers = workers
        self._sock = sock
        self._resp_wait_time = RESULT_WAIT_TIME_SECS
        self._assignment_range = ASSIGNMENT_SIZE
        self._problem = ProblemState(problem_state_path)

    def _recv_results(self):
        """
        Receive as many results as possible without blocking, record them, and delete the tracking of the corresponding assignments.
        """
        try:
            msg = self._sock.recv(1024)
            while len(msg) > 0:
                (lower, higher, count, worker) = msg.decode().split(",")
                assert worker in self._problem.busy_workers
                del self._problem.busy_workers[worker]
                # The assignment may have been sent to more than one workers (for fault tolerance).
                # Record it only once, when the assignment is in pending.
                assignment = f"{lower},{higher}"
                if assignment in self._problem.pending:
                    self._problem.finished += int(count)
                    del self._problem.pending[assignment]
                # Recv next msg.
                msg = self._sock.recv(1024)
        except socket.error as se:
            # Errno 35, Resource temporarily unavailable, is expected when a non-blocking socket has no data to recv.
            if se.errno != 35:
                print(f"Manager {self._name} throws unexpected socket.error while recv: {se}")
        except Exception as e:
            print(f"Manager {self._name} throws unexpected exception while recv: {e}")

    def _get_assignment(self):
        """
        Get an assignment.  Prioritize the pending ones that are too old.

        Returns:
            tuple:  Represents an assignment range.
        """
        for (assignment, timestamp) in self._problem.pending.items():
            if timestamp + self._resp_wait_time < time.time():
                r = assignment.split(",")
                return (int(r[0]), int(r[1]))
        return (self._problem.next, self._problem.next + self._assignment_range)

    def _send_assignment(self, assignment, worker):
        """
        Send an assignment to a worker.

        Args:
            assignment (tuple):  A tuple representing the assignment range.
            worker (str):  The name of the worker to whom the assignment is sent.
        """
        message = ",".join(map(str, (assignment[0], assignment[1])))
        worker_port = int(worker)
        self._sock.sendto(message.encode(), ("localhost", worker_port))
        if assignment[1] > self._problem.next:
            assert assignment[0] == self._problem.next  # Just to make sure we don't skip over any number.
            self._problem.next = assignment[1]
        self._problem.pending[message] = time.time()
        self._problem.busy_workers[worker] = message

    def do_work(self, t_end):
        """
        Perform the manager work.  In this demonstration, it is assigning ranges of numbers to the workers
        and aggregates the counts they send back.

        Returns
            int:  The number of assignments that have been assigned to the workers.
        """
        # Load the problem state from Registry Service.
        self._problem.load()
        self._sock.setblocking(False)
        assigned = 0
        # Only perform the manager work when in the manager role.  And to make sure that the machine stays in
        # the manager role while performing the manager work, check that the machine is in the manager role in
        # the next 500 ms, assuming each iteration of the manager work takes less than 500 ms.
        while t_end - time.time() >= 0.5 and self._role.is_holding(0.5):
            self._recv_results()
            free_workers = self._workers.active_players() - self._problem.busy_workers.keys()
            for worker in free_workers:
                assignment = self._get_assignment()
                self._send_assignment(assignment, worker)
                assigned += 1
            # One can select the saving frequency to balance between the duplicated work
            # when the manager fails and the overhead of saving in the normal case.  What
            # important is the integrity of the state.  That means any task that has been
            # assigned (sent to workers) must be either tracked as pending or have its
            # result recorded.
            self._problem.save()
        return assigned

class Worker:
    """
    Represents a worker in the manager-worker framework.

    Args:
        name (str):  The name of this worker.  In this example, it is the port that the worker receives msgs.
        role (leader_election.Role):  The `Role` that must be held to perform worker's work.
        manager (leader_election.Role):  The `Role` to look up who the manager is.
        sock (socket.Socket): The UDP socket for sending/receiving messages to/from the manager.
    """
    def __init__(self, name, role, manager, sock):
        self._name = name
        self._role = role
        self._manager = manager
        self._sock = sock
        
    def do_work(self, t_end):
        # Each worker calls this function to receive assignments from the manager, do the work,
        # and respond to the manager.

        solved = 0
        # Only perform the worker work while in the worker role.
        # Be sure to check that the machine is holding the role long enough to perform the work.
        while t_end - time.time() >= 0.5 and self._role.is_holding(0.5):
            # Do not spend all the time waiting for an assignment.  Reserve enough time to solve
            # the assignment and send result to the manager.
            self._sock.settimeout(0.4)
            try:
                msg = self._sock.recv(1024)
                if len(msg) > 0:
                    pair = msg.decode().split(",")
                    lower, higher = int(pair[0]), int(pair[1])

                    # Do the worker's work of iterating through the numbers.
                    count = 0
                    for i in range(lower, higher):
                        count += 1

                    players = self._manager.active_players()
                    if len(players) == 1:
                        manager_port = int(next(iter(players)))
                        message = ",".join(map(str, (lower, higher, count, self._name)))
                        self._sock.sendto(message.encode(), ("localhost", manager_port))
                        solved += 1
            except socket.timeout:
                # The machine may not be a worker anymore, and so it stops waiting for an assignment.
                pass
        return solved

class Machine:
    def __init__(self, run_id, name, sock, termination_time):
        """
        Initializes a Machine instance.

        Args:
            name (str):  The name of the machine.
            socket (socket.socket):  A UDP socket to communicate with other machines.
            problem (Problem):  A Problem for the worker-manager system to solve.
            termination_time (float):  The machine will stop when time.time() > termination_time.
        """
        self._name = name
        self._sock = sock
        self._termination_time = termination_time

        manager_role_path = f"/manager_workers/{run_id}/manager"
        worker_role_path  = f"/manager_workers/{run_id}/workers"

        self.problem_state_path = f"/manager_workers/{run_id}/problem"

        self._manager_role = Role(name, SVC_INSTANCE, ORG_NAME, ORG_KEY, manager_role_path, 1, 10)
        self._worker_role  = Role(name, SVC_INSTANCE, ORG_NAME, ORG_KEY, worker_role_path, NUM_WORKERS, 10)

    def run(self):
        """
        Runs the machine.
        """

        # The machine is manufactured to run until `_termination_time`, but it may crash prematurely.
        t_end = random.uniform(time.time(), self._termination_time)

        print(f"Machine {self._name} starts.")
        machine_status = "STARTED"
        while time.time() < t_end:
            # Prioritize becoming the manager, because the system won't make any progress if there's no manager .
            if self._manager_role.take():
                if machine_status != "MANAGER":
                    print(f"Machine {self._name}: {machine_status} -> MANAGER.")
                else:
                    print(f"Machine {self._name} resumes being MANAGER.")
                print(f"Current number of managers: {len(self._manager_role.active_players())}")
                machine_status = "MANAGER"
                manager = Manager(self._name, self.problem_state_path, self._manager_role, self._worker_role, sock=self._sock)
                assigned = manager.do_work(t_end)
                print(f"Manager {self._name} has assigned {assigned} assignments." )
            elif self._worker_role.take():
                if machine_status != "WORKER":
                    print(f"Machine {self._name}: {machine_status} -> WORKER.")
                else:
                    print(f"Machine {self._name} resumes being WORKER.")
                print(f"Current number of workers: {len(self._worker_role.active_players())}")
                machine_status = "WORKER"
                worker = Worker(self._name, self._worker_role, self._manager_role, self._sock)
                solved = worker.do_work(t_end)
                print(f"Worker {self._name} has solved {solved} assignments.")
            else:
                # Idle backup
                if machine_status != "IDLE":
                    print(f"Machine {self._name}: {machine_status} -> IDLE.")
                machine_status = "IDLE"
                time.sleep(1)
        # To model a crash, do not clean up resources, such as not stepping down from the role.
        # The role will expire and other will be able to take it.
        print(f"Machine {self._name} crashed.")


class MachineFactory:
    """Factory class for creating Machine instances."""

    # This lock is to ensure each socket is created and bound in isolation.
    SOCK_LOCK = threading.RLock()

    def _create_udp_socket():
        """
        Creates a UDP socket and binds it to a random port.

        Returns:
            socket.socket: The created UDP socket.
        """
        with MachineFactory.SOCK_LOCK:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(('', 0))
            return sock

    def create_machine(run_id, termination_time):
        """
        Create a new Machine instance.
        """
        sock = MachineFactory._create_udp_socket()
        name = f"{sock.getsockname()[1]}"
        return Machine(run_id, name, sock, termination_time)

def babysit(run_id, t_end):
    """
    This monitors a machine and, if it crashes, replaces with a new one.
    """
    while time.time() < t_end:
        machine = MachineFactory.create_machine(run_id, t_end)
        machine.run()


if __name__ == "__main__":
    """
    The main thread models an operator, who looks after a fleet of machines and replaces the failed ones.
    """

    run_id = uuid.uuid1().__str__().replace("-", "_")
    t_end = time.time() + RUN_TIME_SECS

    # We will run NUM_WORKERS machines for workers, 1 machine for manager, and 2 redundant machines for fault tolerance.
    num_machines = NUM_WORKERS + 1 + 2
    sitters = []
    for i in range (num_machines):
        t = threading.Thread(target=babysit, args=(run_id, t_end))
        t.start()
        sitters.append(t)
    for t in sitters:
        t.join()

    # Report the run's results to console.
    print(f"\nRun {run_id} Summary:")

    problem = ProblemState(f"/manager_workers/{run_id}/problem").load()
    
    print(f"- Iterated though: {problem.finished}")
    print(f"- Number of pending assignments (each is {ASSIGNMENT_SIZE} numbers): {len(problem.pending)}")
    print(f"- Next number to iterate: {problem.next}")