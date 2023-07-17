#!/usr/bin/env python3

from typing import Tuple


def get_failures(path: str) -> dict:
    # a dict where, keys = failed server addres, values = [t_fail_start_1, t_fail_stop_1, t_fail_start_2, ...]
    fails = {}

    with open(path, "r") as file:
        for line in file:
            data = line.strip().split(",")
            timestamp = int(data[0])
            addr = data[1]
            response_time = -1 if data[2] == "-" else int(data[2])

            # Let's use a negative timestamp to indicate that a timeout is ongoing

            # if timeout is detected
            if response_time == -1:
                # first timeout for a server
                if addr not in fails.keys():
                    fails[addr] = [-timestamp]
                # consecutive timeout
                elif fails[addr][-1] < 0:
                    pass
                # new timeout
                else:
                    fails[addr].append(-timestamp)
            # if timeout has ended
            elif addr in fails.keys() and fails[addr][-1] < 0:
                fails[addr][-1] = -fails[addr][-1]
                fails[addr].append(timestamp)

        # clean up
        for val in fails.values():
            if val[-1] < 0:  # ongoing timeout
                val[-1] = -val[-1]
                val.append(-1)  # TODO: clarify format with requirements giver

    return fails


def get_failures_after_n_times(path: str, n_times: int) -> dict:
    # a dict where, keys = failed server addres, values = [t_fail_start_1, t_fail_stop_1, t_fail_start_2, ...]
    fails = {}

    with open(path, "r") as file:
        for line in file:
            data = line.strip().split(",")
            timestamp = int(data[0])
            addr = data[1]
            response_time = -1 if data[2] == "-" else int(data[2])

            # Let's use a negative timestamp to indicate that a timeout is ongoing
            # Question 2: let's use element 0 of the list as a counter for consecutive timeouts

            # if timeout is detected
            if response_time == -1:
                # first timeout for a server
                if addr not in fails.keys():
                    fails[addr] = [1, -timestamp]
                # consecutive timeout
                elif fails[addr][-1] < 0:
                    fails[addr][0] += 1
                # new timeout
                else:
                    fails[addr].append(-timestamp)
                    fails[addr][0] = 1
            # if timeout has ended
            elif addr in fails.keys() and fails[addr][-1] < 0:
                # it is a failure if consecutive timeout >= N
                if fails[addr][0] >= n_times:
                    fails[addr][-1] = -fails[addr][-1]
                    fails[addr].append(timestamp)
                # else, reset counter and remove latest timestamp
                else:
                    fails[addr][0] = 0
                    fails[addr].pop()

        # clean up
        empty_keys = []
        for key, val in fails.items():
            val.pop(0)  # remove counter
            if len(val) and val[-1] < 0:  # ongoing timeout
                val[-1] = -val[-1]  # TODO: clarify format with requirements giver
                val.append(-1)
            if not val:  # empty list
                empty_keys.append(key)
        for empty_key in empty_keys:
            del fails[empty_key]

    return fails


def get_overloads_and_failures_after_n_times(path: str, n_times: int, m_overload: int, t_overload: float) -> Tuple[dict, dict]:
    # a dict where, keys = failed server addres, values = [t_fail_start_1, t_fail_stop_1, t_fail_start_2, ...]
    fails = {}
    # a dict where, keys = overloaded server address, values = {"periods", "window"}
    overloads = {}

    with open(path, "r") as file:
        for line in file:
            data = line.strip().split(",")
            timestamp = int(data[0])
            addr = data[1]
            response_time = -1 if data[2] == "-" else int(data[2])

            # Let's use a negative timestamp to indicate that a timeout is ongoing
            # Question 2: let's use element 0 of the list as a counter for consecutive timeouts

            # if timeout is detected
            if response_time == -1:
                # first timeout for a server
                if addr not in fails.keys():
                    fails[addr] = [1, -timestamp]
                # consecutive timeout
                elif fails[addr][-1] < 0:
                    fails[addr][0] += 1
                # new timeout
                else:
                    fails[addr].append(-timestamp)
                    fails[addr][0] = 1
            # if timeout has ended
            elif addr in fails.keys() and fails[addr][-1] < 0:
                # it is a failure if consecutive timeout >= N
                if fails[addr][0] >= n_times:
                    fails[addr][-1] = -fails[addr][-1]
                    fails[addr].append(timestamp)
                # else, reset counter and remove latest timestamp
                else:
                    fails[addr][0] = 0
                    fails[addr].pop()

            # Question 3: track overloads
            # add non-timeout ping response times to the floating window
            if addr not in overloads.keys():  # first time encountering address
                if response_time >= 0:
                    overloads[addr] = {"window": [[response_time, timestamp]]}
                else:
                    pass
            else:  # address is already encountered
                if response_time >= 0:
                    overloads[addr]["window"].append([response_time, timestamp])
                else:  # if ping timeout detected, reset the floating window
                    overloads[addr]["window"] = []

            # if length of floating window is sufficient, find average period and remove element 0 from the window
            if addr in overloads.keys() and len(overloads[addr]["window"]) >= m_overload:
                rtimes = [x[0] for x in overloads[addr]["window"]]
                tstamps = [x[1] for x in overloads[addr]["window"]]
                average = float(sum(rtimes)) / len(rtimes)
                if average >= t_overload:
                    if "periods" not in overloads[addr]:
                        overloads[addr] = {"periods": [tstamps[0], tstamps[-1]]}
                    else:
                        overloads[addr]["periods"].append(tstamps[0])
                        overloads[addr]["periods"].append(tstamps[-1])
                overloads[addr]["window"].pop(0)

        # clean up failures
        empty_fail_keys = []
        for key, val in fails.items():
            val.pop(0)  # remove counter
            if len(val) and val[-1] < 0:  # ongoing timeout
                val[-1] = -val[-1]  # TODO: clarify format with requirements giver
                val.append(-1)
            if not val:  # empty list
                empty_fail_keys.append(key)
        for empty_key in empty_fail_keys:
            del fails[empty_key]

        # clean up overloads
        empty_overload_keys = []
        for key, val in overloads.items():
            if "periods" in overloads[key].keys():
                overloads[key] = overloads[key]["periods"]
            else:
                empty_overload_keys.append(key)
        for empty_key in empty_overload_keys:
            del overloads[empty_key]

    return fails, overloads


def get_subnet_failures_after_n_times(path: str, n_times: int) -> Tuple[dict, list]:

    def get_subnet(ipv4, mask):
        n_decimals = int(mask/8)
        decimal_ctr = 0
        idx_end = 0
        for idx in range(len(ipv4)):
            if ipv4[idx] == ".":
                decimal_ctr += 1
                if decimal_ctr == n_decimals:
                    idx_end = idx
                    break
        subnet = ipv4[:idx_end]
        host = ipv4[idx_end+1:]
        return subnet, host

    # a dict where, keys = failed server addres, values = [t_fail_start_1, t_fail_stop_1, t_fail_start_2, ...]
    fails = {}
    subnets = {}

    with open(path, "r") as file:
        for line in file:
            data = line.strip().split(",")
            timestamp = int(data[0])
            addr = data[1]
            response_time = -1 if data[2] == "-" else int(data[2])

            # Let's use a negative timestamp to indicate that a timeout is ongoing
            # Question 2: let's use element 0 of the list as a counter for consecutive timeouts

            fail_flag = False  # Question 4 helper

            # if timeout is detected
            if response_time == -1:
                # first timeout for a server
                if addr not in fails.keys():
                    fails[addr] = [1, -timestamp]
                # consecutive timeout
                elif fails[addr][-1] < 0:
                    fails[addr][0] += 1
                # new timeout
                else:
                    fails[addr].append(-timestamp)
                    fails[addr][0] = 1
            # if timeout has ended
            elif addr in fails.keys() and fails[addr][-1] < 0:
                # it is a failure if consecutive timeout >= N
                if fails[addr][0] >= n_times:
                    fails[addr][-1] = -fails[addr][-1]
                    fails[addr].append(timestamp)
                    fail_flag = True
                # else, reset counter and remove latest timestamp
                else:
                    fails[addr][0] = 0
                    fails[addr].pop()

            # Question 4:
            ipv4, mask = data[1].split("/")
            mask = int(mask)
            subnet_addr, host_addr = get_subnet(ipv4, mask)
            if subnet_addr not in subnets.keys():  # new subnet encountered
                subnets[subnet_addr] = {host_addr: fail_flag}
            elif host_addr not in subnets[subnet_addr].keys():  # new host encountered
                subnets[subnet_addr][host_addr] = fail_flag
            elif not subnets[subnet_addr][host_addr] and fail_flag is True:
                subnets[subnet_addr][host_addr] = fail_flag

        # clean up host failures
        empty_keys = []
        for key, val in fails.items():
            val.pop(0)  # remove counter
            if len(val) and val[-1] < 0:  # ongoing timeout
                val[-1] = -val[-1]  # TODO: clarify format with requirements giver
                val.append(-1)
            if not val:  # empty list
                empty_keys.append(key)
        for empty_key in empty_keys:
            del fails[empty_key]

        # clean up subnet failures
        subnet_fails = []
        for subnet_addr in subnets.keys():
            if all(subnets[subnet_addr].values()):
                subnet_fails.append(subnet_addr)

    return fails, subnet_fails


if __name__=="__main__":
    file_path = "test_monitoring_log.txt"

    # Question 1
    failures = get_failures(file_path)

    print("Question 1:")
    for address, stamps in failures.items():
        print(f"Server {address} failed {len(stamps)/2} times:")
        for i in range(int(len(stamps)/2)):
            print(f"Failure {i+1} at timestamp {stamps[i*2]} with duration {stamps[i*2+1]-stamps[i*2]} seconds.")

    # Question 2
    n_times = 3
    failures = get_failures_after_n_times(file_path, n_times)

    print("\nQuestion 2:")
    for address, stamps in failures.items():
        print(f"Server {address} failed {len(stamps) / 2} times:")
        for i in range(int(len(stamps) / 2)):
            print(
                f"Failure {i+1} at timestamp {stamps[i * 2]} with duration {stamps[i * 2 + 1] - stamps[i * 2]} seconds.")

    # Question 3
    n_times = 3
    m_overload = 5
    t_overload = 20.
    failures, overloads = get_overloads_and_failures_after_n_times(file_path, n_times, m_overload, t_overload)

    print("\nQuestion 3:")
    for address, stamps in failures.items():
        print(f"Server {address} failed {len(stamps) / 2} times:")
        for i in range(int(len(stamps) / 2)):
            print(
                f"Failure {i + 1} at timestamp {stamps[i * 2]} with duration {stamps[i * 2 + 1] - stamps[i * 2]} seconds.")

    for address, stamps in overloads.items():
        print(f"Server {address} overloaded {len(stamps) / 2} times:")
        for i in range(int(len(stamps) / 2)):
            print(
                f"Overload {i+1} at timestamp {stamps[i * 2]} with duration {stamps[i * 2 + 1] - stamps[i * 2]} seconds.")

    # Question 4
    n_times = 3
    failures, failed_subnets = get_subnet_failures_after_n_times(file_path, n_times)

    print("\nQuestion 4:")
    for address, stamps in failures.items():
        print(f"Server {address} failed {len(stamps) / 2} times:")
        for i in range(int(len(stamps) / 2)):
            print(
                f"Failure {i + 1} at timestamp {stamps[i * 2]} with duration {stamps[i * 2 + 1] - stamps[i * 2]} seconds.")

    for subnet in failed_subnets:
        print(f"Subnet {subnet} has failed.")
