import socket
import json
import re
import csv
import time
from io import StringIO
from collections import defaultdict
import redis

class FreeSWITCHClient:
    def __init__(self, host, port, password):
        self.host = host
        self.port = port
        self.password = password
        self.connection = None

    def connect(self):
        if self.connection is None:
            print("Connecting to FreeSWITCH")
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connection.settimeout(30)  # Set timeout to 30 seconds
            self.connection.connect((self.host, self.port))
            auth_response = self.connection.recv(1024).decode()
            if "Content-Type: auth/request" in auth_response:
                self.connection.send(f"auth {self.password}\n\n".encode())
                response = self.connection.recv(1024).decode()
                if "Reply-Text: +OK" in response:
                    print("Connected to FreeSWITCH")
                else:
                    raise ConnectionError("Failed to authenticate with FreeSWITCH")
            else:
                raise ConnectionError("Failed to connect to FreeSWITCH")

    def execute(self, command):
        self.connect()
        self.connection.send(f"api {command}\n\n".encode())
        response = self._recv_response()
        return response

    def _recv_response(self):
        response = ""
        content_length = None
        while True:
            part = self.connection.recv(1024).decode()
            response += part
            if content_length is None:
                content_length_match = re.search(r"Content-Length: (\d+)", response)
                if content_length_match:
                    content_length = int(content_length_match.group(1))
            if content_length and len(response.split("\n\n", 1)[1]) >= content_length:
                break
        return response

    def execute_multiple_commands(self, commands):
        results = {}
        for command in commands:
            try:
                results[command] = self.execute(command)
            except Exception as e:
                results[command] = str(e)
        return results

    def parse_response(self, response):
        try:
            data = response.split('\n\n', 1)[1].strip()
        except IndexError:
            raise ValueError("Invalid response format")
        reader = csv.DictReader(StringIO(data), delimiter='|')
        return [row for row in reader]

    def data_to_json(self, data):
        return [json.dumps(item) for item in data]


def convert_seconds_to_hhmmss(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

def epoch_to_readable(epoch_time):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch_time))

def calculate_duration_since_last_status_change(last_status_change_epoch):
    current_time = time.time()
    duration_seconds = current_time - last_status_change_epoch
    return convert_seconds_to_hhmmss(duration_seconds)


def execute_and_process_commands(client, commands, redis_client):
    try:
        currenttimestamp_in_epoc = time.time() 
        responses = client.execute_multiple_commands(commands)

        # Parsing the responses
        agent_data = client.parse_response(responses["callcenter_config agent list"])
        tier_data = client.parse_response(responses["callcenter_config tier list"])
        queue_data = client.parse_response(responses["callcenter_config queue list"])

        # Convert to JSON and remove last entries
        agent_data.pop()
        tier_data.pop()
        queue_data.pop()

        agents = [json.loads(json.dumps(agent)) for agent in agent_data]
        tiers = [json.loads(json.dumps(tier)) for tier in tier_data]
        queues = [json.loads(json.dumps(queue)) for queue in queue_data]

        # List to hold the combined data for each queue
        combined_data_list = []

        # Dictionary to hold the combined data for each queue
        combined_data = defaultdict(lambda: {
            "Name": "",
            "Online": 0,
            "InaQueueCall": 0,
            "OnBreak": 0,
            "ACW": 0,
            "LoggedOut": 0,
            "Available": 0,
            "Idle": 0,
            "Waiting": 0,
            "Receiving": 0,
            "Handled": 0,
            "Abandoned": 0,
            "AHT": 0,
            "SL60": 0,
        })

        # Initialize agent count for AHT calculation
        aht_agent_count = 0

        # Process each queue
        for queue in queues:
            queue_name = queue["name"]

            # Check if there are tiers associated with this queue (agents assigned)
            if any(tier["queue"] == queue_name for tier in tiers):
                combined_data[queue_name]["Handled"] = int(queue["calls_answered"])
                combined_data[queue_name]["Abandoned"] = int(queue["calls_abandoned"])
                total_calls = combined_data[queue_name]["Handled"] + combined_data[queue_name]["Abandoned"]
                if total_calls > 0:
                    combined_data[queue_name]["SL60"] = round(combined_data[queue_name]["Handled"] * 100 / total_calls, 2)
                else:
                    combined_data[queue_name]["SL60"] = 0

                # Find agents for the queue from tier data
                queue_tiers = [tier for tier in tiers if tier["queue"] == queue_name]
                for tier in queue_tiers:
                    agent_name = tier["agent"]
                    agent = next((agent for agent in agents if agent["name"] == agent_name), None)
                    if agent:
                        status = agent["status"]
                        state = agent["state"]
                        combined_data[queue_name]["Name"] = queue_name
                        combined_data[queue_name]["Online"] += 1 if status != "Logged Out" else 0
                        combined_data[queue_name]["InaQueueCall"] += 1 if state == "In a queue call" else 0
                        combined_data[queue_name]["OnBreak"] += 1 if status == "On Break" else 0
                        combined_data[queue_name]["ACW"] += 1 if status == "Available (On Demand)" else 0
                        combined_data[queue_name]["LoggedOut"] += 1 if status == "Logged Out" else 0
                        combined_data[queue_name]["Available"] += 1 if status == "Available" and state != "In a queue call" and state != "Receiving" else 0
                        combined_data[queue_name]["Idle"] += 1 if state == "Idle" else 0
                        combined_data[queue_name]["Waiting"] += 1 if state == "Waiting" and status == "Available" else 0
                        combined_data[queue_name]["Receiving"] += 1 if state == "Receiving" else 0

                        # Calculate AHT
                        try:
                            last_bridge_start = int(agent["last_bridge_start"])
                            last_bridge_end = int(agent["last_bridge_end"])
                            combined_data[queue_name]["AHT"] += (last_bridge_end - last_bridge_start)
                            aht_agent_count += 1
                        except (ValueError, KeyError):
                            pass

                # Calculate the average AHT for the queue
                if aht_agent_count > 0:
                    average_aht = combined_data[queue_name]["AHT"] / aht_agent_count
                    combined_data[queue_name]["AHT"] = convert_seconds_to_hhmmss(average_aht)

                # Add combined data to the list
                combined_data_list.append(combined_data[queue_name])

                # Reset agent count for the next queue
                aht_agent_count = 0

        # Convert combined data to JSON
        combined_data_json = json.dumps(combined_data_list, indent=2)

        # Store the combined data in Redis
        print("Sending Realtime_Queue_Metrics_data to Redis")
        redis_client.set("Realtime_Queue_Metrics_data", combined_data_json)

        # Print the combined data for each queue
        print("Successfully inserted this Realtime_Queue_Metrics_data into Redis:", combined_data_json)

# Calculate agent metrics
        agent_metrics_list = []
        for agent in agents:
            last_bridge_start = int(agent.get("last_bridge_start", 0))
            last_bridge_end = int(agent.get("last_bridge_end", 0))
            last_offered_call = int(agent.get("last_offered_call", 0))
            print(currenttimestamp_in_epoc,last_bridge_start)
            last_status_change_epoch = int(agent.get("last_status_change", 0))
            agent_queues = [tier["queue"] for tier in tiers if tier["agent"] == agent["name"]]
            agent_levels_positions = [(tier["level"], tier["position"]) for tier in tiers if tier["agent"] == agent["name"]]
            
            # Only add agent metrics if the agent is assigned to at least one queue
            if agent_queues:
                agent_metrics = {
                    "Name": agent["name"],
                    "Type": agent["type"],
                    "Status": agent["status"],
                    # "Statusduration": calculate_duration_since_last_status_change(last_status_change_epoch) if agent["last_status_change"] > 0 else 0,
                    "Statusduration": calculate_duration_since_last_status_change(last_status_change_epoch) if agent["last_status_change"] != "0" else 0,

                    "Capacity": 1,  # Adjust if capacity information is available
                    "Availability": 1 if agent["status"] == "Available" else 0,  # Placeholder for availability metric
                    "contact_state": (
                        "In a queue call" if agent["state"] == "In a queue call" else
                        "Receiving" if agent["state"] == "Receiving" else
                        "Available (On Demand)" if agent["status"] == "Available (On Demand)" else
                        "-"
                    ),
                    # "Contact_state_Duration": (
                    #     calculate_duration_since_last_status_change(last_status_change_epoch)
                    #     if agent["state"] == "In a queue call" or agent["state"] == "Receiving" or agent["status"] == "Available (On Demand)"
                    #     else "-"
                    # ),
                    #"Contact_state_Duration":calculate_duration_since_last_status_change(last_status_change_epoch) if agent["status"]=="Available (On Demand)" else 0,
                    "Contact_state_Duration":(
                    convert_seconds_to_hhmmss(currenttimestamp_in_epoc - last_offered_call) if agent["state"]=="Receiving" else
                    convert_seconds_to_hhmmss(currenttimestamp_in_epoc - last_bridge_start) if agent["state"]=="In a queue call" else
                    calculate_duration_since_last_status_change(last_status_change_epoch) if agent["status"]=="Available (On Demand)" else 
                    "-"
                    ),

                    "Queue": ", ".join(agent_queues),
                    "ACW": calculate_duration_since_last_status_change(last_status_change_epoch) if agent["status"] == "Available (On Demand)" else "-", 
                    "Agent_non_response": agent["no_answer_count"],
                    "Calls_handled": agent["calls_answered"],
                    #"LCHT": convert_seconds_to_hhmmss(last_bridge_end - last_bridge_start)
                    "LCHT": convert_seconds_to_hhmmss(last_bridge_end - last_bridge_start) if agent["state"]!="In a queue call" else "-",
                    "Level": ", ".join([str(level) for level, position in agent_levels_positions]),
                    "Position": ", ".join([str(position) for level, position in agent_levels_positions])
                }
                agent_metrics_list.append(agent_metrics)

        # Store agent metrics in Redis
        agent_metrics_json = json.dumps(agent_metrics_list, indent=2)
        print("Sending Realtime_Agent_Metrics_data to Redis")
        redis_client.set('Realtime_Agent_Metrics_data', agent_metrics_json)
       
        print("Successfully inserted Realtime_Agent_Metrics_data into Redis:", agent_metrics_json)


    except Exception as e:
        print("Error executing commands:", e)


# Usage example with parsing and converting to JSON
host = "10.16.7.11"
port = 8021
password = "ClueCon"

client = FreeSWITCHClient(host, port, password)
commands = [
    "callcenter_config agent list",
    "callcenter_config tier list",
    "callcenter_config queue list"
]

# Connect to Redis
redis_client = redis.StrictRedis(host='10.16.7.11', port=6379, db=0)

#execute_and_process_commands(client, commands, redis_client)

# Main loop to execute commands every 5 minutes
while True:
    print("Executing commands...")
    execute_and_process_commands(client, commands, redis_client)
    print("Waiting for 5 minutes...")
    time.sleep(20)  # Sleep for 5 minutes












# import socket
# import json
# import re
# import csv
# import time
# from io import StringIO
# from collections import defaultdict
# import redis

# class FreeSWITCHClient:
#     def __init__(self, host, port, password):
#         self.host = host
#         self.port = port
#         self.password = password
#         self.connection = None

#     def connect(self):
#         if self.connection is None:
#             print("Connecting to FreeSWITCH")
#             self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             self.connection.settimeout(30)  # Set timeout to 30 seconds
#             self.connection.connect((self.host, self.port))
#             auth_response = self.connection.recv(1024).decode()
#             if "Content-Type: auth/request" in auth_response:
#                 self.connection.send(f"auth {self.password}\n\n".encode())
#                 response = self.connection.recv(1024).decode()
#                 if "Reply-Text: +OK" in response:
#                     print("Connected to FreeSWITCH")
#                 else:
#                     raise ConnectionError("Failed to authenticate with FreeSWITCH")
#             else:
#                 raise ConnectionError("Failed to connect to FreeSWITCH")

#     def execute(self, command):
#         self.connect()
#         self.connection.send(f"api {command}\n\n".encode())
#         response = self._recv_response()
#         return response

#     def _recv_response(self):
#         response = ""
#         content_length = None
#         while True:
#             part = self.connection.recv(1024).decode()
#             response += part
#             if content_length is None:
#                 content_length_match = re.search(r"Content-Length: (\d+)", response)
#                 if content_length_match:
#                     content_length = int(content_length_match.group(1))
#             if content_length and len(response.split("\n\n", 1)[1]) >= content_length:
#                 break
#         return response

#     def execute_multiple_commands(self, commands):
#         results = {}
#         for command in commands:
#             try:
#                 results[command] = self.execute(command)
#             except Exception as e:
#                 results[command] = str(e)
#         return results

#     def parse_response(self, response):
#         try:
#             data = response.split('\n\n', 1)[1].strip()
#         except IndexError:
#             raise ValueError("Invalid response format")
#         reader = csv.DictReader(StringIO(data), delimiter='|')
#         return [row for row in reader]

#     def data_to_json(self, data):
#         return [json.dumps(item) for item in data]


# def convert_seconds_to_hhmmss(seconds):
#     hours = seconds // 3600
#     minutes = (seconds % 3600) // 60
#     seconds = seconds % 60
#     return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

# def epoch_to_readable(epoch_time):
#     return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch_time))

# def calculate_duration_since_last_status_change(last_status_change_epoch):
#     current_time = time.time()
#     duration_seconds = current_time - last_status_change_epoch
#     return convert_seconds_to_hhmmss(duration_seconds)


# def execute_and_process_commands(client, commands, redis_client):
#     try:
#         responses = client.execute_multiple_commands(commands)

#         # Parsing the responses
#         agent_data = client.parse_response(responses["callcenter_config agent list"])
#         tier_data = client.parse_response(responses["callcenter_config tier list"])
#         queue_data = client.parse_response(responses["callcenter_config queue list"])

#         # Convert to JSON and remove last entries
#         agent_data.pop()
#         tier_data.pop()
#         queue_data.pop()

#         agents = [json.loads(json.dumps(agent)) for agent in agent_data]
#         tiers = [json.loads(json.dumps(tier)) for tier in tier_data]
#         queues = [json.loads(json.dumps(queue)) for queue in queue_data]

#         # List to hold the combined data for each queue
#         combined_data_list = []

#         # Dictionary to hold the combined data for each queue
#         combined_data = defaultdict(lambda: {
#             "Name": "",
#             "Online": 0,
#             "InaQueueCall": 0,
#             "OnBreak": 0,
#             "ACW": 0,
#             "LoggedOut": 0,
#             "Available": 0,
#             "Idle": 0,
#             "Waiting": 0,
#             "Receiving": 0,
#             "Handled": 0,
#             "Abandoned": 0,
#             "AHT": 0,
#             "SL60": 0,
#         })

#         # Initialize agent count for AHT calculation
#         aht_agent_count = 0

#         # Process each queue
#         for queue in queues:
#             queue_name = queue["name"]

#             # Check if there are tiers associated with this queue (agents assigned)
#             if any(tier["queue"] == queue_name for tier in tiers):
#                 combined_data[queue_name]["Handled"] = int(queue["calls_answered"])
#                 combined_data[queue_name]["Abandoned"] = int(queue["calls_abandoned"])
#                 total_calls = combined_data[queue_name]["Handled"] + combined_data[queue_name]["Abandoned"]
#                 if total_calls > 0:
#                     combined_data[queue_name]["SL60"] = round(combined_data[queue_name]["Handled"] * 100 / total_calls, 2)
#                 else:
#                     combined_data[queue_name]["SL60"] = 0

#                 # Find agents for the queue from tier data
#                 queue_tiers = [tier for tier in tiers if tier["queue"] == queue_name]
#                 for tier in queue_tiers:
#                     agent_name = tier["agent"]
#                     agent = next((agent for agent in agents if agent["name"] == agent_name), None)
#                     if agent:
#                         status = agent["status"]
#                         state = agent["state"]
#                         combined_data[queue_name]["Name"] = queue_name
#                         combined_data[queue_name]["Online"] += 1 if status != "Logged Out" else 0
#                         combined_data[queue_name]["InaQueueCall"] += 1 if state == "In a queue call" else 0
#                         combined_data[queue_name]["OnBreak"] += 1 if status == "On Break" else 0
#                         combined_data[queue_name]["ACW"] += 1 if status == "Available (On Demand)" else 0
#                         combined_data[queue_name]["LoggedOut"] += 1 if status == "Logged Out" else 0
#                         combined_data[queue_name]["Available"] += 1 if status == "Available" and state != "In a queue call" and state != "Receiving" else 0
#                         combined_data[queue_name]["Idle"] += 1 if state == "Idle" else 0
#                         combined_data[queue_name]["Waiting"] += 1 if state == "Waiting" and status == "Available" else 0
#                         combined_data[queue_name]["Receiving"] += 1 if state == "Receiving" else 0

#                         # Calculate AHT
#                         try:
#                             last_bridge_start = int(agent["last_bridge_start"])
#                             last_bridge_end = int(agent["last_bridge_end"])
#                             combined_data[queue_name]["AHT"] += (last_bridge_end - last_bridge_start)
#                             aht_agent_count += 1
#                         except (ValueError, KeyError):
#                             pass

#                 # Calculate the average AHT for the queue
#                 if aht_agent_count > 0:
#                     average_aht = combined_data[queue_name]["AHT"] / aht_agent_count
#                     combined_data[queue_name]["AHT"] = convert_seconds_to_hhmmss(average_aht)

#                 # Add combined data to the list
#                 combined_data_list.append(combined_data[queue_name])

#                 # Reset agent count for the next queue
#                 aht_agent_count = 0

#         # Convert combined data to JSON
#         combined_data_json = json.dumps(combined_data_list, indent=2)

#         # Store the combined data in Redis
#         print("Sending Realtime_Queue_Metrics_data to Redis")
#         redis_client.set("Realtime_Queue_Metrics_data", combined_data_json)

#         # Print the combined data for each queue
#         print("Successfully inserted this Realtime_Queue_Metrics_data into Redis:", combined_data_json)

# # Calculate agent metrics
#         agent_metrics_list = []
#         for agent in agents:
#             last_bridge_start = int(agent.get("last_bridge_start", 0))
#             last_bridge_end = int(agent.get("last_bridge_end", 0))
#             last_status_change_epoch = int(agent.get("last_status_change", 0))
#             agent_queues = [tier["queue"] for tier in tiers if tier["agent"] == agent["name"]]
            
#             # Only add agent metrics if the agent is assigned to at least one queue
#             if agent_queues:
#                 agent_metrics = {
#                     "Name": agent["name"],
#                     "Type": agent["type"],
#                     "Status": agent["status"],
#                     "Statusduration": calculate_duration_since_last_status_change(last_status_change_epoch) if agent["last_status_change"] != "0" else 0,
#                     "Capacity": 1,  # Adjust if capacity information is available
#                     "Availability": 1 if agent["status"] == "Available" else 0,  # Placeholder for availability metric
#                     "contact_state": (
#                         "In a queue call" if agent["state"] == "In a queue call" else
#                         "Receiving" if agent["state"] == "Receiving" else
#                         "Available (On Demand)" if agent["status"] == "Available (On Demand)" else
#                         "-"
#                     ),
#                     # "Contact_state_Duration": (
#                     #     calculate_duration_since_last_status_change(last_status_change_epoch)
#                     #     if agent["state"] == "In a queue call" or agent["state"] == "Receiving" or agent["status"] == "Available (On Demand)"
#                     #     else "-"
#                     # ),
#                     "Contact_state_Duration":calculate_duration_since_last_status_change(last_status_change_epoch) if agent["status"]=="Available (On Demand)" else 0,
#                     "Queue": ", ".join(agent_queues),
#                     "ACW": calculate_duration_since_last_status_change(last_status_change_epoch) if agent["status"] == "Available (On Demand)" else "-", 
#                     "Agent_non_response": agent["no_answer_count"],
#                     "Calls_handled": agent["calls_answered"],
#                     #"LCHT": convert_seconds_to_hhmmss(last_bridge_end - last_bridge_start)
#                     "LCHT": convert_seconds_to_hhmmss(last_bridge_end - last_bridge_start) if agent["state"]!="In a queue call" else "In Call"
#                 }
#                 agent_metrics_list.append(agent_metrics)

#         # Store agent metrics in Redis
#         agent_metrics_json = json.dumps(agent_metrics_list, indent=2)
#         print("Sending Realtime_Agent_Metrics_data to Redis")
#         redis_client.set('Realtime_Agent_Metrics_data', agent_metrics_json)
       
#         print("Successfully inserted Realtime_Agent_Metrics_data into Redis:", agent_metrics_json)


#     except Exception as e:
#         print("Error executing commands:", e)


# # Usage example with parsing and converting to JSON
# host = "10.16.7.11"
# port = 8021
# password = "ClueCon"

# client = FreeSWITCHClient(host, port, password)
# commands = [
#     "callcenter_config agent list",
#     "callcenter_config tier list",
#     "callcenter_config queue list"
# ]

# # Connect to Redis
# redis_client = redis.StrictRedis(host='10.16.7.11', port=6379, db=0)

# # execute_and_process_commands(client, commands, redis_client)

# # Main loop to execute commands every 5 minutes
# while True:
#     print("Executing commands...")
#     execute_and_process_commands(client, commands, redis_client)
#     print("Waiting for 5 minutes...")
#     time.sleep(10)  # Sleep for 5 minutes





