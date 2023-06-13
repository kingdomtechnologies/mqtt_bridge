#!/usr/bin/env python3

from threading import Timer
import time
import rospy
import json
from kingdom_message_definitions.msg import FlexbeStates
from flexbe_msgs.msg import BehaviorLog
from enum import Enum, auto
import yaml
import rospkg
import kingdom_definitions as kdef
from std_msgs.msg import String, Bool
from datetime import datetime


class FSMState(Enum):
    NotStarted = 0
    Running = 1
    Failed = 2
    Preempted = 3


class fsmStatesMonitorToMQTTStatus(Enum):
    INITIALISING = "unknown_initilising"
    READY = "unknown_ready"
    MQTT_DISCONNECTED = "unknown_offline"
    MQTT_CONNECTED = "unknown_online"


class fsmStatesMonitorToMQTT():
    def __init__(self) -> None:
        self.fsm_status = FSMState.NotStarted
        self.node_name = "states_monitor_mqtt"
        # self.node_name = "kigndom_behaviour_process"
        self.status = fsmStatesMonitorToMQTTStatus.INITIALISING
        rospy.init_node(self.node_name, anonymous=True)

        rospy.loginfo("Starting states monitor MQTT")

        self.rp = rospkg.RosPack()
        self.package_path = self.rp.get_path('kingdom_fsm_robot_flexbe_states')

        # Load fsm states groups definition
        self.fsm_states_groups = self.load_fsm_states_groups()
        if self.fsm_states_groups is None:
            raise Exception("fsm_states_group.yaml failed to be loaded")

        # Instantiate publishers
        self.behaviour_pub = rospy.Publisher('/flexbe/state_logger/processed', FlexbeStates, queue_size=10, latch=True)
        self.fsm_state_pub = rospy.Publisher('/FSM/state', String, queue_size=10, latch=True)

        # Instantiate the mqtt connection monitor.
        # This topic is a latch topic, so mqtt_connection_status_callback will called right after the subscruber be set
        self.mqtt_conn_status = False
        rospy.Subscriber(kdef.MQTT_BRIDGE_CONNECTION_STATUS_TOPIC_NAME, Bool, self.mqtt_connection_status_callback)

        # Wait for mqtt bridge be connected in order to publishes the states msgs right after being connected to mqtt broker
        while not self.mqtt_conn_status:
            self.printd("Waiting for mqtt_bridge establish connection to mqtt broker")
            time.sleep(2.0)

        # Start monitoring fsm topics
        rospy.Subscriber("/flexbe/state_logger", String, self.fsm_state_logger_callback)
        rospy.Subscriber("/flexbe/log", BehaviorLog, self.fsm_status_callback)

        rospy.spin()

    def fsm_state_logger_callback(self, msg):
        ''' Process fsm messages on /flexbe/state_logger nad publish processed states'''
        self.printd(f"Received raw: {msg.data}", func="fsm_state_logger_callback")

        # parse fsm msg
        processed_data = msg.data.replace("'", "\"").replace("- ", "").replace("- ", "")

        data = json.loads(processed_data)

        self.printd(f"Received processed[{type(data)}]: {msg.data}", func="fsm_state_logger_callback")

        # Publish the full fsm state data
        self.publish_fsm_state(state_name=data["name"],
                               state_time=data['time'],
                               state=data["state"],
                               event=data["event"],
                               path=data["path"],
                               duration=data["duration"],
                               logger=data["logger"],
                               loglevel=data["loglevel"])

    def fsm_status_callback(self, msg):
        ''' Process fsm messages on /flexbe/log topic and publish processed states'''
        flag = String()

        # Process events when fsm starts a behaviour
        if "Starting new behavior" in msg.text:
            self.printd("FSM behavior started", func="fsm_status_callback")
            self.fsm_status = FSMState.Running
            flag.data = "Running"

        # Process events when fsm finishes a behaviour
        elif "Behavior execution finished with result" in msg.text:
            self.printd(f"FSM finished", func="fsm_status_callback")
            stat = msg.text.split(" ")[-1][:-1]  # to avoid the . in the end

            fsm_state_name = "fsm_finished"
            flag.data = "finished"

            if stat == "failed":
                fsm_state_name = "fsm_failed"
                self.fsm_status = FSMState.Failed
                flag.data = "Failed"
            
            if stat == "preempted":
                fsm_state_name = "fsm_preempted"
                self.fsm_status = FSMState.Preempted
                flag.data = "Preempted"

            self.publish_fsm_state(state_name=fsm_state_name, state="LogState")

        # Publish the current predefined status of FSM 
        if len(flag.data) > 0:
            self.fsm_state_pub.publish(flag)

    def publish_fsm_state(self, state_name, state_time=None, state="", event="enter", path="NOPATH", duration=0.001, logger="flexbe.events", loglevel="INFO"):
        ''' Publish the given fsm state to the process fsm state topic.
            It will set the group for the given state and add a UTC timestamp in case it is not given '''
        state_msg = FlexbeStates()
        state_msg.name = state_name

        # Enforce UTC timestamp if not given
        state_msg.time = datetime.utcnow().timestamp() if state_time is None else state_time

        # Find the group for the given state name
        state_msg.state = state
        state_msg.state_group = self.fsm_states_groups[state_name] if self.fsm_states_groups.get(
            state_name) else kdef.FSM_STATE_GROUP_NOT_DEFINED
        
        # Only the event "enter" is being considered on cloud now, so the default event is set as "enter"
        state_msg.event = event

        state_msg.path = path
        state_msg.duration = duration
        state_msg.logger = logger
        state_msg.loglevel = loglevel

        self.behaviour_pub.publish(state_msg)
        self.printd(f"State {state_name} - group {state_msg.state} - has been published. State_msg: {state_msg}")

    def mqtt_connection_status_callback(self, req):
        '''Monitor the mqtt connection topic and publish predefined fsm states for connected and disconnected status'''
        self.mqtt_conn_status = True if req.data else False
        if self.mqtt_conn_status:
            self.status = fsmStatesMonitorToMQTTStatus.MQTT_CONNECTED

        else:
            self.status = fsmStatesMonitorToMQTTStatus.MQTT_DISCONNECTED

        self.publish_fsm_state(self.status.value)

    def load_fsm_states_groups(self):
        ''' Loads the yaml file with fsm states groups definition and return it as a dict of {state:group}
            the fsm_states_groups.yaml has to be under config folder'''
        fsm_states_groups = None
        try:
            filepath = f"{self.package_path}/config/fsm_states_groups.yaml"
            self.printd(f"Opening configure file at {filepath}", func="load_fsm_states_groups")
            with open(filepath, "r") as f:
                try:
                    fsm_states_groups = yaml.safe_load(f).get("fsm_states_groups")
                    self.printd(f"fsm_states_groups: {fsm_states_groups}")
                    # Reformat the yaml loaded
                    fsm_states_groups_aux = {}
                    for group in fsm_states_groups:
                        for state in fsm_states_groups[group]:
                            fsm_states_groups_aux[state] = group
                    fsm_states_groups = fsm_states_groups_aux
                    self.printd(f"Reformatted fsm_states_groups: {fsm_states_groups}")
                except yaml.YAMLError as exc:
                    self.printd(f"Error raised while loading yaml [YAMLError]: {exc}")
                except Exception as exc:
                    self.printd(f"Error raised while loading yaml [Exception]: {exc}")
        except:
            self.printd(f"could not load fsm states groups configs")

        return fsm_states_groups

    def printd(self, msg, func=None):
        func = f"::{func}" if func else ""
        # print(f"[{ref}{func}] {msg}")
        rospy.loginfo(f"[{self.node_name}{func}] {msg}")

if __name__ == "__main__":
    states_monitor = fsmStatesMonitorToMQTT()

# # s = '{"time": 1665163918.576645, "name": "battery_low_preempt", "state": "LogState", "path": "/cutting_task_fsm/battery_low_preempt", "event": "stop", "duration": 0.0001575946807861328, "logger": "flexbe.events", "loglevel": "INFO"}'
# # #s = '{"success": "true", "status": 200, "message": "Hello"}'
# # d = json.loads(s)
# # #print(d["success"], d["status"])
# # print(d["time"])

# # fsm_state = FSMState.NotStarted


# def status_callback(msg):
#     global fsm_state, behaviour_pub, fsm_state_pub
#     if "Starting new behavior" in msg.text:
#         rospy.loginfo("FSM behavior started")
#         fsm_state = FSMState.Running
#         flag = String()
#         flag.data = "Running"
#         fsm_state_pub.publish(flag)
#     elif "Behavior execution finished with result" in msg.text:
#         rospy.loginfo(f"FSM finished")
#         stat = msg.text.split(" ")[-1][:-1]  # to avoid the . in the end
#         state = FlexbeStates()
#         flag = String()
#         state.time = time.time()
#         state.name = "fsm_finished"
#         state.state = "LogState"
#         state.path = "NOPATH"
#         if stat == "failed":
#             state.name = "fsm_failed"
#             fsm_state = FSMState.Failed
#             state.event = "enter"
#             flag.data = "Failed"
#         if stat == "preempted":
#             state.name = "fsm_preempted"
#             fsm_state = FSMState.Preempted
#             state.event = "enter"
#             flag.data = "Preempted"
#         state.duration = 0.001
#         state.logger = "flexbe.events"
#         state.loglevel = "INFO"
#         fsm_state_pub.publish(flag)
#         behaviour_pub.publish(state)


# def raw_callback(msg):
#     global behaviour_pub
#     print(f"Raw: {msg.data}")
#     processed_data = msg.data.replace("'", "\"").replace("- ", "").replace("- ", "")
#     #print(f"substitute quotes: {processed_data}")
#     #processed_data = processed_data.replace("- ","")
#     #print(f"Eliminate dash: {processed_data}")
#     #processed_data = processed_data.replace("'","\"")
#     #print(f"final: {processed_data}")
#     # print(type(processed_data))
#     # print(processed_data[0])
#     # behaviour_pub.publish(msg)
#     #processed_data = str(processed_data)
#     # print(processed_data)
#     # print(s)
#     data = json.loads(processed_data)
#     print(type(data))
#     print(f"processed json data: {data}")
#     state = FlexbeStates()
#     # print(state)
#     state.time = data['time']
#     state.name = data["name"]
#     state.state = data["state"]
#     state.path = data["path"]
#     state.event = data["event"]
#     state.duration = data["duration"]
#     state.logger = data["logger"]
#     state.loglevel = data["loglevel"]
#     print(state)
#     behaviour_pub.publish(state)


# def timer_func():
#     global fsm_state, behaviour_pub
#     if fsm_state == FSMState.NotStarted:
#         rospy.loginfo("FSM has not started yet.. will publish that on the status processed topic!")
#         state = FlexbeStates()
#         state.time = time.time()
#         state.name = "fsm_not_started"
#         state.state = "LogState"
#         state.path = "NOPATH"
#         state.event = "enter"
#         state.duration = 0.001
#         state.logger = "flexbe.events"
#         state.loglevel = "INFO"
#         print(state)
#         # TODO: group states
#         behaviour_pub.publish(state)


# # rospy.init_node('kigndom_behaviour_process', anonymous=True)

# # rospy.loginfo("behaviour process node starting...")


# # rospy.loginfo("Timer starting now...")
# # # Timer(10*60,function=timer_func)

# # behaviour_pub = rospy.Publisher('/flexbe/state_logger/processed', FlexbeStates, queue_size=10)
# # fsm_state_pub = rospy.Publisher('/FSM/state', String, latch=True, queue_size=10)
# # time.sleep(2.0)
# # # timer_func()
# # state = FlexbeStates()
# # state.time = time.time()
# # state.name = "fsm_not_started"
# # state.state = "LogState"
# # state.path = "NOPATH"
# # state.event = "enter"
# # state.duration = 0.001
# # state.logger = "flexbe.events"
# # state.loglevel = "INFO"
# # behaviour_pub.publish(state)
# # print("message published")
# # rospy.Subscriber("/flexbe/state_logger", String, raw_callback)

# # rospy.Subscriber("/flexbe/log", BehaviorLog, status_callback)
# # rospy.spin()

# - {"time": 1686317166.810619, "name": "charging", "state": "KingdomHandleClientState","path": "/dock_undock_fsm/charging", "event": "enter", "duration": 0.011437416076660156,"logger": "flexbe.events", "loglevel": "INFO"}

# \"- {'time': 1686317166.810619, 'name': 'charging', 'state': 'KingdomHandleClientState','path': '/dock_undock_fsm/charging', 'event': 'enter', 'duration': 0.011437416076660156,'logger': 'flexbe.events', 'loglevel': 'INFO'}\"
