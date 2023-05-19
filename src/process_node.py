#!/usr/bin/env python3

from threading import Timer

import rospy
from std_msgs.msg import String
import json
from kingdom_message_definitions.msg import FlexbeStates
from flexbe_msgs.msg import BEStatus
from enum import Enum

class FSMState(Enum):
    NotStarted = 0
    Running    = 1
    Failed     = 2
    Preempted  = 3


s = '{"time": 1665163918.576645, "name": "battery_low_preempt", "state": "LogState", "path": "/cutting_task_fsm/battery_low_preempt", "event": "stop", "duration": 0.0001575946807861328, "logger": "flexbe.events", "loglevel": "INFO"}'
#s = '{"success": "true", "status": 200, "message": "Hello"}'
d = json.loads(s)
#print(d["success"], d["status"])
print(d["time"])

fsm_state = FSMState.NotStarted 

def status_callback(msg):
    global fsm_state, behaviour_pub
    rospy.loginfo("got this msg")
    rospy.loginfo(msg)
    if len(msg.args) == 0:
        rospy.loginfo("FSM got called")
        fsm_state = FSMState.Running
    elif msg.args[0] == "failed":
        rospy.loginfo(f"FSM failed")
        state = FlexbeStates()
        state.time = rospy.Time.now()
        state.name = "FSM_failed"
        state.state = "LogState"
        state.path = "NOPATH"
        state.event = "failure"
        state.duration = "0.001"
        state.logger = "flexbe.events"
        state.loglevel = "INFO"
        print(state)
        behaviour_pub.publish(state)
    elif msg.args[0] == "preempted":
        rospy.loginfo(f"FSM preempted")
        state = FlexbeStates()
        state.time = rospy.Time.now()
        state.name = "FSM_preempted"
        state.state = "LogState"
        state.path = "NOPATH"
        state.event = "preemption"
        state.duration = "0.001"
        state.logger = "flexbe.events"
        state.loglevel = "INFO"
        print(state)
        behaviour_pub.publish(state)
 

def raw_callback(msg):
    global behaviour_pub
    print(f"Raw: {msg.data}")
    processed_data = msg.data.replace("'","\"").replace("- ","").replace("- ","")
    #print(f"substitute quotes: {processed_data}")
    #processed_data = processed_data.replace("- ","")
    #print(f"Eliminate dash: {processed_data}")
    #processed_data = processed_data.replace("'","\"")
    #print(f"final: {processed_data}")
    #print(type(processed_data))
    #print(processed_data[0])
    #behaviour_pub.publish(msg)
    #processed_data = str(processed_data)
    #print(processed_data)
    #print(s)
    data = json.loads(processed_data)
    print(type(data))
    print(f"processed json data: {data}")
    state = FlexbeStates()
    #print(state)
    state.time = data['time']
    state.name = data["name"]
    state.state = data["state"]
    state.path = data["path"]
    state.event = data["event"]
    state.duration = data["duration"]
    state.logger = data["logger"]
    state.loglevel = data["loglevel"]
    print(state)
    behaviour_pub.publish(state)


def timer_func():
    if fsm_state== FSMState.NotStarted:
        rospy.loginfo("FSM has not started yet.. will publish that on the status processed topic!")         
        state = FlexbeStates()
        state.time = rospy.Time.now()
        state.name = "FSM_not_started"
        state.state = "LogState"
        state.path = "NOPATH"
        state.event = "not_started"
        state.duration = "0.001"
        state.logger = "flexbe.events"
        state.loglevel = "INFO"
        print(state)
        behaviour_pub.publish(state)


rospy.init_node('kigndom_behaviour_process', anonymous=True)

rospy.loginfo("behaviour process node starting...")

rospy.Subscriber("/flexbe/state_logger", String, raw_callback)

rospy.Subscriber("/flexbe/status", BEStatus, raw_callback)

Timer(10*60,function=timer_func)

behaviour_pub = rospy.Publisher('/flexbe/state_logger/processed', FlexbeStates, queue_size=10)

rospy.spin()
