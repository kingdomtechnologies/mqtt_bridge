#!/usr/bin/env python3

import rospy
from std_msgs.msg import String
import json
from kingdom_message_definitions.msg import FlexbeStates

s = '{"time": 1665163918.576645, "name": "battery_low_preempt", "state": "LogState", "path": "/cutting_task_fsm/battery_low_preempt", "event": "stop", "duration": 0.0001575946807861328, "logger": "flexbe.events", "loglevel": "INFO"}'
#s = '{"success": "true", "status": 200, "message": "Hello"}'
d = json.loads(s)
#print(d["success"], d["status"])
print(d["time"])

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

rospy.init_node('kigndom_behaviour_process', anonymous=True)

rospy.loginfo("behaviour process node starting...")

rospy.Subscriber("/flexbe/state_logger", String, raw_callback)

behaviour_pub = rospy.Publisher('/flexbe/state_logger/processed', FlexbeStates, queue_size=10)

rospy.spin()
