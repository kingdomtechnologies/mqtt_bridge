#!/usr/bin/env python3

import rospy
from std_msgs.msg import String

def raw_callback(msg):
    global behaviour_pub
    print(f"Raw: {msg.data}")
    processed_data = msg.data.replace("'","\"")
    print(f"substitute quotes: {msg.data}")
    processed_data = msg.data.replace("- ","")
    print(f"Eliminate dash: {msg.data}")
    behaviour_pub.publish(msg)



rospy.init_node('kigndom_behaviour_process', anonymous=True)

rospy.loginfo("behaviour process node starting...")

rospy.Subscriber("/flexbe/state_logger", String, raw_callback)

behaviour_pub = rospy.Publisher('/flexbe/state_logger/processed', String, queue_size=10)


rospy.spin()
