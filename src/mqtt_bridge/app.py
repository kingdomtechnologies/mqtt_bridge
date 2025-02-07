import inject
import paho.mqtt.client as mqtt
import rospy

from .bridge import create_bridge
from .mqtt_client import create_private_path_extractor
from .util import lookup_object
import kingdom_definitions as kdef
from std_msgs.msg import Bool 
import time

state_pub = None

def create_config(mqtt_client, serializer, deserializer, mqtt_private_path):
    if isinstance(serializer, str):
        serializer = lookup_object(serializer)
    if isinstance(deserializer, str):
        deserializer = lookup_object(deserializer)
    private_path_extractor = create_private_path_extractor(mqtt_private_path)
    def config(binder):
        binder.bind('serializer', serializer)
        binder.bind('deserializer', deserializer)
        binder.bind(mqtt.Client, mqtt_client)
        binder.bind('mqtt_private_path_extractor', private_path_extractor)
    return config


def mqtt_bridge_node():
    # init node
    global state_pub
    rospy.init_node('mqtt_bridge_node')

    # connection status topic
    state_pub = rospy.Publisher(kdef.MQTT_BRIDGE_CONNECTION_STATUS_TOPIC_NAME, Bool, queue_size=0,latch=True)
    time.sleep(1.0)
    state_pub.publish(False)

    # load parameters
    params = rospy.get_param("~", {})
    mqtt_params = params.pop("mqtt", {})
    conn_params = mqtt_params.pop("connection")
    mqtt_private_path = mqtt_params.pop("private_path", "")
    bridge_params = params.get("bridge", [])

    # create mqtt client
    mqtt_client_factory_name = rospy.get_param(
        "~mqtt_client_factory", ".mqtt_client:default_mqtt_client_factory")
    mqtt_client_factory = lookup_object(mqtt_client_factory_name)
    mqtt_client = mqtt_client_factory(mqtt_params)

    # load serializer and deserializer
    serializer = params.get('serializer', 'msgpack:dumps')
    deserializer = params.get('deserializer', 'msgpack:loads')

    # dependency injection
    config = create_config(
        mqtt_client, serializer, deserializer, mqtt_private_path)
    inject.configure(config)

    # configure and connect to MQTT broker
    mqtt_client.on_connect = _on_connect
    mqtt_client.on_disconnect = _on_disconnect
    mqtt_client.connect(**conn_params)

    # configure bridges
    bridges = []
    for bridge_args in bridge_params:
        bridges.append(create_bridge(**bridge_args))

    # start MQTT loop
    mqtt_client.loop_start()

    # register shutdown callback and spin
    rospy.on_shutdown(_on_disconnect)
    rospy.on_shutdown(mqtt_client.disconnect)
    rospy.on_shutdown(mqtt_client.loop_stop)
    rospy.spin()


def _on_connect(client, userdata, flags, response_code):
    global state_pub
    state_pub.publish(True)
    rospy.loginfo('MQTT connected')


def _on_disconnect(client, userdata, response_code):
    global state_pub
    state_pub.publish(False)
    rospy.loginfo('MQTT disconnected')

def _on_shutdown():
    global state_pub
    state_pub.publish(False)
    rospy.loginfo('MQTT on shutdown')

__all__ = ['mqtt_bridge_node']
