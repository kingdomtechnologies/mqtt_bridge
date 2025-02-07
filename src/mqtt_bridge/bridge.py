from abc import ABCMeta
from typing import Optional, Type, Dict, Union

import inject
import paho.mqtt.client as mqtt
import rospy

import datetime, socket, time

from .util import lookup_object, extract_values, populate_instance
from kingdom_params_handler import ParamsHandler
from threading import Thread, Event, Lock
from kingdom_telegram import Notifier


def create_bridge(factory: Union[str, "Bridge"], msg_type: Union[str, Type[rospy.Message]], topic_from: str,
                  topic_to: str, frequency: Optional[float] = None, **kwargs) -> "Bridge":
    """ generate bridge instance using factory callable and arguments. if `factory` or `meg_type` is provided as string,
     this function will convert it to a corresponding object.
    """
    if isinstance(factory, str):
        factory = lookup_object(factory)
    if not issubclass(factory, Bridge):
        raise ValueError("factory should be Bridge subclass")
    if isinstance(msg_type, str):
        msg_type = lookup_object(msg_type)
    if not issubclass(msg_type, rospy.Message):
        raise TypeError(
            "msg_type should be rospy.Message instance or its string"
            "reprensentation")
    return factory(
        topic_from=topic_from, topic_to=topic_to, msg_type=msg_type, frequency=frequency, **kwargs)


class Bridge(object, metaclass=ABCMeta):
    """ Bridge base class """
    _mqtt_client = inject.attr(mqtt.Client)
    _serialize = inject.attr('serializer')
    _deserialize = inject.attr('deserializer')
    _extract_private_path = inject.attr('mqtt_private_path_extractor')


class RosToMqttBridge(Bridge):
    """ Bridge from ROS topic to MQTT

    bridge ROS messages on `topic_from` to MQTT topic `topic_to`. expect `msg_type` ROS message type.
    """

    def __init__(self, topic_from: str, topic_to: str, msg_type: rospy.Message, frequency: Optional[float] = None):
        self.device_id = str(socket.gethostname())

        # Instantiate notifier and associated variables
        self.notifier = Notifier()

        self.site_id_handler = ParamsHandler("site/site_id")
        while self.site_id_handler.param is None:
            rospy.loginfo_throttle(60, "Waiting for configs to be loaded")
            time.sleep(1.0)
            
        self.site_id = self.site_id_handler.read_params()

        self.topic_to = topic_to
        self._topic_from = topic_from

        #self._topic_to = self._extract_private_path(topic_to)
        if self.topic_to.startswith('~/'):
            self._topic_to = f"/kingdom/{self.site_id}/{self.device_id}/{self.topic_to[2:]}"
        elif self.topic_to.startswith('/'):
            self._topic_to = f"/kingdom/{self.site_id}/{self.device_id}/{self.topic_to[1:]}"
        else:
            self._topic_to = f"/kingdom/{self.site_id}/{self.device_id}/{self.topic_to}"

        rospy.loginfo(f"Ready to publishing MQTT topic {self._topic_to}")

        self._last_published = rospy.get_time()
        self._interval = 0 if frequency is None else 1.0 / frequency
        rospy.Subscriber(topic_from, msg_type, self._callback_ros)
        
        self.rate = rospy.Rate(0.1)
        self.param_thread = Thread(target=self.new_site_id_handler, daemon=True)
        self.param_thread.start()
        self.halt_publishing = False


    def new_site_id_handler(self):

        while not rospy.is_shutdown():
            rospy.logdebug(f"Param Changed: {self.site_id_handler.param_changed}")
            if self.site_id_handler.read_params():
                self.halt_publishing = True
                prev_site_id = self.site_id
                self.site_id = self.site_id_handler.param
                msg = f"[{self.topic_to}]: Handling a new site_id: changing from {prev_site_id} to {self.site_id}"
                self.notifier.notify_warn(msg)


                if self.topic_to.startswith('~/'):
                    self._topic_to = f"/kingdom/{self.site_id}/{self.device_id}/{self.topic_to[2:]}"
                elif self.topic_to.startswith('/'):
                    self._topic_to = f"/kingdom/{self.site_id}/{self.device_id}/{self.topic_to[1:]}"
                else:
                    self._topic_to = f"/kingdom/{self.site_id}/{self.device_id}/{self.topic_to}"

                self.halt_publishing = False

            self.rate.sleep()

    def _callback_ros(self, msg: rospy.Message):
        rospy.logdebug("ROS received from {}".format(self._topic_from))
        now = rospy.get_time()
        if now - self._last_published >= self._interval:
            self._publish(msg)
            self._last_published = now

    def _publish(self, msg: rospy.Message):
        payload = extract_values(msg) #self._serialize(extract_values(msg))
        date_time = str(datetime.datetime.utcnow())
        header = {"device_id": self.device_id, "date_time": date_time, "site_id": self.site_id, "ros_topic": self._topic_from}
        header.update(payload)
        payload = self._serialize(header)

        self._mqtt_client.publish(topic=self._topic_to, payload=payload,qos=1)


class MqttToRosBridge(Bridge):
    """ Bridge from MQTT to ROS topic

    bridge MQTT messages on `topic_from` to ROS topic `topic_to`. MQTT messages will be converted to `msg_type`.
    """

    def __init__(self, topic_from: str, topic_to: str, msg_type: Type[rospy.Message],
                 frequency: Optional[float] = None, queue_size: int = 10):
        self._topic_from = self._extract_private_path(topic_from)
        self._topic_to = topic_to
        self._msg_type = msg_type
        self._queue_size = queue_size
        self._last_published = rospy.get_time()
        self._interval = None if frequency is None else 1.0 / frequency
        # Adding the correct topic to subscribe to
        self._mqtt_client.subscribe(self._topic_from)
        self._mqtt_client.message_callback_add(self._topic_from, self._callback_mqtt)
        self._publisher = rospy.Publisher(
            self._topic_to, self._msg_type, queue_size=self._queue_size)

    def _callback_mqtt(self, client: mqtt.Client, userdata: Dict, mqtt_msg: mqtt.MQTTMessage):
        """ callback from MQTT """
        rospy.logdebug("MQTT received from {}".format(mqtt_msg.topic))
        now = rospy.get_time()

        if self._interval is None or now - self._last_published >= self._interval and not self.halt_publishing:
            try:
                ros_msg = self._create_ros_message(mqtt_msg)
                self._publisher.publish(ros_msg)
                self._last_published = now
            except Exception as e:
                rospy.logerr(e)

    def _create_ros_message(self, mqtt_msg: mqtt.MQTTMessage) -> rospy.Message:
        """ create ROS message from MQTT payload """
        # Hack to enable both, messagepack and json deserialization.
        if self._serialize.__name__ == "packb":
            msg_dict = self._deserialize(mqtt_msg.payload, raw=False)
        else:
            msg_dict = self._deserialize(mqtt_msg.payload)
        return populate_instance(msg_dict, self._msg_type())


__all__ = ['create_bridge', 'Bridge', 'RosToMqttBridge', 'MqttToRosBridge']
