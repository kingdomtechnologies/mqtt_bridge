from typing import Dict, Callable
import socket
import paho.mqtt.client as mqtt

def default_mqtt_client_factory(params: Dict) -> mqtt.Client:
    """ MQTT Client factory """
    # create client
    client_params = params.get('client', {})

    # Define mqtt client id as our device hostname
    hostname = str(socket.gethostname())
    client_params['client_id'] = hostname
    print(f"Setting client_id as the device hostname: { client_params['client_id'] }")
    
    client = mqtt.Client(**client_params)

    # configure tls
    tls_params = params.get('tls', {})
    if "credentials_base_folder" in tls_params:
        base_folder = tls_params["credentials_base_folder"]
        tls_params["ca_certs"] = f"{base_folder}/AmazonRootCA1.pem"
        tls_params["certfile"] = f"{base_folder}/{hostname}-certificate.pem.crt"
        tls_params["keyfile"] = f"{base_folder}/{hostname}-private.pem.key"
        tls_params.pop('credentials_base_folder', False)
    if tls_params:
        tls_insecure = tls_params.pop('tls_insecure', False)
        client.tls_set(**tls_params)
        client.tls_insecure_set(tls_insecure)

    # configure username and password
    account_params = params.get('account', {})
    if account_params:
        client.username_pw_set(**account_params)

    # configure message params
    message_params = params.get('message', {})
    if message_params:
        inflight = message_params.get('max_inflight_messages')
        if inflight is not None:
            client.max_inflight_messages_set(inflight)
        queue_size = message_params.get('max_queued_messages')
        if queue_size is not None:
            client.max_queued_messages_set(queue_size)
        retry = message_params.get('message_retry')
        if retry is not None:
            client.message_retry_set(retry)

    # configure userdata
    userdata = params.get('userdata', {})
    if userdata:
        client.user_data_set(userdata)

    # configure will params
    will_params = params.get('will', {})
    if will_params:
        client.will_set(**will_params)

    return client


def create_private_path_extractor(mqtt_private_path: str) -> Callable[[str], str]:
    def extractor(topic_path):
        if topic_path.startswith('~/'):
            return '{}/{}'.format(mqtt_private_path, topic_path[2:])
        return topic_path
    return extractor


__all__ = ['default_mqtt_client_factory', 'create_private_path_extractor']
