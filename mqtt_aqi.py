import asyncio
import logging
import threading

import aqi

from hbmqtt.client import MQTTClient
from hbmqtt.mqtt.constants import QOS_1, QOS_2

DEFAULT_BROKER= '127.0.0.1'
DEFAULT_ROOM = 'Default'

def run_decoder_pump(decoder):
  decoder.read_pump()

def get_decoder_callback(client, topic_room):
  return def callback(params, desc):
    topic = 'environment/%s/aqi' % topic_room
    message = desc
    f = syncio.ensure_future(client.publish(topic, message), qos=QOS_2)
    asyncio.get_event_loop().add_task(f)

async def main(argv):
  broker_url = DEFAULT_BROKER
  topic_room = DEFAULT_ROOM

  with serial.Serial('/dev/ttyUSB1', 9600, timeout=1) as device:
    C = MQTTClient()
    await C.connect(broker_url)

    d = Decoder(device, get_decoder_callback(C, topic_room))
    threading.Thread(target=run_decoder_pump, (d,))

if __name__ == '__main__':
    formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)

    asyncio.get_event_loop().run_until_complete(main)
    asyncio.get_event_loop().run_forever()
    

