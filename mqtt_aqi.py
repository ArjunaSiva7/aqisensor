import asyncio
import logging
import threading
import serial

import aqi

from hbmqtt.client import MQTTClient
from hbmqtt.mqtt.constants import QOS_1, QOS_2

DEFAULT_BROKER='mqtt://127.0.0.1'
DEFAULT_ROOM='Default'

def run_decoder_pump(loop, mqtt_client, topic_room):
  with serial.Serial('/dev/ttyUSB0', 9600, timeout=1) as device:
    decoder = aqi.Decoder(device, get_decoder_callback(loop, mqtt_client, topic_room))
    decoder.read_pump()

def get_decoder_callback(loop, client, topic_room):
  def callback(params, desc):
    topic = 'environment/%s/aqi' % topic_room
    message = desc
    logging.debug('Publishing topic %s: %s', topic, params)
    f = client.publish(topic + '/description', bytes(message,'utf-8'), qos=QOS_2)
    loop.create_task(f)
    
    for p in params.keys():
      try:
        message = '%0.1f' % params[p]
      except TypeError:
        message = str(params[p])

      f = client.publish(topic + '/' + p, bytes(message, 'utf-8'), qos=QOS_2)
      loop.create_task(f)


  return callback

async def main(argv):
  broker_url = DEFAULT_BROKER
  topic_room = DEFAULT_ROOM

  C = MQTTClient()
  await C.connect(broker_url)

  t = threading.Thread(target=run_decoder_pump, args=(asyncio.get_event_loop(), C, topic_room))
  t.start()

if __name__ == '__main__':
    formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)

    asyncio.get_event_loop().run_until_complete(main([]))
    asyncio.get_event_loop().run_forever()
    

