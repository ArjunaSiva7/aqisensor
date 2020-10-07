import asyncio
import argparse
import logging
import threading
import serial
import sys

import aqi

from hbmqtt.client import MQTTClient
from hbmqtt.mqtt.constants import QOS_1, QOS_2

DEFAULT_BROKER='mqtt://127.0.0.1'
DEFAULT_ROOM='Default'

def run_decoder_pump(loop, device_path, mqtt_client, topic_room):
  with serial.Serial(device_path, 9600, timeout=1) as device:
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

async def bootstrap(args):
  C = MQTTClient()
  await C.connect(args.broker)

  t = threading.Thread(target=run_decoder_pump, args=(asyncio.get_event_loop(), args.device, C, args.room))
  t.start()

def main(argv):
    parser = argparse.ArgumentParser(description='MQTT AQI')

    parser.add_argument('-d', 
      '--device',
      dest='device',
      type=str,
      help='The device to use')
    parser.add_argument('-r',
      '--room',
      default=DEFAULT_ROOM,
      dest='room',
      type=str,
      help='The room of this sensor')
    parser.add_argument('-b',
      '--broker',
      default=DEFAULT_BROKER,
      dest='broker',
      type=str,
      help='The MQTT Broker URL to use')

    args = parser.parse_args(argv)

    
    formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    
    if not args.device:
      logging.error('You need to specify a device')
      parser.print_help()
      return

    if not args.room:
      logging.error('You need to specify a room')
      parser.print_help()
      return

    logging.info('Using broker %s and Room %s' % (args.broker, args.room))

    asyncio.get_event_loop().run_until_complete(bootstrap(args))
    asyncio.get_event_loop().run_forever()
    
if __name__ == '__main__':
    main(sys.argv[1:])
