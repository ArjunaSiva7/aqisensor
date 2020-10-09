import asyncio
import argparse
import logging
import logging.handlers
import pandas
import threading
import serial
import sys

import aqi

from hbmqtt.client import MQTTClient
from hbmqtt.mqtt.constants import QOS_1, QOS_2

DEFAULT_BROKER='mqtt://127.0.0.1'
DEFAULT_ROOM='Default'
DEFAULT_HISTORY_SECONDS=3600*48

SAMPLES_PER_HOUR=int(3600/aqi.UPDATE_INTERVAL_SECONDS)

class AQIData(object):
  def __init__(self, logger):
    self.data = None
    self.logger = logger

  def run_decoder_pump(self, loop, device_path, mqtt_client, topic_room):
    with serial.Serial(device_path, 9600, timeout=1) as device:
      decoder = aqi.Decoder(device, self.get_decoder_callback(loop, mqtt_client, topic_room))
      decoder.read_pump()

  def get_decoder_callback(self, loop, client, topic_room):
    def callback(params, desc):
      topic = 'environment/%s/aqi' % topic_room
      message = desc
      self.logger.debug('Publishing topic %s: %s', topic, params)
      f = client.publish(topic + '/description', bytes(message,'utf-8'), qos=QOS_2)
      loop.create_task(f)

      if self.data is None:
        self.data = pandas.DataFrame(columns=params.keys())

      self.data = self.data.append(params, ignore_index=True)
      samples = int(DEFAULT_HISTORY_SECONDS/aqi.UPDATE_INTERVAL_SECONDS)
      self.data = self.data.head(samples)
      # self.logger.debug('AQI data: %s' % self.data)
      # self.logger.debug('AQI data: %s' % self.data['AQI'])

      aqi_grouped = self.data['AQI'].groupby(self.data.index // SAMPLES_PER_HOUR).mean()
      aqi_grouped = [int(v) for v in aqi_grouped.tolist()]
      if len(aqi_grouped) == 1:
        aqi_grouped.append(aqi_grouped[0])
      aqi_grouped.reverse()
      self.logger.debug('AQI hourly: %s' % aqi_grouped)
      f = client.publish(topic + '/' + 'AQI_hourly', bytes(str(aqi_grouped), 'utf-8'), qos=QOS_2)
      loop.create_task(f)
      
      for p in params.keys():
        try:
          message = '%0.1f' % params[p]
        except TypeError:
          message = str(params[p])

        f = client.publish(topic + '/' + p, bytes(message, 'utf-8'), qos=QOS_2)
        loop.create_task(f)

    return callback

async def bootstrap(args, logger):
  C = MQTTClient()
  await C.connect(args.broker)

  data = AQIData(logger)
  t = threading.Thread(target=data.run_decoder_pump, args=(asyncio.get_event_loop(), args.device, C, args.room))
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
    logger = logging.getLogger('Logger')
    logger.setLevel(logging.DEBUG)
    handler = logging.handlers.SysLogHandler(address='/dev/log')
    logger.addHandler(handler)
    
    if not args.device:
      print('You need to specify a device')
      parser.print_help()
      return

    if not args.room:
      print('You need to specify a room')
      parser.print_help()
      return

    print('Using broker %s and Room %s' % (args.broker, args.room))

    asyncio.get_event_loop().run_until_complete(bootstrap(args, logger))
    asyncio.get_event_loop().run_forever()
    
if __name__ == '__main__':
    main(sys.argv[1:])
