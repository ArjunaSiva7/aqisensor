import serial
import datetime

"""
PACKET FORMAT:
0 packet header AA

1 Instruction number C0

2 Data 1 PM2.5 low byte

3 Data 2 PM2.5 high byte

4 Data 3 PM10 low byte

5 Data 4 PM10 high byte

6 Data 5 0 (reserved)

7 Data 6 0 (reserved)

8 Checksum Checksum

9 Trailer AB
"""
PACKET_HEADER = 0xAA
INSTRUCTION_NUMBER = 0xC0
PREFIX = bytes([PACKET_HEADER, INSTRUCTION_NUMBER])
TRAILER = 0xAB
FRAME_SIZE = 10


# https://forum.airnowtech.org/t/the-aqi-equation/169
TABLE_DESCRIPTIONS = [
        "Good", 
        "Moderate",
        "Unhealthy for Sensitive Groups",
        "Unhealthy",
        "Very Unhealthy",
        "Hazardous"]

# Table format: conc_low, conc_high, aqi_low, aqi_high
PM2P5_TABLE = [
  [0.0, 12.0, 0, 50],
  [12.1, 35.4, 51, 100],
  [35.5, 55.4, 101, 150],
  [55.5, 150.4, 151, 200],
  [150.5, 250.4, 201, 300],
  [250.5, 500.4, 301, 500]
]
PM2P5_TABLE.reverse()

PM10_TABLE = [
  [0, 54, 0, 50],
  [55, 154, 51, 100],
  [155, 254, 100, 150],
  [255, 354, 151, 200],
  [355, 424, 201, 300],
  [425, 604, 301, 500]
]
PM10_TABLE.reverse()

class Decoder(object):
  def __init__(self, device, callback):
    self.buffer = bytes()
    self.device = device
    self.callback = callback

  def read_pump(self):
    while True:
      self.buffer = self.buffer + device.read()
      while len(self.buffer) >= FRAME_SIZE:
        # print('buffer: %s' % list(self.buffer))
        read_again = self.find_frame()
        if read_again:
          break

  def find_frame(self):
    location = self.buffer.find(PREFIX)
    if location >= 0:
      frame = self.buffer[location:]

      if len(frame) < FRAME_SIZE:
        return False

      if frame[FRAME_SIZE - 1] == TRAILER:
         frame = frame[:FRAME_SIZE - 1]
         self.parse_frame(frame)
      self.buffer = self.buffer[location + FRAME_SIZE:]

      return True
    else:
      self.buffer = bytes()

      return False

  def calculate_aqi(self, pollutant_conc, table):
    for i, row in enumerate(table):
      conc_low, conc_high, aqi_low, aqi_high = row

      if pollutant_conc >= conc_low:
        aqi = ((aqi_high - aqi_low) / (conc_high - conc_low) * 
          (pollutant_conc - conc_low) + aqi_low)
        return aqi, len(table)-i-1

    print("Eh? %s, %s" % (pollutant_conc, table))

    return 0

  def parse_frame(self, frame):
    # print('frame: %s' % frame)
    
    pm2p5 = (frame[2] | frame[3] << 8)/10.0
    pm10 = (frame[4] | frame[5] << 8)/10.0

    pm2p5_aqi, pm2p5_desc = self.calculate_aqi(pm2p5, PM2P5_TABLE)
    pm10_aqi, pm10_desc = self.calculate_aqi(pm10, PM10_TABLE)

    max_aqi = max(pm2p5_aqi, pm10_aqi)
    max_desc = max(pm2p5_desc, pm10_desc)

    pm2p5_desc = TABLE_DESCRIPTIONS[pm2p5_desc]
    pm10_desc = TABLE_DESCRIPTIONS[pm10_desc]
    max_desc = TABLE_DESCRIPTIONS[max_desc]

    now = datetime.datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    desc = 'PM2.5:%0.1f PM10:%0.1f AQI: %u, %u(PM2.5 %s), %u(PM10 %s)' % (now_str, pm2p5, pm10, max_aqi, pm2p5_aqi, pm2p5_desc, pm10_aqi, pm10_desc)
    print('%s: %s' % desc)

    params = {
      'AQI': max_aqi,
      'PM2.5': pm2p5,
      'PM2.5 AQI': pm2p5_aqi,
      'PM2.5 Description': pm2p5_desc,
      'PM10': pm10,
      'PM10 AQI': pm10_aqi,
      'PM10 Description': pm10_desc,
    }
    self.callback(params, desc)

if __name__ == "__main__":
  with serial.Serial('/dev/ttyUSB1', 9600, timeout=1) as device:
    d = Decoder(device)
    d.read_pump()
