#!/usr/bin/env python3
from paho.mqtt import client as mqtt
import uuid
import bleak
import time
import struct
import threading
import sys
import uuid
import asyncio
from enum import Enum
import queue
from datetime import datetime

# Config variables
mqtt_interval = 10
#mqtt_broker = '127.0.0.1'
mqtt_broker= '192.168.2.123'
mqtt_port = 1883
mqtt_topic = 'dzd/bojler'
mqtt_client_id = 'smartbojler'
mqtt_username = 'waveplus'
mqtt_password = 'DJl7tLrQEmjWz0guniRM8lHCO1eDzSM1' # yeah, I know having passwords in repository is a bad practice
mqtt_retry = 30

bluetooth_retries = 10

dzd_mac = "e7:68:c3:35:ef:c0"
report_interval = 30

# Global variables
mqtt_connected = False
mqtt_client = None
mqtt_lock = threading.Lock()
mqtt_connecting = False
mqtt_last_connect = None

print_stdout = True
bojler = None


class Bojler_Mode(Enum):
    STOP = 0;
    NORMAL = 1;
    HDO = 2;
    SMART = 3;
    SMARTHDO = 4;
    ANTIFROST = 5;
    NIGHT = 6;
    TEST = 7;
    HOLIDAY = 8;
 

class Packet_Number(Enum):
    NONE = 0;
    LOG_MSG = 1;
    HOME_BOILERMODEL = 2;
    HOME_FWVERSION = 3;
    HOME_MODE = 4;
    HOME_ERROR = 5;
    HOME_HSRCSTATE = 6;
    HOME_SENSOR1 = 7;
    HOME_SENSOR2 = 8;
    HOME_TEMPERATURE = 9;
    HOME_TEMPNIGHT = 10;
    HOME_TIME = 11;
    HOME_ALL = 12;
    HOME_SETTIME = 13;
    HOME_SETNORMALTEMPERATURE = 14;
    HOME_TEMPNIGHTLOW = 15;
    HOME_SETTEMPNIGHT = 16;
    HOME_SETTEMPNIGHTLOW = 17;
    HOME_SETMODE = 18;
    HOME_SETNORMALMODE = 19;
  
    HDO_ONOFF = 20;
    HDO_SELECTION_A = 21;
    HDO_SELECTION_B = 22;
    HDO_SELECTION_DP = 23;
    HDO_FREQUENCY = 24;
    HDO_SETTING = 25;
    HDO_ALL = 26;
    HDO_SET_ONOFF = 27;
    HDO_SET_SELECTION_A = 28;
    HDO_SET_SELECTION_B = 29;
    HDO_SET_SELECTION_DP = 30;
    HDO_SET_FREQUENCY = 31;
    HDO_LASTHDOTIME = 32;
    HDO_LESSEXPTARIFFAVAILABLENOW = 33;
    HDO_INFO = 34;
    HDO_MANUAL_SET = 35;
    HDO_MANUAL_GET = 36;

    NIGHT_GETDAY = 40;
    NIGHT_SAVEDAY = 41;
    NIGHT_SAVEMINMAX = 42;
    NIGHT_SAVEDAYS = 43;
    NIGHT_GETDAYS = 44;
    NIGHT_SAVEDAYS2 = 45;
    NIGHT_GETDAYS2 = 46;

    GLOBAL_STARTSIMULATION = 50;
    GLOBAL_CONFIRMUID = 51;
    GLOBAL_ERRORUID = 52;
    GLOBAL_PAIRPIN = 53;
    GLOBAL_FIRSTLOG = 54;
    GLOBAL_NEXTLOG = 55;
    GLOBAL_RESETBERR = 56;
    GLOBAL_PINRESULT = 57;
    GLOBAL_DEVICEBONDED = 58;
    HOME_FWRESET = 59;
 
    HOLIDAY_GET = 60;
    HOLIDAY_SET = 61;
    HOLIDAY_ENABLE = 62;
    HOLIDAY_DISABLE = 63;
    HOLIDAY_DELETE = 64;
    HOLIDAY_ENABLED = 65;
    RQ_GLOBAL_MAC = 68;

    RQ_UI_BUTTONUP = 70;
    RQ_UI_BUTTONDOWN = 71;
    RQ_UI_D7SEG = 72;
    POWERCONS_RESET = 73;
    POWERCONS_OBTAIN = 74;
    
    HOME_ANTILEGIO = 76;
    HOME_NEWERR = 77;
    HOME_BOILERNAME = 80;
    HOME_SETBOILERNAME = 81;
    HOME_TEMPNIGHTCURR = 82;
    HOME_CAPACITY = 83;
    HOME_SETCAPACITY = 84;
    HOME_FWBEGIN = 86;
    HOME_FWCONFIRM = 87;
    HOME_FWCHECK = 88;
    HOME_FWCOPY = 89;

    STATISTICS_WEEK = 90;
    STATISTICS_YEAR = 91;
    STATISTICS_RESET = 92;
    STATISTICS_GETALL = 93;
    ANODE_VOLTAGE = 94;
    ANODE_PARAMS = 95;

SBCORE_SERVICE_UUID = uuid.UUID("00001899-0000-1000-8000-00805f9b34fb")
SBCORE_CHARACTERISTIC_UUID = uuid.UUID("00002B99-0000-1000-8000-00805f9b34fb")
LOG_SERVICE_UUID = uuid.UUID("00001898-0000-1000-8000-00805f9b34fb")
LOG_CHARACTERISTIC_UUID = uuid.UUID("00002B98-0000-1000-8000-00805f9b34fb")
CLIENT_CHARACTERISTIC_CONFIG_DESCRIPTOR_UUID = uuid.UUID("00002902-0000-1000-8000-00805f9b34fb")
UID_CONSUMPTION=0x69

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
    sys.stderr.flush()

def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    eprint("Connected to mqtt broker with return code: "+str(rc))
    if rc == 0:
        mqtt_connected = True
    else:
        mqtt_connected = False

def on_disconnect(client, userdata, rc):
    eprint("Disconnected with return code: "+str(rc))
    mqtt_connected = False

def on_message(client, userdata, message):
    global bojler
    message.payload = message.payload.decode("ascii")
    print("Incoming message for topic: '"+message.topic+"' with content: '"+message.payload+"'")
    if bojler is None:
        return
#    print("putting message to queue")
    bojler.command_queue.put((message.topic, message.payload))

def mqtt_connect():
    global mqtt_last_connect
    global mqtt_client
    mqtt_lock.acquire()
    if mqtt_last_connect is not None and (datetime.now() - mqtt_last_connect).total_seconds() > mqtt_retry:
        mqtt_lock.release()
        return

    mqtt_last_connect = datetime.now()

    mqtt_client = mqtt.Client(mqtt_client_id)
    mqtt_client.username_pw_set(mqtt_username, mqtt_password)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    eprint("Connecting to mqtt broker: "+mqtt_broker+":"+str(mqtt_port))
    mqtt_client.enable_logger()
    mqtt_client.connect(mqtt_broker, mqtt_port)
    mqtt_client.loop_start()
    mqtt_client.subscribe(mqtt_topic+"/control")
    mqtt_client.subscribe(mqtt_topic+"/control/#")
    mqtt_lock.release()

def send_mqtt(topic, value):
    global mqtt_connected
    global mqtt_client
    if not mqtt_connected:
        if mqtt_interval is not None:
            mqtt_connect()
        return
    mqtt_client.publish(mqtt_topic+"/"+topic, value)


class Bojler_Handler():
    def __init__(self, client, pair = False):
        self.bluetooth_retries = 15
        self.log_char = None
        self.main_char = None
        self.client = client
        self.comm_lock = threading.Lock()
        self.pair = pair
        self.scanner = None
        self.response_cond = threading.Condition()
        self.response = None
        self.command_queue = queue.Queue()

    async def handleNotification(self, cHandle, data):
        code = Packet_Number(int(chr(data[0])+chr(data[1])))
        del data[1]
        del data[0]
        
        if code == Packet_Number.GLOBAL_CONFIRMUID:
            if data[0] == UID_CONSUMPTION:
                del data[1]
                del data[0]
                del data[4:]
                consumption = int.from_bytes(data, "little")
                send_mqtt(Packet_Number.STATISTICS_GETALL.name, consumption)
                response = Packet_Number.STATISTICS_GETALL
        else:
            response = data.decode("ascii").rstrip('\x00')
#            eprint("Parsed: "+str(code)+": "+str(response))
            if code == Packet_Number.HOME_MODE:
                response = Bojler_Mode(int(response))
                send_mqtt(code.name, response.name)
            elif code == Packet_Number.HOME_HSRCSTATE or \
                code == Packet_Number.HDO_ONOFF or \
                code == Packet_Number.HDO_LESSEXPTARIFFAVAILABLENOW:
                response = True if response == "001" else False
                send_mqtt(code.name, str(response))
            elif code == Packet_Number.HOME_SENSOR1 or \
                code == Packet_Number.HOME_SENSOR2:
                response = float(response)
                send_mqtt(code.name, str(response))
            elif code == Packet_Number.HOME_TEMPERATURE or \
                code == Packet_Number.ANODE_VOLTAGE:
                response = int(response)
                send_mqtt(code.name, str(response))
 

    
    async def enable_hdo(self, enable):
        eprint("Setting HDO state to: "+str(enable))
        await asyncio.sleep(0.5)
        await self.send_request_long(Packet_Number.HDO_SET_ONOFF, 1 if enable else 0)
        await asyncio.sleep(0.5)

    async def set_mode(self, mode):
        eprint("Setting bojler mode to: "+str(mode))
        await asyncio.sleep(0.5)
        await self.send_request_long(Packet_Number.HOME_SETMODE, mode.value)
        await asyncio.sleep(0.5)
 
    async def set_temperature(self, temp):
        eprint("Setting temperature for normal mode: "+str(temp))
        await asyncio.sleep(0.5)
        await self.send_request_long(Packet_Number.HOME_SETNORMALTEMPERATURE, temp)
        await asyncio.sleep(0.5)

    async def send_request(self, value, arg=0x42):
        value = value.value
#        eprint("Get value: "+hex(value)+" with argument "+hex(arg & 0xff))
        self.response = None
        await self.client.write_gatt_char(self.main_char, bytes([value & 0xff, 0x00, arg & 0xff, 0x00, 0x00, 0x00, 0x00, 0x00]))
        await asyncio.sleep(0.1)

    async def send_request_long(self, value, arg=0x00):
        value = value.value
#        eprint("Get value: "+hex(value)+" with argument "+hex(arg & 0xff))
        self.response = None
        await self.client.write_gatt_char(self.main_char, bytes([value & 0xff, 0x00, 0x00, 0x00, arg & 0xff, 0x00, 0x00, 0x00]))
        await asyncio.sleep(0.1)

    def is_smart_bojler(self):
        return self.client.services.get_service(SBCORE_SERVICE_UUID) and self.client.services.get_service(LOG_SERVICE_UUID)

    async def ble_connect(self):
        if not self.client.is_connected:
            eprint("Connecting to device...")
            await self.client.connect()

        eprint("Connected: "+str(self.client.is_connected)+" Getting services.")

        if not self.is_smart_bojler():
            eprint("Device is not smart bojler.")
            return False

        time.sleep(0.1)
            
        srv = self.client.services.get_service(SBCORE_SERVICE_UUID)
        char = srv.get_characteristic(SBCORE_CHARACTERISTIC_UUID)
        if char is None:
            eprint("Core characteristic not found!")
            return False
        self.main_char = char

        time.sleep(0.1)
        srv = self.client.services.get_service(LOG_SERVICE_UUID)
        char = srv.get_characteristic(LOG_CHARACTERISTIC_UUID)
        if char is None:
            eprint("Log characteristic not found!")
            return False
        self.log_char = char

        await self.client.start_notify(self.main_char, self.handleNotification)
        await self.client.start_notify(self.log_char, self.handleNotification)

        # we're pairing as ID brmbrm. TODO: make it user-changeable
        await self.client.write_gatt_char(self.main_char, bytes([Packet_Number.RQ_GLOBAL_MAC.value, 0x00, 0x00, 0x00, 0x62, 0x72, 0x62, 0x62, 0x72, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]))
        if self.pair:
            eprint("Please enter the 4 digit pin visible on bojler unit and press enter")
            pin = int(input())
            await self.client.write_gatt_char(self.main_char, bytes([Packet_Number.GLOBAL_PAIRPIN.value, 0x00, 0x01, 0x00, pin & 0xff, (pin >> 8) & 0xff]))

        eprint("Connected")
        return True

    async def process_queue(self):
        try:
#            eprint("processing message queue")
            message = self.command_queue.get(False)
#            eprint("got message: t: "+message[0]+", p: "+message[1])
            if message[0] == mqtt_topic+"/control/enable_hdo":
                await bojler.enable_hdo(message[1].lower() == "true")
            elif message[0] == mqtt_topic+"/control/set_temperature":
                await bojler.set_temperature(int(message[1]))
            elif message[0] == mqtt_topic+"/control/set_mode":
                await bojler.set_mode(Bojler_Mode[message[1]])
        except Exception as e:
            if str(e) != "":
                eprint("Exception while processing queue: "+str(e))
            pass

    async def tick(self):
        if not self.client.is_connected:
            self.client.connect()
        # todo: code

        await self.send_request(Packet_Number.HOME_BOILERMODEL)
        await self.send_request(Packet_Number.HOME_FWVERSION)
        await self.send_request(Packet_Number.HOME_MODE)
        await self.send_request(Packet_Number.HOME_HSRCSTATE)
        await self.send_request(Packet_Number.HOME_SENSOR1)
        await self.send_request(Packet_Number.HOME_SENSOR2)
        await self.send_request(Packet_Number.HOME_TEMPERATURE)
        await self.send_request(Packet_Number.HDO_ONOFF)
        await self.send_request(Packet_Number.HDO_LASTHDOTIME)
        await self.send_request(Packet_Number.HDO_LESSEXPTARIFFAVAILABLENOW)
        await self.send_request(Packet_Number.HOME_BOILERNAME)
        await self.send_request(Packet_Number.STATISTICS_GETALL, UID_CONSUMPTION)
        await self.process_queue()
        return True


async def main():
    global bojler
    pair = len(sys.argv) > 1 and sys.argv[1] == "pair"
    eprint("Pair mode: "+str(pair))

    async with bleak.BleakClient(dzd_mac) as client:
        bojler = Bojler_Handler(client, pair)
        if not await bojler.ble_connect():
            return
        while True:
            if not await bojler.tick():
                break
            time.sleep(10)



#def main_loop():
#    data = read_data()
#    if not mqtt_connected:
#        eprint("Waiting for MQTT connection...")
#        mqtt_connect()
#        return

#    send_mqtt("temperature", data.temperature)

mqtt_connect()

asyncio.run(main())
