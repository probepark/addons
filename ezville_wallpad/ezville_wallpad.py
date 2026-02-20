import socket
import threading
import serial
import paho.mqtt.client as paho_mqtt
import json

import sys
import time
import logging
from logging.handlers import TimedRotatingFileHandler
from collections import defaultdict
import os.path
import re

RS485_DEVICE = {
    "light": {
        "state": {
            "id": 0x0E,
            "cmd": 0x81,
        },
        "last": {},
        "power": {
            "id": 0x0E,
            "cmd": 0x41,
            "ack": 0xC1,
        },
    },
    "thermostat": {
        "state": {
            "id": 0x36,
            "cmd": 0x81,
        },
        "last": {},
        "away": {
            "id": 0x36,
            "cmd": 0x46,
            "ack": 0xC6,
        },
        "target": {
            "id": 0x36,
            "cmd": 0x44,
            "ack": 0xC4,
        },
        "power": {
            "id": 0x36,
            "cmd": 0x43,
            "ack": 0xC3,
        },
    },
    "batch": {  # 안보임
        "state": {"id": 0x33, "cmd": 0x81},
        "press": {"id": 0x33, "cmd": 0x41, "ack": 0xC1},
    },
    "plug": {
        "state": {"id": 0x39, "cmd": 0x81},
        "power": {"id": 0x39, "cmd": 0x41, "ack": 0xC1},
    },
}

DISCOVERY_DEVICE = {
    "ids": [
        "ezville_wallpad",
    ],
    "name": "ezville_wallpad",
    "mf": "EzVille",
    "mdl": "EzVille Wallpad",
    "sw": "probepark/ha_addons/ezville_wallpad",
}

DISCOVERY_PAYLOAD = {
    "light": [
        {
            "_intg": "light",
            "~": "{prefix}/light/{grp}_{rm}_{id}",
            "name": "{prefix}_light_{grp}_{rm}_{id}",
            "opt": True,
            "stat_t": "~/power/state",
            "cmd_t": "~/power/command",
        }
    ],
    "thermostat": [
        {
            "_intg": "climate",
            "~": "{prefix}/thermostat/{grp}_{id}",
            "name": "{prefix}_thermostat_{grp}_{id}",
            "mode_stat_t": "~/power/state",
            "mode_cmd_t": "~/power/command",
            "temp_stat_t": "~/target/state",
            "temp_cmd_t": "~/target/command",
            "curr_temp_t": "~/current/state",
            "away_stat_t": "~/away/state",
            "away_cmd_t": "~/away/command",
            "modes": ["off", "heat"],
            "min_temp": 5,
            "max_temp": 40,
        }
    ],
    "plug": [
        {
            "_intg": "switch",
            "~": "{prefix}/plug/{idn}/power",
            "name": "{prefix}_plug_{idn}",
            "stat_t": "~/state",
            "cmd_t": "~/command",
            "icon": "mdi:power-plug",
        },
        {
            "_intg": "sensor",
            "~": "{prefix}/plug/{idn}",
            "name": "{prefix}_plug_{idn}_power_usage",
            "stat_t": "~/current/state",
            "unit_of_meas": "W",
        },
    ],
    "cutoff": [
        {
            "_intg": "switch",
            "~": "{prefix}/cutoff/{idn}/power",
            "name": "{prefix}_light_cutoff_{idn}",
            "stat_t": "~/state",
            "cmd_t": "~/command",
        }
    ],
    "energy": [
        {
            "_intg": "sensor",
            "~": "{prefix}/energy/{idn}",
            "name": "_",
            "stat_t": "~/current/state",
            "unit_of_meas": "_",
            "val_tpl": "_",
        }
    ],
}

STATE_HEADER = {
    prop["state"]["id"]: (device, prop["state"]["cmd"])
    for device, prop in RS485_DEVICE.items()
    if "state" in prop
}
ACK_HEADER = {
    prop[cmd]["id"]: (device, prop[cmd]["ack"])
    for device, prop in RS485_DEVICE.items()
    for cmd, code in prop.items()
    if "ack" in code
}
ACK_MAP = defaultdict(lambda: defaultdict(dict))
for device, prop in RS485_DEVICE.items():
    for cmd, code in prop.items():
        if "ack" in code:
            ACK_MAP[code["id"]][code["cmd"]] = code["ack"]

HEADER_0_FIRST = [[0x12, 0x01], [0x12, 0x0F]]
header_0_first_candidate = [[[0x33, 0x01], [0x33, 0x0F]], [[0x36, 0x01], [0x36, 0x0F]]]

serial_queue = {}
serial_ack = {}

last_query = int(0).to_bytes(2, "big")
last_topic_list = {}

mqtt = paho_mqtt.Client(paho_mqtt.CallbackAPIVersion.VERSION2)
mqtt_connected = False

logger = logging.getLogger(__name__)

# ----- EzVilleSerial 클래스는 동일 -----
class EzVilleSerial:
    def __init__(self):
        self._ser = serial.Serial()
        self._ser.port = Options["serial"]["port"]
        self._ser.baudrate = Options["serial"]["baudrate"]
        self._ser.bytesize = Options["serial"]["bytesize"]
        self._ser.parity = Options["serial"]["parity"]
        self._ser.stopbits = Options["serial"]["stopbits"]

        self._ser.close()
        self._ser.open()

        self._pending_recv = 0

        self.set_timeout(5.0)
        data = self._recv_raw(1)
        self.set_timeout(None)
        if not data:
            logger.critical("no active packet at this serial port!")

    def _recv_raw(self, count=1):
        return self._ser.read(count)

    def recv(self, count=1):
        self._pending_recv = max(self._pending_recv - count, 0)
        return self._recv_raw(count)

    def send(self, a):
        self._ser.write(a)

    def set_pending_recv(self):
        self._pending_recv = self._ser.in_waiting

    def check_pending_recv(self):
        return self._pending_recv

    def check_in_waiting(self):
        return self._ser.in_waiting

    def set_timeout(self, a):
        self._ser.timeout = a

# ----- EzVilleSocket 클래스는 동일 -----
class EzVilleSocket:
    def __init__(self, addr, port, capabilities="ALL"):
        self.capabilities = capabilities
        self._addr = addr
        self._port = port
        self._soc = socket.socket()
        self._soc.settimeout(10)
        self._soc.connect((addr, port))

        self._recv_buf = bytearray()
        self._pending_recv = 0

        # 타임아웃 5초로 설정한 후, 초기 패킷이 수신될 때까지 무한 재시도
        self.set_timeout(5.0)
        while True:
            try:
                data = self._recv_raw(1)
                if data:
                    logger.info("초기 패킷 수신 성공")
                    break
            except socket.timeout as e:
                logger.warning("초기 패킷 수신 타임아웃 발생, 재시도 중...: %s", e)
        self.set_timeout(None)

    def _recv_raw(self, count=1):
        return self._soc.recv(count)

    def recv(self, count=1):
        if len(self._recv_buf) < count:
            try:
                new_data = self._recv_raw(128)
                if not new_data:
                    raise OSError("소켓 연결 끊김 (recv returned empty)")
                self._recv_buf.extend(new_data)
            except socket.timeout:
                raise  # 타임아웃은 그대로 전파
        if len(self._recv_buf) < count:
            return None

        self._pending_recv = max(self._pending_recv - count, 0)

        res = self._recv_buf[0:count]
        del self._recv_buf[0:count]
        return res

    def send(self, a):
        self._soc.sendall(a)

    def set_pending_recv(self):
        self._pending_recv = len(self._recv_buf)

    def check_pending_recv(self):
        return self._pending_recv

    def check_in_waiting(self):
        if len(self._recv_buf) == 0:
            new_data = self._recv_raw(128)
            self._recv_buf.extend(new_data)
        return len(self._recv_buf)

    def set_timeout(self, a):
        self._soc.settimeout(a)

# 로거 초기화 등 함수들 그대로 유지
def init_logger():
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def init_logger_file():
    if Options["log"]["to_file"]:
        filename = Options["log"]["filename"]
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler = TimedRotatingFileHandler(
            os.path.abspath(Options["log"]["filename"]), when="midnight", backupCount=7
        )
        handler.setFormatter(formatter)
        handler.suffix = "%Y%m%d"
        logger.addHandler(handler)

def init_option(argv):
    global Options
    if len(argv) == 1:
        option_file = "./options_standalone.json"
    else:
        option_file = argv[1]

    default_file = os.path.join(os.path.dirname(os.path.abspath(argv[0])), "config.json")

    with open(default_file, encoding="utf-8") as f:
        config = json.load(f)
        logger.info("addon version %s", config["version"])
        Options = config["options"]
    with open(option_file, encoding="utf-8") as f:
        Options2 = json.load(f)

    for k, v in Options.items():
        if isinstance(v, dict) and k in Options2:
            Options[k].update(Options2[k])
            for k2 in Options[k].keys():
                if k2 not in Options2[k].keys():
                    logger.warning(
                        "no configuration value for '%s:%s'! try default value (%s)...",
                        k,
                        k2,
                        Options[k][k2],
                    )
        else:
            if k not in Options2:
                logger.warning(
                    "no configuration value for '%s'! try default value (%s)...",
                    k,
                    Options[k],
                )
            else:
                Options[k] = Options2[k]

    Options["mqtt"]["server"] = re.sub("[a-z]*://", "", Options["mqtt"]["server"])
    if Options["mqtt"]["server"] == "127.0.0.1":
        logger.warning("MQTT server address should be changed!")
    Options["mqtt"]["_discovery"] = Options["mqtt"]["discovery"]

# MQTT 관련 함수들도 동일
def mqtt_discovery(payload):
    intg = payload.pop("_intg")
    payload["device"] = DISCOVERY_DEVICE
    payload["uniq_id"] = payload["name"]
    topic = f"homeassistant/{intg}/ezville_wallpad/{payload['name']}/config"
    logger.info("Add new device: %s", topic)
    mqtt.publish(topic, json.dumps(payload))

def mqtt_debug(topics, payload):
    device = topics[2]
    command = topics[3]
    if device == "packet":
        if command == "send":
            packet = bytearray.fromhex(payload)
            packet[-2], packet[-1] = serial_generate_checksum(packet)
            packet = bytes(packet)
            logger.info("prepare packet:  {}".format(packet.hex()))
            serial_queue[packet] = time.time()

def mqtt_device(topics, payload):
    device = topics[1]
    idn = topics[2]
    cmd = topics[3]
    if device not in RS485_DEVICE:
        logger.error("    unknown device!")
        return
    if cmd not in RS485_DEVICE[device]:
        logger.error("    unknown command!")
        return
    if payload == "":
        logger.error("    no payload!")
        return

    cmd = RS485_DEVICE[device][cmd]
    packet = None

    # 명령 생성 예시 (light, thermostat, plug)
    if device == "light":
        if payload == "ON":
            payload = 0x01
        elif payload == "OFF":
            payload = 0x00
        length = 10
        packet = bytearray(length)
        packet[0] = 0xF7
        packet[1] = cmd["id"]
        packet[2] = int(idn.split("_")[0]) << 4 | int(idn.split("_")[1])
        packet[3] = cmd["cmd"]
        packet[4] = 0x03
        packet[5] = int(idn.split("_")[2])
        packet[6] = payload
        packet[7] = 0x00
        packet[8], packet[9] = serial_generate_checksum(packet)
    elif device == "thermostat":
        if payload == "heat":
            payload = 0x01
        elif payload == "off":
            payload = 0x00
        length = 8
        packet = bytearray(length)
        packet[0] = 0xF7
        packet[1] = cmd["id"]
        packet[2] = int(idn.split("_")[0]) << 4 | int(idn.split("_")[1])
        packet[3] = cmd["cmd"]
        packet[4] = 0x01
        packet[5] = int(float(payload))
        packet[6], packet[7] = serial_generate_checksum(packet)
    elif device == "plug":
        length = 8
        packet = bytearray(length)
        packet[0] = 0xF7
        packet[1] = cmd["id"]
        packet[2] = int(idn.split("_")[0]) << 4 | int(idn.split("_")[1])
        packet[3] = cmd["cmd"]
        packet[4] = 0x01
        packet[5] = 0x11 if payload == "ON" else 0x10
        packet[6], packet[7] = serial_generate_checksum(packet)

    if packet:
        packet = bytes(packet)
        serial_queue[packet] = time.time()

def mqtt_init_discovery():
    Options["mqtt"]["_discovery"] = Options["mqtt"]["discovery"]
    for device in RS485_DEVICE:
        RS485_DEVICE[device]["last"] = {}
    global last_topic_list
    last_topic_list = {}

def mqtt_on_message(mqtt, userdata, msg):
    topics = msg.topic.split("/")
    payload = msg.payload.decode()
    logger.info("recv. from HA:   %s = %s", msg.topic, payload)

    device = topics[1]
    if device == "status":
        if payload == "online":
            mqtt_init_discovery()
    elif device == "debug":
        mqtt_debug(topics, payload)
    else:
        mqtt_device(topics, payload)

def mqtt_on_connect(mqtt, userdata, flags, rc, properties):
    if rc == 0:
        logger.info("MQTT connect successful!")
        global mqtt_connected
        mqtt_connected = True
    else:
        logger.error("MQTT connection return with:  %s", paho_mqtt.connack_string(rc))

    mqtt_init_discovery()

    topic = "homeassistant/status"
    logger.info("subscribe %s", topic)
    mqtt.subscribe(topic, 0)

    prefix = Options["mqtt"]["prefix"]
    if Options["wallpad_mode"] != "off":
        topic = f"{prefix}/+/+/+/command"
        logger.info("subscribe %s", topic)
        mqtt.subscribe(topic, 0)

def mqtt_on_disconnect(client, userdata, flags, rc, properties):
    logger.warning("MQTT disconnected! (%s)", rc)
    global mqtt_connected
    mqtt_connected = False

def start_mqtt_loop():
    logger.info("initialize mqtt...")
    mqtt.on_message = mqtt_on_message
    mqtt.on_connect = mqtt_on_connect
    mqtt.on_disconnect = mqtt_on_disconnect

    if Options["mqtt"]["need_login"]:
        mqtt.username_pw_set(Options["mqtt"]["user"], Options["mqtt"]["passwd"])

    try:
        mqtt.connect(Options["mqtt"]["server"], Options["mqtt"]["port"])
    except Exception as e:
        logger.error("MQTT server address/port may be incorrect! (%s)", e)
        sys.exit(1)

    mqtt.loop_start()
    delay = 1
    while not mqtt_connected:
        logger.info("waiting MQTT connected ...")
        time.sleep(delay)
        delay = min(delay * 2, 10)

def serial_verify_checksum(packet):
    checksum = 0
    for b in packet[:-1]:
        checksum ^= b

    add = sum(packet[:-1]) & 0xFF
    if checksum or add != packet[-1]:
        logger.warning(
            "checksum fail! {}, {:02x}, {:02x}".format(packet.hex(), checksum, add)
        )
        return False
    return True

def serial_generate_checksum(packet):
    checksum = 0
    for b in packet[:-1]:
        checksum ^= b
    add = (sum(packet) + checksum) & 0xFF
    return checksum, add

def serial_new_device(device, packet, idn=None):
    prefix = Options["mqtt"]["prefix"]
    if device == "light":
        grp_id = int(packet[2] >> 4)
        rm_id = int(packet[2] & 0x0F)
        light_count = int(packet[4]) - 1
        for light_id in range(1, light_count + 1):
            payload = DISCOVERY_PAYLOAD[device][0].copy()
            payload["~"] = payload["~"].format(prefix=prefix, grp=grp_id, rm=rm_id, id=light_id)
            payload["name"] = payload["name"].format(prefix=prefix, grp=grp_id, rm=rm_id, id=light_id)
            mqtt_discovery(payload)

    elif device == "thermostat":
        grp_id = int(packet[2] >> 4)
        room_count = int((int(packet[4]) - 5) / 2)
        for room_id in range(1, room_count + 1):
            payload = DISCOVERY_PAYLOAD[device][0].copy()
            payload["~"] = payload["~"].format(prefix=prefix, grp=grp_id, id=room_id)
            payload["name"] = payload["name"].format(prefix=prefix, grp=grp_id, id=room_id)
            mqtt_discovery(payload)

    elif device == "plug":
        grp_id = int(packet[2] >> 4)
        plug_count = int(packet[4] / 3)
        for plug_id in range(1, plug_count + 1):
            payload = DISCOVERY_PAYLOAD[device][0].copy()
            payload["~"] = payload["~"].format(prefix=prefix, idn=f"{grp_id}_{plug_id}")
            payload["name"] = payload["name"].format(prefix=prefix, idn=f"{grp_id}_{plug_id}")
            mqtt_discovery(payload)
    elif device in DISCOVERY_PAYLOAD:
        for payloads in DISCOVERY_PAYLOAD[device]:
            payload = payloads.copy()
            payload["~"] = payload["~"].format(prefix=prefix, idn=idn)
            payload["name"] = payload["name"].format(prefix=prefix, idn=idn)
            if device == "energy":
                payload["name"] = "{}_{}_consumption".format(prefix, ("power", "gas", "water")[idn])
                payload["unit_of_meas"] = ("W", "m³/h", "m³/h")[idn]
                payload["val_tpl"] = (
                    "{{ value }}",
                    "{{ value | float / 100 }}",
                    "{{ value | float / 100 }}",
                )[idn]
            mqtt_discovery(payload)

def serial_receive_state(device, packet):
    form = RS485_DEVICE[device]["state"]
    last = RS485_DEVICE[device]["last"]
    idn = (packet[1] << 8) | packet[2]

    if last.get(idn) == packet:
        return

    if Options["mqtt"]["_discovery"] and not last.get(idn):
        serial_new_device(device, packet, idn)
        last[idn] = True
        return
    else:
        last[idn] = packet

    prefix = Options["mqtt"]["prefix"]

    if device == "light":
        grp_id = int(packet[2] >> 4)
        rm_id = int(packet[2] & 0x0F)
        light_count = int(packet[4]) - 1
        for light_id in range(1, light_count + 1):
            topic = f"{prefix}/{device}/{grp_id}_{rm_id}_{light_id}/power/state"
            value = "ON" if packet[5 + light_id] & 1 else "OFF"
            if last_topic_list.get(topic) != value:
                logger.debug("publish to HA:   %s = %s (%s)", topic, value, packet.hex())
                mqtt.publish(topic, value)
                last_topic_list[topic] = value

    elif device == "thermostat":
        grp_id = int(packet[2] >> 4)
        room_count = int((int(packet[4]) - 5) / 2)
        for thermostat_id in range(1, room_count + 1):
            # power on/off: packet[7] bitmask (1=ON)
            if ((packet[7] & 0x1F) >> (room_count - thermostat_id)) & 1:
                power_mode = "heat"
            else:
                power_mode = "off"
            # away mode: packet[6] bitmask
            if ((packet[6] & 0x1F) >> (room_count - thermostat_id)) & 1:
                away_val = "ON"
            else:
                away_val = "OFF"

            target_temp = packet[8 + thermostat_id * 2]
            current_temp = packet[9 + thermostat_id * 2]

            for sub_topic, value in zip(
                ["power", "away", "target", "current"],
                [
                    power_mode,
                    away_val,
                    target_temp,
                    current_temp,
                ],
            ):
                topic = f"{prefix}/{device}/{grp_id}_{thermostat_id}/{sub_topic}/state"
                if last_topic_list.get(topic) != value:
                    logger.debug("publish to HA:   %s = %s (%s)", topic, value, packet.hex())
                    mqtt.publish(topic, value)
                    last_topic_list[topic] = value

    elif device == "plug":
        grp_id = int(packet[2] >> 4)
        plug_count = int(packet[4] / 3)
        for plug_id in range(1, plug_count + 1):
            # 예: plug_id * 3 + 3, 4, 5
            for sub_topic, value in zip(
                ["power", "current"],
                [
                    "ON" if packet[plug_id * 3 + 3] & 0x10 else "OFF",
                    f"{format(packet[plug_id * 3 + 4], 'x')}.{format(packet[plug_id * 3 + 5], 'x')}",
                ],
            ):
                topic = f"{prefix}/{device}/{grp_id}_{plug_id}/{sub_topic}/state"
                if last_topic_list.get(topic) != value:
                    logger.debug("publish to HA:   %s = %s (%s)", topic, value, packet.hex())
                    mqtt.publish(topic, value)
                    last_topic_list[topic] = value

def serial_get_header(conn):
    try:
        while True:
            header_0 = conn.recv(1)[0]
            if header_0 == 0xF7:
                break
        while True:
            header_1 = conn.recv(1)[0]
            if header_1 != 0xF7:
                break
            header_0 = header_1
        header_2 = conn.recv(1)[0]
        header_3 = conn.recv(1)[0]
    except socket.timeout:
        # 타임아웃 — 연결은 살아있지만 데이터 없음, 큐 명령 처리 기회 제공
        return None, None, None, None
    except (OSError, serial.SerialException) as e:
        logger.error("header 수신 중 예외 발생: %s", e)
        return 0, 0, 0, 0
    return header_0, header_1, header_2, header_3

def serial_ack_command(packet):
    logger.info("ack from device: {} ({:x})".format(serial_ack[packet].hex(), packet))
    serial_queue.pop(serial_ack[packet], None)
    serial_ack.pop(packet)

def serial_send_command(conn):
    cmd = next(iter(serial_queue))
    if conn.capabilities != "ALL" and ACK_HEADER[cmd[1]][0] not in conn.capabilities:
        return
    conn.send(cmd)
    ack = bytearray(cmd[0:4])
    ack[3] = ACK_MAP[cmd[1]][cmd[3]]
    waive_ack = False
    if ack[3] == 0x00:
        waive_ack = True
    ack = int.from_bytes(ack, "big")

    elapsed = time.time() - serial_queue[cmd]
    if elapsed > Options["rs485"]["max_retry"]:
        logger.error("send to device:  %s max retry time exceeded!", cmd.hex())
        serial_queue.pop(cmd)
        serial_ack.pop(ack, None)
    elif elapsed > 3:
        logger.warning(
            "send to device:  {}, try another {:.01f} seconds...".format(
                cmd.hex(), Options["rs485"]["max_retry"] - elapsed
            )
        )
        serial_ack[ack] = cmd
    elif waive_ack:
        logger.info("waive ack:  %s", cmd.hex())
        serial_queue.pop(cmd)
        serial_ack.pop(ack, None)
    else:
        logger.info("send to device:  %s", cmd.hex())
        serial_ack[ack] = cmd

# 헬스체크: 데이터 수신 모니터링
_last_data_time = time.time()
_health_check_running = True

def health_check_thread():
    """데이터 수신 여부 모니터링 — 5분간 데이터 없으면 경고"""
    global _last_data_time, _health_check_running
    while _health_check_running:
        time.sleep(60)
        silent = time.time() - _last_data_time
        if silent > 300:
            logger.warning("헬스체크: %.0f초 동안 데이터 수신 없음 — 연결 상태 확인 필요", silent)
        elif silent > 120:
            logger.info("헬스체크: %.0f초 동안 데이터 수신 없음", silent)

# ★★★ 여기가 핵심: daemon()을 감싸는 함수 추가 ★★★
def run_daemon(conn):
    """
    소켓/시리얼이 끊기더라도 재연결을 시도하기 위한 래퍼 함수
    예외가 발생하면 일정 시간 대기 후 다시 연결 후 daemon 실행
    """
    while True:
        try:
            logger.info("daemon() 시작 - 연결 유지 중...")
            daemon(conn)
        except (OSError, serial.SerialException, socket.error) as e:
            logger.warning(f"연결이 끊어졌습니다. 5초 후 재시도합니다: {e}")
            time.sleep(5)
            try:
                # 재연결 시도
                if isinstance(conn, EzVilleSocket):
                    logger.info("소켓 재연결 시도...")
                    conn._soc.close()
                    conn._soc = socket.socket()
                    conn._soc.settimeout(10)
                    conn._soc.connect((conn._addr, conn._port))
                    conn._recv_buf = bytearray()
                    conn._pending_recv = 0
                else:
                    logger.info("시리얼 재연결 시도...")
                    conn._ser.close()
                    conn._ser.open()
                logger.info("재연결 성공! daemon 재시작합니다.")
            except Exception as e2:
                logger.error(f"재연결 실패: {e2}. 다시 대기합니다.")
                time.sleep(5)

def daemon(conn):
    global _last_data_time
    logger.info("start loop ...")
    scan_count = 0
    send_aggressive = False

    # 소켓에 타임아웃 설정 — recv 블로킹 방지 (5초)
    if isinstance(conn, EzVilleSocket):
        conn.set_timeout(5.0)

    while True:
        sys.stdout.flush()

        header_0, header_1, header_2, header_3 = serial_get_header(conn)

        # 타임아웃 — 데이터는 없지만 연결은 살아있음, 큐된 명령 처리
        if header_0 is None:
            if serial_queue:
                try:
                    serial_send_command(conn=conn)
                except Exception as e:
                    logger.error("타임아웃 중 명령 송신 실패: %s", e)
                    raise OSError(f"명령 송신 중 소켓 오류: {e}")
            continue

        if (header_0, header_1, header_2, header_3) == (0, 0, 0, 0):
            # 헤더를 못 읽었으면(에러), 재연결 루프로 빠지도록 예외 발생
            raise OSError("잘못된 헤더 수신(연결 끊김 또는 기타 에러)")

        _last_data_time = time.time()

        if header_1 in STATE_HEADER and header_3 in STATE_HEADER[header_1]:
            device = STATE_HEADER[header_1][0]
            header_4 = conn.recv(1)[0]
            data_length = int(header_4)
            packet = bytes([header_0, header_1, header_2, header_3, header_4])
            packet += conn.recv(data_length + 2)
            if not serial_verify_checksum(packet):
                continue

            if serial_queue and not conn.check_pending_recv():
                serial_send_command(conn=conn)
                conn.set_pending_recv()
            serial_receive_state(device, packet)

        elif header_1 in ACK_HEADER and header_3 in ACK_HEADER[header_1]:
            header_val = header_0 << 24 | header_1 << 16 | header_2 << 8 | header_3
            if header_val in serial_ack:
                serial_ack_command(header_val)

        elif header_3 in (0x81, 0x8F, 0x0F) or send_aggressive:
            scan_count += 1
            if serial_queue and not conn.check_pending_recv():
                serial_send_command(conn=conn)
                conn.set_pending_recv()

def init_connect(conn):
    dump_time = Options["rs485"]["dump_time"]
    if dump_time > 0:
        if dump_time < 10:
            logger.warning("dump_time is too short! automatically changed to 10 seconds...")
            dump_time = 10

        start_time = time.time()
        logger.warning("packet dump for {} seconds!".format(dump_time))

        conn.set_timeout(2)
        logs = []
        while time.time() - start_time < dump_time:
            try:
                data = conn.recv(128)
            except:
                continue
            if data:
                for b in data:
                    if b == 0xF7 or len(logs) > 500:
                        logger.info("".join(logs))
                        logs = ["{:02X}".format(b)]
                    else:
                        logs.append(",  {:02X}".format(b))
        logger.info("".join(logs))
        logger.warning("dump done.")
        conn.set_timeout(None)

if __name__ == "__main__":
    init_logger()
    init_option(sys.argv)
    init_logger_file()
    start_mqtt_loop()

    # 헬스체크 스레드 시작
    hc_thread = threading.Thread(target=health_check_thread, daemon=True)
    hc_thread.start()

    if Options["serial_mode"] == "sockets":
        for _socket in Options["sockets"]:
            conn = EzVilleSocket(_socket["address"], _socket["port"], _socket["capabilities"])
            init_connect(conn=conn)
            thread = threading.Thread(target=run_daemon, args=(conn,))
            thread.daemon = True
            thread.start()
        while True:
            time.sleep(10 ** 8)
    elif Options["serial_mode"] == "socket":
        logger.info("initialize socket...")
        conn = EzVilleSocket(Options["socket"]["address"], Options["socket"]["port"])
        init_connect(conn=conn)
        try:
            run_daemon(conn=conn)  # ★ daemon() 대신 run_daemon()
        except:
            logger.exception("addon finished!")
    else:
        logger.info("initialize serial...")
        conn = EzVilleSerial()
        init_connect(conn=conn)
        try:
            run_daemon(conn=conn)  # ★ daemon() 대신 run_daemon()
        except:
            logger.exception("addon finished!")
