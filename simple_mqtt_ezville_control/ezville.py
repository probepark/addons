import paho.mqtt.client as mqtt
import json
import time
import asyncio
import socket
import random
import signal
import sys
from queue import Queue
from typing import Dict, List, Any, Tuple, Optional

# DEVICE 별 패킷 정보
RS485_DEVICE = {
    "light": {
        "state": {"id": "0E", "cmd": "81"},
        "power": {"id": "0E", "cmd": "41", "ack": "C1"},
    },
    "thermostat": {
        "state": {"id": "36", "cmd": "81"},
        "power": {"id": "36", "cmd": "43", "ack": "C3"},
        "away": {"id": "36", "cmd": "45", "ack": "C5"},
        "target": {"id": "36", "cmd": "44", "ack": "C4"},
    },
    "plug": {
        "state": {"id": "39", "cmd": "81"},
        "power": {"id": "39", "cmd": "41", "ack": "C1"},
    },
    "gasvalve": {
        "state": {"id": "12", "cmd": "81"},
        "power": {"id": "12", "cmd": "41", "ack": "C1"},  # 잠그기만 가능
    },
    "batch": {
        "state": {"id": "33", "cmd": "81"},
        "press": {"id": "33", "cmd": "41", "ack": "C1"},
    },
    "door": {
        "state": {"id": "40", "cmd": "82"},
        "open": {"id": "40", "cmd": "10", "ack": "90"},
    },
}

# MQTT Discovery를 위한 Preset 정보
DISCOVERY_DEVICE = {
    "ids": ["ezville_wallpad"],
    "name": "ezville_wallpad",
    "mf": "EzVille",
    "mdl": "EzVille Wallpad",
    "sw": "probepark/addons/ezville_wallpad",
}

# MQTT Discovery를 위한 Payload 정보
DISCOVERY_PAYLOAD = {
    "light": [
        {
            "_intg": "light",
            "~": "ezville/light_{:0>2d}_{:0>2d}",
            "name": "ezville_light_{:0>2d}_{:0>2d}",
            "opt": True,
            "stat_t": "~/power/state",
            "cmd_t": "~/power/command",
        }
    ],
    "thermostat": [
        {
            "_intg": "climate",
            "~": "ezville/thermostat_{:0>2d}_{:0>2d}",
            "name": "ezville_thermostat_{:0>2d}_{:0>2d}",
            "mode_cmd_t": "~/power/command",
            "mode_stat_t": "~/power/state",
            "temp_stat_t": "~/setTemp/state",
            "temp_cmd_t": "~/setTemp/command",
            "curr_temp_t": "~/curTemp/state",
            "modes": ["heat", "off"],
            "min_temp": "5",
            "max_temp": "40",
        }
    ],
    "plug": [
        {
            "_intg": "switch",
            "~": "ezville/plug_{:0>2d}_{:0>2d}",
            "name": "ezville_plug_{:0>2d}_{:0>2d}",
            "stat_t": "~/power/state",
            "cmd_t": "~/power/command",
            "icon": "mdi:leaf",
        },
        {
            "_intg": "binary_sensor",
            "~": "ezville/plug_{:0>2d}_{:0>2d}",
            "name": "ezville_plug-automode_{:0>2d}_{:0>2d}",
            "stat_t": "~/auto/state",
            "icon": "mdi:leaf",
        },
        {
            "_intg": "sensor",
            "~": "ezville/plug_{:0>2d}_{:0>2d}",
            "name": "ezville_plug_{:0>2d}_{:0>2d}_powermeter",
            "stat_t": "~/current/state",
            "unit_of_meas": "W",
        },
    ],
    "gasvalve": [
        {
            "_intg": "switch",
            "~": "ezville/gasvalve_{:0>2d}_{:0>2d}",
            "name": "ezville_gasvalve_{:0>2d}_{:0>2d}",
            "stat_t": "~/power/state",
            "cmd_t": "~/power/command",
            "icon": "mdi:valve",
        }
    ],
    "batch": [
        {
            "_intg": "button",
            "~": "ezville/batch_{:0>2d}_{:0>2d}",
            "name": "ezville_batch-elevator-up_{:0>2d}_{:0>2d}",
            "cmd_t": "~/elevator-up/command",
            "icon": "mdi:elevator-up",
        },
        {
            "_intg": "button",
            "~": "ezville/batch_{:0>2d}_{:0>2d}",
            "name": "ezville_batch-elevator-down_{:0>2d}_{:0>2d}",
            "cmd_t": "~/elevator-down/command",
            "icon": "mdi:elevator-down",
        },
        {
            "_intg": "binary_sensor",
            "~": "ezville/batch_{:0>2d}_{:0>2d}",
            "name": "ezville_batch-groupcontrol_{:0>2d}_{:0>2d}",
            "stat_t": "~/group/state",
            "icon": "mdi:lightbulb-group",
        },
        {
            "_intg": "binary_sensor",
            "~": "ezville/batch_{:0>2d}_{:0>2d}",
            "name": "ezville_batch-outing_{:0>2d}_{:0>2d}",
            "stat_t": "~/outing/state",
            "icon": "mdi:home-circle",
        },
    ],
    "door": [
        {
            "_intg": "lock",
            "~": "ezville/door_{:0>2d}_{:0>2d}",
            "name": "ezville_door_{:0>2d}_{:0>2d}",
            "stat_t": "~/lock/state",
            "cmd_t": "~/lock/command",
            "icon": "mdi:door",
            "pl_lock": "LOCK",
            "pl_unlk": "UNLOCK",
        },
    ],
}

# 상태 및 ACK 헤더 초기화
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

CONFIG_DIR = "/data"
HA_TOPIC = "ezville"
STATE_TOPIC = f"{HA_TOPIC}/{{}}/{{}}/state"
EW11_TOPIC = "ew11"
EW11_SEND_TOPIC = f"{EW11_TOPIC}/send"

# Connection health tracking
HEALTH_CHECK_INTERVAL = 60  # seconds
MAX_SILENT_PERIOD = 120  # seconds without data = connection problem


# 로깅 함수
def log(string: str, level: str = "INFO") -> None:
    date = time.strftime("%Y-%m-%d %p %I:%M:%S", time.localtime(time.time()))
    print(f"[{date}] [{level}] {string}", flush=True)


# 체크섬 계산
def checksum(input_hex: str) -> Optional[str]:
    try:
        input_hex = input_hex[:-4]
        packet = bytes.fromhex(input_hex)
        checksum_val = 0
        for b in packet:
            checksum_val ^= b
        add = (sum(packet) + checksum_val) & 0xFF
        return input_hex + f"{checksum_val:02X}{add:02X}"
    except Exception as e:
        log(f"Checksum 계산 오류: {e}", "ERROR")
        return None


# 설정 관리 클래스
class Config:
    def __init__(self, config_path: str):
        with open(config_path) as f:
            self.data = json.load(f)
        self.validate()

    def validate(self):
        required = ["mqtt_server", "mqtt_id", "mqtt_password", "mode", "ew11_server", "ew11_port"]
        for key in required:
            if key not in self.data:
                raise ValueError(f"설정에 {key}가 누락되었습니다.")

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)


# MQTT 클라이언트 관리
class MQTTHandler:
    RECONNECT_DELAY_MIN = 1
    RECONNECT_DELAY_MAX = 60

    def __init__(self, config: Config):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "mqtt-ezville")
        self.client.username_pw_set(config["mqtt_id"], config["mqtt_password"])
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.reconnect_delay_set(self.RECONNECT_DELAY_MIN, self.RECONNECT_DELAY_MAX)
        self.config = config
        self.msg_queue = asyncio.Queue(maxsize=1000)
        self.online = False
        self._reconnect_count = 0
        self._last_message_time = time.time()

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            self._reconnect_count = 0
            session_present = flags.get('session present', 0) if isinstance(flags, dict) else getattr(flags, 'session_present', 0)
            log(f"MQTT Broker 연결 성공 (session_present={session_present})")
            topics = [(f"{HA_TOPIC}/#", 0), ("homeassistant/status", 0), (f"{HA_TOPIC}/debug/send", 0)]
            if self.config["mode"] in ["mixed", "mqtt"]:
                topics.append((f"{EW11_TOPIC}/recv", 0))
            if self.config["mode"] == "mqtt":
                topics.append((f"{EW11_TOPIC}/send", 1))
            result = client.subscribe(topics)
            log(f"MQTT 구독 요청: {[t[0] for t in topics]}, result={result}")
            self.online = True
            self._last_message_time = time.time()
        else:
            RC_CODES = {1: "프로토콜 버전 불일치", 2: "클라이언트ID 거부", 3: "브로커 불가",
                        4: "인증 실패", 5: "권한 없음"}
            log(f"MQTT 연결 실패: rc={rc} ({RC_CODES.get(rc, '알 수 없음')})", "ERROR")

    def on_disconnect(self, client, userdata, rc, *args):
        self.online = False
        self._reconnect_count += 1
        if rc != 0:
            log(f"MQTT 예기치 않은 연결 해제 (rc={rc}), 자동 재연결 시도 #{self._reconnect_count}", "WARNING")
        else:
            log("MQTT 정상 연결 해제")

    def on_subscribe(self, client, userdata, mid, granted_qos, *args):
        log(f"MQTT 구독 확인: mid={mid}, granted_qos={granted_qos}")

    def on_message(self, client, userdata, msg):
        self._last_message_time = time.time()
        if not hasattr(self, '_msg_count'):
            self._msg_count = 0
            self._msg_count_by_topic = {}
        self._msg_count += 1
        topic_key = msg.topic.split('/')[0]
        self._msg_count_by_topic[topic_key] = self._msg_count_by_topic.get(topic_key, 0) + 1
        if msg.topic == "homeassistant/status":
            status = msg.payload.decode("utf-8")
            self.online = (status == "online")
            log(f"MQTT Integration {'온라인' if self.online else '오프라인'}")
        else:
            try:
                self.msg_queue.put_nowait(msg)
            except asyncio.QueueFull:
                log("메시지 큐가 가득 찼습니다. 메시지 드롭", "WARNING")

    async def start(self):
        self.client.connect_async(self.config["mqtt_server"], keepalive=60)
        self.client.loop_start()
        # Wait for connection with timeout
        timeout = 30
        while not self.online and timeout > 0:
            log("MQTT 연결 대기...")
            await asyncio.sleep(1)
            timeout -= 1
        if not self.online:
            log("MQTT 초기 연결 타임아웃 — 백그라운드 재연결 진행", "WARNING")

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

    def publish(self, topic: str, payload: Any):
        try:
            result = self.client.publish(topic, payload)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                log(f"MQTT publish 실패: {topic} rc={result.rc}", "WARNING")
        except Exception as e:
            log(f"MQTT publish 예외: {e}", "ERROR")

    @property
    def seconds_since_last_message(self) -> float:
        return time.time() - self._last_message_time


# 소켓 관리 (non-blocking asyncio 기반)
class SocketHandler:
    RECONNECT_DELAY = 3
    MAX_RECONNECT_DELAY = 60

    def __init__(self, address: str, port: int, buffer_size: int, timeout: float = 30.0):
        self.address = address
        self.port = port
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._reconnect_delay = self.RECONNECT_DELAY
        self._last_data_time = time.time()

    async def connect(self):
        """비동기 연결 with exponential backoff"""
        while True:
            try:
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(self.address, self.port),
                    timeout=self.timeout,
                )
                self._connected = True
                self._reconnect_delay = self.RECONNECT_DELAY
                self._last_data_time = time.time()
                log(f"Socket 연결 성공: {self.address}:{self.port}")
                return
            except (OSError, asyncio.TimeoutError) as e:
                log(f"Socket 연결 실패: {e}, {self._reconnect_delay}초 후 재시도", "ERROR")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self.MAX_RECONNECT_DELAY)

    async def reconnect(self):
        """재연결"""
        self._connected = False
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        self.reader = None
        self.writer = None
        await self.connect()

    async def recv_loop(self, msg_queue: asyncio.Queue):
        """비동기 수신 루프"""
        await self.connect()
        while True:
            try:
                data = await asyncio.wait_for(self.reader.read(self.buffer_size), timeout=self.timeout)
                if not data:
                    log("Socket: 빈 데이터 수신, 연결 끊김 감지", "WARNING")
                    await self.reconnect()
                    continue
                self._last_data_time = time.time()
                msg = type("MSG", (), {"topic": f"{EW11_TOPIC}/recv", "payload": data})()
                try:
                    msg_queue.put_nowait(msg)
                except asyncio.QueueFull:
                    log("Socket 메시지 큐 가득참", "WARNING")
            except asyncio.TimeoutError:
                # 타임아웃 = 데이터 없음, 연결 확인
                silent = time.time() - self._last_data_time
                if silent > MAX_SILENT_PERIOD:
                    log(f"Socket: {silent:.0f}초 동안 데이터 없음, 재연결", "WARNING")
                    await self.reconnect()
            except (OSError, ConnectionResetError) as e:
                log(f"Socket 수신 오류: {e}, 재연결", "ERROR")
                await self.reconnect()

    async def send(self, data: bytes):
        """비동기 송신"""
        if not self._connected or not self.writer:
            log("Socket 미연결, 재연결 시도", "WARNING")
            await self.reconnect()
        try:
            self.writer.write(data)
            await self.writer.drain()
        except (OSError, ConnectionResetError) as e:
            log(f"Socket 송신 오류: {e}, 재연결", "ERROR")
            await self.reconnect()
            try:
                self.writer.write(data)
                await self.writer.drain()
            except Exception as e2:
                log(f"Socket 재송신 실패: {e2}", "ERROR")

    async def close(self):
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass


# 디바이스 핸들러
class DeviceHandler:
    def __init__(self, mqtt_handler: MQTTHandler, config: Config):
        self.mqtt = mqtt_handler
        self.device_state: Dict[str, str] = {}
        self.msg_cache: Dict[str, str] = {}
        self.discovery_list: List[str] = []
        self.residue = ""
        self.cmd_queue = asyncio.Queue(maxsize=100)
        self.force_update = False
        self.config = config
        self.force_target_time = time.time() + config.get("force_update_period", 300)
        self.force_stop_time = self.force_target_time + config.get("force_update_duration", 60)
        self._packet_count = 0
        self._error_count = 0

    async def process_ew11(self, raw_data: str):
        raw_data = self.residue + raw_data.upper()
        k = 0
        while k < len(raw_data):
            if raw_data[k:k + 2] == "F7":
                if k + 10 > len(raw_data):
                    self.residue = raw_data[k:]
                    break
                data_length = int(raw_data[k + 8:k + 10], 16)
                packet_length = 10 + data_length * 2 + 4
                if k + packet_length > len(raw_data):
                    self.residue = raw_data[k:]
                    break
                packet = raw_data[k:k + packet_length]
                if packet == checksum(packet):
                    self._packet_count += 1
                    await self.handle_packet(packet)
                else:
                    self._error_count += 1
                    if self._error_count % 100 == 0:
                        log(f"체크섬 오류 누적: {self._error_count}건", "WARNING")
                self.residue = ""
                k += packet_length
            else:
                k += 1

    async def handle_packet(self, packet: str):
        device_id = packet[2:4]
        cmd = packet[6:8]
        state_packet = device_id in STATE_HEADER and cmd == STATE_HEADER[device_id][1]
        ack_packet = device_id in ACK_HEADER and cmd == ACK_HEADER[device_id][1]

        if not (state_packet or ack_packet):
            # Log unknown packets to MQTT for debugging
            if device_id not in STATE_HEADER and device_id not in ACK_HEADER:
                self.mqtt.publish(
                    f"{HA_TOPIC}/debug/unknown",
                    f"device=0x{device_id} sub=0x{packet[4:6]} cmd=0x{cmd} | {packet}"
                )
            return

        if self.msg_cache.get(packet[:10]) != packet[10:] or self.force_update:
            device_name = STATE_HEADER.get(device_id, ACK_HEADER.get(device_id))[0]
            handler = getattr(self, f"handle_{device_name}", None)
            if handler:
                try:
                    await handler(packet, state_packet)
                except Exception as e:
                    log(f"패킷 처리 오류 ({device_name}): {e}", "ERROR")
            if state_packet:
                self.msg_cache[packet[:10]] = packet[10:]

    async def handle_light(self, packet: str, state_packet: bool):
        rid = int(packet[5], 16)
        slc = int(packet[8:10], 16)
        for id in range(1, slc):
            name = f"light_{rid:02d}_{id:02d}"
            if name not in self.discovery_list:
                self.discovery_list.append(name)
                payload = DISCOVERY_PAYLOAD["light"][0].copy()
                payload.update({"~": f"ezville/{name}", "name": name, "device": DISCOVERY_DEVICE, "uniq_id": name})
                await self.mqtt_discovery(payload)
                await asyncio.sleep(self.config.get("discovery_delay", 0.2))
            onoff = "ON" if int(packet[10 + 2 * id:12 + 2 * id], 16) & 1 else "OFF"
            await self.update_state("light", "power", rid, id, onoff)

    async def handle_thermostat(self, packet: str, state_packet: bool):
        rid = int(packet[5], 16)
        slc = int(packet[8:10], 16)
        for id in range(1, slc):
            name = f"thermostat_{rid:02d}_{id:02d}"
            if name not in self.discovery_list:
                self.discovery_list.append(name)
                payload = DISCOVERY_PAYLOAD["thermostat"][0].copy()
                payload.update({"~": f"ezville/{name}", "name": name, "device": DISCOVERY_DEVICE, "uniq_id": name})
                await self.mqtt_discovery(payload)
                await asyncio.sleep(self.config.get("discovery_delay", 0.2))
            offset = 10 + (id - 1) * 8
            onoff_byte = int(packet[offset:offset + 2], 16)
            set_temp = int(packet[offset + 2:offset + 4], 16)
            cur_temp = int(packet[offset + 4:offset + 6], 16)
            if onoff_byte & 0x01:
                mode = "heat"
            else:
                mode = "off"
            await self.update_state("thermostat", "power", rid, id, mode)
            await self.update_state("thermostat", "setTemp", rid, id, str(set_temp))
            await self.update_state("thermostat", "curTemp", rid, id, str(cur_temp))

    async def handle_plug(self, packet: str, state_packet: bool):
        rid = int(packet[5], 16)
        slc = int(packet[8:10], 16)
        for id in range(1, slc):
            name = f"plug_{rid:02d}_{id:02d}"
            if name not in self.discovery_list:
                self.discovery_list.append(name)
                for p in DISCOVERY_PAYLOAD["plug"]:
                    payload = p.copy()
                    intg_name = name if "_intg" not in ["binary_sensor", "sensor"] else f"{name}_{p.get('name', '').split('_')[-1]}"
                    payload.update({
                        "~": f"ezville/{name}",
                        "name": payload["name"].format(rid, id),
                        "device": DISCOVERY_DEVICE,
                        "uniq_id": payload["name"].format(rid, id),
                    })
                    await self.mqtt_discovery(payload)
                    await asyncio.sleep(self.config.get("discovery_delay", 0.2))
            offset = 10 + (id - 1) * 8
            onoff = "ON" if int(packet[offset:offset + 2], 16) & 1 else "OFF"
            auto = "ON" if int(packet[offset:offset + 2], 16) & 2 else "OFF"
            current = int(packet[offset + 2:offset + 4], 16) * 256 + int(packet[offset + 4:offset + 6], 16)
            await self.update_state("plug", "power", rid, id, onoff)
            await self.update_state("plug", "auto", rid, id, auto)
            await self.update_state("plug", "current", rid, id, str(current))

    async def handle_gasvalve(self, packet: str, state_packet: bool):
        rid = int(packet[5], 16)
        name = f"gasvalve_{rid:02d}_01"
        if name not in self.discovery_list:
            self.discovery_list.append(name)
            payload = DISCOVERY_PAYLOAD["gasvalve"][0].copy()
            payload.update({"~": f"ezville/{name}", "name": name, "device": DISCOVERY_DEVICE, "uniq_id": name})
            await self.mqtt_discovery(payload)
            await asyncio.sleep(self.config.get("discovery_delay", 0.2))
        onoff = "ON" if int(packet[10:12], 16) & 1 else "OFF"
        await self.update_state("gasvalve", "power", rid, 1, onoff)

    async def handle_batch(self, packet: str, state_packet: bool):
        rid = int(packet[5], 16)
        name = f"batch_{rid:02d}_01"
        if name not in self.discovery_list:
            self.discovery_list.append(name)
            for p in DISCOVERY_PAYLOAD["batch"]:
                payload = p.copy()
                payload.update({
                    "~": f"ezville/{name}",
                    "name": payload["name"].format(rid, 1),
                    "device": DISCOVERY_DEVICE,
                    "uniq_id": payload["name"].format(rid, 1),
                })
                await self.mqtt_discovery(payload)
                await asyncio.sleep(self.config.get("discovery_delay", 0.2))
        group = "ON" if int(packet[10:12], 16) & 1 else "OFF"
        outing = "ON" if int(packet[10:12], 16) & 2 else "OFF"
        await self.update_state("batch", "group", rid, 1, group)
        await self.update_state("batch", "outing", rid, 1, outing)

    async def handle_door(self, packet: str, state_packet: bool):
        """현관문 상태 처리 (device_id=0x40)
        
        Packet structure (captured via RS485 analysis):
          State (cmd=0x82): F7 40 02 82 02 00 00 35 F2
            - packet[10:12] = door state byte
          Events:
            cmd=0x10: door open command
            cmd=0x90: door open ACK
            cmd=0x13: door opened state
            cmd=0x93: door opened ACK
            cmd=0x12: door closing
            cmd=0x92: door closing ACK
            cmd=0x22: door locked
            cmd=0xA2: door locked ACK
            cmd=0x11: door back to idle
            cmd=0x91: door idle ACK
        """
        rid = int(packet[5], 16)
        name = f"door_{rid:02d}_01"
        if name not in self.discovery_list:
            self.discovery_list.append(name)
            for p in DISCOVERY_PAYLOAD["door"]:
                payload = p.copy()
                payload.update({
                    "~": f"ezville/{name}",
                    "name": payload["name"].format(rid, 1),
                    "device": DISCOVERY_DEVICE,
                    "uniq_id": payload["name"].format(rid, 1),
                })
                await self.mqtt_discovery(payload)
                await asyncio.sleep(self.config.get("discovery_delay", 0.2))
        
        cmd = packet[6:8]
        if state_packet:
            # cmd=0x82 is normal state poll, door is locked
            await self.update_state("door", "lock", rid, 1, "LOCKED")
        elif cmd == "90":
            # Door open ACK - door is unlocking
            log(f"현관문 열림 (door_{rid:02d}_01)")
            await self.update_state("door", "lock", rid, 1, "UNLOCKED")

    async def mqtt_discovery(self, payload: Dict[str, Any]):
        intg = payload.pop("_intg")
        topic = f"homeassistant/{intg}/ezville_wallpad/{payload['name']}/config"
        self.mqtt.publish(topic, json.dumps(payload))
        log(f"장치 등록: {topic}")

    async def update_state(self, device: str, state: str, id1: int, id2: int, value: str):
        key = f"{device}_{id1:02d}_{id2:02d}{state}"
        if value != self.device_state.get(key) or self.force_update:
            self.device_state[key] = value
            topic = STATE_TOPIC.format(f"{device}_{id1:02d}_{id2:02d}", state)
            self.mqtt.publish(topic, value.encode())

    async def process_ha(self, topics: List[str], value: str):
        if len(topics) < 3:
            return
        device_info = topics[1].split("_")
        if len(device_info) < 3:
            return
        device = device_info[0]
        try:
            idx, sid = int(device_info[1]), int(device_info[2])
        except (ValueError, IndexError):
            return
        key = f"{topics[1]}{topics[2]}"
        if device in RS485_DEVICE and value != self.device_state.get(key):
            generator = getattr(self, f"generate_{device}_cmd", None)
            if generator:
                sendcmd, recvcmd, statcmd = generator(idx, sid, topics[2], value)
                try:
                    self.cmd_queue.put_nowait({"sendcmd": sendcmd, "recvcmd": recvcmd, "statcmd": statcmd})
                except asyncio.QueueFull:
                    log("명령 큐 가득참", "WARNING")

    def generate_light_cmd(self, idx: int, sid: int, subtopic: str, value: str) -> Tuple[str, str, List[str]]:
        pwr = "01" if value == "ON" else "00"
        if idx == 1 and sid in [1, 2] and pwr == "01":
            pwr = "F1"
        sendcmd = checksum(f"F70E1{idx}41030{sid}{pwr}000000")
        recvcmd = f"F70E1{idx}C1"
        statcmd = [f"light_{idx:02d}_{sid:02d}power", value]
        return sendcmd, recvcmd, statcmd

    def generate_thermostat_cmd(self, idx: int, sid: int, subtopic: str, value: str) -> Tuple[str, str, List[str]]:
        if subtopic == "power":
            if value == "heat":
                sendcmd = checksum(f"F7361{idx}43020{sid}01000000")
            elif value == "off":
                sendcmd = checksum(f"F7361{idx}43020{sid}04000000")
            else:
                sendcmd = checksum(f"F7361{idx}43020{sid}00000000")
            recvcmd = f"F7361{idx}C3"
            statcmd = [f"thermostat_{idx:02d}_{sid:02d}power", value]
        elif subtopic == "setTemp":
            temp = int(float(value))
            sendcmd = checksum(f"F7361{idx}44020{sid}{temp:02X}000000")
            recvcmd = f"F7361{idx}C4"
            statcmd = [f"thermostat_{idx:02d}_{sid:02d}setTemp", str(temp)]
        else:
            return None, None, ["", "NULL"]
        return sendcmd, recvcmd, statcmd

    def generate_plug_cmd(self, idx: int, sid: int, subtopic: str, value: str) -> Tuple[str, str, List[str]]:
        pwr = "01" if value == "ON" else "00"
        sendcmd = checksum(f"F7391{idx}41030{sid}{pwr}000000")
        recvcmd = f"F7391{idx}C1"
        statcmd = [f"plug_{idx:02d}_{sid:02d}power", value]
        return sendcmd, recvcmd, statcmd

    def generate_gasvalve_cmd(self, idx: int, sid: int, subtopic: str, value: str) -> Tuple[str, str, List[str]]:
        # Fix: gasvalve polling sub_id is 0{idx} (e.g., 01), not 1{idx}
        sendcmd = checksum(f"F7120{idx}41020100000000")
        recvcmd = f"F7120{idx}C1"
        statcmd = [f"gasvalve_{idx:02d}_{sid:02d}power", value]
        return sendcmd, recvcmd, statcmd

    def generate_batch_cmd(self, idx: int, sid: int, subtopic: str, value: str) -> Tuple[str, str, List[str]]:
        # Fix: sub_id from polling is 0{idx} (e.g., 01), not 1{idx} (e.g., 11)
        # Polling packet: F7 33 01 81 03 00 04 00 → sub_id=01, rid=int('1',16)=1
        # Command must use same sub_id format: F7 33 0{idx} 41 ...
        if subtopic == "elevator-up":
            sendcmd = checksum(f"F7330{idx}41020103000000")
        elif subtopic == "elevator-down":
            sendcmd = checksum(f"F7330{idx}41020104000000")
        else:
            return None, None, ["", "NULL"]
        recvcmd = f"F7330{idx}C1"
        statcmd = [f"batch_{idx:02d}_{sid:02d}{subtopic}", "NULL"]
        return sendcmd, recvcmd, statcmd

    def generate_door_cmd(self, idx: int, sid: int, subtopic: str, value: str) -> Tuple[str, str, List[str]]:
        """현관문 열기 명령 생성
        
        Captured open command: F7 40 02 10 02 63 02 [XOR] [SUM]
        - device_id=0x40, sub_id=0x02, cmd=0x10
        - data: 02 63 02
        """
        if subtopic == "lock" and value == "UNLOCK":
            # Door open command (captured from wallpad RS485 packet analysis)
            sendcmd = checksum("F740021002630200" + "0000")
            recvcmd = "F7400290"
            statcmd = [f"door_{idx:02d}_{sid:02d}lock", "UNLOCKED"]
        else:
            return None, None, ["", "NULL"]
        return sendcmd, recvcmd, statcmd

    async def send_to_ew11(self, send_data: Dict[str, Any], comm_mode: str, socket_handler: SocketHandler):
        if not send_data.get("sendcmd"):
            return
        retry_count = self.config.get("command_retry_count", 30)
        first_wait = self.config.get("first_waittime", 0.5)
        use_backoff = self.config.get("random_backoff", True)
        cmd_interval = self.config.get("command_interval", 0.5)

        for attempt in range(retry_count):
            try:
                if comm_mode == "mqtt":
                    # MQTT 연결 상태 확인
                    if hasattr(self.mqtt.client, 'is_connected') and not self.mqtt.client.is_connected():
                        log(f"MQTT 미연결 상태 — 재연결 시도 (attempt {attempt + 1})", "WARNING")
                        try:
                            self.mqtt.client.reconnect()
                            await asyncio.sleep(1)
                        except Exception as re:
                            log(f"MQTT 재연결 실패: {re}", "ERROR")
                            await asyncio.sleep(2)
                            continue
                    log(f"send to device:  {send_data['sendcmd']}")
                    result = self.mqtt.publish(EW11_SEND_TOPIC, bytes.fromhex(send_data["sendcmd"]))
                    if hasattr(result, 'rc') and result.rc != 0:
                        log(f"MQTT publish 실패 (rc={result.rc}, attempt {attempt + 1})", "ERROR")
                else:
                    log(f"send to device:  {send_data['sendcmd']}")
                    await socket_handler.send(bytes.fromhex(send_data["sendcmd"]))
            except Exception as e:
                log(f"명령 송신 오류 (attempt {attempt + 1}): {e}", "ERROR")
                await asyncio.sleep(1)
                continue

            if send_data["statcmd"][1] == "NULL":
                return
            await asyncio.sleep(first_wait)
            if send_data["statcmd"][1] == self.device_state.get(send_data["statcmd"][0]):
                return
            if use_backoff:
                await asyncio.sleep(random.uniform(0, cmd_interval))
            else:
                await asyncio.sleep(cmd_interval)
        log(f"명령 실패 ({retry_count}회 시도): {send_data.get('sendcmd', '')[:20]}...", "WARNING")

    async def state_update_loop(self, msg_queue: asyncio.Queue):
        state_delay = self.config.get("state_loop_delay", 0.2)
        while True:
            try:
                # Process messages with timeout
                try:
                    msg = await asyncio.wait_for(msg_queue.get(), timeout=state_delay)
                    topics = msg.topic.split("/")
                    if topics[0] == HA_TOPIC and topics[-1] == "command":
                        await self.process_ha(topics, msg.payload.decode("utf-8"))
                    elif msg.topic == f"{HA_TOPIC}/debug/send":
                        # Debug: send raw hex packet to RS485 via EW11
                        raw_hex = msg.payload.decode("utf-8").strip()
                        log(f"[DEBUG] Raw RS485 send: {raw_hex}")
                        try:
                            self.cmd_queue.put_nowait({
                                "sendcmd": raw_hex,
                                "recvcmd": "",
                                "statcmd": ["", "NULL"]
                            })
                            self.mqtt.publish(f"{HA_TOPIC}/debug/result", f"sent: {raw_hex}")
                        except Exception as e:
                            log(f"[DEBUG] Send error: {e}", "ERROR")
                            self.mqtt.publish(f"{HA_TOPIC}/debug/result", f"error: {e}")
                    elif topics[0] == EW11_TOPIC and topics[-1] == "recv":
                        await self.process_ew11(msg.payload.hex().upper())
                except asyncio.TimeoutError:
                    pass

                # 강제 업데이트 로직
                timestamp = time.time()
                if timestamp > self.force_target_time and not self.force_update:
                    self.force_stop_time = timestamp + self.config.get("force_update_duration", 60)
                    self.force_update = True
                    log("상태 강제 업데이트 시작")

                if timestamp > self.force_stop_time and self.force_update:
                    self.force_target_time = timestamp + self.config.get("force_update_period", 300)
                    self.force_update = False
                    log(f"상태 강제 업데이트 종료 (패킷: {self._packet_count}, 오류: {self._error_count})")

            except Exception as e:
                log(f"상태 업데이트 루프 오류: {e}", "ERROR")
                await asyncio.sleep(1)

    async def command_loop(self, comm_mode: str, socket_handler: SocketHandler):
        cmd_delay = self.config.get("command_loop_delay", 0.2)
        while True:
            try:
                send_data = await asyncio.wait_for(self.cmd_queue.get(), timeout=cmd_delay)
                await self.send_to_ew11(send_data, comm_mode, socket_handler)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                log(f"명령 루프 오류: {e}", "ERROR")
                await asyncio.sleep(1)


# 헬스체크 루프
RECOVERY_INTERVAL = 300  # 5분간 데이터 없으면 복구 시도
MAX_RECOVERY_ATTEMPTS = 3  # 복구 시도 횟수 (초과 시 프로세스 재시작)

async def health_check_loop(mqtt_handler: MQTTHandler, socket_handler: SocketHandler, config: Config):
    """주기적 헬스체크 — MQTT 및 Socket 연결 상태 모니터링 + 자동 복구"""
    recovery_attempts = 0
    last_recovery_time = 0

    while True:
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)
        silent_seconds = mqtt_handler.seconds_since_last_message

        # MQTT health
        if not mqtt_handler.online:
            log(f"헬스체크: MQTT 오프라인 (재연결 #{mqtt_handler._reconnect_count})", "WARNING")
        elif silent_seconds > MAX_SILENT_PERIOD:
            # 진단 정보 포함
            msg_count = getattr(mqtt_handler, '_msg_count', 0)
            msg_by_topic = getattr(mqtt_handler, '_msg_count_by_topic', {})
            mqtt_connected = mqtt_handler.client.is_connected() if hasattr(mqtt_handler.client, 'is_connected') else 'unknown'
            log(f"헬스체크: MQTT 메시지 {silent_seconds:.0f}초 동안 없음 | "
                f"총수신={msg_count}, 토픽별={msg_by_topic}, "
                f"connected={mqtt_connected}, reconnect횟수={mqtt_handler._reconnect_count}",
                "WARNING")

        # 자동 복구: N초 동안 데이터 없으면 MQTT reconnect + 재구독
        recovery_interval = config.get("recovery_interval", RECOVERY_INTERVAL)
        if silent_seconds > recovery_interval and (time.time() - last_recovery_time) > recovery_interval:
            recovery_attempts += 1
            last_recovery_time = time.time()

            if recovery_attempts > config.get("max_recovery_attempts", MAX_RECOVERY_ATTEMPTS):
                log(f"복구 {recovery_attempts-1}회 실패 — 프로세스 재시작", "ERROR")
                sys.exit(1)  # HA will auto-restart the addon

            msg_count = getattr(mqtt_handler, '_msg_count', 0)
            msg_by_topic = getattr(mqtt_handler, '_msg_count_by_topic', {})
            log(f"자동 복구 시도 #{recovery_attempts}: "
                f"silent={silent_seconds:.0f}초, 총수신={msg_count}, 토픽별={msg_by_topic}, "
                f"reconnect횟수={mqtt_handler._reconnect_count}", "WARNING")

            # Step 1: EW11 TCP ping — 장치가 살아있는지 확인
            ew11_host = config["ew11_server"]
            ew11_port = config["ew11_port"]
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(ew11_host, ew11_port), timeout=5
                )
                # 데이터 수신 시도 (1초 대기)
                try:
                    probe_data = await asyncio.wait_for(reader.read(256), timeout=2)
                    if probe_data:
                        log(f"EW11 TCP 연결 확인: {ew11_host}:{ew11_port} 정상, "
                            f"수신 {len(probe_data)}바이트: {probe_data[:20].hex()}", "INFO")
                    else:
                        log(f"EW11 TCP 연결 확인: {ew11_host}:{ew11_port} 연결됨, 데이터 없음", "WARNING")
                except asyncio.TimeoutError:
                    log(f"EW11 TCP 연결 확인: {ew11_host}:{ew11_port} 연결됨, 2초 내 데이터 없음", "WARNING")
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                log(f"EW11 TCP 연결 실패: {e} — 장치 전원/네트워크 확인 필요", "ERROR")

            # Step 2: MQTT disconnect + reconnect (forces resubscription via on_connect)
            try:
                mqtt_connected_before = mqtt_handler.client.is_connected() if hasattr(mqtt_handler.client, 'is_connected') else 'unknown'
                log(f"MQTT 재연결 시작 (현재 connected={mqtt_connected_before})")
                mqtt_handler.client.disconnect()
                await asyncio.sleep(2)
                mqtt_handler.client.reconnect()
                await asyncio.sleep(3)  # on_connect 콜백 대기
                mqtt_connected_after = mqtt_handler.client.is_connected() if hasattr(mqtt_handler.client, 'is_connected') else 'unknown'
                log(f"MQTT 재연결 완료 (connected={mqtt_connected_after})", "INFO")
            except Exception as e:
                log(f"MQTT 재연결 실패: {e}", "ERROR")

        # 데이터 수신 복구 시 카운터 리셋
        if silent_seconds < MAX_SILENT_PERIOD and recovery_attempts > 0:
            log(f"데이터 수신 복구 성공 (시도 {recovery_attempts}회 후)", "INFO")
            recovery_attempts = 0

        # Reboot control (기존 옵션 호환)
        if config.get("reboot_control", False):
            reboot_delay = config.get("reboot_delay", 300)
            if silent_seconds > reboot_delay:
                log(f"MQTT {reboot_delay}초 동안 메시지 없음 — 프로세스 재시작", "ERROR")
                sys.exit(1)  # HA will auto-restart the addon


# 메인 루프
async def main(config: Config):
    mqtt_handler = MQTTHandler(config)
    socket_handler = SocketHandler(
        config["ew11_server"],
        config["ew11_port"],
        config["ew11_buffer_size"],
        timeout=config.get("ew11_timeout", 30),
    )
    device_handler = DeviceHandler(mqtt_handler, config)

    comm_mode = config["mode"]
    await mqtt_handler.start()

    tasks = [
        asyncio.create_task(device_handler.state_update_loop(mqtt_handler.msg_queue)),
        asyncio.create_task(device_handler.command_loop(comm_mode, socket_handler)),
        asyncio.create_task(health_check_loop(mqtt_handler, socket_handler, config)),
    ]
    if comm_mode in ["socket", "mixed"]:
        tasks.append(asyncio.create_task(socket_handler.recv_loop(mqtt_handler.msg_queue)))

    # Graceful shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: [t.cancel() for t in tasks])

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log("정상 종료")
    except Exception as e:
        log(f"예기치 않은 오류: {e}", "ERROR")
    finally:
        mqtt_handler.stop()
        await socket_handler.close()


if __name__ == "__main__":
    log("=== EzVille Wallpad Control 시작 ===")
    config = Config(f"{CONFIG_DIR}/options.json")
    log(f"모드: {config['mode']}, MQTT: {config['mqtt_server']}, EW11: {config['ew11_server']}:{config['ew11_port']}")
    try:
        asyncio.run(main(config))
    except KeyboardInterrupt:
        log("사용자 종료")
    except Exception as e:
        log(f"치명적 오류: {e}", "ERROR")
        sys.exit(1)
