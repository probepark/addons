import paho.mqtt.client as mqtt
import json
import time
import asyncio
import telnetlib
import socket
import random
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
}

# MQTT Discovery를 위한 Preset 정보
DISCOVERY_DEVICE = {
    "ids": [
        "ezville_wallpad",
    ],
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
            #        "modes": [ "off", "heat", "fan_only" ],     # 외출 모드는 fan_only로 매핑
            "modes": ["heat", "off"],  # 외출 모드는 off로 매핑
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

# 로깅 함수
def log(string: str, level: str = "INFO") -> None:
    date = time.strftime("%Y-%m-%d %p %I:%M:%S", time.localtime(time.time()))
    print(f"[{date}] [{level}] {string}")

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
        return self.data.get(key)

# MQTT 클라이언트 관리
class MQTTHandler:
    def __init__(self, config: Config):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "mqtt-ezville")
        self.client.username_pw_set(config["mqtt_id"], config["mqtt_password"])
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.config = config
        self.msg_queue = asyncio.Queue(maxsize=1000)
        self.online = False

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            log("MQTT Broker 연결 성공")
            topics = [(f"{HA_TOPIC}/#", 0), ("homeassistant/status", 0)]
            if self.config["mode"] in ["mixed", "mqtt"]:
                topics.append((f"{EW11_TOPIC}/recv", 0))
            if self.config["mode"] == "mqtt":
                topics.append((f"{EW11_TOPIC}/send", 1))
            client.subscribe(topics)
            self.online = True
        else:
            log(f"MQTT 연결 실패: 코드 {rc}", "ERROR")

    def on_disconnect(self, client, userdata, rc):
        log("MQTT 연결 해제")
        self.online = False

    def on_message(self, client, userdata, msg):
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
        self.client.connect_async(self.config["mqtt_server"])
        self.client.loop_start()
        while not self.online and self.config.get("reboot_control", False):
            log("MQTT 연결 대기")
            await asyncio.sleep(1)

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

    def publish(self, topic: str, payload: Any):
        self.client.publish(topic, payload)

# 소켓 관리
class SocketHandler:
    def __init__(self, address: str, port: int, buffer_size: int):
        self.address = address
        self.port = port
        self.buffer_size = buffer_size
        self.socket = None

    def connect(self) -> socket.socket:
        retry_count = 0
        while True:
            try:
                soc = socket.socket()
                soc.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                soc.settimeout(10)  # 타임아웃 추가
                soc.connect((self.address, self.port))
                log("Socket 연결 성공")
                return soc
            except (ConnectionRefusedError, socket.timeout) as e:
                log(f"Socket 연결 실패 ({retry_count}회 재시도): {e}", "ERROR")
                time.sleep(1)
                retry_count += 1

    async def recv_loop(self, msg_queue: asyncio.Queue):
        if not self.socket:
            self.socket = self.connect()
        while True:
            try:
                data = self.socket.recv(self.buffer_size)
                if data:
                    msg = type("MSG", (), {"topic": f"{EW11_TOPIC}/recv", "payload": data})()
                    await msg_queue.put(msg)
            except (OSError, socket.timeout):
                log("Socket 수신 오류, 재연결 시도", "ERROR")
                self.socket.close()
                self.socket = self.connect()
            await asyncio.sleep(0.1)  # SERIAL_RECV_DELAY 대체

    def send(self, data: bytes):
        try:
            self.socket.sendall(data)
        except OSError:
            log("Socket 송신 오류, 재연결", "ERROR")
            self.socket.close()
            self.socket = self.connect()
            self.socket.sendall(data)

    def close(self):
        if self.socket:
            self.socket.close()

# 디바이스 핸들러
class DeviceHandler:
    def __init__(self, mqtt_handler: MQTTHandler):
        self.mqtt = mqtt_handler
        self.device_state: Dict[str, str] = {}
        self.msg_cache: Dict[str, str] = {}
        self.discovery_list: List[str] = []
        self.residue = ""
        self.cmd_queue = asyncio.Queue(maxsize=100)
        self.force_update = False

    async def process_ew11(self, raw_data: str):
        raw_data = self.residue + raw_data.upper()
        k = 0
        while k < len(raw_data):
            if raw_data[k:k+2] == "F7":
                if k + 10 > len(raw_data):
                    self.residue = raw_data[k:]
                    break
                data_length = int(raw_data[k+8:k+10], 16)
                packet_length = 10 + data_length * 2 + 4
                if k + packet_length > len(raw_data):
                    self.residue = raw_data[k:]
                    break
                packet = raw_data[k:k+packet_length]
                if packet == checksum(packet):
                    await self.handle_packet(packet)
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
            return

        if self.msg_cache.get(packet[:10]) != packet[10:] or self.force_update:
            device_name = STATE_HEADER.get(device_id, ACK_HEADER.get(device_id))[0]
            handler = getattr(self, f"handle_{device_name}", None)
            if handler:
                await handler(packet, state_packet)
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
                await asyncio.sleep(0.5)
            onoff = "ON" if int(packet[10 + 2 * id:12 + 2 * id], 16) & 1 else "OFF"
            await self.update_state("light", "power", rid, id, onoff)

    # 다른 디바이스 핸들러 (thermostat, plug 등)는 유사하게 구현
    # 예시로 생략, 전체 코드에서 구현

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
            log(f"상태 업데이트: {topic} -> {value}")

    async def process_ha(self, topics: List[str], value: str):
        device_info = topics[1].split("_")
        device = device_info[0]
        idx, sid = int(device_info[1]), int(device_info[2])
        key = f"{topics[1]}{topics[2]}"
        if device in RS485_DEVICE and value != self.device_state.get(key):
            generator = getattr(self, f"generate_{device}_cmd", None)
            if generator:
                sendcmd, recvcmd, statcmd = generator(idx, sid, topics[2], value)
                await self.cmd_queue.put({"sendcmd": sendcmd, "recvcmd": recvcmd, "statcmd": statcmd})

    def generate_light_cmd(self, idx: int, sid: int, subtopic: str, value: str) -> Tuple[str, str, List[str]]:
        pwr = "01" if value == "ON" else "00"
        if idx == 1 and sid in [1, 2] and pwr == "01":
            pwr = "F1"
        sendcmd = checksum(f"F70E1{idx}41030{sid}{pwr}000000")
        recvcmd = f"F70E1{idx}C1"
        statcmd = [f"light_{idx:02d}_{sid:02d}power", value]
        return sendcmd, recvcmd, statcmd

    # 다른 명령 생성 함수도 유사하게 구현

    async def send_to_ew11(self, send_data: Dict[str, Any], comm_mode: str, socket_handler: SocketHandler):
        for _ in range(3):  # CMD_RETRY_COUNT
            if comm_mode == "mqtt":
                self.mqtt.publish(EW11_SEND_TOPIC, bytes.fromhex(send_data["sendcmd"]))
            else:
                socket_handler.send(bytes.fromhex(send_data["sendcmd"]))
            if send_data["statcmd"][1] == "NULL":
                return
            await asyncio.sleep(0.5)  # FIRST_WAITTIME
            if send_data["statcmd"][1] == self.device_state.get(send_data["statcmd"][0]):
                return
            await asyncio.sleep(random.uniform(0, 0.2))  # CMD_INTERVAL with RANDOM_BACKOFF
        log(f"명령 실패: {send_data}", "WARNING")

# 메인 루프
async def main(config: Config):
    mqtt_handler = MQTTHandler(config)
    socket_handler = SocketHandler(config["ew11_server"], config["ew11_port"], config["ew11_buffer_size"])
    device_handler = DeviceHandler(mqtt_handler)

    comm_mode = config["mode"]
    await mqtt_handler.start()

    tasks = [
        asyncio.create_task(device_handler.state_update_loop(mqtt_handler.msg_queue)),
        asyncio.create_task(device_handler.command_loop(comm_mode, socket_handler)),
    ]
    if comm_mode == "socket":
        tasks.append(asyncio.create_task(socket_handler.recv_loop(mqtt_handler.msg_queue)))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log("Task 종료")
    finally:
        mqtt_handler.stop()
        socket_handler.close()

if __name__ == "__main__":
    config = Config(f"{CONFIG_DIR}/options.json")
    asyncio.run(main(config))
