import asyncio
import importlib.util
import sys
import time
import types
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[1] / "ezville.py"


def load_ezville_module():
    # ezville.py imports paho-mqtt at module import time. Unit tests only need
    # pure health-check logic, so provide a tiny stub instead of requiring the
    # external runtime dependency on the host running tests.
    paho = types.ModuleType("paho")
    mqtt_pkg = types.ModuleType("paho.mqtt")
    mqtt_client = types.ModuleType("paho.mqtt.client")

    class CallbackAPIVersion:
        VERSION2 = object()

    class Client:
        pass

    setattr(mqtt_client, "CallbackAPIVersion", CallbackAPIVersion)
    setattr(mqtt_client, "Client", Client)
    setattr(mqtt_client, "MQTT_ERR_SUCCESS", 0)
    setattr(mqtt_pkg, "client", mqtt_client)
    setattr(paho, "mqtt", mqtt_pkg)

    previous = {name: sys.modules.get(name) for name in ("paho", "paho.mqtt", "paho.mqtt.client")}
    sys.modules["paho"] = paho
    sys.modules["paho.mqtt"] = mqtt_pkg
    sys.modules["paho.mqtt.client"] = mqtt_client
    try:
        spec = importlib.util.spec_from_file_location("ezville_test_module", MODULE_PATH)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Cannot load ezville module from {MODULE_PATH}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, value in previous.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value


ezville = load_ezville_module()


class Msg:
    def __init__(self, topic, payload=b""):
        self.topic = topic
        self.payload = payload


class DummyClient:
    def is_connected(self):
        return True

    def disconnect(self):
        pass

    def reconnect(self):
        pass


class FakeMqttHandler:
    def __init__(self, mqtt_silent=0, ew11_silent=0):
        now = time.time()
        self._last_message_time = now - mqtt_silent
        self._last_ew11_message_time = now - ew11_silent
        self._reconnect_count = 0
        self._msg_count = 5
        self._msg_count_by_topic = {"homeassistant": 5}
        self.online = True
        self.client = DummyClient()

    @property
    def seconds_since_last_message(self):
        return time.time() - self._last_message_time

    @property
    def seconds_since_last_ew11_message(self):
        return time.time() - self._last_ew11_message_time

    def reset_ew11_health_timer(self):
        self._last_ew11_message_time = time.time()


class FakeSocketHandler:
    seconds_since_last_data = 0


async def no_sleep(_seconds):
    return None


class HealthTrackingTest(unittest.TestCase):
    def test_homeassistant_status_does_not_refresh_ew11_health_timer(self):
        handler = ezville.MQTTHandler.__new__(ezville.MQTTHandler)
        now = time.time()
        handler._last_message_time = now - 500
        handler._last_ew11_message_time = now - 500
        handler._msg_count = 0
        handler._msg_count_by_topic = {}
        handler.online = True
        handler.msg_queue = asyncio.Queue()

        handler.on_message(None, None, Msg("homeassistant/status", b"online"))

        self.assertLess(handler.seconds_since_last_message, 5)
        self.assertGreater(handler.seconds_since_last_ew11_message, 400)

    def test_ew11_recv_refreshes_ew11_health_timer(self):
        handler = ezville.MQTTHandler.__new__(ezville.MQTTHandler)
        now = time.time()
        handler._last_message_time = now - 500
        handler._last_ew11_message_time = now - 500
        handler._msg_count = 0
        handler._msg_count_by_topic = {}
        handler.online = True
        handler.msg_queue = asyncio.Queue()

        handler.on_message(None, None, Msg("ew11/recv", b"\xf7\x0e"))

        self.assertLess(handler.seconds_since_last_message, 5)
        self.assertLess(handler.seconds_since_last_ew11_message, 5)


class HealthLoopTest(unittest.IsolatedAsyncioTestCase):
    async def test_recovery_uses_ew11_silence_even_when_mqtt_is_active(self):
        mqtt_handler = FakeMqttHandler(mqtt_silent=0, ew11_silent=600)
        config = {
            "mode": "mqtt",
            "recovery_interval": 1,
            "max_recovery_attempts": 0,
            "reboot_control": False,
        }

        with patch.object(ezville.asyncio, "sleep", no_sleep):
            with self.assertRaises(SystemExit):
                await ezville.health_check_loop(mqtt_handler, FakeSocketHandler(), config)


if __name__ == "__main__":
    unittest.main()
