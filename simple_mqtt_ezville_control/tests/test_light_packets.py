import importlib.util
import pathlib
import sys
import types
import unittest

sys.modules.setdefault("paho", types.ModuleType("paho"))
sys.modules.setdefault("paho.mqtt", types.ModuleType("paho.mqtt"))
sys.modules.setdefault("paho.mqtt.client", types.ModuleType("paho.mqtt.client"))

MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / "ezville.py"
SPEC = importlib.util.spec_from_file_location("ezville", MODULE_PATH)
ezville = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ezville)


class LightPacketTests(unittest.TestCase):
    def setUp(self):
        self.handler = object.__new__(ezville.DeviceHandler)

    def test_living_room_light_1_on_uses_standard_on_byte(self):
        sendcmd, recvcmd, statcmd = self.handler.generate_light_cmd(1, 1, "power", "ON")

        self.assertEqual(sendcmd, "F70E114103010100AA06")
        self.assertEqual(recvcmd, "F70E11C1")
        self.assertEqual(statcmd, ["light_01_01power", "ON"])

    def test_living_room_light_2_on_uses_standard_on_byte(self):
        sendcmd, _, _ = self.handler.generate_light_cmd(1, 2, "power", "ON")

        self.assertEqual(sendcmd, "F70E114103020100A906")

    def test_light_off_uses_zero_byte(self):
        sendcmd, _, _ = self.handler.generate_light_cmd(1, 1, "power", "OFF")

        self.assertEqual(sendcmd, "F70E114103010000AB06")


if __name__ == "__main__":
    unittest.main()
