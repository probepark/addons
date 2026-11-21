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


class FakePublisher:
    def publish(self, _topic, _payload):
        pass


def make_handler():
    handler = ezville.DeviceHandler.__new__(ezville.DeviceHandler)
    handler.discovery_list = [f"thermostat_01_{i:02d}" for i in range(1, 6)]
    handler.device_state = {}
    handler.mqtt = FakePublisher()
    handler.config = {"discovery_delay": 0}
    handler.force_update = False
    return handler


class ThermostatStatePacketTest(unittest.IsolatedAsyncioTestCase):
    async def test_live_group_state_packet_does_not_raise(self):
        # Captured from the wallpad on 2026-11-21: group 1 (all rooms), away bits 1F, set 5, current 22-24.
        handler = make_handler()

        await handler.handle_thermostat("F7361F810F00001F0000051605170516051605165DE0", True)

        self.assertEqual(handler.device_state["thermostat_01_01power"], "off")
        self.assertEqual(handler.device_state["thermostat_01_01setTemp"], "5")
        self.assertEqual(handler.device_state["thermostat_01_01curTemp"], "22")
        self.assertEqual(handler.device_state["thermostat_01_02curTemp"], "23")
        self.assertEqual(handler.device_state["thermostat_01_05curTemp"], "22")
        self.assertNotIn("thermostat_15_01power", handler.device_state)

    async def test_heating_bits_map_room_one_to_highest_bit(self):
        # PACKETS.md example: heating bits 0B = rooms 2, 4, 5.
        handler = make_handler()

        await handler.handle_thermostat("F7361F810F800B1400001415181805160D140500C1D6", True)

        modes = [handler.device_state[f"thermostat_01_{i:02d}power"] for i in range(1, 6)]
        self.assertEqual(modes, ["off", "heat", "off", "heat", "heat"])
        self.assertEqual(handler.device_state["thermostat_01_01setTemp"], "20")
        self.assertEqual(handler.device_state["thermostat_01_01curTemp"], "21")

    async def test_half_degree_bit(self):
        self.assertEqual(ezville.thermostat_temp("97"), "23.5")
        self.assertEqual(ezville.thermostat_temp("17"), "23")


class ThermostatCommandTest(unittest.TestCase):
    def setUp(self):
        self.handler = object.__new__(ezville.DeviceHandler)

    def test_set_temp_matches_documented_packet(self):
        sendcmd, recvcmd, statcmd = self.handler.generate_thermostat_cmd(1, 1, "setTemp", "23")

        self.assertEqual(sendcmd, "F73611440117821C")
        self.assertEqual(recvcmd, "F73611C4")
        self.assertEqual(statcmd, ["thermostat_01_01setTemp", "23"])

    def test_power_heat_addresses_room(self):
        sendcmd, recvcmd, _ = self.handler.generate_thermostat_cmd(1, 3, "power", "heat")

        self.assertTrue(sendcmd.startswith("F73613430101"))
        self.assertEqual(recvcmd, "F73613C3")


if __name__ == "__main__":
    unittest.main()
