#!/usr/bin/env python3
"""
Feetech SM-80BL-MB sweep utility and register library.

This file provides a small reusable API for Modbus register read/write access,
including the complete register map from the .mdat decoder, plus a CLI sweep
example.

Press Ctrl+C to stop the sweep (torque is disabled on exit).

Position: 0-4095 over 360°
  0    = -180°
  2048 = 0° (center)
  4095 = +180°
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
import sys
import time
from typing import Dict, Optional, Union

from pymodbus.client import ModbusSerialClient  # type: ignore[import-not-found]

# --- Config ---
PORT = "/dev/ttyUSB0"
BAUDRATE = 115200
SERVO_ID = 1


@dataclass(frozen=True)
class RegisterSpec:
    address: int
    name: str
    area: str
    access: str


# Complete register map from feetech_decode_mdat.py
REGISTER_SPECS: Dict[int, RegisterSpec] = {
    # INFO
    0: RegisterSpec(0, "Firmware Main Version NO.", "INFO", "r"),
    1: RegisterSpec(1, "Firmware Secondary Version", "INFO", "d"),
    2: RegisterSpec(2, "Firmware Release Date (Year)", "INFO", "r"),
    3: RegisterSpec(3, "Firmware Release Date (MMDD)", "INFO", "r"),
    # EPROM
    10: RegisterSpec(10, "ID", "EPROM", "rw"),
    11: RegisterSpec(11, "Baud Rate", "EPROM", "rw"),
    12: RegisterSpec(12, "Return Delay Time", "EPROM", "rw"),
    13: RegisterSpec(13, "Min Position Limit", "EPROM", "rw"),
    14: RegisterSpec(14, "Max Position Limit", "EPROM", "rw"),
    15: RegisterSpec(15, "Position Offset Value", "EPROM", "rw"),
    16: RegisterSpec(16, "Work Mode", "EPROM", "rw"),
    17: RegisterSpec(17, "Position P Gain", "EPROM", "rw"),
    18: RegisterSpec(18, "Position D Gain", "EPROM", "rw"),
    19: RegisterSpec(19, "Position I Gain", "EPROM", "rw"),
    20: RegisterSpec(20, "Velocity P Gain", "EPROM", "rw"),
    21: RegisterSpec(21, "Velocity I Gain", "EPROM", "rw"),
    # SRAM
    128: RegisterSpec(128, "Goal Position", "SRAM", "rw"),
    129: RegisterSpec(129, "Torque Enable", "SRAM", "rw"),
    130: RegisterSpec(130, "Goal Acceleration", "SRAM", "rw"),
    131: RegisterSpec(131, "Goal Velocity", "SRAM", "rw"),
    132: RegisterSpec(132, "Max Torque Limit", "SRAM", "rw"),
    133: RegisterSpec(133, "EPROM Lock Sign", "SRAM", "rw"),
    134: RegisterSpec(134, "Error Reset", "SRAM", "rw"),
    # RONLY
    256: RegisterSpec(256, "Hardware Error Status", "RONLY", "r"),
    257: RegisterSpec(257, "Present Position", "RONLY", "r"),
    258: RegisterSpec(258, "Present Velocity", "RONLY", "r"),
    259: RegisterSpec(259, "Present PWM", "RONLY", "r"),
    260: RegisterSpec(260, "Present Input Voltage", "RONLY", "r"),
    261: RegisterSpec(261, "Present Temperature", "RONLY", "r"),
    262: RegisterSpec(262, "Moving Status", "RONLY", "r"),
    263: RegisterSpec(263, "Present Current", "RONLY", "r"),
    # DEFAULT
    386: RegisterSpec(386, "Max Velocity Limit", "DEFAULT", "d"),
    387: RegisterSpec(387, "Min Velocity Limit", "DEFAULT", "d"),
    388: RegisterSpec(388, "Acceleration Limit", "DEFAULT", "d"),
    389: RegisterSpec(389, "Starting Torque", "DEFAULT", "d"),
    390: RegisterSpec(390, "CW Dead Band", "DEFAULT", "d"),
    391: RegisterSpec(391, "CCW Dead Band", "DEFAULT", "d"),
    392: RegisterSpec(392, "Setting Byte", "DEFAULT", "d"),
    393: RegisterSpec(393, "Protection Switch", "DEFAULT", "d"),
    394: RegisterSpec(394, "LED Alarm Condition", "DEFAULT", "d"),
    395: RegisterSpec(395, "Max Temperature Limit", "DEFAULT", "d"),
    396: RegisterSpec(396, "Max Input Voltage", "DEFAULT", "d"),
    397: RegisterSpec(397, "Min Input Voltage", "DEFAULT", "d"),
    398: RegisterSpec(398, "Overload Current", "DEFAULT", "d"),
    399: RegisterSpec(399, "Overcurrent Protection Time", "DEFAULT", "d"),
    400: RegisterSpec(400, "Protect Torque", "DEFAULT", "d"),
    401: RegisterSpec(401, "Overload Torque", "DEFAULT", "d"),
    402: RegisterSpec(402, "Overload Protection Time", "DEFAULT", "d"),
    403: RegisterSpec(403, "Angular Resolution", "DEFAULT", "d"),
    404: RegisterSpec(404, "Torque Limit Default Value", "DEFAULT", "d"),
    405: RegisterSpec(405, "Acceleration Default Value", "DEFAULT", "d"),
    406: RegisterSpec(406, "Velocity Default Value", "DEFAULT", "d"),
}


class FeetechRegister(IntEnum):
    # INFO
    FIRMWARE_MAIN_VERSION_NO = 0
    FIRMWARE_SECONDARY_VERSION = 1
    FIRMWARE_RELEASE_DATE_YEAR = 2
    FIRMWARE_RELEASE_DATE_MMDD = 3
    # EPROM
    ID = 10
    BAUD_RATE = 11
    RETURN_DELAY_TIME = 12
    MIN_POSITION_LIMIT = 13
    MAX_POSITION_LIMIT = 14
    POSITION_OFFSET_VALUE = 15
    WORK_MODE = 16
    POSITION_P_GAIN = 17
    POSITION_D_GAIN = 18
    POSITION_I_GAIN = 19
    VELOCITY_P_GAIN = 20
    VELOCITY_I_GAIN = 21
    # SRAM
    GOAL_POSITION = 128
    TORQUE_ENABLE = 129
    GOAL_ACCELERATION = 130
    GOAL_VELOCITY = 131
    MAX_TORQUE_LIMIT = 132
    EPROM_LOCK_SIGN = 133
    ERROR_RESET = 134
    # RONLY
    HARDWARE_ERROR_STATUS = 256
    PRESENT_POSITION = 257
    PRESENT_VELOCITY = 258
    PRESENT_PWM = 259
    PRESENT_INPUT_VOLTAGE = 260
    PRESENT_TEMPERATURE = 261
    MOVING_STATUS = 262
    PRESENT_CURRENT = 263
    # DEFAULT
    MAX_VELOCITY_LIMIT = 386
    MIN_VELOCITY_LIMIT = 387
    ACCELERATION_LIMIT = 388
    STARTING_TORQUE = 389
    CW_DEAD_BAND = 390
    CCW_DEAD_BAND = 391
    SETTING_BYTE = 392
    PROTECTION_SWITCH = 393
    LED_ALARM_CONDITION = 394
    MAX_TEMPERATURE_LIMIT = 395
    MAX_INPUT_VOLTAGE = 396
    MIN_INPUT_VOLTAGE = 397
    OVERLOAD_CURRENT = 398
    OVERCURRENT_PROTECTION_TIME = 399
    PROTECT_TORQUE = 400
    OVERLOAD_TORQUE = 401
    OVERLOAD_PROTECTION_TIME = 402
    ANGULAR_RESOLUTION = 403
    TORQUE_LIMIT_DEFAULT_VALUE = 404
    ACCELERATION_DEFAULT_VALUE = 405
    VELOCITY_DEFAULT_VALUE = 406


def _normalize_register_name(name: str) -> str:
    return (
        name.strip()
        .upper()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("(", "")
        .replace(")", "")
        .replace(".", "")
    )


REGISTER_BY_NORMALIZED_NAME: Dict[str, FeetechRegister] = {
    _normalize_register_name(reg.name): reg for reg in FeetechRegister
}
for addr, spec in REGISTER_SPECS.items():
    REGISTER_BY_NORMALIZED_NAME[_normalize_register_name(spec.name)] = FeetechRegister(addr)


# Backward-compatible aliases for the old script constants.
REG_GOAL_POSITION = FeetechRegister.GOAL_POSITION
REG_TORQUE_ENABLE = FeetechRegister.TORQUE_ENABLE
REG_GOAL_ACCEL = FeetechRegister.GOAL_ACCELERATION
REG_GOAL_VELOCITY = FeetechRegister.GOAL_VELOCITY
REG_MAX_TORQUE_LIMIT = FeetechRegister.MAX_TORQUE_LIMIT
REG_PRESENT_POSITION = FeetechRegister.PRESENT_POSITION

# Position limits
POS_MIN = 0       # -180°
POS_MAX = 4095    # +180°
POS_CENTER = 2048 #    0°


RegisterRef = Union[int, FeetechRegister, str]


def position_to_degrees(position: int) -> float:
    return (position / 4095.0) * 360.0 - 180.0


def degrees_to_position(degrees: float) -> int:
    if degrees < -180.0 or degrees > 180.0:
        raise ValueError("Degrees must be in [-180, 180]")
    return int(round(((degrees + 180.0) / 360.0) * 4095.0))


class FeetechModbusServo:
    """Small reusable Modbus client wrapper with register-aware helpers."""

    def __init__(
        self,
        port: str = PORT,
        baudrate: int = BAUDRATE,
        servo_id: int = SERVO_ID,
        timeout: float = 0.5,
    ) -> None:
        self.servo_id = servo_id
        self.client = ModbusSerialClient(
            port=port,
            baudrate=baudrate,
            bytesize=8,
            parity="N",
            stopbits=1,
            timeout=timeout,
        )

    def connect(self) -> bool:
        return bool(self.client.connect())

    def close(self) -> None:
        self.client.close()

    @staticmethod
    def register_spec(register: RegisterRef) -> Optional[RegisterSpec]:
        addr = FeetechModbusServo.resolve_address(register)
        return REGISTER_SPECS.get(addr)

    @staticmethod
    def resolve_address(register: RegisterRef) -> int:
        if isinstance(register, FeetechRegister):
            return int(register)
        if isinstance(register, int):
            return register
        normalized = _normalize_register_name(register)
        resolved = REGISTER_BY_NORMALIZED_NAME.get(normalized)
        if resolved is None:
            raise KeyError(f"Unknown register: {register}")
        return int(resolved)

    def _read_holding(self, address: int):
        try:
            return self.client.read_holding_registers(address=address, count=1, slave=self.servo_id)
        except TypeError:
            # Compatibility with older pymodbus versions.
            return self.client.read_holding_registers(address=address, count=1, unit=self.servo_id)

    def _write_holding(self, address: int, value: int):
        try:
            return self.client.write_register(address=address, value=value, slave=self.servo_id)
        except TypeError:
            return self.client.write_register(address=address, value=value, unit=self.servo_id)

    def read_register(self, register: RegisterRef) -> Optional[int]:
        addr = self.resolve_address(register)
        result = self._read_holding(addr)
        if result.isError() or not getattr(result, "registers", None):
            return None
        return int(result.registers[0])

    def write_register(self, register: RegisterRef, value: int) -> None:
        addr = self.resolve_address(register)
        spec = REGISTER_SPECS.get(addr)
        if spec is not None and spec.access != "rw":
            raise ValueError(
                f"Register {addr} ({spec.name}) is not writable (access={spec.access})"
            )
        if value < 0 or value > 0xFFFF:
            raise ValueError(f"Register values must be 16-bit unsigned integers, got {value}")

        result = self._write_holding(addr, int(value))
        if result.isError():
            raise IOError(f"Failed to write register {addr}: {result}")

    def read_named(self, name: str) -> Optional[int]:
        return self.read_register(name)

    def write_named(self, name: str, value: int) -> None:
        self.write_register(name, value)


def run_sweep(
    servo: FeetechModbusServo,
    pos_min: int = POS_MIN,
    pos_max: int = POS_MAX,
    max_torque_limit: int = 1000,
    goal_accel: int = 100,
    goal_velocity: int = 500,
    settle_tolerance: int = 30,
    poll_steps: int = 100,
    poll_interval_s: float = 0.1,
    endpoint_pause_s: float = 0.3,
) -> None:
    pos = servo.read_register(FeetechRegister.PRESENT_POSITION)
    print(f"Current position: {pos}")

    print("Enabling torque...")
    servo.write_register(FeetechRegister.TORQUE_ENABLE, 1)
    time.sleep(0.1)

    servo.write_register(FeetechRegister.MAX_TORQUE_LIMIT, max_torque_limit)
    servo.write_register(FeetechRegister.GOAL_ACCELERATION, goal_accel)
    servo.write_register(FeetechRegister.GOAL_VELOCITY, goal_velocity)
    time.sleep(0.1)

    torque = servo.read_register(FeetechRegister.TORQUE_ENABLE)
    print(f"Torque enable: {torque}")

    print(f"\nSweeping {pos_min} <-> {pos_max} (press Ctrl+C to stop)\n")

    targets = [pos_min, pos_max]
    target_idx = 0

    while True:
        target = targets[target_idx]
        degrees = position_to_degrees(target)
        print(f"  -> Moving to {target} ({degrees:+.1f}°)")

        servo.write_register(FeetechRegister.GOAL_POSITION, target)

        for _ in range(poll_steps):
            time.sleep(poll_interval_s)
            pos = servo.read_register(FeetechRegister.PRESENT_POSITION)
            if pos is not None:
                pos_deg = position_to_degrees(pos)
                sys.stdout.write(f"     position: {pos:4d} ({pos_deg:+6.1f}°)  \r")
                sys.stdout.flush()
                if abs(pos - target) < settle_tolerance:
                    print(f"     reached: {pos:4d} ({pos_deg:+6.1f}°)       ")
                    break
        else:
            print(f"\n     timeout waiting for target {target}")

        time.sleep(endpoint_pause_s)
        target_idx = 1 - target_idx


def main():
    servo = FeetechModbusServo(port=PORT, baudrate=BAUDRATE, servo_id=SERVO_ID, timeout=0.5)

    if not servo.connect():
        print(f"Failed to connect to {PORT} at {BAUDRATE} baud")
        return 1

    try:
        run_sweep(servo)

    except KeyboardInterrupt:
        print("\n\nStopping...")
    finally:
        print("Disabling torque...")
        try:
            servo.write_register(FeetechRegister.TORQUE_ENABLE, 0)
        except Exception as exc:  # best-effort shutdown on communication errors
            print(f"Failed to disable torque cleanly: {exc}")
        servo.close()
        print("Done.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
