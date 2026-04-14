#!/usr/bin/env python3
"""Decode FTServo .mdat binary files into a human-readable register table."""

import struct
import sys

# Register name lookup: address -> (name, area, r/w)
REGISTER_MAP = {
    # INFO
    0:   ("Firmware Main Version NO.",   "INFO",    "r"),
    1:   ("Firmware Secondary Version",  "INFO",    "d"),
    2:   ("Firmware Release Date (Year)","INFO",    "r"),
    3:   ("Firmware Release Date (MMDD)","INFO",    "r"),
    # EPROM
    10:  ("ID",                          "EPROM",   "rw"),
    11:  ("Baud Rate",                   "EPROM",   "rw"),
    12:  ("Return Delay Time",           "EPROM",   "rw"),
    13:  ("Min Position Limit",          "EPROM",   "rw"),
    14:  ("Max Position Limit",          "EPROM",   "rw"),
    15:  ("Position Offset Value",       "EPROM",   "rw"),
    16:  ("Work Mode",                   "EPROM",   "rw"),
    17:  ("Position P Gain",             "EPROM",   "rw"),
    18:  ("Position D Gain",             "EPROM",   "rw"),
    19:  ("Position I Gain",             "EPROM",   "rw"),
    20:  ("Velocity P Gain",             "EPROM",   "rw"),
    21:  ("Velocity I Gain",             "EPROM",   "rw"),
    # SRAM
    128: ("Goal Position",               "SRAM",    "rw"),
    129: ("Torque Enable",               "SRAM",    "rw"),
    130: ("Goal Acceleration",           "SRAM",    "rw"),
    131: ("Goal Velocity",               "SRAM",    "rw"),
    132: ("Max Torque Limit",            "SRAM",    "rw"),
    133: ("EPROM Lock Sign",             "SRAM",    "rw"),
    134: ("Error Reset",                 "SRAM",    "rw"),
    # RONLY
    256: ("Hardware Error Status",       "RONLY",   "r"),
    257: ("Present Position",            "RONLY",   "r"),
    258: ("Present Velocity",            "RONLY",   "r"),
    259: ("Present PWM",                 "RONLY",   "r"),
    260: ("Present Input Voltage",       "RONLY",   "r"),
    261: ("Present Temperature",         "RONLY",   "r"),
    262: ("Moving Status",               "RONLY",   "r"),
    263: ("Present Current",             "RONLY",   "r"),
    # DEFAULT
    386: ("Max Velocity Limit",          "DEFAULT", "d"),
    387: ("Min Velocity Limit",          "DEFAULT", "d"),
    388: ("Acceleration Limit",          "DEFAULT", "d"),
    389: ("Starting Torque",             "DEFAULT", "d"),
    390: ("CW Dead Band",               "DEFAULT", "d"),
    391: ("CCW Dead Band",              "DEFAULT", "d"),
    392: ("Setting Byte",               "DEFAULT", "d"),
    393: ("Protection Switch",           "DEFAULT", "d"),
    394: ("LED Alarm Condition",         "DEFAULT", "d"),
    395: ("Max Temperature Limit",       "DEFAULT", "d"),
    396: ("Max Input Voltage",           "DEFAULT", "d"),
    397: ("Min Input Voltage",           "DEFAULT", "d"),
    398: ("Overload Current",            "DEFAULT", "d"),
    399: ("Overcurrent Protection Time", "DEFAULT", "d"),
    400: ("Protect Torque",              "DEFAULT", "d"),
    401: ("Overload Torque",             "DEFAULT", "d"),
    402: ("Overload Protection Time",    "DEFAULT", "d"),
    403: ("Angular Resolution",          "DEFAULT", "d"),
    404: ("Torque Limit Default Value",  "DEFAULT", "d"),
    405: ("Acceleration Default Value",  "DEFAULT", "d"),
    406: ("Velocity Default Value",      "DEFAULT", "d"),
}


def decode_mdat(filepath):
    """Decode an .mdat file and return a list of (address, name, value, area, rw) tuples."""
    with open(filepath, "rb") as f:
        data = f.read()

    entries = []
    offset = 0

    while offset < len(data) - 3:  # need at least 4 bytes for a section header
        start_addr = struct.unpack_from("<H", data, offset)[0]
        count = struct.unpack_from("<H", data, offset + 2)[0]
        offset += 4

        for i in range(count):
            if offset + 1 >= len(data):
                break
            value = struct.unpack_from("<H", data, offset)[0]
            addr = start_addr + i
            offset += 2

            if addr in REGISTER_MAP:
                name, area, rw = REGISTER_MAP[addr]
            else:
                name, area, rw = f"Unknown_{addr}", "?", "?"

            entries.append((addr, name, value, area, rw))

    return entries


def print_table(entries):
    """Print decoded entries as a formatted table."""
    header = f"{'Address':>7}  {'Memory':<35}  {'Value':>7}  {'Area':<8}  {'R/W'}"
    print(header)
    print("-" * len(header))
    for addr, name, value, area, rw in entries:
        print(f"{addr:>7}  {name:<35}  {value:>7}  {area:<8}  {rw}")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "smbl.mdat"
    entries = decode_mdat(path)
    print_table(entries)
