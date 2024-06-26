VIOLATION_TYPE_MAP = {
    "1": {"Code": "16.20.130-C", "Name": "FIRE HYDRANT"},
    "2": {"Code": "16.20.120-P", "Name": "ABANDONED AUTO"},
    "3": {"Code": "16.20.120-B", "Name": "ANGLE LOADING"},
    "4": {"Code": "16.20.110-D", "Name": "ANGLE PARKING"},
    "5": {"Code": "16.20.860-E", "Name": "AREA PERMIT ABUSE"},
    "6": {
        "Code": "16.20.860-C-1,2",
        "Name": "AREA PERMIT REQUIRED",
    },
    "7": {"Code": "16.20.130-V", "Name": "BLOCKED DRIVEWAY"},
    "8": {
        "Code": "16.20.130-A",
        "Name": "BLOCKING VIEW AT INTERS.",
    },
    "9": {"Code": "16.20.130-K", "Name": "BRIDGE APPROACH"},
    "10": {"Code": "16.20.230", "Name": "BUS ZONE"},
    "11": {"Code": "16.20.270", "Name": "CARPOOL ZONE"},
    "12": {"Code": "16.10.050", "Name": "COMPACTS ONLY"},
    "13": {"Code": "16.20.510", "Name": "CONSTRUCTION ZONE"},
    "14": {"Code": "16.20.280", "Name": "CYCLES ONLY"},
    "15": {"Code": "16.20.130-T", "Name": "DIVIDED HIGHWAY"},
    "16": {"Code": "16.20.120-J", "Name": "DOUBLE PARKING"},
    "17": {"Code": "16.10.050", "Name": "DRIVER REMAIN AT WHEEL"},
    "19": {"Code": "16.20.120-L", "Name": "EXPIRED REGISTRATION"},
    "20": {"Code": "16.20.120-F", "Name": "EDGE LINE"},
    "22": {
        "Code": "16.20.150-A",
        "Name": "FOR SALE on public street",
    },
    "23": {"Code": "16.20.130-D", "Name": "HANDICAPPED RAMP"},
    "24": {"Code": "16.20.280", "Name": "HOTEL ZONE"},
    "25": {
        "Code": "16.20.120-M,5",
        "Name": "IMPROPERLY SECURED VEH.",
    },
    "27": {"Code": "16.20.120-M,4", "Name": "KEYS LEFT IN CAR"},
    "28": {"Code": "16.20.120-M,1", "Name": "MOTOR RUNNING"},
    "29": {"Code": "16.20.220-B", "Name": "LOADING ZONE"},
    "30": {"Code": "16.10.050", "Name": "MAIL ZONE"},
    "31": {"Code": "16.20.520", "Name": "MAINTENANCE ZONE"},
    "32": {
        "Code": "16.20.130-H",
        "Name": "MASS-TRANSIT LANE/STREET",
    },
    "33": {"Code": "16.20.430-B", "Name": "METER FEEDING"},
    "34": {"Code": "16.20.220", "Name": "METERED LOADING ZONE"},
    "35": {"Code": "16.20.210", "Name": "NO PARKING ANYTIME"},
    "36": {"Code": "16.20.130-F", "Name": "NON-DESIGNATED PARKING"},
    "37": {"Code": "16.20.280-B", "Name": "OFFICIAL ZONE"},
    "38": {"Code": "16.20.130-Q", "Name": "PARK OVER CROSSWALK"},
    "39": {"Code": "16.20.110-D", "Name": "OVER 1 FT FROM CURB"},
    "40": {"Code": "16.20.130-I", "Name": "PARK OVER SIDEWALK"},
    "41": {"Code": "16.20.120-D", "Name": "OVER SPACE LINE"},
    "42": {"Code": "16.20.260", "Name": "OVERTIME PARKING"},
    "43": {"Code": "16.20.430", "Name": "OVERTIME METER"},
    "45": {"Code": "16.20.130-I", "Name": "PEDESTRIAN WAY"},
    "46": {"Code": "16.20.130-I", "Name": "PLANTING STRIP"},
    "47": {"Code": "16.20.130", "Name": "PROHIBITED AREA"},
    "48": {"Code": "16.20.205", "Name": "PROHIBITED TIME"},
    "49": {"Code": "16.20.260", "Name": "PUBLIC BLDG ZONE"},
    "50": {"Code": "16.20.280-B", "Name": "RESERVED ZONE"},
    "51": {"Code": "16.20.205-C", "Name": "SCHOOL ZONE"},
    "53": {"Code": "16.20.240-B", "Name": "TAXI ZONE"},
    "54": {"Code": "16.10.050", "Name": "TEMPORARY NO PARKING"},
    "55": {"Code": "16.20.215", "Name": "THEATER ZONE"},
    "56": {"Code": "16.30.210-A,1", "Name": "TOW AWAY ZONE"},
    "57": {"Code": "16.20.120-Q", "Name": "TRAFFIC HAZARD"},
    "58": {
        "Code": "16.20.120-H",
        "Name": "TRUCK/TRAILER PROHIBITED",
    },
    "59": {"Code": "16.20.205-B,C", "Name": "NO PARKING IN BLOCK"},
    "61": {
        "Code": "16.20.640-C",
        "Name": "UNLAWF. USE DISAB. PERM.",
    },
    "61B": {
        "Code": "16.20.640C,641C,645D",
        "Name": "DISA PERM PRIOR OFF",
    },
    "62": {"Code": "16.20.110-B", "Name": "WRONG WAY"},
    "63": {"Code": "16.20.120-G", "Name": "YELLOW CURB"},
    "64": {"Code": "16.20.120-A", "Name": "ALARM (VEHICLE)"},
    "65": {"Code": "16.20.130-E", "Name": "10 FT RURAL MAILBOX"},
    "66": {
        "Code": "16.20.130-P",
        "Name": "PARK WITHIN INTERSECTION",
    },
    "67": {"Code": "16.20.130-U", "Name": "BIKE LANE"},
    "68": {"Code": "16.20.213", "Name": "NO STOPPING OR PARKING"},
    "69": {"Code": "16.20.640-A,1", "Name": "DISABL ZONE PCO 1ST"},
    "69B": {"Code": "16.20.250", "Name": "DISA WHCH ZN PRIOR OFF"},
    "70": {"Code": "16.20.250", "Name": "DISABLED ZONE"},
    "70B": {"Code": "16.20.250", "Name": "DISA ZONE ORS PRIOR OFF"},
    "71": {
        "Code": "16.20.120-I",
        "Name": "I.LIEU OF OFF STR. STOR.",
    },
    "72": {"Code": "16.20.640-C,1", "Name": "DIS PLC-INVALID USE"},
    "73": {"Code": "16.20.640-C,2", "Name": "DIS PLC-MISUSE"},
    "73B": {
        "Code": "16.20.640D,641D,645E",
        "Name": "DISA PLA MIS PRIOR OFF",
    },
    "74": {"Code": "16.20.120 (L)", "Name": "Fail to Display Reg."},
    "75": {"Code": "16.20.120-R", "Name": "NO FRONT OR REAR PLATE"},
    "76": {"Code": "16.20.120-E", "Name": "BLOCKED STREET/ALLEY"},
    "77": {"Code": "16.10.050", "Name": "COMPLIANCE REQUIRED"},
    "78": {
        "Code": "16.10.050",
        "Name": "GOVERNMENT VEHICLES PROH.",
    },
    "79": {
        "Code": "16.20.595",
        "Name": "IMPROPER USE CARPOOL PERM",
    },
    "80": {
        "Code": "16.20.595",
        "Name": "IMPR. USE CONSTRUCT. HOOD",
    },
    "81": {"Code": "16.20.595", "Name": "IMPR. USE OF MAINT. HOOD"},
    "82": {
        "Code": "16.20.595",
        "Name": "IMPR. USE OF RESERV. ZONE",
    },
    "83": {
        "Code": "16.20.050",
        "Name": "NON-COMPLIANCE-ELECTR.CAR",
    },
    "84": {
        "Code": "16.20.110-E",
        "Name": "RIGHT OF WAY FOR PARKING",
    },
    "85": {"Code": "16.30.240", "Name": "TOW FOR TAG WARRANT"},
    "86": {
        "Code": "16.70.450",
        "Name": "TRUCK OFF STR. PARK. REQ.",
    },
    "87": {"Code": "16.20.130-M", "Name": "WATER METER BLOCKED"},
    "88": {
        "Code": "16.20.120-E",
        "Name": "PREVENTING FREE PASSAGE",
    },
    "89": {
        "Code": "16.20.445",
        "Name": "IMPR.DISPL.PARK.METER REC",
    },
    "90": {
        "Code": "16.30.225",
        "Name": "TOW WITH NOTICE AUTHORIZ.",
    },
    "91": {
        "Code": "16.30.220",
        "Name": "TOW WITHOUT NOT.AUTHORIZ.",
    },
    "92": {"Code": "16.20.431", "Name": "Impr Disp Met Rec Off St"},
    "93": {
        "Code": "16.20.120(S)",
        "Name": "GOVERNMENT VEHICLE PROHIB",
    },
    "94": {"Code": "16.20.280", "Name": "GOVERNMENT VEHICLES ONLY"},
    "95": {"Code": "16.20.445", "Name": "NO METER RECEIPT"},
    "96": {"Code": "16.20.220(B)", "Name": "TRUCK LOADING ZONE OT"},
    "97": {"Code": "16.20.190", "Name": "OT PARKING METER - 2ND"},
    "98": {
        "Code": "16.20.190",
        "Name": "OVERTIME PARKING ZONE-2ND",
    },
    "99": {"Code": 16, "Name": "OTHER"},
    "101": {
        "Code": "16.20.220(C)",
        "Name": "NO PARKING - AUX LANE",
    },
    "102": {"Code": "16.20.470(E)", "Name": "UNLAWFUL RECEIPT"},
    "103": {
        "Code": "16.20.260(B)",
        "Name": "RE-PARK SAME BLK TIME ZN",
    },
    "104": {
        "Code": "16.20.430(D)",
        "Name": "RE-PARK SAME BLOCK METER",
    },
    "105": {"Code": "16.20.190", "Name": "OT PARKING METER - 3RD"},
    "106": {"Code": "16.20.190", "Name": "OT PARKING ZONE - 3RD"},
    "109": {
        "Code": "16.20.120 (L)",
        "Name": "FAIL TO DISP REG 0 TO 90",
    },
    "110": {
        "Code": "16.20.120 (L)",
        "Name": "FAIL TO DISP REG 91 DAYS",
    },
    "111": {
        "Code": "16.35.120",
        "Name": "UPPER NW AREA PERMIT REQ",
    },
    "112": {
        "Code": "16.35.130",
        "Name": "UPPER NW NO METER RECEIPT",
    },
    "113": {
        "Code": "16.35.130 (B)",
        "Name": "UPPER NW OT LONG-TERM MTR",
    },
    "114": {
        "Code": "16.35.130",
        "Name": "UPPER NW OT LG-TR MTR 2ND",
    },
    "115": {
        "Code": "16.35.130",
        "Name": "UPPER NW OT LG-TR MTR 3RD",
    },
    "116": {
        "Code": "16.35.130 (D)",
        "Name": "UPPR NW OT SH-TRM NO REC",
    },
    "117": {
        "Code": "16.35.130 (D)",
        "Name": "UPPER NW OT SH-TERM MTR",
    },
    "118": {
        "Code": "16.35.130",
        "Name": "UPPER NW OT SH-TR MTR 2ND",
    },
    "119": {
        "Code": "16.35.130",
        "Name": "UPPER NW OT SH-TR MTR 3RD",
    },
    "120": {
        "Code": "16.35.220",
        "Name": "CEID AREA PERMIT REQUIRED",
    },
}
