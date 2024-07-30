import argparse
import os
import re
import shutil
import tempfile

import pandas as pd

import arcpy
import arcpy.da

from arcpy.management import (
    CreateDatabaseConnection,
)


parser = argparse.ArgumentParser(
    description="Extract an asset table to a file geodatabase"
)

parser.add_argument("-s", "--server", type=str, dest="server")

parser.add_argument("-d", "--database", type=str, dest="database")

parser.add_argument("-u", "--username", type=str, dest="username")

parser.add_argument("-p", "--password", type=str, dest="password")

parser.add_argument(
    "asset_name",
    type=str,
)

parser.add_argument(
    "output_file",
    type=str,
)

args = parser.parse_args()

server = args.server.upper()
asset_name = args.asset_name

tdir = tempfile.mkdtemp()
cwd = os.path.dirname(os.path.realpath(__file__))

try:
    print("Extracting {}...".format(asset_name))

    sde = CreateDatabaseConnection(
        tdir,
        "db",
        "SQL_SERVER",
        server,
        "DATABASE_AUTH",
        username=args.username,
        password=args.password,
        database=args.database,
    )

    fc = os.path.join(
        str(sde), "{}.{}.{}".format(args.database, args.username, asset_name)
    )

    OIDFieldName = arcpy.Describe(fc).OIDFieldName

    fields = [
        field.name
        for field in arcpy.ListFields(fc)
        if not re.match("Shape", field.name)
    ]

    print("Extracting {}...".format(fields))

    data = []

    for row in arcpy.da.SearchCursor(fc, fields):
        data.append(row)

    df = pd.DataFrame(data, columns=fields)
    df = df.set_index(OIDFieldName, drop=True)
    df.to_json(args.output_file, date_format="iso", orient="records")

finally:
    shutil.rmtree(tdir)
