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
    "input_file",
    type=str,
)

args = parser.parse_args()

server = args.server.upper()
asset_name = args.asset_name

tdir = tempfile.mkdtemp()
cwd = os.path.dirname(os.path.realpath(__file__))

try:
    print("Inserting to {}...".format(asset_name))

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

    df = pd.read_json(args.input_file, convert_dates=["ModifiedOn"], orient="records")

    if len(df) == 0:
        print("No inserts to process. Exiting...")
        raise SystemExit()

    print("Will insert {} rows...".format(len(df)))

    with arcpy.da.InsertCursor(fc, df.columns.tolist()) as cursor:
        for row in df.itertuples(index=False):
            cursor.insertRow(row)

finally:
    shutil.rmtree(tdir)
