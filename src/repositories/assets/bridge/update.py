import argparse
import os
import re
import shutil
import tempfile

import numpy as np
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

try:
    print("Updating {}...".format(asset_name))

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

    print("Will update {} rows...".format(len(df)))

    for row in df.itertuples(index=False):
        values = [None if pd.isnull(x) else x for x in row]

        where_clause = "{} = '{}'".format(
            arcpy.AddFieldDelimiters(fc, "AssetID"), row.AssetID
        )

        with arcpy.da.UpdateCursor(fc, df.columns.tolist(), where_clause) as cursor:
            for dest in cursor:
                print("Updating row with AssetID '{}'...".format(row.AssetID))
                cursor.updateRow(values)

finally:
    shutil.rmtree(tdir)
