#!/usr/bin/env python3
import glob
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet
import pyarrow.csv

schema = {
    "cards": ["guid", "cardType", "level"],
    "device": ["guid", "device", "os"],
    "ip": ["guid"],
    "user": ["guid", "fraudMoneyTransfer", "moneyTransferErrorCancelAcmount"],
    "has_ip": ["start_guid", "start_label", "end_guid", "end_label", "ipDate"],
    "has_cc": ["start_guid", "start_label", "end_guid", "end_label", "cardDate"],
    "p2p": ["start_guid", "start_label", "end_guid", "end_label", "totalAmount", "transactionDateTime"],
    "referred": ["start_guid", "start_label", "end_guid", "end_label"],
    "used": ["start_guid", "start_label", "end_guid", "end_label", "deviceDate"]
}

def run(root_dir=Path(".")):
    if not isinstance(root_dir, Path):
        root_dir = Path(str(root_dir))

    for path in root_dir.glob("*.csv*"):
        name = path.name
        for suffix in path.suffixes:
            name = name.replace(suffix, '')

        headers = schema.get(name)
        if headers is None:
            raise Exception(f"no known schema for '{name}'")

        read_options = pa.csv.ReadOptions(column_names=headers)
        csv = pa.csv.read_csv(path, read_options=read_options)
        print(f"read {path} with {csv.num_rows:,} rows")

        pq = path.with_name(f"{name}.parquet")
        pa.parquet.write_table(csv, pq, compression="SNAPPY")
        print(f"wrote {pq}")


if __name__ == '__main__':
    import sys
    run(Path(sys.argv[-1]))
