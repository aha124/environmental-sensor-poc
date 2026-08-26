"""Load the YUAG object-to-sensor baseline into the pipeline's parquet format.

Rob's export is pipe-delimited: accession|TMS|sensor[;sensor...]
TMS is the join key (it matches the LUX URL, obj/<TMS>.json). Accession is
carried for display only, per Rob: accession is stable across migrations but
cannot be used to look an object up directly.

ValidFrom is set to the load date rather than backdated. We do not know when
these assignments actually began; the LUX record carries no timespan. Claiming
an earlier date would be inventing history we do not have.
"""
import sys, os, csv
sys.path.insert(0, '/src'); os.chdir('/src')
import polars as pl
from datetime import datetime, timezone

SRC = sys.argv[1] if len(sys.argv) > 1 else '/tmp/yuag_sensors.txt'
OUT = sys.argv[2] if len(sys.argv) > 2 else '/src/data/object_sensor_scratch'
CONSERV_CUSTOMER = '307'          # every sensor in this baseline is customer 307
FAR_FUTURE = '9999-12-31'

os.makedirs(OUT, exist_ok=True)
today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

rows, skipped = [], []
with open(SRC) as f:
    for n, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue
        parts = line.split('|')
        if len(parts) != 3:
            skipped.append((n, line)); continue
        accession, tms, sensors = (p.strip() for p in parts)
        if not tms.isdigit():
            skipped.append((n, line)); continue
        for s in (x.strip() for x in sensors.split(';') if x.strip()):
            rows.append({
                'ObjectID': f'https://media.art.yale.edu/content/lux/obj/{tms}.json',
                'TMSNumber': int(tms),
                'AccessionNumber': accession,
                'DeviceID': f'conserv:{CONSERV_CUSTOMER}:{s}',
                'ValidFrom': today,
                'ValidTo': FAR_FUTURE,
            })

df = pl.DataFrame(rows)
path = os.path.join(OUT, f'object_sensor_map_{today}.parquet')
df.write_parquet(path)
csv_path = path.replace('.parquet', '.csv')
df.write_csv(csv_path)

print(f"source rows read     : {n}")
print(f"skipped (bad shape)  : {len(skipped)}")
print(f"output rows          : {df.height:,}")
print(f"distinct objects     : {df['TMSNumber'].n_unique():,}")
print(f"distinct devices     : {df['DeviceID'].n_unique()}")
print(f"\nwrote {path}")
print(f"wrote {csv_path}")
print()
print(df.head(5))
if skipped:
    print("\nskipped lines:")
    for n, l in skipped[:5]:
        print(f"  line {n}: {l!r}")
