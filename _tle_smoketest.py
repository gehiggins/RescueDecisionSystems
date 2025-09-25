from flask_app.app.sat_fetch_tle import load_tle_snapshot

print("\\n--- NOAA-19 by NORAD ---")
df = load_tle_snapshot(["33591"])               # pass as strings
print(df.head().to_string(index=False))
print("\\nCols:", list(df.columns))
print("dtypes:\\n", df.dtypes, "\\n")

print("\\n--- Galileo group (via E-prefix) ---")
dg = load_tle_snapshot(["E401"])                # any E… infers GROUP=galileo
print(dg.head().to_string(index=False))
print("\\nRows returned:", len(dg))
