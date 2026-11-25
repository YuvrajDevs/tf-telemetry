"""Download and parse NASA's C-MAPSS turbofan engine degradation dataset.

Source: NASA Prognostics Center of Excellence data repository (public, no login).
Each row = one operating cycle (flight) of one engine, run until failure.
Columns: unit, cycle, 3 operational settings, 21 sensor readings.
"""
import io
import random
import urllib.request
import zipfile

from .config import DATA_DIR

URL = "https://phm-datasets.s3.amazonaws.com/NASA/6.+Turbofan+Engine+Degradation+Simulation+Data+Set.zip"
SENSORS = [f"s{i}" for i in range(1, 22)]
COLUMNS = ["unit", "cycle", "op_setting_1", "op_setting_2", "op_setting_3"] + SENSORS
# FD001 and FD003 run at a single operating condition, so sensor drift = wear.
# (FD002/FD004 mix six conditions and would need per-condition normalisation.)
DEFAULT_SUBSETS = ("FD001", "FD003")


def download(force: bool = False) -> None:
    """Fetch the zip once and extract train_FD00x.txt files into data/cmapss/."""
    out = DATA_DIR / "cmapss"
    if (out / "train_FD001.txt").exists() and not force:
        print(f"already present: {out}")
        return
    out.mkdir(parents=True, exist_ok=True)
    print(f"downloading {URL} ...")
    with urllib.request.urlopen(URL, timeout=120) as r:
        outer = zipfile.ZipFile(io.BytesIO(r.read()))
    inner_name = next(n for n in outer.namelist() if n.endswith("CMAPSSData.zip"))
    inner = zipfile.ZipFile(io.BytesIO(outer.read(inner_name)))
    for name in inner.namelist():
        if name.endswith(".txt"):
            (out / name).write_bytes(inner.read(name))
    print(f"extracted to {out}")


def load(subset: str) -> list[dict]:
    """Rows of train_<subset>.txt as dicts. Falls back to synthetic data if the file is missing."""
    path = DATA_DIR / "cmapss" / f"train_{subset}.txt"
    if not path.exists():
        print(f"{path} not found: using synthetic engines (run `python -m telemetry.cli download` for real NASA data)")
        return synthetic(subset)
    rows = []
    for line in path.read_text().split("\n"):
        parts = line.split()
        if not parts:
            continue
        vals = [float(x) for x in parts[: len(COLUMNS)]]
        row = dict(zip(COLUMNS, vals))
        row["unit"], row["cycle"] = int(row["unit"]), int(row["cycle"])
        rows.append(row)
    return rows


# Baseline means used by the synthetic generator (from healthy FD001 cycles)
_BASE = {"s1": 518.67, "s2": 642.38, "s3": 1586.9, "s4": 1402.8, "s5": 14.62, "s6": 21.61, "s7": 553.95, "s8": 2388.06,
         "s9": 9055.8, "s10": 1.3, "s11": 47.36, "s12": 521.9, "s13": 2388.05, "s14": 8137.4, "s15": 8.418, "s16": 0.03,
         "s17": 392.3, "s18": 2388.0, "s19": 100.0, "s20": 38.93, "s21": 23.36}
_DRIFT = {"s2": 1.3, "s3": 16, "s4": 28, "s7": -2.6, "s9": 43, "s11": 0.84, "s12": -2.2, "s14": 31, "s15": 0.105,
          "s17": 4.5, "s20": -0.53, "s21": -0.31}


def synthetic(subset: str, engines: int = 100, seed: int = 7) -> list[dict]:
    """Engines whose degrading sensors drift exponentially toward failure, same shape as C-MAPSS."""
    rnd = random.Random(f"{subset}-{seed}")
    rows = []
    for unit in range(1, engines + 1):
        life = rnd.randint(128, 360)
        for cycle in range(1, life + 1):
            wear = (cycle / life) ** 3
            row = {"unit": unit, "cycle": cycle, "op_setting_1": rnd.gauss(0, 0.002), "op_setting_2": rnd.gauss(0, 0.0003), "op_setting_3": 100.0}
            for s in SENSORS:
                noise = abs(_BASE[s]) * 0.0008 if s in _DRIFT else 0
                row[s] = round(_BASE[s] + _DRIFT.get(s, 0) * wear + rnd.gauss(0, noise), 4)
            rows.append(row)
    return rows
