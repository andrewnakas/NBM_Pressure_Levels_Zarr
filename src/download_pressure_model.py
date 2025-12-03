"""
Download pressure-level GRIB2 data for supported models (HRRR, NAM) and
write a consolidated Zarr store. Designed for GitHub Actions with LFS cleanup.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import numpy as np
import requests
import xarray as xr

# Model configuration -----------------------------------------------------------------

MODEL_CONFIG = {
    "hrrr": {
        "bases": [
            ("aws", "https://noaa-hrrr-bdp-pds.s3.amazonaws.com", "conus"),
            ("nomads", "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod", "conus"),
        ],
        "file_tmpl": "hrrr.t{cycle}z.wrfprsf{fhr:02d}.grib2",
        "path_tmpl": "{base}/hrrr.{date}/{domain}/{filename}",
        "default_hours": list(range(0, 19)),  # 0-18
        "allowed_cycles": None,  # hourly
        "default_lookback": 10,
    },
    "nam": {
        "bases": [
            ("aws", "https://noaa-nam-pds.s3.amazonaws.com", None),
            ("nomads", "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod", None),
        ],
        "file_tmpl": "nam.t{cycle}z.awphys{fhr:02d}.tm00.grib2",
        "path_tmpl": "{base}/nam.{date}/{filename}",
        "default_hours": list(range(0, 37)),  # 0-36 hourly 12 km
        "allowed_cycles": ["00", "06", "12", "18"],
        "default_lookback": 36,
    },
}


# Helpers -----------------------------------------------------------------------------

def parse_hours(arg: Optional[str], default: List[int]) -> List[int]:
    if arg is None:
        return default
    if "," in arg:
        return sorted({int(x.strip()) for x in arg.split(",") if x.strip()})
    if "-" in arg:
        if ":" in arg:
            range_part, step_part = arg.split(":")
            step = int(step_part)
        else:
            range_part, step = arg, 1
        start_str, end_str = range_part.split("-")
        start, end = int(start_str), int(end_str)
        return list(range(start, end + 1, step))
    return [int(arg)]


def head_ok(url: str, timeout: float = 6.0) -> bool:
    try:
        r = requests.head(url, timeout=timeout)
        if r.status_code == 200:
            return True
    except requests.RequestException:
        pass
    try:
        r = requests.get(url, headers={"Range": "bytes=0-32"}, timeout=timeout, stream=True)
        return r.status_code in (200, 206)
    except requests.RequestException:
        return False


def build_url(base: str, path_tmpl: str, date_str: str, cycle: str, fhr: int, domain: Optional[str], file_tmpl: str) -> str:
    filename = file_tmpl.format(cycle=cycle, fhr=fhr)
    return path_tmpl.format(base=base.rstrip("/"), date=date_str, cycle=cycle, domain=domain or "", filename=filename)


def find_latest_run(
    model: str,
    hours: Iterable[int],
    lookback_hours: int,
) -> Tuple[str, str, Tuple[str, str, Optional[str]]]:
    cfg = MODEL_CONFIG[model]
    allowed = cfg["allowed_cycles"]
    now = datetime.now(timezone.utc) - timedelta(minutes=40)  # allow files to appear
    fh_probe = min(hours)
    for offset in range(lookback_hours):
        cand = now - timedelta(hours=offset)
        date_str = cand.strftime("%Y%m%d")
        cycle = cand.strftime("%H")
        if allowed and cycle not in allowed:
            continue
        for base_name, base_url, domain in cfg["bases"]:
            url = build_url(
                base_url,
                cfg["path_tmpl"],
                date_str,
                cycle,
                fh_probe,
                domain,
                cfg["file_tmpl"],
            )
            if head_ok(url):
                return date_str, cycle, (base_name, base_url, domain)
    raise RuntimeError("No recent cycle found within lookback window.")


def download_file(url: str, dest: Path, retries: int = 3) -> None:
    sess = requests.Session()
    for attempt in range(1, retries + 1):
        try:
            with sess.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                dest.parent.mkdir(parents=True, exist_ok=True)
                with dest.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)
            return
        except requests.RequestException as exc:  # noqa: BLE001
            if attempt == retries:
                raise
            wait = 2 * attempt
            print(f"[warn] {url} failed ({exc}); retrying in {wait}s")
            import time

            time.sleep(wait)


def open_pressure_dataset(path: Path) -> xr.Dataset:
    import cfgrib
    xr.set_options(use_new_combine_kwarg_defaults=True)

    parts = cfgrib.open_datasets(
        path,
        backend_kwargs={
            "filter_by_keys": {"typeOfLevel": "isobaricInhPa"},
            "indexpath": "",
        },
    )
    if not parts:
        raise RuntimeError("No isobaric levels found.")
    ds = xr.merge(parts, compat="override", join="outer").squeeze(drop=True)

    # Extract forecast hour from GRIB step if present, else filename
    if "step" in ds:
        fh = ds["step"].astype("timedelta64[h]").astype(np.int32).item()
        ds = ds.assign_coords(step=fh).rename(step="forecast_hour")
    else:
        # derive from filename (f##)
        try:
            fh = int(path.name.split("f")[-1][:3])
        except Exception:
            fh = 0
        ds = ds.expand_dims(forecast_hour=[fh])

    chunk_map = {}
    for dim in ["y", "x", "latitude", "longitude"]:
        if dim in ds.dims:
            chunk_map[dim] = min(300, max(80, ds.dims[dim] // 6))
    if "isobaricInhPa" in ds.dims:
        chunk_map["isobaricInhPa"] = -1
    if "forecast_hour" in ds.dims:
        chunk_map["forecast_hour"] = min(4, ds.dims["forecast_hour"])
    ds = ds.chunk(chunk_map)
    return ds


def save_zarr(ds: xr.Dataset, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        shutil.rmtree(out_path)
    ds.to_zarr(out_path, mode="w", consolidated=True)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Download pressure-level data to Zarr")
    parser.add_argument("--model", choices=MODEL_CONFIG.keys(), default=os.environ.get("MODEL", "hrrr"))
    parser.add_argument("--forecast-hours", default=os.environ.get("FORECAST_HOURS"))
    parser.add_argument("--zarr-path", default=os.environ.get("ZARR_PATH"))
    parser.add_argument("--lookback-hours", type=int, default=None)
    parser.add_argument("--domain", default=os.environ.get("DOMAIN"))
    args = parser.parse_args(argv)

    cfg = MODEL_CONFIG[args.model]
    hours = parse_hours(args.forecast_hours, cfg["default_hours"])
    lookback = args.lookback_hours or int(os.environ.get("LOOKBACK_HOURS", cfg["default_lookback"]))
    domain = args.domain or cfg["bases"][0][2]
    date_str, cycle, base_tuple = find_latest_run(args.model, hours, lookback)
    base_name, base_url, _domain_from_cfg = base_tuple

    print(f"[info] {args.model.upper()} cycle {date_str} {cycle}z via {base_name} ({base_url}), domain={domain}")

    tmpdir = Path(tempfile.mkdtemp(prefix=f"{args.model}_"))
    downloaded: List[Path] = []
    try:
        for fh in hours:
            url = build_url(base_url, cfg["path_tmpl"], date_str, cycle, fh, domain, cfg["file_tmpl"])
            dest = tmpdir / Path(url).name
            try:
                download_file(url, dest)
            except Exception as exc:  # noqa: BLE001
                print(f"[warn] skipping f{fh:03d}: {exc}")
                continue
            downloaded.append(dest)

        if not downloaded:
            raise RuntimeError("No GRIB files downloaded.")

        datasets = []
        for p in downloaded:
            try:
                ds = open_pressure_dataset(p)
            except Exception as exc:  # noqa: BLE001
                print(f"[warn] failed to open {p.name}: {exc}")
                continue
            datasets.append(ds)

        if not datasets:
            raise RuntimeError("No datasets opened successfully; aborting.")

        combined = xr.concat(datasets, dim="forecast_hour").sortby("forecast_hour")
        out_path = Path(args.zarr_path or f"data/{args.model}_pressure.zarr")
        save_zarr(combined, out_path)

        meta = {
            "model": args.model,
            "cycle_date": date_str,
            "cycle_hour": cycle,
            "forecast_hours": hours,
            "source": base_url,
            "domain": domain,
            "generated_utc": datetime.now(timezone.utc).isoformat(),
        }
        meta_path = out_path.with_suffix(".metadata.json")
        meta_path.write_text(json.dumps(meta, indent=2))
        print(f"[info] Wrote Zarr -> {out_path}")
        return 0
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
