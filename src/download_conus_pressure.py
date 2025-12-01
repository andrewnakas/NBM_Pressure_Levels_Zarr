"""
Fetch latest NBM CONUS pressure-level GRIB2 files and aggregate to a single Zarr store.

The script is designed to run inside GitHub Actions hourly.  It:
- Finds the most recent available NBM cycle on the S3 public bucket.
- Downloads a configurable set of forecast hours for the CONUS grid ("co").
- Opens only pressure-level fields (typeOfLevel=isobaricInhPa) via cfgrib.
- Concatenates along forecast_hour and writes a consolidated Zarr store.
- Cleans temporary downloads afterwards.

Environment variables / CLI flags let you tune the range of forecast hours,
output location, and basic chunking.  Defaults aim to keep the job light
enough for GitHub-hosted runners while still capturing useful lead times.
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
from typing import Iterable, List, Optional

import numpy as np
import requests
import xarray as xr


NBM_BASE = os.environ.get(
    "NBM_BASE_URL", "https://noaa-nbm-grib2-pds.s3.amazonaws.com"
)
DEFAULT_PRODUCT = os.environ.get("NBM_PRODUCT", "co")  # CONUS grid


def parse_forecast_hours(arg: Optional[str]) -> List[int]:
    """
    Parse a simple hour range string.

    Examples:
        "0-36" -> 0..36 inclusive
        "0-48:3" -> every 3 hours from 0..48
        "0,1,2,3,6,9" -> explicit list
    """
    if arg is None:
        return list(range(0, 37))  # 0-36 hourly

    if "," in arg:
        return sorted({int(x.strip()) for x in arg.split(",") if x.strip()})

    if "-" in arg:
        # Format start-end[:step]
        if ":" in arg:
            range_part, step_part = arg.split(":")
            step = int(step_part)
        else:
            range_part, step = arg, 1
        start_str, end_str = range_part.split("-")
        start, end = int(start_str), int(end_str)
        return list(range(start, end + 1, step))

    return [int(arg)]


def build_url(date_str: str, cycle: str, fhr: int, product: str) -> str:
    # Example: blend.20241130/12/core/blend.t12z.core.f001.co.grib2
    return (
        f"{NBM_BASE}/blend.{date_str}/{cycle}/core/"
        f"blend.t{cycle}z.core.f{fhr:03d}.{product}.grib2"
    )


def head_ok(url: str, timeout: float = 5.0) -> bool:
    try:
        resp = requests.head(url, timeout=timeout)
        return resp.status_code == 200
    except requests.RequestException:
        return False


def find_latest_run(
    product: str,
    forecast_hours: Iterable[int],
    lookback_hours: int = 18,
) -> tuple[str, str]:
    """
    Look backwards hour by hour to find the most recent cycle that has
    the first forecast-hour file available.
    """
    now = datetime.now(timezone.utc) - timedelta(hours=1)
    fh0 = min(forecast_hours)
    for offset in range(lookback_hours):
        candidate = now - timedelta(hours=offset)
        date_str = candidate.strftime("%Y%m%d")
        cycle = candidate.strftime("%H")
        probe_url = build_url(date_str, cycle, fh0, product)
        if head_ok(probe_url):
            return date_str, cycle
    raise RuntimeError("No recent NBM cycle found in lookback window.")


def download_file(url: str, dest: Path, retries: int = 3) -> None:
    session = requests.Session()
    for attempt in range(1, retries + 1):
        try:
            with session.get(url, stream=True, timeout=30) as resp:
                resp.raise_for_status()
                dest.parent.mkdir(parents=True, exist_ok=True)
                with dest.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)
            return
        except requests.RequestException as exc:
            if attempt == retries:
                raise
            wait = 2 * attempt
            print(f"[warn] {url} download failed ({exc}); retrying in {wait}s...")
            import time

            time.sleep(wait)


def open_pressure_dataset(path: Path) -> xr.Dataset:
    """
    Open a single GRIB file retaining only pressure-level fields.
    """
    import cfgrib  # local import to keep import time down

    # cfgrib.open_datasets splits per distinct paramId; merge afterwards.
    ds_parts = cfgrib.open_datasets(
        path,
        backend_kwargs={"filter_by_keys": {"typeOfLevel": "isobaricInhPa"}, "indexpath": ""},
    )
    if not ds_parts:
        raise RuntimeError(f"No pressure-level data found in {path}")
    ds = xr.merge(ds_parts, compat="override")

    # Normalize dimension for concatenation: forecast_hour as int hours
    if "step" in ds:
        fh = (
            ds["step"]
            .astype("timedelta64[h]")
            .astype(np.int32)
            .item()  # should be scalar
        )
        ds = ds.assign_coords(step=fh)
        ds = ds.rename(step="forecast_hour")
    else:
        # Fall back to using filename encoded hour
        fh = int(path.stem.split(".")[-2][1:])  # f003 -> 3
        ds = ds.expand_dims(forecast_hour=[fh])

    ds = ds.squeeze(drop=True)
    # Chunk lightly to avoid memory blowups when consolidating
    chunk_map = {}
    for dim in ["y", "x", "latitude", "longitude"]:
        if dim in ds.dims:
            chunk_map[dim] = min(250, max(50, ds.dims[dim] // 6))
    if "isobaricInhPa" in ds.dims:
        chunk_map["isobaricInhPa"] = -1
    if "forecast_hour" in ds.dims:
        chunk_map["forecast_hour"] = 4
    if chunk_map:
        ds = ds.chunk(chunk_map)
    return ds


def save_zarr(ds: xr.Dataset, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Remove existing store to keep history small
    if out_path.exists():
        shutil.rmtree(out_path)
    ds.to_zarr(out_path, mode="w", consolidated=True)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Download NBM CONUS pressure-level data into Zarr."
    )
    parser.add_argument(
        "--forecast-hours",
        default=os.environ.get("FORECAST_HOURS"),
        help="Hour range, e.g. '0-36' or '0-48:3' or comma list. Default 0-36.",
    )
    parser.add_argument(
        "--product",
        default=DEFAULT_PRODUCT,
        help="NBM grid id (co=CONUS, ak=Alaska, pr=PuertoRico, hi=Hawaii).",
    )
    parser.add_argument(
        "--zarr-path",
        default=os.environ.get("ZARR_PATH", "data/nbm_conus_pressure.zarr"),
        help="Where to write the consolidated Zarr store.",
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=int(os.environ.get("LOOKBACK_HOURS", "18")),
        help="How far back (hours) to search for an available cycle.",
    )
    args = parser.parse_args(argv)

    forecast_hours = parse_forecast_hours(args.forecast_hours)
    date_str, cycle = find_latest_run(args.product, forecast_hours, args.lookback_hours)
    print(f"[info] Using NBM cycle {date_str} {cycle}z")

    tmpdir = Path(tempfile.mkdtemp(prefix="nbm_dl_"))
    downloaded: List[Path] = []
    try:
        for fh in forecast_hours:
            url = build_url(date_str, cycle, fh, args.product)
            dest = tmpdir / f"blend.t{cycle}z.core.f{fh:03d}.{args.product}.grib2"
            try:
                download_file(url, dest)
            except Exception as exc:  # noqa: BLE001
                print(f"[warn] skipping f{fh:03d}: {exc}")
                continue
            downloaded.append(dest)

        if not downloaded:
            raise RuntimeError("No GRIB files downloaded; aborting.")

        datasets = []
        for path in downloaded:
            try:
                ds = open_pressure_dataset(path)
            except Exception as exc:  # noqa: BLE001
                print(f"[warn] failed to open {path.name}: {exc}")
                continue
            datasets.append(ds)

        if not datasets:
            raise RuntimeError("No datasets opened successfully; aborting.")

        combined = xr.concat(datasets, dim="forecast_hour")
        combined = combined.sortby("forecast_hour")
        save_zarr(combined, Path(args.zarr_path))

        meta = {
            "cycle_date": date_str,
            "cycle_hour": cycle,
            "forecast_hours": forecast_hours,
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "source": NBM_BASE,
            "product": args.product,
        }
        meta_path = Path(args.zarr_path).with_suffix(".metadata.json")
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(json.dumps(meta, indent=2))
        print(f"[info] Wrote Zarr -> {args.zarr_path}")
        return 0
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
