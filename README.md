# CONUS Pressure Levels → Zarr (HRRR & NAM)

Hourly GitHub Action grabs the latest HRRR and NAM pressure‑level GRIB2 files for CONUS, converts each model to its own Zarr store, and keeps the repo lean with LFS + aggressive cleanup.

## Models covered
- **NAM 12 km CONUS pressure levels** (`nam.tCCz.awphysFF.tm00.grib2` on AWS `noaa-nam-pds` or NOMADS). citeturn1search11

*(Workflow runs both HRRR (0–18h) and NAM (0–18h). Old Zarr stores are deleted before each write, and commits include the LFS-tracked Zarr directories.)*

## What it does
- Finds the most recent available cycle on AWS, falling back to NOMADS if needed.
- Downloads configurable forecast hours (default NAM 0–24 in workflow).
- Reads only `typeOfLevel=isobaricInhPa` messages with `cfgrib`, concatenates along `forecast_hour`, and writes:
  - `data/nam_conus_pressure.zarr`
- Writes small `.metadata.json` alongside each store.
- Cleans temp GRIBs, prunes LFS/Git objects, and force‑pushes a single commit to keep history tiny.

## Running locally
```bash
micromamba create -f environment.yml -n wx
micromamba activate wx

# HRRR
python src/download_pressure_model.py --model hrrr --forecast-hours 0-18 --zarr-path data/hrrr_conus_pressure.zarr

# NAM
python src/download_pressure_model.py --model nam --forecast-hours 0-36 --zarr-path data/nam_conus_pressure.zarr
```

Environment variables mirror the flags:
- `MODEL` (`hrrr` | `nam`)
- `FORECAST_HOURS`
- `ZARR_PATH`
- `LOOKBACK_HOURS`
- `DOMAIN` (HRRR domain, default `conus`)

## GitHub Action
`.github/workflows/update.yml` runs hourly. It:
1. Checks out with LFS and sets up micromamba.
2. Runs HRRR download → `data/hrrr_conus_pressure.zarr`.
3. Runs NAM download → `data/nam_conus_pressure.zarr`.
4. Commits, prunes LFS/Git objects, force‑pushes to `main`.

## Storage notes
- Zarr stores are tracked via LFS (`data/hrrr_conus_pressure.zarr/**`, `data/nam_conus_pressure.zarr/**`).
- Each run replaces the existing store to avoid history bloat.
- `git lfs prune` + `git gc --aggressive` keep both local and remote lean.
