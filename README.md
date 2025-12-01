# NBM CONUS Pressure Levels → Zarr

Hourly GitHub Action that grabs the latest National Blend of Models (NBM) CONUS pressure‑level GRIB2 files, converts them to a consolidated Zarr store, and keeps the repo tidy so Git/LFS storage stays minimal.

## What it does
- Finds the most recent available NBM cycle on the public S3 bucket `noaa-nbm-grib2-pds`.
- Falls back to NOMADS if the AWS open-data bucket is briefly missing a cycle.
- Probes `master` GRIB files first (where pressure-level fields live), falling back to `core` if needed.
- Downloads a configurable list of forecast hours (default 0–36) for the CONUS grid (`co`).
- Reads only pressure‑level fields (`typeOfLevel=isobaricInhPa`) with `cfgrib` and merges them.
- Concatenates along `forecast_hour` and writes `data/nbm_conus_pressure.zarr` (plus a small metadata JSON).
- Cleans temp GRIB downloads, prunes LFS objects, and force‑pushes a single commit to keep history small.

## Running locally
```bash
micromamba create -f environment.yml -n nbm
micromamba activate nbm
python src/download_conus_pressure.py --forecast-hours 0-36 --product co --zarr-path data/nbm_conus_pressure.zarr
```

Environment variables override the same flags:
- `FORECAST_HOURS` (e.g., `0-48:3` or `0,1,2,3,6`)
- `NBM_PRODUCT` (`co`, `ak`, `hi`, `pr`)
- `ZARR_PATH` (default `data/nbm_conus_pressure.zarr`)
- `LOOKBACK_HOURS` (default `18`)

## GitHub Action
The workflow `.github/workflows/update.yml` runs hourly (`cron: "0 * * * *"`) and on manual dispatch. It:
1. Checks out with LFS.
2. Spins up the `environment.yml` env via micromamba.
3. Executes `src/download_conus_pressure.py`.
4. Commits changes, prunes LFS/Git objects, and force‑pushes to `main`.

## Notes on storage
- Zarr store is tracked via LFS pattern `data/nbm_conus_pressure.zarr/**`.
- Each run removes the previous Zarr directory before writing the new one to avoid version bloat.
- `git lfs prune` + aggressive `git gc` keep local objects lean; force‑push keeps remote history to a single commit.

## Adjusting coverage
- Increase lead time by setting `FORECAST_HOURS` (e.g., `0-84:3` for 3‑hourly out to 84 h).
- Change domain with `NBM_PRODUCT`: `ak` (Alaska), `hi` (Hawaii), `pr` (Puerto Rico).
- If you need more recent cycles, raise `LOOKBACK_HOURS`; if bandwidth is a concern, lower the hour range.
