#!/usr/bin/env python3
"""
Run one AERA stocktake for NorESM2-AERA on Betzy and create scaled
NorESM CO2 emission netCDF.

Example:

python run_aera_stocktake_betzy.py \
    --case NSSP126frc2esm_f19_tn14_noresm2.0.9_esm-ssp126-AERA_T_Betzy_20260709 \
    --rundir /cluster/work/users/yongyub/noresm/NSSP126frc2esm_f19_tn14_noresm2.0.9_esm-ssp126-AERA_T_Betzy_20260709/run \
    --archive-root /cluster/projects/nn2980k/yongyub/NORESM/NorESM2/archives \
    --aera-id AERA_T_Betzy \
    --initial-state-dat /cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/AERA_hist/AERA_T_Betzy/AERA_T_hist_nird_ann_tas_co2_ems_2014.dat \
    --future-output-dir /cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/AERA_future/AERA_T_Betzy \
    --hist-input-csv /cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/AERA_hist/NorESM2-LM_esm-hist_r1i1p1f1_1850-2014_AERA_temp_ff_input.csv \
    --emission-output-dir /cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/emissions/AERA/AERA_T_Betzy \
    --year-x 2019 \
    --tas-mode absolute
"""

from __future__ import annotations

import argparse
import glob
import math
import pickle
import shutil
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import xarray as xr
from netCDF4 import Dataset

import aera


# ============================================================
# Argument parser
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run one AERA stocktake and write NorESM emission netCDF."
    )

    p.add_argument("--case", required=True, help="NorESM case name.")
    p.add_argument("--rundir", required=True, type=Path, help="NorESM run directory.")

    p.add_argument(
        "--archive-root",
        default="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/archives",
        type=Path,
        help="NorESM archive root.",
    )
    p.add_argument(
        "--archive-case",
        default=None,
        help="Case name under archive-root. Default: same as --case.",
    )

    p.add_argument(
        "--aera-id",
        default="AERA_T_Betzy",
        help="Prefix for newly written AERA files.",
    )
    p.add_argument(
        "--initial-state-dat",
        required=True,
        type=Path,
        help="Initial AERA pickle state, e.g. NIRD-generated *_2014.dat.",
    )
    p.add_argument(
        "--future-output-dir",
        required=True,
        type=Path,
        help="Directory where new AERA outputs are written and read by later stocktakes.",
    )
    p.add_argument(
        "--hist-input-csv",
        default=None,
        type=Path,
        help=(
            "Optional historical CSV with columns year,temp,ff_emission. "
            "Used to fill missing 1850-2014 values if needed."
        ),
    )
    p.add_argument(
        "--cam-hist-dir",
        default=None,
        type=Path,
        help=(
            "Directory containing archived CAM h0 monthly files. "
            "If provided, this directory is searched before RUNDIR."
        ),
    )

    p.add_argument("--year-x", required=True, type=int, help="Stocktake year.")
    p.add_argument("--y0", default=1850, type=int)
    p.add_argument("--nyr", default=5, type=int)

    p.add_argument("--temp-target-rel", default=2.0, type=float)
    p.add_argument(
        "--temp-target-type",
        default=2,
        type=int,
        help="2 means target relative to model 1850-1900 mean.",
    )
    p.add_argument("--model-start-year", default=1850, type=int)

    p.add_argument(
        "--tas-mode",
        choices=["absolute", "old_anomaly"],
        default="absolute",
        help=(
            "absolute: use global mean TREFHT [K]. "
            "old_anomaly: use TREFHT - model_pi_trefht + obs_bias_correction."
        ),
    )
    p.add_argument("--model-pi-trefht", default=287.652, type=float)
    p.add_argument("--obs-bias-correction", default=0.358, type=float)

    # Emission netCDF settings
    p.add_argument(
        "--emission-template-nc",
        default=(
            "/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/emissions/"
            "AERA/AERA_T_Betzy/"
            "emissions-cmip6_CO2_anthro_surface_AERA_T_hist_nird_2015-2019_"
            "201401-210112_fv_1.9x2.5.nc"
        ),
        type=Path,
        help="Reference emission netCDF used as spatial/monthly template.",
    )
    p.add_argument(
        "--emission-output-dir",
        default=(
            "/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/emissions/"
            "AERA/AERA_T_Betzy"
        ),
        type=Path,
        help="Directory where scaled emission netCDF files are written.",
    )
    p.add_argument(
        "--base-pattern-year",
        default=2014,
        type=int,
        help="Template year used as monthly/spatial pattern for scaling.",
    )
    p.add_argument(
        "--skip-emission-nc",
        action="store_true",
        help="Run AERA only and skip emission netCDF creation.",
    )

    args = p.parse_args()

    if args.archive_case is None:
        args.archive_case = args.case

    return args


# ============================================================
# Previous AERA state
# ============================================================

def load_previous_pickle(
    path: Path,
    y0: int,
    y_last: int,
    n_total: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Load legacy AERA pickle:
        [year, ann_tas, ann_co2, ann_ems]

    Returns arrays aligned as index = year - y0.
    """

    print(f"Load previous AERA state pickle: {path}")

    with open(path, "rb") as f:
        field = pickle.load(f)

    old_year = np.asarray(field[0], dtype=float)
    old_tas = np.asarray(field[1], dtype=float)
    old_co2 = np.asarray(field[2], dtype=float)
    old_ems = np.asarray(field[3], dtype=float)

    ann_tas = np.full(n_total, np.nan)
    ann_co2 = np.full(n_total, np.nan)
    ann_ems = np.full(n_total, np.nan)

    # Preferred robust path: map using stored year values.
    if np.isfinite(old_year).any() and np.nanmax(old_year) > 1000:
        n_old = min(len(old_year), len(old_tas), len(old_co2), len(old_ems))

        for ii in range(n_old):
            yy_float = old_year[ii]

            if not np.isfinite(yy_float):
                continue

            yy = int(round(yy_float))

            if yy < y0 or yy > y_last:
                continue

            jj = yy - y0

            ann_tas[jj] = old_tas[ii]
            ann_co2[jj] = old_co2[ii]
            ann_ems[jj] = old_ems[ii]

    # Fallback: assume old arrays start at y0.
    else:
        print("WARNING: stored year array is not usable. Falling back to positional mapping.")
        n = min(n_total, len(old_tas), len(old_co2), len(old_ems))
        ann_tas[:n] = old_tas[:n]
        ann_co2[:n] = old_co2[:n]
        ann_ems[:n] = old_ems[:n]

    return ann_tas, ann_co2, ann_ems


def fill_from_hist_csv(
    hist_csv: Optional[Path],
    y0: int,
    year_x: int,
    ann_tas: np.ndarray,
    ann_ems: np.ndarray,
) -> None:
    """
    Fill missing temp / ff_emission from historical CSV.
    This does not overwrite existing non-NaN values from the previous .dat file.
    """

    if hist_csv is None:
        return

    if not hist_csv.exists():
        print(f"Historical CSV not found, skip: {hist_csv}")
        return

    print(f"Read historical AERA input CSV: {hist_csv}")

    df_input = pd.read_csv(hist_csv)

    if "year" in df_input.columns:
        df_input = df_input.set_index("year")

    df_input.index = df_input.index.astype(int)
    df_input = df_input.sort_index()

    years = np.arange(y0, year_x + 1)
    valid_years = years[np.isin(years, df_input.index.values)]

    if "temp" in df_input.columns:
        for yy in valid_years:
            if np.isnan(ann_tas[yy - y0]) and pd.notna(df_input.loc[yy, "temp"]):
                ann_tas[yy - y0] = float(df_input.loc[yy, "temp"])

    if "ff_emission" in df_input.columns:
        for yy in valid_years:
            if np.isnan(ann_ems[yy - y0]) and pd.notna(df_input.loc[yy, "ff_emission"]):
                ann_ems[yy - y0] = float(df_input.loc[yy, "ff_emission"])


# ============================================================
# NorESM TAS reading
# ============================================================

def find_cam_file(
    rundir: Path,
    archive_root: Path,
    case: str,
    archive_case: str,
    year: int,
    month: int,
    cam_hist_dir: Optional[Path] = None,
) -> Path:
    """
    Find CAM monthly h0 file.

    Search order:
      1. Explicit CAM history directory, if provided
      2. RUNDIR
      3. archive_root / archive_case / atm / hist

    This allows the AERA job to read archived CAM files after st_archive.
    """

    patterns = []

    if cam_hist_dir is not None:
        patterns.extend(
            [
                cam_hist_dir / f"{case}.cam.h0.{year}-{month:02d}.nc",
                cam_hist_dir / f"{case}.cam.h0.{year}-{month:02d}-*.nc",
                cam_hist_dir / f"{archive_case}.cam.h0.{year}-{month:02d}.nc",
                cam_hist_dir / f"{archive_case}.cam.h0.{year}-{month:02d}-*.nc",
            ]
        )

    patterns.extend(
        [
            rundir / f"{case}.cam.h0.{year}-{month:02d}.nc",
            rundir / f"{case}.cam.h0.{year}-{month:02d}-*.nc",
            archive_root / archive_case / "atm" / "hist" / f"{archive_case}.cam.h0.{year}-{month:02d}.nc",
            archive_root / archive_case / "atm" / "hist" / f"{archive_case}.cam.h0.{year}-{month:02d}-*.nc",
            archive_root / archive_case / "atm" / "hist" / f"{case}.cam.h0.{year}-{month:02d}.nc",
            archive_root / archive_case / "atm" / "hist" / f"{case}.cam.h0.{year}-{month:02d}-*.nc",
        ]
    )

    for pat in patterns:
        matches = sorted(glob.glob(str(pat)))
        if matches:
            return Path(matches[0])

    searched = "\n".join(str(p) for p in patterns)
    raise FileNotFoundError(
        f"Could not find CAM monthly file for {year}-{month:02d}.\n"
        f"Searched:\n{searched}"
    )

def make_area_from_cam_file(sample_file: Path) -> xr.DataArray:
    """
    Make CAM lat-lon grid-cell areas using latitude bounds inferred from lat centers.
    """

    print(f"Create area weights from sample file: {sample_file}")

    with xr.open_dataset(sample_file) as ds:
        if "lat" not in ds or "lon" not in ds:
            raise ValueError(f"Cannot find lat/lon in {sample_file}")

        alat = ds["lat"].values
        alon = ds["lon"].values

    if alat.ndim != 1 or alon.ndim != 1:
        raise ValueError("This script assumes 1D CAM lat/lon coordinates.")

    nj = len(alat)
    ni = len(alon)

    slat = 0.5 * (np.roll(alat, 1) + alat)
    slat[0] = -90.0
    slat = np.append(slat, 90.0)

    rad = math.pi / 180.0

    area = np.zeros((nj, ni), dtype=float)

    for j in range(nj):
        area[j, :] = math.sin(slat[j + 1] * rad) - math.sin(slat[j] * rad)

    area = area * 5.1e14 / np.sum(area)

    return xr.DataArray(
        area,
        coords={"lat": alat, "lon": alon},
        dims=("lat", "lon"),
        name="area",
    )


def read_monthly_global_tas(
    fn: Path,
    area: xr.DataArray,
    tas_mode: str,
    model_pi_trefht: float,
    obs_bias_correction: float,
) -> float:
    """
    Read monthly global mean TAS from CAM TREFHT.

    tas_mode:
      absolute    -> global mean TREFHT [K]
      old_anomaly -> global mean TREFHT - model_pi_trefht + obs_bias_correction
    """

    print(f"Getting TAS from {fn}")

    with xr.open_dataset(fn) as ds:
        if "TREFHT" not in ds:
            raise ValueError(f"TREFHT not found in {fn}")

        da = ds["TREFHT"]

        if "time" in da.dims:
            da = da.mean("time")

        da = da.squeeze(drop=True)

        if not {"lat", "lon"}.issubset(set(da.dims)):
            raise ValueError(f"TREFHT dims are not lat/lon-compatible in {fn}: {da.dims}")

        if tas_mode == "absolute":
            tas_for_aera = da
        elif tas_mode == "old_anomaly":
            tas_for_aera = da - model_pi_trefht + obs_bias_correction
        else:
            raise ValueError(f"Unknown tas_mode: {tas_mode}")

        gm = (tas_for_aera * area).sum(dim=("lat", "lon")) / area.sum(dim=("lat", "lon"))

        return float(gm.values)


def calculate_model_annual_tas(
    args: argparse.Namespace,
    ystart: int,
    yend: int,
) -> dict[int, float]:
    """
    Calculate annual global mean TREFHT for the just-finished 5-year segment.
    """

    sample = find_cam_file(
        rundir=args.rundir,
        archive_root=args.archive_root,
        case=args.case,
        archive_case=args.archive_case,
        year=ystart,
        month=1,
        cam_hist_dir=args.cam_hist_dir,
    )

    area = make_area_from_cam_file(sample)

    out: dict[int, float] = {}

    for yy in range(ystart, yend + 1):
        vals = []

        for mm in range(1, 13):
            fn = find_cam_file(
                rundir=args.rundir,
                archive_root=args.archive_root,
                case=args.case,
                archive_case=args.archive_case,
                year=yy,
                month=mm,
                cam_hist_dir=args.cam_hist_dir,
            )

            vals.append(
                read_monthly_global_tas(
                    fn=fn,
                    area=area,
                    tas_mode=args.tas_mode,
                    model_pi_trefht=args.model_pi_trefht,
                    obs_bias_correction=args.obs_bias_correction,
                )
            )

        out[yy] = float(np.mean(vals))
        print(f"Annual global TAS {yy}: {out[yy]:.8f}")

    return out


# ============================================================
# Emission netCDF writer
# ============================================================

def area_from_latlon(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    """
    Return approximate grid-cell area [m2] for regular lat-lon grid.
    This assumes global regular lat-lon grid.
    """

    lat = np.asarray(lat)
    lon = np.asarray(lon)

    slat = 0.5 * (np.roll(lat, 1) + lat)
    slat[0] = -90.0
    slat = np.append(slat, 90.0)

    rad = np.pi / 180.0
    lat_weight = np.sin(slat[1:] * rad) - np.sin(slat[:-1] * rad)

    area = np.repeat(lat_weight[:, None], len(lon), axis=1)
    area = area * 5.1e14 / area.sum()

    return area


def get_year_month_from_date(date_values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    date is expected as YYYYMMDD integer.
    Return arrays of year and month.
    """

    date_values = np.asarray(date_values).astype(int)
    years = date_values // 10000
    months = (date_values // 100) % 100
    return years, months


def get_seconds_per_time(ds: Dataset, months: np.ndarray) -> np.ndarray:
    """
    Return seconds for each monthly timestep.
    Prefer time_bnds if available.
    """

    if "time_bnds" in ds.variables:
        time_bnds = ds.variables["time_bnds"][:]
        sec_per_time = (time_bnds[:, 1] - time_bnds[:, 0]) * 24.0 * 3600.0
    else:
        month_days = np.array(
            [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        )
        sec_per_time = np.array(
            [month_days[m - 1] * 24.0 * 3600.0 for m in months]
        )

    return np.asarray(sec_per_time)


def annual_emission_pgc(
    co2_flux,
    area: np.ndarray,
    sec_per_time: np.ndarray,
    idxs: np.ndarray,
) -> float:
    """
    Integrate monthly CO2_flux over area and time.

    Assumes CO2_flux unit is kg CO2 m-2 s-1.
    Output unit: Pg C yr-1.
    """

    kgco2_to_pgc = 12.0 / 44.0 * 1e-12

    total = 0.0

    for idx in idxs:
        total += (
            np.sum(co2_flux[idx, :, :] * area)
            * sec_per_time[idx]
            * kgco2_to_pgc
        )

    return float(total)


def write_scaled_emission_netcdf(
    template_file: Path,
    out_dir: Path,
    aera_id: str,
    stocktake_yr: int,
    nyr: int,
    s_emission: pd.Series,
    base_year: int = 2014,
) -> Path:
    """
    Copy template emission netCDF and replace CO2_flux for the next nyr years.

    The monthly/spatial pattern is taken from base_year in the template.
    Each target year is scaled so that annual integral equals AERA ff_emission.
    """

    target_start = stocktake_yr + 1
    target_end = stocktake_yr + nyr
    target_years = np.arange(target_start, target_end + 1)

    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / (
        f"emissions-cmip6_CO2_anthro_surface_{aera_id}_"
        f"{target_start:04d}-{target_end:04d}_"
        f"201401-210112_fv_1.9x2.5.nc"
    )

    print("============================================================")
    print("Write scaled emission netCDF")
    print("============================================================")
    print(f"Template : {template_file}")
    print(f"Output   : {out_file}")
    print(f"Years    : {target_start}-{target_end}")
    print(f"Base year: {base_year}")

    if not template_file.exists():
        raise FileNotFoundError(template_file)

    missing_years = [yy for yy in target_years if yy not in s_emission.index]

    if len(missing_years) > 0:
        raise ValueError(
            f"AERA emission series does not contain target years: {missing_years}"
        )

    if s_emission.loc[target_years].isna().any():
        bad = s_emission.loc[target_years].index[
            s_emission.loc[target_years].isna()
        ].tolist()
        raise ValueError(f"AERA ff_emission has NaN for years: {bad}")

    print("\nAERA target emissions:")
    print(s_emission.loc[target_years])

    shutil.copy2(template_file, out_file)

    with Dataset(out_file, "r+") as ds:
        if "CO2_flux" not in ds.variables:
            raise KeyError("Cannot find variable 'CO2_flux' in template netCDF.")

        if "lat" not in ds.variables or "lon" not in ds.variables:
            raise KeyError("Cannot find lat/lon in template netCDF.")

        if "date" not in ds.variables:
            raise KeyError("Cannot find date variable in template netCDF.")

        co2 = ds.variables["CO2_flux"]
        lat = ds.variables["lat"][:]
        lon = ds.variables["lon"][:]
        date = ds.variables["date"][:]

        years, months = get_year_month_from_date(date)

        template_years = sorted(set(years.tolist()))
        first_year = min(template_years)
        last_year = max(template_years)

        print(f"\nTemplate years: {first_year}-{last_year}")

        years_to_replace = [yy for yy in target_years if yy in template_years]

        if len(years_to_replace) == 0:
            raise ValueError(
                "No overlapping years to replace. "
                f"target_years={list(target_years)}, "
                f"template range={first_year}-{last_year}"
            )

        if len(years_to_replace) != len(target_years):
            missing_template_years = sorted(set(target_years) - set(years_to_replace))
            raise ValueError(
                f"Template does not contain some target years: {missing_template_years}"
            )

        print("Years to replace:", years_to_replace)

        area = area_from_latlon(lat, lon)
        sec_per_time = get_seconds_per_time(ds, months)

        # Base pattern from base_year
        base_indices_by_month = {}

        base_idxs_all = np.where(years == base_year)[0]

        if len(base_idxs_all) == 0:
            raise ValueError(f"Template does not contain base year {base_year}")

        for mm in range(1, 13):
            idxs = np.where((years == base_year) & (months == mm))[0]

            if len(idxs) == 0:
                raise ValueError(f"Template does not contain {base_year}-{mm:02d}")

            base_indices_by_month[mm] = int(idxs[0])

        emis_base = annual_emission_pgc(co2, area, sec_per_time, base_idxs_all)

        print(f"\nTemplate {base_year} annual emission = {emis_base:.8f} Pg C yr-1")

        if np.isclose(emis_base, 0.0):
            raise ValueError(f"Base-year emission is zero: {emis_base}")

        # Replace selected years/months
        print("\nScaling and replacing CO2_flux:")

        for yy in years_to_replace:
            target_ems = float(s_emission.loc[yy])
            scale = target_ems / emis_base

            print(f"{yy}: target={target_ems:.8f} PgC yr-1, scale={scale:.10f}")

            for mm in range(1, 13):
                target_idxs = np.where((years == yy) & (months == mm))[0]

                if len(target_idxs) == 0:
                    raise ValueError(f"Template missing target month {yy}-{mm:02d}")

                target_idx = int(target_idxs[0])
                base_idx = base_indices_by_month[mm]

                co2[target_idx, :, :] = co2[base_idx, :, :] * scale

        # Verification
        print("\nVerification after replacement:")

        for yy in years_to_replace:
            idxs = np.where(years == yy)[0]
            out_ems = annual_emission_pgc(co2, area, sec_per_time, idxs)
            target_ems = float(s_emission.loc[yy])
            diff = out_ems - target_ems

            print(
                f"{yy}: output={out_ems:.8f}, "
                f"target={target_ems:.8f}, "
                f"diff={diff:.6e} PgC yr-1"
            )

            if abs(diff) > 1e-5:
                raise ValueError(
                    f"Emission verification failed for {yy}: "
                    f"output={out_ems}, target={target_ems}, diff={diff}"
                )

        # Add global attributes
        ds.setncattr("AERAid", aera_id)
        ds.setncattr("stocktake_year", int(stocktake_yr))
        ds.setncattr("target_year_start", int(target_start))
        ds.setncattr("target_year_end", int(target_end))
        ds.setncattr("template_file", str(template_file))
        ds.setncattr("base_pattern_year", int(base_year))
        ds.setncattr(
            "AERA_note",
            (
                "Template copied exactly; only CO2_flux values for "
                f"{target_start}-{target_end} were replaced using AERA "
                "annual fossil-fuel CO2 emissions. Monthly spatial patterns "
                f"are taken from template year {base_year} and scaled annually."
            ),
        )

    print("\nEmission netCDF done.")
    print(out_file)

    return out_file


# ============================================================
# Output helpers
# ============================================================

def write_state_csv(
    path: Path,
    y0: int,
    y_last: int,
    ann_tas: np.ndarray,
    ann_co2: np.ndarray,
    ann_ems: np.ndarray,
) -> None:
    """
    Write human-readable AERA state:
      year,temp,co2,ff_emission

    It includes years up to y2 + nyr, so future ff_emission is stored.
    Future temp values are normally NaN until the model segment has run.
    """

    years = np.arange(y0, y_last + 1)

    df_state = pd.DataFrame(
        {
            "year": years,
            "temp": ann_tas[years - y0],
            "co2": ann_co2[years - y0],
            "ff_emission": ann_ems[years - y0],
        }
    )

    df_state.to_csv(path, index=False)
    print(f"Wrote state CSV: {path}")


def validate_required_inputs(
    y0: int,
    year_x: int,
    ann_tas: np.ndarray,
    ann_ems: np.ndarray,
) -> None:
    required_years = np.arange(y0, year_x + 1)

    bad_temp = required_years[np.isnan(ann_tas[required_years - y0])]
    bad_ems = required_years[np.isnan(ann_ems[required_years - y0])]

    if len(bad_temp) > 0:
        raise ValueError(
            "TAS is missing for required years. "
            f"Examples: {bad_temp[:20].tolist()}"
        )

    if len(bad_ems) > 0:
        raise ValueError(
            "ff_emission is missing for required years. "
            f"Examples: {bad_ems[:20].tolist()}"
        )


# ============================================================
# Main
# ============================================================

def main() -> None:
    args = parse_args()

    y0 = args.y0
    y2 = args.year_x
    nyr = args.nyr

    # The just-finished model segment.
    # YEAR_X=2019 -> read TAS from 2015-2019
    # YEAR_X=2024 -> read TAS from 2020-2024
    y1 = y2 - nyr + 1

    output_dir = args.future_output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    fout = output_dir / f"{args.aera_id}_ann_tas_co2_ems_{y2}.dat"
    aera_outfile = output_dir / f"{args.aera_id}_{y2}.csv"
    next5_outfile = output_dir / f"{args.aera_id}_{y2}_next5.csv"
    state_csv = output_dir / f"{args.aera_id}_state_{y2}.csv"
    model_tas_csv = output_dir / f"{args.aera_id}_model_tas_{y1}-{y2}.csv"
    emission_nc_path_txt = output_dir / f"{args.aera_id}_{y2}_emission_nc_path.txt"

    # Use a persistent metadata file so the next stocktake can use previous slope.
    meta_file = output_dir / f"{args.aera_id}_meta.nc"

    prev_y2 = y2 - nyr

    if prev_y2 == 2014:
        prev_pickle = args.initial_state_dat
    else:
        prev_pickle = output_dir / f"{args.aera_id}_ann_tas_co2_ems_{prev_y2}.dat"

    n_total = y2 - y0 + nyr + 1
    y_last = y2 + nyr

    year = np.arange(y0, y_last + 1, dtype=float)

    print("============================================================")
    print("AERA stocktake settings")
    print("============================================================")
    print(f"AERA ID             : {args.aera_id}")
    print(f"CASE                : {args.case}")
    print(f"RUNDIR              : {args.rundir}")
    print(f"ARCHIVE_ROOT        : {args.archive_root}")
    print(f"ARCHIVE_CASE        : {args.archive_case}")
    print(f"YEAR_X              : {y2}")
    print(f"Current segment     : {y1}-{y2}")
    print(f"TAS mode            : {args.tas_mode}")
    print(f"Initial state dat   : {args.initial_state_dat}")
    print(f"Previous pickle     : {prev_pickle}")
    print(f"CAM hist dir        : {args.cam_hist_dir}")
    print(f"Future output dir   : {output_dir}")
    print(f"Emission template nc: {args.emission_template_nc}")
    print(f"Emission output dir : {args.emission_output_dir}")
    print(f"Base pattern year   : {args.base_pattern_year}")
    print("============================================================")

    if not prev_pickle.exists():
        raise FileNotFoundError(
            f"Previous AERA state pickle not found: {prev_pickle}\n"
            "For YEAR_X=2019 this should be the NIRD-generated 2014 .dat file. "
            "For later stocktakes this should be the previous future-output .dat file."
        )

    ann_tas, ann_co2, ann_ems = load_previous_pickle(
        path=prev_pickle,
        y0=y0,
        y_last=y_last,
        n_total=n_total,
    )

    # Fill historical gaps from CSV if needed.
    fill_from_hist_csv(
        hist_csv=args.hist_input_csv,
        y0=y0,
        year_x=y2,
        ann_tas=ann_tas,
        ann_ems=ann_ems,
    )

    # Calculate TAS for the current 5-year segment from NorESM output.
    print("============================================================")
    print(f"Calculate model TAS for {y1}-{y2}")
    print("============================================================")

    model_tas = calculate_model_annual_tas(args, y1, y2)

    for yy, val in model_tas.items():
        ann_tas[yy - y0] = val

    pd.DataFrame(
        {
            "year": list(model_tas.keys()),
            "temp": list(model_tas.values()),
        }
    ).to_csv(model_tas_csv, index=False)

    print(f"Wrote model TAS CSV: {model_tas_csv}")

    # Validate before AERA.
    validate_required_inputs(
        y0=y0,
        year_x=y2,
        ann_tas=ann_tas,
        ann_ems=ann_ems,
    )

    print("============================================================")
    print("Populate AERA dataframe")
    print("============================================================")

    df = aera.get_base_df()

    required_years = np.arange(y0, y2 + 1)

    df.loc[required_years, "temp"] = ann_tas[required_years - y0]
    df.loc[required_years, "ff_emission"] = ann_ems[required_years - y0]

    print("============================================================")
    print("Run AERA")
    print("============================================================")

    s_emission = aera.get_adaptive_emissions(
        temp_target_rel=args.temp_target_rel,
        temp_target_type=args.temp_target_type,
        year_x=y2,
        model_start_year=args.model_start_year,
        df=df,
        meta_file=meta_file,
    )

    print("============================================================")
    print("Save AERA outputs")
    print("============================================================")

    future_years = np.arange(y2 + 1, y2 + nyr + 1)

    for yy in future_years:
        ann_ems[yy - y0] = float(s_emission.loc[yy])

    with open(fout, "wb") as f:
        pickle.dump(
            [
                year,
                np.squeeze(ann_tas),
                np.squeeze(ann_co2),
                np.squeeze(ann_ems),
            ],
            f,
        )

    s_emission.to_csv(aera_outfile)
    s_emission.loc[future_years].to_csv(next5_outfile, header=["ff_emission"])

    write_state_csv(
        path=state_csv,
        y0=y0,
        y_last=y_last,
        ann_tas=ann_tas,
        ann_co2=ann_co2,
        ann_ems=ann_ems,
    )

    print(f"Wrote pickle       : {fout}")
    print(f"Wrote AERA CSV     : {aera_outfile}")
    print(f"Wrote next-5 CSV   : {next5_outfile}")
    print(f"Wrote state CSV    : {state_csv}")
    print(f"Wrote metadata     : {meta_file}")

    print("\nNext 5-year emissions:")
    print(s_emission.loc[future_years])

    # Write emission netCDF.
    if args.skip_emission_nc:
        print("skip-emission-nc is set. Emission netCDF creation skipped.")
    else:
        emission_nc = write_scaled_emission_netcdf(
            template_file=args.emission_template_nc,
            out_dir=args.emission_output_dir,
            aera_id=args.aera_id,
            stocktake_yr=y2,
            nyr=nyr,
            s_emission=s_emission,
            base_year=args.base_pattern_year,
        )

        with open(emission_nc_path_txt, "w") as f:
            f.write(str(emission_nc) + "\n")

        print(f"Wrote emission netCDF path file: {emission_nc_path_txt}")

    print("============================================================")
    print("AERA stocktake completed successfully")
    print("============================================================")


if __name__ == "__main__":
    main()
