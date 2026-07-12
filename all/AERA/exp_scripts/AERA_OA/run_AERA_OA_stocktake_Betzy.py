#!/usr/bin/env python3
"""
Run one AERA-OA stocktake for NorESM2-AERA on Betzy and create scaled
NorESM CO2 emission netCDF.

This OA version assumes that the annual OmegaA metric for the just-finished
5-year segment has already been postprocessed into a CSV file with at least:
    year,<metric-column>
where <metric-column> defaults to OmegaA.

The aera_oa package expects df["OmegaA"] and ff_emission through YEAR_X.
"""

from __future__ import annotations

import argparse
import math
import pickle
import shutil
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import xarray as xr
from netCDF4 import Dataset

import aera_oa


# ============================================================
# Argument parser
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run one AERA-OA stocktake and write NorESM emission netCDF."
    )

    p.add_argument("--case", required=True, help="NorESM case name.")
    p.add_argument("--rundir", required=True, type=Path, help="NorESM run directory.")

    p.add_argument(
        "--archive-root",
        default="/cluster/work/users/yongyub/archive",
        type=Path,
        help="NorESM archive root.",
    )
    p.add_argument(
        "--archive-case",
        default=None,
        help="Case name under archive-root. Default: same as --case.",
    )
    p.add_argument(
        "--cam-hist-dir",
        default=None,
        type=Path,
        help="Archived CAM h0 monthly directory; kept for workflow consistency.",
    )

    p.add_argument(
        "--aera-id",
        default="AERA_OmegaA_Betzy",
        help="Prefix for newly written AERA-OA files.",
    )
    p.add_argument(
        "--initial-state-dat",
        required=True,
        type=Path,
        help="Initial OA AERA pickle state, e.g. *_ann_OmegaA_co2_ems_2014.dat.",
    )
    p.add_argument(
        "--future-output-dir",
        required=True,
        type=Path,
        help="Directory where new AERA-OA outputs are written and read by later stocktakes.",
    )
    p.add_argument(
        "--hist-input-csv",
        default=None,
        type=Path,
        help=(
            "Optional historical CSV with columns year,<OmegaA column>,ff_emission. "
            "Used to fill missing 1850-2014 values if needed."
        ),
    )
    p.add_argument(
        "--current-metric-csv",
        default=None,
        type=Path,
        help=(
            "Optional CSV containing annual OmegaA for the just-finished segment. "
            "Required columns: year and metric column. If omitted, the script relies on "
            "values already available from the previous pickle and/or --hist-input-csv."
        ),
    )
    p.add_argument(
        "--metric-column",
        default="OmegaA",
        help="Column name for current-segment OmegaA CSV. Default: OmegaA.",
    )
    p.add_argument(
        "--hist-metric-column",
        default=None,
        help="Column name for historical OmegaA CSV. Default: same as --metric-column, with aliases tried.",
    )

    p.add_argument("--year-x", required=True, type=int, help="Stocktake year.")
    p.add_argument("--y0", default=1850, type=int)
    p.add_argument("--nyr", default=5, type=int)
    p.add_argument("--model-start-year", default=1850, type=int)

    p.add_argument(
        "--omegaa-target-abs",
        required=True,
        type=float,
        help="Absolute OmegaA target passed to aera_oa.get_adaptive_emissions(arag_target_abs=...).",
    )

    # Emission netCDF settings
    p.add_argument(
        "--emission-template-nc",
        default=(
            "/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/emissions/"
            "AERA/AERA_OmegaA_Betzy/"
            "emissions-cmip6_CO2_anthro_surface_AERA_OmegaA_Betzy_2015-2019_"
            "201401-210112_fv_1.9x2.5.nc"
        ),
        type=Path,
        help="Reference emission netCDF used as spatial/monthly template.",
    )
    p.add_argument(
        "--emission-output-dir",
        default=(
            "/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/emissions/"
            "AERA/AERA_OmegaA_Betzy"
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
        help="Run AERA-OA only and skip emission netCDF creation.",
    )

    args = p.parse_args()

    if args.archive_case is None:
        args.archive_case = args.case

    return args


# ============================================================
# Previous AERA-OA state and CSV readers
# ============================================================

def load_previous_pickle(
    path: Path,
    y0: int,
    y_last: int,
    n_total: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Load legacy OA AERA pickle:
        [year, ann_OmegaA, ann_co2, ann_ems]

    Returns arrays aligned as index = year - y0.
    """

    print(f"Load previous AERA-OA state pickle: {path}")

    with open(path, "rb") as f:
        field = pickle.load(f)

    old_year = np.asarray(field[0], dtype=float)
    old_metric = np.asarray(field[1], dtype=float)
    old_co2 = np.asarray(field[2], dtype=float)
    old_ems = np.asarray(field[3], dtype=float)

    ann_metric = np.full(n_total, np.nan)
    ann_co2 = np.full(n_total, np.nan)
    ann_ems = np.full(n_total, np.nan)

    if np.isfinite(old_year).any() and np.nanmax(old_year) > 1000:
        n_old = min(len(old_year), len(old_metric), len(old_co2), len(old_ems))

        for ii in range(n_old):
            yy_float = old_year[ii]
            if not np.isfinite(yy_float):
                continue
            yy = int(round(yy_float))
            if yy < y0 or yy > y_last:
                continue
            jj = yy - y0
            ann_metric[jj] = old_metric[ii]
            ann_co2[jj] = old_co2[ii]
            ann_ems[jj] = old_ems[ii]
    else:
        print("WARNING: stored year array is not usable. Falling back to positional mapping.")
        n = min(n_total, len(old_metric), len(old_co2), len(old_ems))
        ann_metric[:n] = old_metric[:n]
        ann_co2[:n] = old_co2[:n]
        ann_ems[:n] = old_ems[:n]

    return ann_metric, ann_co2, ann_ems


def _read_year_indexed_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "year" in df.columns:
        df = df.set_index("year")
    df.index = df.index.astype(int)
    return df.sort_index()


def _choose_column(df: pd.DataFrame, requested: Optional[str], aliases: list[str]) -> str:
    candidates = []
    if requested:
        candidates.append(requested)
    candidates.extend([c for c in aliases if c not in candidates])

    for col in candidates:
        if col in df.columns:
            return col

    raise KeyError(
        "Could not find a usable OmegaA column. "
        f"Requested={requested}, aliases={aliases}, available={list(df.columns)}"
    )


def fill_from_hist_csv(
    hist_csv: Optional[Path],
    y0: int,
    year_x: int,
    ann_metric: np.ndarray,
    ann_ems: np.ndarray,
    metric_column: str,
    hist_metric_column: Optional[str],
) -> None:
    """
    Fill missing OmegaA / ff_emission from historical CSV.
    This does not overwrite existing non-NaN values from the previous .dat file.
    """

    if hist_csv is None:
        return

    if not hist_csv.exists():
        print(f"Historical CSV not found, skip: {hist_csv}")
        return

    print(f"Read historical AERA-OA input CSV: {hist_csv}")

    df_input = _read_year_indexed_csv(hist_csv)

    metric_col = _choose_column(
        df_input,
        hist_metric_column or metric_column,
        ["OmegaA", "omega_arag_surface_global_mean", "omega_arag_global_mean", "omegaA"],
    )

    years = np.arange(y0, year_x + 1)
    valid_years = years[np.isin(years, df_input.index.values)]

    for yy in valid_years:
        if np.isnan(ann_metric[yy - y0]) and pd.notna(df_input.loc[yy, metric_col]):
            ann_metric[yy - y0] = float(df_input.loc[yy, metric_col])

    if "ff_emission" in df_input.columns:
        for yy in valid_years:
            if np.isnan(ann_ems[yy - y0]) and pd.notna(df_input.loc[yy, "ff_emission"]):
                ann_ems[yy - y0] = float(df_input.loc[yy, "ff_emission"])


def apply_historical_optional_emission_overrides(
    hist_csv: Optional[Path],
    df: pd.DataFrame,
    y0: int,
) -> None:
    """
    If the historical input CSV contains lu_emission and/or non_co2_emission,
    copy them into the AERA-OA base dataframe. This follows the NIRD OA script:
    otherwise, the default AERA-OA values are kept.
    """

    if hist_csv is None or not hist_csv.exists():
        return

    df_input = _read_year_indexed_csv(hist_csv)

    for col in ["lu_emission", "non_co2_emission"]:
        if col not in df_input.columns:
            continue

        valid_years = df_input.index[df_input[col].notna()]
        valid_years = valid_years[
            (valid_years >= y0) & (valid_years <= df.index.max())
        ]

        if len(valid_years) == 0:
            continue

        print(f"Use historical CSV override for {col}: {valid_years.min()}-{valid_years.max()}")
        df.loc[valid_years, col] = df_input.loc[valid_years, col].values


def read_current_metric_csv(
    path: Path,
    metric_column: str,
    ystart: int,
    yend: int,
) -> dict[int, float]:
    """Read current 5-year annual OmegaA values from CSV."""

    print(f"Read current model OmegaA CSV: {path}")

    if not path.exists():
        raise FileNotFoundError(path)

    df = _read_year_indexed_csv(path)
    metric_col = _choose_column(
        df,
        metric_column,
        ["OmegaA", "omega_arag_surface_global_mean", "omega_arag_global_mean", "omegaA"],
    )

    out: dict[int, float] = {}
    missing = []

    for yy in range(ystart, yend + 1):
        if yy not in df.index or pd.isna(df.loc[yy, metric_col]):
            missing.append(yy)
        else:
            out[yy] = float(df.loc[yy, metric_col])

    if missing:
        raise ValueError(f"Current OmegaA CSV is missing values for years: {missing}")

    for yy, val in out.items():
        print(f"Annual OmegaA {yy}: {val:.8f}")

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
    ann_metric: np.ndarray,
    ann_co2: np.ndarray,
    ann_ems: np.ndarray,
) -> None:
    """
    Write human-readable AERA-OA state:
      year,OmegaA,co2,ff_emission
    """

    years = np.arange(y0, y_last + 1)

    df_state = pd.DataFrame(
        {
            "year": years,
            "OmegaA": ann_metric[years - y0],
            "co2": ann_co2[years - y0],
            "ff_emission": ann_ems[years - y0],
        }
    )

    df_state.to_csv(path, index=False)
    print(f"Wrote state CSV: {path}")


def validate_required_inputs(
    y0: int,
    year_x: int,
    ann_metric: np.ndarray,
    ann_ems: np.ndarray,
) -> None:
    required_years = np.arange(y0, year_x + 1)

    bad_metric = required_years[np.isnan(ann_metric[required_years - y0])]
    bad_ems = required_years[np.isnan(ann_ems[required_years - y0])]

    if len(bad_metric) > 0:
        raise ValueError(
            "OmegaA is missing for required years. "
            f"Examples: {bad_metric[:20].tolist()}"
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
    # YEAR_X=2019 -> read OmegaA from 2015-2019
    # YEAR_X=2024 -> read OmegaA from 2020-2024
    y1 = y2 - nyr + 1

    output_dir = args.future_output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    fout = output_dir / f"{args.aera_id}_ann_OmegaA_co2_ems_{y2}.dat"
    aera_outfile = output_dir / f"{args.aera_id}_{y2}.csv"
    next5_outfile = output_dir / f"{args.aera_id}_{y2}_next5.csv"
    state_csv = output_dir / f"{args.aera_id}_state_{y2}.csv"
    model_metric_csv = output_dir / f"{args.aera_id}_model_OmegaA_{y1}-{y2}.csv"
    emission_nc_path_txt = output_dir / f"{args.aera_id}_{y2}_emission_nc_path.txt"

    # Use a persistent metadata file so the next stocktake can use previous slope.
    meta_file = output_dir / f"{args.aera_id}_meta_{y2}.nc"

    prev_y2 = y2 - nyr

    if prev_y2 == 2014:
        prev_pickle = args.initial_state_dat
    else:
        prev_pickle = output_dir / f"{args.aera_id}_ann_OmegaA_co2_ems_{prev_y2}.dat"

    n_total = y2 - y0 + nyr + 1
    y_last = y2 + nyr

    year = np.arange(y0, y_last + 1, dtype=float)

    print("============================================================")
    print("AERA-OA stocktake settings")
    print("============================================================")
    print(f"AERA ID             : {args.aera_id}")
    print(f"CASE                : {args.case}")
    print(f"RUNDIR              : {args.rundir}")
    print(f"ARCHIVE_ROOT        : {args.archive_root}")
    print(f"ARCHIVE_CASE        : {args.archive_case}")
    print(f"YEAR_X              : {y2}")
    print(f"Current segment     : {y1}-{y2}")
    print(f"OmegaA target abs   : {args.omegaa_target_abs}")
    print(f"Metric column       : {args.metric_column}")
    print(f"Hist metric column  : {args.hist_metric_column}")
    print(f"Current metric CSV  : {args.current_metric_csv}")
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
            f"Previous AERA-OA state pickle not found: {prev_pickle}\n"
            "For YEAR_X=2019 this should be the unified 2014 .dat file under AERA_hist. "
            "For later stocktakes this should be the previous future-output .dat file."
        )

    ann_metric, ann_co2, ann_ems = load_previous_pickle(
        path=prev_pickle,
        y0=y0,
        y_last=y_last,
        n_total=n_total,
    )

    fill_from_hist_csv(
        hist_csv=args.hist_input_csv,
        y0=y0,
        year_x=y2,
        ann_metric=ann_metric,
        ann_ems=ann_ems,
        metric_column=args.metric_column,
        hist_metric_column=args.hist_metric_column,
    )

    print("============================================================")
    print(f"Read/update model OmegaA for {y1}-{y2}")
    print("============================================================")

    if args.current_metric_csv is not None:
        if not args.current_metric_csv.exists():
            raise FileNotFoundError(args.current_metric_csv)

        model_metric = read_current_metric_csv(
            path=args.current_metric_csv,
            metric_column=args.metric_column,
            ystart=y1,
            yend=y2,
        )

        for yy, val in model_metric.items():
            ann_metric[yy - y0] = val

        pd.DataFrame(
            {
                "year": list(model_metric.keys()),
                "OmegaA": list(model_metric.values()),
            }
        ).to_csv(model_metric_csv, index=False)

        print(f"Wrote model OmegaA CSV: {model_metric_csv}")
    else:
        print("No --current-metric-csv was provided.")
        print("Using OmegaA values already filled from previous pickle and/or --hist-input-csv.")
        seg_years = np.arange(y1, y2 + 1)
        seg_vals = ann_metric[seg_years - y0]
        if np.isfinite(seg_vals).all():
            pd.DataFrame({"year": seg_years, "OmegaA": seg_vals}).to_csv(model_metric_csv, index=False)
            print(f"Wrote model OmegaA CSV from existing state: {model_metric_csv}")

    validate_required_inputs(
        y0=y0,
        year_x=y2,
        ann_metric=ann_metric,
        ann_ems=ann_ems,
    )

    print("============================================================")
    print("Populate AERA-OA dataframe")
    print("============================================================")

    df = aera_oa.get_base_df()

    # If hist_input_csv contains lu_emission / non_co2_emission, use them.
    # Otherwise, keep AERA-OA package defaults.
    apply_historical_optional_emission_overrides(
        hist_csv=args.hist_input_csv,
        df=df,
        y0=y0,
    )

    required_years = np.arange(y0, y2 + 1)

    if "OmegaA" not in df.columns:
        df["OmegaA"] = np.nan

    df.loc[required_years, "OmegaA"] = ann_metric[required_years - y0]
    df.loc[required_years, "ff_emission"] = ann_ems[required_years - y0]

    print("============================================================")
    print("Run AERA-OA")
    print("============================================================")

    s_emission = aera_oa.get_adaptive_emissions(
        arag_target_abs=args.omegaa_target_abs,
        year_x=y2,
        model_start_year=args.model_start_year,
        df=df,
        meta_file=meta_file,
    )

    print("============================================================")
    print("Save AERA-OA outputs")
    print("============================================================")

    future_years = np.arange(y2 + 1, y2 + nyr + 1)

    for yy in future_years:
        ann_ems[yy - y0] = float(s_emission.loc[yy])

    with open(fout, "wb") as f:
        pickle.dump(
            [
                year,
                np.squeeze(ann_metric),
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
        ann_metric=ann_metric,
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
    print("AERA-OA stocktake completed successfully")
    print("============================================================")


if __name__ == "__main__":
    main()

