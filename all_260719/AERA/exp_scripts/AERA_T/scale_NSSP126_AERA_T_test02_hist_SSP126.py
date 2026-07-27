#!/usr/bin/env python3

from pathlib import Path
import shutil
import numpy as np
import pandas as pd
from netCDF4 import Dataset


# ============================================================
# Settings
# ============================================================
AERAid = "AERA_T_hist_nird"

# AERA run used y2 = 2014, so emissions are calculated for 2015-2019
stocktake_yr = 2014
nyr = 5

AERA_DIR = Path(
    "/nird/datalake/NS2980K/users/yongyub/O2_linearlity/"
    "TipESM/cmor/esm-up2p0/v20251010/AERA"
)

# Reference emission netCDF used as spatial/monthly template
TEMPLATE_FILE = (
    AERA_DIR / "emissions" / "references" /
    "emissions-cmip6_CO2_anthro_surface_ScenarioMIP_IAMC-IMAGE-ssp126_"
    "201401-210112_fv_1.9x2.5_c20221115.nc"
)

# AERA annual fossil-fuel emission output
AERA_CSV = (
    AERA_DIR / "output" / AERAid /
    f"{AERAid}_{stocktake_yr}.csv"
)

OUT_DIR = AERA_DIR / "emissions" / AERAid
OUT_DIR.mkdir(parents=True, exist_ok=True)

target_start = stocktake_yr + 1
target_end = stocktake_yr + nyr

OUT_FILE = OUT_DIR / (
    f"emissions-cmip6_CO2_anthro_surface_{AERAid}_"
    f"{target_start:04d}-{target_end:04d}_"
    f"201401-210112_fv_1.9x2.5.nc"
)

print("Template :", TEMPLATE_FILE)
print("AERA CSV :", AERA_CSV)
print("Output   :", OUT_FILE)

if not TEMPLATE_FILE.exists():
    raise FileNotFoundError(TEMPLATE_FILE)

if not AERA_CSV.exists():
    raise FileNotFoundError(AERA_CSV)


# ============================================================
# Copy template exactly first
# ============================================================
shutil.copy2(TEMPLATE_FILE, OUT_FILE)


# ============================================================
# Read AERA emissions
# ============================================================
s_emission = pd.read_csv(AERA_CSV)

# AERA CSV may have year as unnamed first column
if "year" not in s_emission.columns:
    s_emission = s_emission.rename(columns={s_emission.columns[0]: "year"})

if "ff_emission" not in s_emission.columns:
    raise ValueError(
        f"Cannot find 'ff_emission' column in AERA CSV. "
        f"Available columns: {list(s_emission.columns)}"
    )

s_emission["year"] = s_emission["year"].astype(int)
s_emission["ff_emission"] = pd.to_numeric(
    s_emission["ff_emission"],
    errors="coerce",
)

target_years = np.arange(target_start, target_end + 1)

missing_aera_years = [
    yy for yy in target_years
    if yy not in s_emission["year"].values
]

if len(missing_aera_years) > 0:
    raise ValueError(
        f"AERA CSV does not contain required target years: {missing_aera_years}"
    )

if s_emission.loc[s_emission["year"].isin(target_years), "ff_emission"].isna().any():
    bad = s_emission.loc[
        s_emission["year"].isin(target_years)
        & s_emission["ff_emission"].isna(),
        "year",
    ].tolist()
    raise ValueError(f"AERA ff_emission has NaN for years: {bad}")

print("\nAERA target emissions:")
print(s_emission.loc[s_emission["year"].isin(target_years), ["year", "ff_emission"]])


# ============================================================
# Helper functions
# ============================================================
def area_from_latlon(lat, lon):
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


def get_year_month_from_date(date_values):
    """
    date is expected as YYYYMMDD integer.
    Return arrays of year and month.
    """
    date_values = np.asarray(date_values).astype(int)
    years = date_values // 10000
    months = (date_values // 100) % 100
    return years, months


def get_seconds_per_time(ds, months):
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


def annual_emission_pgc(co2_flux, area, sec_per_time, idxs):
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


# ============================================================
# Modify CO2_flux only
# ============================================================
with Dataset(OUT_FILE, "r+") as ds:

    if "CO2_flux" not in ds.variables:
        raise KeyError("Cannot find variable 'CO2_flux' in template netCDF.")

    co2 = ds.variables["CO2_flux"]
    lat = ds.variables["lat"][:]
    lon = ds.variables["lon"][:]
    date = ds.variables["date"][:]

    years, months = get_year_month_from_date(date)

    template_years = sorted(set(years.tolist()))
    first_year = min(template_years)
    last_year = max(template_years)

    print(f"\nTemplate years: {first_year}-{last_year}")

    years_to_replace = [
        yy for yy in target_years
        if yy in template_years
    ]

    if len(years_to_replace) == 0:
        raise ValueError(
            "No overlapping years to replace. "
            f"target_years={list(target_years)}, "
            f"template range={first_year}-{last_year}"
        )

    print("Years to replace:", years_to_replace)

    area = area_from_latlon(lat, lon)
    sec_per_time = get_seconds_per_time(ds, months)

    # ========================================================
    # Calculate 2014 annual emission from template
    # This is used as the base spatial/monthly pattern.
    # ========================================================
    base_year = 2014
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

    print(f"\nTemplate {base_year} annual emission = {emis_base:.6f} Pg C yr-1")

    if np.isclose(emis_base, 0.0):
        raise ValueError(f"Base-year emission is zero: {emis_base}")

    # ========================================================
    # Replace selected years/months
    # ========================================================
    print("\nScaling and replacing CO2_flux:")

    for yy in years_to_replace:
        target_ems = float(
            s_emission.loc[s_emission["year"] == yy, "ff_emission"].iloc[0]
        )

        scale = target_ems / emis_base

        print(f"{yy}: target={target_ems:.6f} PgC yr-1, scale={scale:.8f}")

        for mm in range(1, 13):
            target_idxs = np.where((years == yy) & (months == mm))[0]

            if len(target_idxs) == 0:
                print(f"  WARNING: template missing {yy}-{mm:02d}; skipping")
                continue

            target_idx = int(target_idxs[0])
            base_idx = base_indices_by_month[mm]

            co2[target_idx, :, :] = co2[base_idx, :, :] * scale

    # ========================================================
    # Verification after replacement
    # ========================================================
    print("\nVerification after replacement:")

    for yy in years_to_replace:
        idxs = np.where(years == yy)[0]
        out_ems = annual_emission_pgc(co2, area, sec_per_time, idxs)

        target_ems = float(
            s_emission.loc[s_emission["year"] == yy, "ff_emission"].iloc[0]
        )

        diff = out_ems - target_ems

        print(
            f"{yy}: output={out_ems:.6f}, "
            f"target={target_ems:.6f}, "
            f"diff={diff:.6e} PgC yr-1"
        )

    # ========================================================
    # Add global attributes
    # ========================================================
    ds.setncattr("AERAid", AERAid)
    ds.setncattr("stocktake_year", stocktake_yr)
    ds.setncattr("target_year_start", int(target_start))
    ds.setncattr("target_year_end", int(target_end))
    ds.setncattr("AERA_csv", str(AERA_CSV))
    ds.setncattr("template_file", str(TEMPLATE_FILE))
    ds.setncattr(
        "AERA_note",
        (
            "Template copied exactly; only CO2_flux values for "
            f"{target_start}-{target_end} were replaced using AERA "
            "annual fossil-fuel CO2 emissions. Monthly spatial patterns "
            f"are taken from template year {base_year} and scaled annually."
        ),
    )

print("\nDone.")
print(OUT_FILE)