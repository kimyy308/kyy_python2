#!/usr/bin/env python3

from pathlib import Path
import shutil
import numpy as np
import pandas as pd
from netCDF4 import Dataset

# ============================================================
# Settings
# ============================================================
AERAid = "AERA_T_20"
stocktake_yr = 2019

BASE_DIR = Path("/cluster/projects/nn2980k/yongyub/NORESM/NorESM2")

TEMPLATE_FILE = Path(
    "/cluster/shared/noresm/inputdata/atm/cam/ggas/"
    "emissions-cmip6_CO2_anthro_surface_AERA300adapt_abs_201401-220112_fv_1.9x2.5.nc"
)

AERA_CSV = (
    BASE_DIR / "archives" / "AERA" / "output" / AERAid /
    f"{AERAid}_{stocktake_yr}.csv"
)

OUT_DIR = BASE_DIR / "input" / "emissions" / "AERA" / AERAid
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / (
    f"emissions-cmip6_CO2_anthro_surface_{AERAid}_"
    f"{stocktake_yr+1:04d}-{stocktake_yr+5:04d}_"
    f"201401-220112_fv_1.9x2.5.nc"
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

if "year" not in s_emission.columns:
    s_emission = s_emission.rename(columns={s_emission.columns[0]: "year"})

if "ff_emission" not in s_emission.columns:
    raise ValueError("Cannot find 'ff_emission' column in AERA CSV.")

s_emission["year"] = s_emission["year"].astype(int)

# AERA years after stocktake
aera_years = sorted(
    int(y) for y in s_emission["year"].values
    if y >= stocktake_yr + 1
)

# ============================================================
# Helper functions
# ============================================================
def area_from_latlon(lat, lon):
    """
    Return approximate grid-cell area [m2] for regular lat-lon grid.
    """
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


# ============================================================
# Modify CO2_flux only
# ============================================================
with Dataset(OUT_FILE, "r+") as ds:

    co2 = ds.variables["CO2_flux"]
    lat = ds.variables["lat"][:]
    lon = ds.variables["lon"][:]
    date = ds.variables["date"][:]

    years, months = get_year_month_from_date(date)

    template_years = sorted(set(years.tolist()))
    first_year = min(template_years)
    last_year = max(template_years)

    print(f"Template years: {first_year}-{last_year}")
    print(f"AERA years available after stocktake: {aera_years[:5]} ... {aera_years[-5:] if len(aera_years) >= 5 else aera_years}")

    # Years to replace = intersection of:
    # 1) AERA CSV years after stocktake
    # 2) years present in template
    # 3) target 5-year segment
    target_years = range(stocktake_yr + 1, stocktake_yr + 6)

    years_to_replace = [
        yy for yy in target_years
        if (yy in aera_years) and (yy in template_years)
    ]

    if len(years_to_replace) == 0:
        raise ValueError(
            "No overlapping years to replace. "
            f"target_years={list(target_years)}, "
            f"AERA years={aera_years[:3]}...{aera_years[-3:]}, "
            f"template range={first_year}-{last_year}"
        )

    print("Years to replace:", years_to_replace)

    # ========================================================
    # Calculate 2014 annual emission from template
    # ========================================================
    area = area_from_latlon(lat, lon)
    kgco2_to_pgc = 12.0 / 44.0 * 1e-12

    # Use actual time bounds if available for seconds/month
    if "time_bnds" in ds.variables:
        time_bnds = ds.variables["time_bnds"][:]
        sec_per_time = (time_bnds[:, 1] - time_bnds[:, 0]) * 24.0 * 3600.0
    else:
        month_days = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
        sec_per_time = np.array([month_days[m - 1] * 24.0 * 3600.0 for m in months])

    emis_2014 = 0.0
    base_indices_by_month = {}

    for mm in range(1, 13):
        idxs = np.where((years == 2014) & (months == mm))[0]
        if len(idxs) == 0:
            raise ValueError(f"Template does not contain 2014 month {mm:02d}")
        idx = int(idxs[0])
        base_indices_by_month[mm] = idx

        emis_2014 += np.sum(co2[idx, :, :] * area) * sec_per_time[idx] * kgco2_to_pgc

    print(f"Template 2014 annual emission = {emis_2014:.6f} Pg C yr-1")

    # ========================================================
    # Replace selected years/months
    # ========================================================
    for yy in years_to_replace:
        target_ems = float(
            s_emission.loc[s_emission["year"] == yy, "ff_emission"].iloc[0]
        )
        scale = target_ems / emis_2014

        print(f"{yy}: target={target_ems:.6f} PgC yr-1, scale={scale:.6f}")

        for mm in range(1, 13):
            target_idxs = np.where((years == yy) & (months == mm))[0]
            if len(target_idxs) == 0:
                print(f"  WARNING: template missing {yy}-{mm:02d}; skipping")
                continue

            target_idx = int(target_idxs[0])
            base_idx = base_indices_by_month[mm]

            co2[target_idx, :, :] = co2[base_idx, :, :] * scale

    # Add minimal global attrs only; all original metadata otherwise preserved
    ds.setncattr("AERAid", AERAid)
    ds.setncattr("stocktake_year", stocktake_yr)
    ds.setncattr("AERA_csv", str(AERA_CSV))
    ds.setncattr("template_file", str(TEMPLATE_FILE))
    ds.setncattr(
        "AERA_note",
        "Template copied exactly; only CO2_flux values for overlapping AERA years were replaced."
    )

print("\nDone.")
print(OUT_FILE)
