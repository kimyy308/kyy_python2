import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path

# ============================================================
# Settings
# ============================================================
AERAid = "AERA_T_20"
stocktake_yr = 2019   # e.g. 2019, 2024, ..., 2195

BASE_DIR = Path("/cluster/projects/nn2980k/yongyub/NORESM/NorESM2")

TEMPLATE_FILE = Path(
    "/cluster/shared/noresm/inputdata/atm/cam/ggas/emissions-cmip6_CO2_anthro_surface_ScenarioMIP_IAMC-IMAGE-ssp126_201401-210112_fv_1.9x2.5_c20221115.nc"
)

AERA_CSV = (
    BASE_DIR / "archives" / "AERA" / "output" / AERAid / 
    f"{AERAid}_{stocktake_yr}.csv"
)

OUT_DIR = BASE_DIR / "input" / "emissions" / "AERA" / AERAid
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / (
    f"emissions-cmip6_CO2_anthro_surface_{AERAid}_"
    f"{stocktake_yr+1:04d}-{min(stocktake_yr+5, 2200):04d}_"
    f"201401-220112_fv_1.9x2.5.nc"
)

print("Template :", TEMPLATE_FILE)
print("AERA csv :", AERA_CSV)
print("Output   :", OUT_FILE)

# ============================================================
# Read template emission file
# ============================================================
ds = xr.open_dataset(TEMPLATE_FILE).load()

co2_flux = ds["CO2_flux"]  # kg CO2 m-2 s-1

lat = ds["lat"].values
lon = ds["lon"].values

# ============================================================
# Area calculation
# ============================================================
slat = 0.5 * (np.roll(lat, 1) + lat)
slat[0] = -90.0
slat = np.append(slat, 90.0)

rad = np.pi / 180.0
lat_weight = np.sin(slat[1:] * rad) - np.sin(slat[:-1] * rad)

area = np.repeat(lat_weight[:, None], len(lon), axis=1)
area = area * 5.1e14 / np.sum(area)  # m2

area_da = xr.DataArray(
    area,
    dims=("lat", "lon"),
    coords={"lat": ds["lat"], "lon": ds["lon"]},
)

# ============================================================
# Calculate annual 2014 fossil-fuel emission in template
# Unit:
# CO2_flux [kg CO2 m-2 s-1]
# area     [m2]
# seconds/month
# /44*12 converts kg CO2 to kg C
# 1e-12 converts kg C to Pg C
# ============================================================
time = ds["time"]
month_length = time.dt.days_in_month

sec_per_month = month_length * 24 * 3600
kgco2_to_pgc = 12.0 / 44.0 * 1e-12

emis_monthly = (co2_flux * area_da).sum(("lat", "lon")) * sec_per_month * kgco2_to_pgc

emis_2014 = float(emis_monthly.sel(time=slice("2014-01-01", "2014-12-31")).sum())

print(f"Template 2014 annual emission = {emis_2014:.6f} Pg C yr-1")

# ============================================================
# Read AERA emission
# ============================================================
s_emission = pd.read_csv(AERA_CSV)

print(s_emission.head())
print(s_emission.columns)

# Expected columns: year, ff_emission
if "year" not in s_emission.columns:
    s_emission = s_emission.rename(columns={s_emission.columns[0]: "year"})

if "ff_emission" not in s_emission.columns:
    raise ValueError("Cannot find column 'ff_emission' in AERA csv.")

s_emission["year"] = s_emission["year"].astype(int)

# ============================================================
# Replace future years after stocktake
# ============================================================
new_flux = co2_flux.copy()

years_to_replace = [
    yy for yy in s_emission["year"].values
    if (yy >= stocktake_yr + 1) and (yy <= 2200)
]

print("Years to replace:", years_to_replace)

for yr in years_to_replace:
    target_ems = float(s_emission.loc[s_emission["year"] == yr, "ff_emission"].iloc[0])
    scale = target_ems / emis_2014

    print(f"{yr}: target={target_ems:.6f} PgC yr-1, scale={scale:.6f}")

    for month in range(1, 13):
        t_sel = f"{yr:04d}-{month:02d}"

        # Use 2014 monthly spatial pattern as template
        base_pattern = co2_flux.sel(time=f"2014-{month:02d}")

        new_flux.loc[dict(time=t_sel)] = base_pattern.values * scale

# ============================================================
# Save new emission file, preserving template metadata
# ============================================================
ds_out = ds.copy(deep=True)
ds_out["CO2_flux"] = new_flux

# Preserve variable attributes from template
for v in ds.variables:
    if v in ds_out:
        ds_out[v].attrs = ds[v].attrs.copy()

# Do NOT keep units/calendar in time attrs
ds_out["time"].attrs.pop("units", None)
ds_out["time"].attrs.pop("calendar", None)

encoding = {}
for v in ds_out.variables:
    encoding[v] = ds[v].encoding.copy() if v in ds.variables else {}

# Time encoding
encoding["time"]["units"] = "days since 1750-01-01 00:00:00"
encoding["time"]["calendar"] = "365_day"
encoding["time"]["_FillValue"] = None

# Coordinate variables should not have _FillValue
for v in ["time_bnds", "lat", "lon", "date"]:
    if v in encoding:
        encoding[v]["_FillValue"] = None

# CO2 flux
encoding["CO2_flux"] = ds["CO2_flux"].encoding.copy()
encoding["CO2_flux"]["_FillValue"] = np.float32(1.0e20)

ds_out.to_netcdf(
    OUT_FILE,
    format="NETCDF4_CLASSIC",
    encoding=encoding,
)


print("\nDone.")
print(OUT_FILE)
