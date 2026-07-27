import numpy as np
import pandas as pd
import pickle
from pathlib import Path

# Use your installed package
# If you installed AERA 2.1 as aera-oxygen:
import aera as aera
# If you still want to use the old package name, use:
# import aera


# ============================================================
# Run settings
# ============================================================

AERAid = "AERA_T_hist_nird"

# Input CSV prepared from historical global annual timeseries
FHIST_AERA_INPUT = Path(
    "/nird/datalake/NS2980K/users/yongyub/O2_linearlity/"
    "TipESM/cmor/esm-up2p0/v20251010/AERA/input/references/"
    "NorESM2-LM_esm-hist_r1i1p1f1_1850-2014_AERA_full_input.csv"
)

# Periods
y0 = 1850

# IMPORTANT:
# If the input CSV only contains 1850-2014, set y2 = 2014.
# Then AERA will calculate emissions for 2015-2019.
# If your CSV already contains 2015-2019, set y2 = 2019.
y2 = 2014

nyr = 5

# AERA target
TEMP_TARGET_REL = 2.0
TEMP_TARGET_TYPE = 2      # 2 = relative to model 1850-1900 mean
YEAR_X = y2
MODEL_START_YEAR = 1850


# ============================================================
# Output paths
# ============================================================

AERA_DIR = Path(
    "/nird/datalake/NS2980K/users/yongyub/O2_linearlity/TipESM/cmor/esm-up2p0/v20251010/AERA"
)

OUTPUT_DIR = AERA_DIR / "output" / AERAid
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FOUT = OUTPUT_DIR / f"{AERAid}_ann_tas_co2_ems_{y2}.dat"
meta_file = OUTPUT_DIR / f"{AERAid}_meta_{y2}.nc"
AERA_outfile = OUTPUT_DIR / f"{AERAid}_{y2}.csv"


# ============================================================
# Allocate arrays for legacy pickle output
# ============================================================

n_total = y2 - y0 + nyr + 1

year = np.full(n_total, np.nan)
ann_tas = np.full(n_total, np.nan)
ann_co2 = np.full(n_total, np.nan)
ann_ems = np.full(n_total, np.nan)


# ============================================================
# Read AERA input CSV
# ============================================================

print("Read AERA input CSV")

df_input = pd.read_csv(FHIST_AERA_INPUT, index_col="year")
df_input.index = df_input.index.astype(int)
df_input = df_input.sort_index()

required_cols = ["temp", "ff_emission"]
missing_cols = [c for c in required_cols if c not in df_input.columns]

if missing_cols:
    raise ValueError(
        f"Input CSV is missing required columns: {missing_cols}. "
        "Required columns are 'temp' and 'ff_emission'."
    )

target_years = np.arange(y0, y2 + 1)

missing_years = target_years[~np.isin(target_years, df_input.index.values)]
if len(missing_years) > 0:
    raise ValueError(
        f"Input CSV does not contain all required years {y0}-{y2}. "
        f"Missing years include: {missing_years[:10]}"
    )

if df_input.loc[target_years, "temp"].isna().any():
    bad = df_input.loc[target_years].index[df_input.loc[target_years, "temp"].isna()]
    raise ValueError(f"'temp' has NaN values for years: {bad[:10].tolist()}")

if df_input.loc[target_years, "ff_emission"].isna().any():
    bad = df_input.loc[target_years].index[df_input.loc[target_years, "ff_emission"].isna()]
    raise ValueError(f"'ff_emission' has NaN values for years: {bad[:10].tolist()}")


# ============================================================
# Populate legacy arrays
# ============================================================

ix = target_years - y0

year[ix] = target_years
ann_tas[ix] = df_input.loc[target_years, "temp"].values
ann_ems[ix] = df_input.loc[target_years, "ff_emission"].values


# ============================================================
# Prepare AERA dataframe
# ============================================================

print("Populate data to AERA dataframe")

# Start from AERA default dataframe.
# This provides lu_emission and non_co2_emission through the future.
df = aera.get_base_df()

# Fill temperature and fossil-fuel CO2 emissions from the input CSV.
df.loc[target_years, "temp"] = df_input.loc[target_years, "temp"].values
df.loc[target_years, "ff_emission"] = df_input.loc[target_years, "ff_emission"].values

# If the input CSV also contains lu_emission / non_co2_emission, use them.
# Otherwise, the default AERA values are kept.
for col in ["lu_emission", "non_co2_emission"]:
    if col in df_input.columns:
        valid_years = df_input.index[df_input[col].notna()]
        valid_years = valid_years[(valid_years >= y0) & (valid_years <= df.index.max())]
        df.loc[valid_years, col] = df_input.loc[valid_years, col].values


# ============================================================
# Run AERA
# ============================================================

print("Calculate adaptive emissions")

s_emission = aera.get_adaptive_emissions(
    temp_target_rel=TEMP_TARGET_REL,
    temp_target_type=TEMP_TARGET_TYPE,
    year_x=YEAR_X,
    model_start_year=MODEL_START_YEAR,
    df=df,
    meta_file=meta_file,
)


# ============================================================
# Save next 5 years emissions
# ============================================================

print("Save outputs")

future_years = np.arange(y2 + 1, y2 + nyr + 1)

for yr in future_years:
    year[yr - y0] = yr
    ann_ems[yr - y0] = s_emission.loc[yr]

pickle.dump(
    [year, np.squeeze(ann_tas), np.squeeze(ann_co2), np.squeeze(ann_ems)],
    open(FOUT, "wb"),
)

s_emission.to_csv(AERA_outfile)

print(f"Wrote pickle: {FOUT}")
print(f"Wrote AERA emission CSV: {AERA_outfile}")
print(f"Wrote metadata: {meta_file}")

print("\nNext 5-year emissions:")
print(s_emission.loc[future_years])