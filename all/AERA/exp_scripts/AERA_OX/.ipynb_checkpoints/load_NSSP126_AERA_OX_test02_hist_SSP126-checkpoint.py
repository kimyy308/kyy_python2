import numpy as np
import pandas as pd
import pickle
from pathlib import Path

# ============================================================
# Use Oxygen version of AERA
# ============================================================
import aera_oxygen as aera


# ============================================================
# Run settings
# ============================================================

# Target setting
# 1.5 means target = 1850-1899 mean * (1 - 1.5/100)
O2_TARGET_DECREASE_PERCENT = 1.5

target_tag = str(O2_TARGET_DECREASE_PERCENT).replace(".", "p")
AERAid = f"AERA_O2var_0_200m_minus{target_tag}pct_hist_nird"

# Input CSV prepared from historical global annual timeseries
# This file should contain at least:
# year, O2var, ff_emission
FHIST_AERA_INPUT = Path(
    "/nird/datalake/NS2980K/users/yongyub/O2_linearlity/"
    "TipESM/cmor/esm-up2p0/v20251010/AERA/input/references/"
    "NorESM2-LM_esm-hist_r1i1p1f1_1850-2014_AERA_O2var_0_200m_full_input.csv"
)

# Control variable column name expected by aera_oxygen
O2_COL = "O2var"

# Periods
y0 = 1850
y2 = 2014
nyr = 5

# Reference period for target calculation
ref_start = 1850
ref_end = 1899

YEAR_X = y2
MODEL_START_YEAR = 1850


# ============================================================
# Output paths
# ============================================================
AERA_DIR = Path(
    "/nird/datalake/NS2980K/users/yongyub/O2_linearlity/"
    "TipESM/cmor/esm-up2p0/v20251010/AERA"
)

OUTPUT_DIR = AERA_DIR / "output" / AERAid
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FOUT = OUTPUT_DIR / f"{AERAid}_ann_O2var_co2_ems_{y2}.dat"
meta_file = OUTPUT_DIR / f"{AERAid}_meta_{y2}.nc"
AERA_outfile = OUTPUT_DIR / f"{AERAid}_{y2}.csv"


# ============================================================
# Allocate arrays for legacy pickle output
# ============================================================
n_total = y2 - y0 + nyr + 1

year = np.full(n_total, np.nan)
ann_o2var = np.full(n_total, np.nan)
ann_co2 = np.full(n_total, np.nan)
ann_ems = np.full(n_total, np.nan)


# ============================================================
# Read AERA-Oxygen input CSV
# ============================================================
print("Read AERA-Oxygen input CSV")

df_input = pd.read_csv(FHIST_AERA_INPUT, index_col="year")
df_input.index = df_input.index.astype(int)
df_input = df_input.sort_index()

required_cols = [O2_COL, "ff_emission"]
missing_cols = [c for c in required_cols if c not in df_input.columns]

if missing_cols:
    raise ValueError(
        f"Input CSV is missing required columns: {missing_cols}. "
        f"Required columns are '{O2_COL}' and 'ff_emission'."
    )

target_years = np.arange(y0, y2 + 1)
ref_years = np.arange(ref_start, ref_end + 1)

missing_years = target_years[~np.isin(target_years, df_input.index.values)]
if len(missing_years) > 0:
    raise ValueError(
        f"Input CSV does not contain all required years {y0}-{y2}. "
        f"Missing years include: {missing_years[:10]}"
    )

missing_ref_years = ref_years[~np.isin(ref_years, df_input.index.values)]
if len(missing_ref_years) > 0:
    raise ValueError(
        f"Input CSV does not contain all reference years {ref_start}-{ref_end}. "
        f"Missing years include: {missing_ref_years[:10]}"
    )

if df_input.loc[target_years, O2_COL].isna().any():
    bad = df_input.loc[target_years].index[
        df_input.loc[target_years, O2_COL].isna()
    ]
    raise ValueError(f"'{O2_COL}' has NaN values for years: {bad[:10].tolist()}")

if df_input.loc[target_years, "ff_emission"].isna().any():
    bad = df_input.loc[target_years].index[
        df_input.loc[target_years, "ff_emission"].isna()
    ]
    raise ValueError(f"'ff_emission' has NaN values for years: {bad[:10].tolist()}")


# ============================================================
# Calculate absolute oxygen target from percentage decrease
# ============================================================
O2_REF_MEAN = df_input.loc[ref_years, O2_COL].astype(float).mean()
O2_TARGET_ABS = O2_REF_MEAN * (1.0 - O2_TARGET_DECREASE_PERCENT / 100.0)

print("\nOxygen target calculation")
print("--------------------------------")
print(f"Reference period         : {ref_start}-{ref_end}")
print(f"Reference mean {O2_COL}  : {O2_REF_MEAN:.10f}")
print(f"Target decrease          : {O2_TARGET_DECREASE_PERCENT:.3f} %")
print(f"Absolute target {O2_COL} : {O2_TARGET_ABS:.10f}")
print(f"Stocktake-year {O2_COL}  : {df_input.loc[y2, O2_COL]:.10f}")


# ============================================================
# Populate legacy arrays
# ============================================================
ix = target_years - y0

year[ix] = target_years
ann_o2var[ix] = df_input.loc[target_years, O2_COL].values
ann_ems[ix] = df_input.loc[target_years, "ff_emission"].values


# ============================================================
# Prepare AERA-Oxygen dataframe
# ============================================================
print("\nPopulate data to AERA-Oxygen dataframe")

# Start from AERA-Oxygen default dataframe.
# This should provide lu_emission and non_co2_emission through the future.
df = aera.get_base_df()

# Ensure O2var column exists
if O2_COL not in df.columns:
    df[O2_COL] = np.nan

# Fill O2var and fossil-fuel CO2 emissions from the input CSV
df.loc[target_years, O2_COL] = df_input.loc[target_years, O2_COL].values
df.loc[target_years, "ff_emission"] = df_input.loc[target_years, "ff_emission"].values

# If the input CSV also contains lu_emission / non_co2_emission, use them.
# Otherwise, the default AERA-Oxygen values are kept.
for col in ["lu_emission", "non_co2_emission"]:
    if col in df_input.columns:
        valid_years = df_input.index[df_input[col].notna()]
        valid_years = valid_years[
            (valid_years >= y0) & (valid_years <= df.index.max())
        ]
        df.loc[valid_years, col] = df_input.loc[valid_years, col].values


# ============================================================
# Sanity check before running AERA-Oxygen
# ============================================================
print("\nCheck input values:")
print(
    df.loc[
        1850:1855,
        [O2_COL, "ff_emission", "lu_emission", "non_co2_emission"],
    ]
)

print(
    df.loc[
        2010:2014,
        [O2_COL, "ff_emission", "lu_emission", "non_co2_emission"],
    ]
)

print("\nTarget check:")
print(f"{ref_start}-{ref_end} mean {O2_COL}: {df.loc[ref_start:ref_end, O2_COL].mean():.10f}")
print(f"Target {O2_COL}: {O2_TARGET_ABS:.10f}")
print(f"{y2} {O2_COL}: {df.loc[y2, O2_COL]:.10f}")


# ============================================================
# Run AERA-Oxygen
# ============================================================
print("\nCalculate adaptive emissions")

s_emission = aera.get_adaptive_emissions(
    oxygen_target_abs=O2_TARGET_ABS,
    year_x=YEAR_X,
    model_start_year=MODEL_START_YEAR,
    df=df,
    meta_file=meta_file,
)


# ============================================================
# Save next 5 years emissions
# ============================================================
print("\nSave outputs")

future_years = np.arange(y2 + 1, y2 + nyr + 1)

for yr in future_years:
    year[yr - y0] = yr
    ann_ems[yr - y0] = s_emission.loc[yr]

pickle.dump(
    [
        year,
        np.squeeze(ann_o2var),
        np.squeeze(ann_co2),
        np.squeeze(ann_ems),
    ],
    open(FOUT, "wb"),
)

s_emission.to_csv(AERA_outfile)

# Save target information separately for bookkeeping
target_info_file = OUTPUT_DIR / f"{AERAid}_target_info_{y2}.csv"

target_info = pd.DataFrame(
    {
        "AERAid": [AERAid],
        "stocktake_year": [y2],
        "variable": [O2_COL],
        "reference_start": [ref_start],
        "reference_end": [ref_end],
        "reference_mean": [O2_REF_MEAN],
        "target_decrease_percent": [O2_TARGET_DECREASE_PERCENT],
        "oxygen_target_abs": [O2_TARGET_ABS],
        "stocktake_value": [df_input.loc[y2, O2_COL]],
    }
)

target_info.to_csv(target_info_file, index=False)

print(f"Wrote pickle: {FOUT}")
print(f"Wrote AERA emission CSV: {AERA_outfile}")
print(f"Wrote metadata: {meta_file}")
print(f"Wrote target info: {target_info_file}")

print("\nNext 5-year emissions:")
print(s_emission.loc[future_years])