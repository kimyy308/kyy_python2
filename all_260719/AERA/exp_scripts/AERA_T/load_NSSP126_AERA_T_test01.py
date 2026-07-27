import xarray as xr
import glob
import math
import numpy as np
import matplotlib.pyplot as plt
import pickle
import aera
import scipy.io as sio
import time
from pathlib import Path

# MODEL INPUT PATH & EXP names
AERAid = 'AERA_T_20'
ARC_DIR = '/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/archives'
case1 = 'NSSP126frc2esm_f19_tn14_noresm2.0.9_esm-ssp126-AERA_T_20260630'

# Periods
y0=1850
y1=2015 # 2015 # Initial year prior to stocktake, usually y2-4
y2=2019 # 2025 # stocktake year
nyr=5

# Assign AERA variables
TEMP_TARGET_REL = 2.0
TEMP_TARGET_TYPE = 2 # 1=relative to obs
YEAR_X = y2
MODEL_CO2_PREINDUSTRIAL = 280.50 # mean of 50 years piCtrl prior to historical
MODEL_START_YEAR = 1850 

# AERA INPUT & OUTPUT PATH
AERA_DIR = Path("/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/archives/AERA")
FHIST_TAS = AERA_DIR / "defaults" / "HadCRUT.5.0.1.0.analysis.summary_series.global.annual.nc"
FF_EMS_FILE = AERA_DIR / "defaults" / "co2_ff_GCP_plus_NDC_v1.dat"
OUTPUT_DIR = AERA_DIR / "output" / AERAid
FOUT = OUTPUT_DIR / f"{AERAid}_ann_tas_co2_ems_{y2}.dat"
meta_file = OUTPUT_DIR / f"{AERAid}_meta_{y2}.nc"
AERA_outfile = OUTPUT_DIR / f"{AERAid}_{y2}.csv"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ann_tas = np.zeros(y2-y0+nyr+1)
ann_co2 = np.zeros(y2-y0+nyr+1)
ann_ems = np.zeros(y2-y0+nyr+1)
ann_tas[:] = np.NaN
ann_co2[:] = np.NaN
ann_ems[:] = np.NaN
year = np.zeros(y2-y0+nyr+1)

#Get existing data
print('Get existing data')
if y1 == 2015: 
	# ------------------------------------------------------------
	# Read historical annual global mean TAS from HadCRUT5
	# tas_mean is temperature anomaly [K], relative to 1961-1990
	# ------------------------------------------------------------
	ds_hist = xr.open_dataset(FHIST_TAS)

	hist_year = ds_hist["time"].dt.year.values.astype(int)
	hist_tas  = ds_hist["tas_mean"].values

	# We need historical values from y0 to y1-1
	target_years = np.arange(y0, y1)

	# Check availability
	missing_years = target_years[~np.isin(target_years, hist_year)]
	if len(missing_years) > 0:
	    raise ValueError(
	        f"HadCRUT file does not contain all required years {y0}-{y1-1}. "
	        f"Missing years include: {missing_years[:10]}"
	    )

	# Select tas_mean for y0:y1-1 in the correct order
	hist_tas_sel = np.array([
	    hist_tas[np.where(hist_year == yy)[0][0]]
	    for yy in target_years
	])

	year[0:y1-y0] = target_years
	ann_tas[0:y1-y0] = hist_tas_sel

	# ------------------------------------------------------------
	# Read historical/future fossil-fuel CO2 emissions
	# ------------------------------------------------------------
	field_ems = np.genfromtxt(FF_EMS_FILE)
	ann_ems[0:y2-y0+1] = field_ems[100:101+y2-y0, 1]

else:
    field = pickle.load(open(FOUT, 'rb')) # 
    year[0:y1-y0] = field[0][0:y1-y0]
    ann_tas[0:y1-y0] = field[1][0:y1-y0]
    ann_ems[0:y2-y0+1] = field[3][0:y2-y0+1]

#calculate area
ARCHIVE_DIR = Path(ARC_DIR)
CASE_DIR = ARCHIVE_DIR / case1
FN = CASE_DIR / "atm" / "hist" / f"{case1}.cam.h0.{y1}-01.nc"
fields = xr.open_dataset(FN)
alat = fields['lat'].values
alon = fields['lon'].values
nj = len(alat)
ni = len(alon)

slat=0.5*(np.roll(alat, 1)+alat)
slat[0] = -90
slat=np.append(slat,90)

rad=math.pi/180
area = np.zeros((nj, ni))
for j in range(nj): 
    for i in range(ni): 
        area[j,i] = math.sin(slat[j+1]*rad)-math.sin(slat[j]*rad)

area = area*5.1e14/np.sum(area)

#calculate data for the next period

for yr in np.arange(y1, y2+1):
    yix = yr-1850
    year[yix] = yr
    ann_tas[yix] = 0.
    ann_co2[yix] = 0.
    for m in np.arange(1, 13):
        FN = '{0:s}/{1:s}/atm/hist/{2:s}.cam.h0.{3:d}-{4:0>2d}.nc'.format(ARC_DIR,case1,case1,yr,m)
        print('Getting tas and co2 from ' + FN )
        fid = xr.open_dataset(FN)
        tas = fid['TREFHT'].values - 287.652 + 0.358 # 1850-1899 historical GMSAT mean + bias correction based on HadCRUT 
        ann_tas[yix] += np.sum(tas[:,:]*area[:,:])/np.sum(area[:,:])/12

# Assign time series to `df`
print('Populate data to df')
df = aera.get_base_df()
df['temp'].loc[y0:y2] = list(ann_tas[0:y2-y0+1])
#df['co2_conc'].loc[y0:y2] = list(ann_co2[0:y2-y0+1])   
df['ff_emission'].loc[y0:y2] = list(ann_ems[0:y2-y0+1])

# Calculate ifuture emissions
s_emission = aera.get_adaptive_emissions(
    temp_target_rel=TEMP_TARGET_REL,
    temp_target_type=TEMP_TARGET_TYPE,
    year_x=YEAR_X,
    model_start_year=MODEL_START_YEAR,
    df=df,
    meta_file=meta_file,
    )

# populate the next five years emissions
for yr in np.arange(y2+1, y2+nyr+1):
    year[yr-1850] = yr
    ann_ems[yr-1850] = s_emission.loc[yr]

pickle.dump([year, np.squeeze(ann_tas), np.squeeze(ann_co2), np.squeeze(ann_ems)], open(FOUT, 'wb')) 
s_emission.to_csv(AERA_outfile)
