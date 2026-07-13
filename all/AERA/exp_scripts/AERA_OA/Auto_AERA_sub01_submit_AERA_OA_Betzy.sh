#!/bin/bash -l
#SBATCH --job-name=AERA_OA_prep
#SBATCH --account=nn2980k
#SBATCH --partition=preproc
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --output=/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/logs/AERA_OmegaA_Betzy/aera_jobs/AERA_OA_prep_%j.out
#SBATCH --error=/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/logs/AERA_OmegaA_Betzy/aera_jobs/AERA_OA_prep_%j.err
#SBATCH --export=ALL

set -euo pipefail
ulimit -s unlimited

# ============================================================
# Helper functions
# ============================================================
timestamp () {
    date "+%Y-%m-%d %H:%M:%S"
}

die () {
    echo "[$(timestamp)] ERROR: $*" >&2
    exit 1
}

usage () {
    cat << EOF
Usage:
  sbatch Auto_AERA_sub01_submit_AERA_OA_Betzy.sh --year-x YEAR [options]

Required:
  --year-x YEAR

Optional:
  --case CASE
  --aera-id AERAID
  --cam-hist-dir DIR
  --archive-root DIR
  --future-output-dir DIR
  --emission-output-dir DIR
  --emission-template-nc FILE
  --log-root DIR
  --omegaa-target-abs VALUE
  --ocn-hist-dir DIR
  --ocn-stream STREAM
  --omegaa-direct-var NAME
  --omegaa-numerator-var NAME
  --omegaa-denominator-var NAME
  --area-var NAME
  --current-metric-csv FILE   # optional override; normally not used
  --metric-column NAME
  --hist-metric-column NAME
  --skip-namelist-update
  --skip-preview

Example:
  sbatch Auto_AERA_sub01_submit_AERA_OA_Betzy.sh --year-x 2019
EOF
}

# ============================================================
# Default settings
# ============================================================
YEAR_X=""

CASE="NSSP126frc2esm_f19_tn14_noresm2.0.9_esm-ssp126-AERA_OmegaA_Betzy_20260709"
AERAID="AERA_OmegaA_Betzy"

ARCHIVE_ROOT="/cluster/work/users/yongyub/archive"
CAM_HIST_DIR=""
OCN_HIST_DIR=""
OCN_STREAM="hbgcm"
OMEGAA_DIRECT_VAR="omegaalvl"
OMEGAA_NUMERATOR_VAR="co3os"
OMEGAA_DENOMINATOR_VAR="co3satos"
AREA_VAR="parea"
AREA_GRID_FILE="/cluster/shared/noresm/inputdata/ocn/blom/grid/grid_tnx1v4_20170622.nc"

AERA_SCRIPT="${HOME}/Dropbox/source/python/all/AERA/exp_scripts/AERA_OA/run_AERA_OA_stocktake_Betzy.py"

AERA_HIST_DIR="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/AERA_hist/AERA_OmegaA_Betzy"
AERA_FUTURE_DIR="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/AERA_future/AERA_OmegaA_Betzy"

INITIAL_STATE_DAT="${AERA_HIST_DIR}/${AERAID}_ann_OmegaA_co2_ems_2014.dat"

# Optional. The 2014 .dat normally already contains 1850-2014 OmegaA and ff_emission.
# Leave empty unless you explicitly prepared a full historical input CSV.
HIST_INPUT_CSV=""

EMISSION_DIR="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/emissions/AERA/AERA_OmegaA_Betzy"

EMISSION_TEMPLATE_NC="${EMISSION_DIR}/emissions-cmip6_CO2_anthro_surface_${AERAID}_2015-2019_201401-210112_fv_1.9x2.5.nc"

LOG_ROOT="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/logs/AERA_OmegaA_Betzy"

OMEGAA_TARGET_ABS="${OMEGAA_TARGET_ABS:-}"
CURRENT_METRIC_CSV=""
METRIC_COLUMN="${METRIC_COLUMN:-OmegaA}"
HIST_METRIC_COLUMN="${HIST_METRIC_COLUMN:-}"

SKIP_NAMELIST_UPDATE="FALSE"
SKIP_PREVIEW="FALSE"

# ============================================================
# Parse command-line arguments passed after the sbatch script name
# ============================================================
while [[ $# -gt 0 ]]; do
    case "$1" in
        --year-x)
            YEAR_X="$2"
            shift 2
            ;;
        --case)
            CASE="$2"
            shift 2
            ;;
        --aera-id)
            AERAID="$2"
            shift 2
            ;;
        --cam-hist-dir)
            CAM_HIST_DIR="$2"
            shift 2
            ;;
        --ocn-hist-dir)
            OCN_HIST_DIR="$2"
            shift 2
            ;;
        --ocn-stream)
            OCN_STREAM="$2"
            shift 2
            ;;
        --omegaa-direct-var)
            OMEGAA_DIRECT_VAR="$2"
            shift 2
            ;;
        --omegaa-numerator-var)
            OMEGAA_NUMERATOR_VAR="$2"
            shift 2
            ;;
        --omegaa-denominator-var)
            OMEGAA_DENOMINATOR_VAR="$2"
            shift 2
            ;;
        --area-var)
            AREA_VAR="$2"
            shift 2
            ;;
        --area-grid-file)
            AREA_GRID_FILE="$2"
            shift 2
            ;;
        --archive-root)
            ARCHIVE_ROOT="$2"
            shift 2
            ;;
        --future-output-dir)
            AERA_FUTURE_DIR="$2"
            shift 2
            ;;
        --emission-output-dir)
            EMISSION_DIR="$2"
            shift 2
            ;;
        --emission-template-nc)
            EMISSION_TEMPLATE_NC="$2"
            shift 2
            ;;
        --log-root)
            LOG_ROOT="$2"
            shift 2
            ;;
        --omegaa-target-abs)
            OMEGAA_TARGET_ABS="$2"
            shift 2
            ;;
        --current-metric-csv)
            CURRENT_METRIC_CSV="$2"
            shift 2
            ;;
        --metric-column)
            METRIC_COLUMN="$2"
            shift 2
            ;;
        --hist-metric-column)
            HIST_METRIC_COLUMN="$2"
            shift 2
            ;;
        --skip-namelist-update)
            SKIP_NAMELIST_UPDATE="TRUE"
            shift 1
            ;;
        --skip-preview)
            SKIP_PREVIEW="TRUE"
            shift 1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage
            die "Unknown argument: $1"
            ;;
    esac
done

if [[ -z "${YEAR_X}" ]]; then
    usage
    die "--year-x is required."
fi

# ============================================================
# Derived paths
# ============================================================
CASEROOT="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/cases/${CASE}"
WORKROOT="/cluster/work/users/yongyub/noresm/${CASE}"
RUNDIR="${WORKROOT}/run"
EXE="${WORKROOT}/bld/cesm.exe"

if [[ -z "${CAM_HIST_DIR}" ]]; then
    CAM_HIST_DIR="${ARCHIVE_ROOT}/${CASE}/atm/hist"
fi

if [[ -z "${OCN_HIST_DIR}" ]]; then
    OCN_HIST_DIR="${ARCHIVE_ROOT}/${CASE}/ocn/hist"
fi

LOGDIR="${LOG_ROOT}/aera_internal"
MARKER_DIR="${LOG_ROOT}/markers"
LOCKDIR="${LOG_ROOT}/locks"
BACKUP_DIR="${LOG_ROOT}/backups"
JOB_WORK_DIR="${LOG_ROOT}/aera_work"

mkdir -p \
    "${LOG_ROOT}" \
    "${LOGDIR}" \
    "${MARKER_DIR}" \
    "${LOCKDIR}" \
    "${BACKUP_DIR}" \
    "${JOB_WORK_DIR}" \
    "${AERA_FUTURE_DIR}" \
    "${EMISSION_DIR}"

RUN_START=$((YEAR_X + 1))
RUN_END=$((YEAR_X + 5))
SEG_START=$((YEAR_X - 4))
SEG_END="${YEAR_X}"
TAG="${RUN_START}-${RUN_END}"

# Current-segment OmegaA is computed directly from OCN_HIST_DIR by default.
# CURRENT_METRIC_CSV is only an optional override.
# Avoid creating accidental relative-path logs in the git-managed script directory.
cd "${JOB_WORK_DIR}"

# ============================================================
# Python environment
# ============================================================
source ~/.bash_conda
conda activate kdask

echo "======================================"
echo "Job started at: $(date)"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Node list: ${SLURM_NODELIST}"
echo "Working directory: $(pwd)"
echo "Python executable: $(which python)"
python --version
echo "======================================"

# ============================================================
# Print settings
# ============================================================
echo "[$(timestamp)] CASE                  = ${CASE}"
echo "[$(timestamp)] YEAR_X                = ${YEAR_X}"
echo "[$(timestamp)] Current OmegaA segment   = ${SEG_START}-${SEG_END}"
echo "[$(timestamp)] Next emission period  = ${RUN_START}-${RUN_END}"
echo "[$(timestamp)] CASEROOT              = ${CASEROOT}"
echo "[$(timestamp)] RUNDIR                = ${RUNDIR}"
echo "[$(timestamp)] ARCHIVE_ROOT          = ${ARCHIVE_ROOT}"
echo "[$(timestamp)] CAM_HIST_DIR          = ${CAM_HIST_DIR}"
echo "[$(timestamp)] OCN_HIST_DIR          = ${OCN_HIST_DIR}"
echo "[$(timestamp)] OCN_STREAM            = ${OCN_STREAM}"
echo "[$(timestamp)] OMEGAA_DIRECT_VAR    = ${OMEGAA_DIRECT_VAR}"
echo "[$(timestamp)] OMEGAA_NUMERATOR_VAR  = ${OMEGAA_NUMERATOR_VAR}"
echo "[$(timestamp)] OMEGAA_DENOMINATOR_VAR= ${OMEGAA_DENOMINATOR_VAR}"
echo "[$(timestamp)] AREA_VAR              = ${AREA_VAR}"
echo "[$(timestamp)] AREA_GRID_FILE        = ${AREA_GRID_FILE}"
echo "[$(timestamp)] AERA_SCRIPT           = ${AERA_SCRIPT}"
echo "[$(timestamp)] AERAID                = ${AERAID}"
echo "[$(timestamp)] INITIAL_STATE_DAT     = ${INITIAL_STATE_DAT}"
echo "[$(timestamp)] HIST_INPUT_CSV        = ${HIST_INPUT_CSV}"
echo "[$(timestamp)] AERA_FUTURE_DIR       = ${AERA_FUTURE_DIR}"
echo "[$(timestamp)] EMISSION_TEMPLATE_NC  = ${EMISSION_TEMPLATE_NC}"
echo "[$(timestamp)] EMISSION_DIR          = ${EMISSION_DIR}"
echo "[$(timestamp)] LOG_ROOT              = ${LOG_ROOT}"
echo "[$(timestamp)] LOGDIR                = ${LOGDIR}"
echo "[$(timestamp)] OMEGAA_TARGET_ABS     = ${OMEGAA_TARGET_ABS}"
echo "[$(timestamp)] CURRENT_METRIC_CSV    = ${CURRENT_METRIC_CSV}"
echo "[$(timestamp)] METRIC_COLUMN         = ${METRIC_COLUMN}"
echo "[$(timestamp)] HIST_METRIC_COLUMN    = ${HIST_METRIC_COLUMN}"
echo "[$(timestamp)] MARKER_DIR            = ${MARKER_DIR}"
echo "[$(timestamp)] BACKUP_DIR            = ${BACKUP_DIR}"
echo "[$(timestamp)] SKIP_NAMELIST_UPDATE  = ${SKIP_NAMELIST_UPDATE}"
echo "[$(timestamp)] SKIP_PREVIEW          = ${SKIP_PREVIEW}"

# ============================================================
# Sanity checks
# ============================================================
test -d "${CASEROOT}" || die "CASEROOT does not exist: ${CASEROOT}"
test -d "${RUNDIR}" || die "RUNDIR does not exist: ${RUNDIR}"
test -d "${OCN_HIST_DIR}" || die "OCN_HIST_DIR does not exist: ${OCN_HIST_DIR}"
test -f "${AERA_SCRIPT}" || die "AERA script does not exist: ${AERA_SCRIPT}"
test -f "${INITIAL_STATE_DAT}" || die "Initial state dat does not exist: ${INITIAL_STATE_DAT}"
if [[ -n "${HIST_INPUT_CSV}" ]]; then
    test -f "${HIST_INPUT_CSV}" || die "Historical input CSV does not exist: ${HIST_INPUT_CSV}"
else
    echo "[$(timestamp)] HIST_INPUT_CSV is not set. The Python script will rely on the NIRD 2014 pickle for historical OmegaA/ff_emission."
fi
test -f "${EMISSION_TEMPLATE_NC}" || die "Emission template netCDF does not exist: ${EMISSION_TEMPLATE_NC}"
if [[ -z "${OMEGAA_TARGET_ABS}" ]]; then
    die "OMEGAA_TARGET_ABS is not set. Pass --omegaa-target-abs or export OMEGAA_TARGET_ABS."
fi
if [[ -n "${CURRENT_METRIC_CSV}" ]]; then
    test -f "${CURRENT_METRIC_CSV}" || die "Current OmegaA metric CSV override does not exist: ${CURRENT_METRIC_CSV}"
else
    echo "[$(timestamp)] CURRENT_METRIC_CSV is not set. OmegaA will be computed from ${OCN_HIST_DIR}/${CASE}.blom.${OCN_STREAM}.YYYY-MM.nc using direct variable ${OMEGAA_DIRECT_VAR} (surface level)."

    # Do not require exact ${SEG_START}-01 or ${SEG_END}-12 files here.
    # Native NorESM monthly files can be timestamped at the end of the averaging
    # interval, so the first useful file may be ${SEG_START}-02. The Python
    # script assigns samples to years using the time coordinate.
    test -f "${AREA_GRID_FILE}" || die "AREA_GRID_FILE does not exist: ${AREA_GRID_FILE}"
    if ! compgen -G "${OCN_HIST_DIR}/${CASE}.blom.${OCN_STREAM}.*.nc" >/dev/null; then
        die "No OCN files found: ${OCN_HIST_DIR}/${CASE}.blom.${OCN_STREAM}.*.nc"
    fi
fi


if [[ "${YEAR_X}" -gt 2019 ]]; then
    PREV_YEAR=$((YEAR_X - 5))
    PREV_STATE="${AERA_FUTURE_DIR}/${AERAID}_ann_OmegaA_co2_ems_${PREV_YEAR}.dat"
    test -s "${PREV_STATE}" || die "Previous AERA state is missing: ${PREV_STATE}"
fi

# ============================================================
# Avoid duplicate AERA job for the same stocktake year
# ============================================================
LOCKFILE="${LOCKDIR}/${CASE}.AERA_YEARX${YEAR_X}.lock"
exec 9>"${LOCKFILE}"
flock -n 9 || die "Another AERA job for YEAR_X=${YEAR_X} seems to be running."

# ============================================================
# Run AERA stocktake and create the scaled emission netCDF file
# ============================================================
echo
echo "============================================================"
echo "[$(timestamp)] Running AERA stocktake for YEAR_X=${YEAR_X}"
echo "============================================================"

EXTRA_AERA_ARGS=()
if [[ -n "${HIST_INPUT_CSV}" ]]; then
    EXTRA_AERA_ARGS+=(--hist-input-csv "${HIST_INPUT_CSV}")
fi
if [[ -n "${CURRENT_METRIC_CSV}" ]]; then
    EXTRA_AERA_ARGS+=(--current-metric-csv "${CURRENT_METRIC_CSV}")
fi
if [[ -n "${HIST_METRIC_COLUMN}" ]]; then
    EXTRA_AERA_ARGS+=(--hist-metric-column "${HIST_METRIC_COLUMN}")
fi

python -u "${AERA_SCRIPT}" \
    --case "${CASE}" \
    --rundir "${RUNDIR}" \
    --archive-root "${ARCHIVE_ROOT}" \
    --cam-hist-dir "${CAM_HIST_DIR}" \
    --ocn-hist-dir "${OCN_HIST_DIR}" \
    --ocn-stream "${OCN_STREAM}" \
    --omegaa-direct-var "${OMEGAA_DIRECT_VAR}" \
    --omegaa-numerator-var "${OMEGAA_NUMERATOR_VAR}" \
    --omegaa-denominator-var "${OMEGAA_DENOMINATOR_VAR}" \
    --area-var "${AREA_VAR}" \
    --area-grid-file "${AREA_GRID_FILE}" \
    --aera-id "${AERAID}" \
    --initial-state-dat "${INITIAL_STATE_DAT}" \
    --future-output-dir "${AERA_FUTURE_DIR}" \
    --emission-template-nc "${EMISSION_TEMPLATE_NC}" \
    --emission-output-dir "${EMISSION_DIR}" \
    --base-pattern-year 2014 \
    --year-x "${YEAR_X}" \
    --omegaa-target-abs "${OMEGAA_TARGET_ABS}" \
    --metric-column "${METRIC_COLUMN}" \
    "${EXTRA_AERA_ARGS[@]}" \
    --model-start-year 1850 \
    > "${LOGDIR}/aera_YEARX${YEAR_X}_${SLURM_JOB_ID}.log" 2>&1

echo "[$(timestamp)] AERA finished for YEAR_X=${YEAR_X}"

# ============================================================
# Check AERA outputs
# ============================================================
AERA_PICKLE="${AERA_FUTURE_DIR}/${AERAID}_ann_OmegaA_co2_ems_${YEAR_X}.dat"
AERA_CSV="${AERA_FUTURE_DIR}/${AERAID}_${YEAR_X}.csv"
AERA_NEXT5_CSV="${AERA_FUTURE_DIR}/${AERAID}_${YEAR_X}_next5.csv"
AERA_STATE_CSV="${AERA_FUTURE_DIR}/${AERAID}_state_${YEAR_X}.csv"
AERA_MODEL_METRIC_CSV="${AERA_FUTURE_DIR}/${AERAID}_model_OmegaA_${SEG_START}-${SEG_END}.csv"
EMISSION_PATH_TXT="${AERA_FUTURE_DIR}/${AERAID}_${YEAR_X}_emission_nc_path.txt"

test -s "${AERA_PICKLE}" || die "AERA pickle not found: ${AERA_PICKLE}"
test -s "${AERA_CSV}" || die "AERA CSV not found: ${AERA_CSV}"
test -s "${AERA_NEXT5_CSV}" || die "AERA next-5 CSV not found: ${AERA_NEXT5_CSV}"
test -s "${AERA_STATE_CSV}" || die "AERA state CSV not found: ${AERA_STATE_CSV}"
test -s "${AERA_MODEL_METRIC_CSV}" || die "AERA model OmegaA CSV not found: ${AERA_MODEL_METRIC_CSV}"
test -s "${EMISSION_PATH_TXT}" || die "Emission path txt not found: ${EMISSION_PATH_TXT}"

EMISSION_NC="$(head -1 "${EMISSION_PATH_TXT}")"
test -s "${EMISSION_NC}" || die "Emission netCDF not found: ${EMISSION_NC}"

echo
echo "[$(timestamp)] AERA output files:"
ls -lh "${AERA_PICKLE}" \
       "${AERA_CSV}" \
       "${AERA_NEXT5_CSV}" \
       "${AERA_STATE_CSV}" \
       "${AERA_MODEL_METRIC_CSV}" \
       "${EMISSION_PATH_TXT}" \
       "${EMISSION_NC}"

echo
echo "[$(timestamp)] Next-5 emissions:"
cat "${AERA_NEXT5_CSV}"

echo
echo "[$(timestamp)] Emission netCDF:"
echo "${EMISSION_NC}"

# ============================================================
# Prepare the case for the next NorESM run through case.submit
# ============================================================
if [[ "${SKIP_NAMELIST_UPDATE}" == "FALSE" ]]; then

    echo
    echo "============================================================"
    echo "[$(timestamp)] Preparing NorESM case for ${RUN_START}-${RUN_END}"
    echo "============================================================"

    cd "${CASEROOT}"

    echo "[$(timestamp)] Updating CIME XML settings"

    ./xmlchange CONTINUE_RUN=TRUE
    ./xmlchange STOP_OPTION=nyears,STOP_N=5
    ./xmlchange REST_OPTION=nyears,REST_N=5
    ./xmlchange RESUBMIT=0

    echo "[$(timestamp)] Current XML settings:"
    ./xmlquery CONTINUE_RUN STOP_OPTION STOP_N REST_OPTION REST_N RESUBMIT

    echo "[$(timestamp)] Patching user_nl_cam with new emission file"

    export CASEROOT
    export YEAR_X
    export RUN_START
    export RUN_END
    export EMISSION_NC
    export EMISSION_TEMPLATE_NC
    export SLURM_JOB_ID
    export BACKUP_DIR

    python - << 'PYEOF'
from pathlib import Path
import os
import re
import shutil

caseroot = Path(os.environ["CASEROOT"])
year_x = os.environ["YEAR_X"]
jobid = os.environ.get("SLURM_JOB_ID", "nojobid")
new_file = os.environ["EMISSION_NC"]
old_file = os.environ["EMISSION_TEMPLATE_NC"]

user_nl_cam = caseroot / "user_nl_cam"

if not user_nl_cam.exists():
    raise FileNotFoundError(f"user_nl_cam not found: {user_nl_cam}")

text = user_nl_cam.read_text()
original = text

# If user_nl_cam already points to the new emission file, this is success.
if new_file in text or Path(new_file).name in text:
    print("user_nl_cam already contains the new emission file.")
    print(f"New emission file: {new_file}")
    raise SystemExit(0)

# Match any AERA-style CO2 anthropogenic surface emission file.
pattern = re.compile(
    r'[/A-Za-z0-9._-]*emissions-cmip6_CO2_anthro_surface_[A-Za-z0-9._-]+_[0-9]{4}-[0-9]{4}_201401-210112_fv_1\.9x2\.5\.nc'
)

# Replace exact old template path/name if present.
text = text.replace(old_file, new_file)
text = text.replace(Path(old_file).name, new_file)

# Replace any previous AERA emission filename if present.
text = pattern.sub(new_file, text)

if text == original:
    raise SystemExit(
        "ERROR: user_nl_cam does not contain a replaceable CO2 emission filename, "
        "and it does not already contain the new emission file. "
        "Check co2flux_fuel_file manually."
    )

backup_dir = Path(os.environ["BACKUP_DIR"])
backup_dir.mkdir(parents=True, exist_ok=True)

backup = backup_dir / f"user_nl_cam.bak_YEARX{year_x}_{jobid}"
shutil.copy2(user_nl_cam, backup)
user_nl_cam.write_text(text)

print(f"Patched user_nl_cam: {user_nl_cam}")
print(f"Backup             : {backup}")
print(f"New emission file  : {new_file}")
PYEOF
    if [[ "${SKIP_PREVIEW}" == "FALSE" ]]; then
        echo "[$(timestamp)] Running preview_namelists for verification"

        ./preview_namelists > "${LOGDIR}/preview_namelists_YEARX${YEAR_X}_${SLURM_JOB_ID}.log" 2>&1

        echo "[$(timestamp)] Verifying generated namelists"

        if ! grep -R "$(basename "${EMISSION_NC}")" "${RUNDIR}" "${CASEROOT}/CaseDocs" >/dev/null 2>&1; then
            die "New emission file does not appear in generated namelists: ${EMISSION_NC}"
        fi

        echo "[$(timestamp)] New emission file is present in generated namelists"
    else
        echo "[$(timestamp)] SKIP_PREVIEW=TRUE, preview_namelists skipped"
    fi

    echo "[$(timestamp)] Checking rpointer files"

    if ! ls "${RUNDIR}"/rpointer.* >/dev/null 2>&1; then
        die "No rpointer files found in RUNDIR. case.submit may not be able to continue."
    fi

    ls -lh "${RUNDIR}"/rpointer.*

    READY_MARKER="${MARKER_DIR}/AERA_YEARX${YEAR_X}_READY_FOR_CASE_SUBMIT_${RUN_START}-${RUN_END}.marker"

    {
        echo "AERA preparation completed successfully."
        echo "YEAR_X=${YEAR_X}"
        echo "RUN_START=${RUN_START}"
        echo "RUN_END=${RUN_END}"
        echo "EMISSION_NC=${EMISSION_NC}"
        echo "CASEROOT=${CASEROOT}"
        echo "RUNDIR=${RUNDIR}"
        echo "DATE=$(date)"
    } > "${READY_MARKER}"

    echo "[$(timestamp)] Wrote ready marker:"
    echo "${READY_MARKER}"

else
    echo "[$(timestamp)] SKIP_NAMELIST_UPDATE=TRUE, case preparation skipped"
fi

echo
echo "============================================================"
echo "[$(timestamp)] AERA preparation job completed successfully."
echo "Job finished at: $(date)"
echo "============================================================"


