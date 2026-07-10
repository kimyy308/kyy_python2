#!/bin/bash -l
#SBATCH --job-name=AERA_T_prep
#SBATCH --account=nn2980k
#SBATCH --partition=preproc
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --output=AERA_T_prep_%j.out
#SBATCH --error=AERA_T_prep_%j.err
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
  sbatch submit_AERA_T_Betzy_AERAjob.sh --year-x YEAR [options]

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
  --skip-namelist-update
  --skip-preview

Example:
  sbatch submit_AERA_T_Betzy_AERAjob.sh --year-x 2019
EOF
}

# ============================================================
# Default settings
# ============================================================
YEAR_X=""

CASE="NSSP126frc2esm_f19_tn14_noresm2.0.9_esm-ssp126-AERA_T_Betzy_20260709"
AERAID="AERA_T_Betzy"

ARCHIVE_ROOT="/cluster/work/users/yongyub/archive"
CAM_HIST_DIR=""

AERA_SCRIPT="${HOME}/Dropbox/source/python/all/AERA/exp_scripts/AERA_T/run_AERA_stocktake_Betzy.py"

AERA_HIST_DIR="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/AERA_hist/AERA_T_Betzy"
AERA_FUTURE_DIR="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/AERA_future/AERA_T_Betzy"

INITIAL_STATE_DAT="${AERA_HIST_DIR}/AERA_T_hist_nird_ann_tas_co2_ems_2014.dat"

HIST_INPUT_CSV="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/AERA_hist/NorESM2-LM_esm-hist_r1i1p1f1_1850-2014_AERA_temp_ff_input.csv"

EMISSION_DIR="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/input/emissions/AERA/AERA_T_Betzy"

EMISSION_TEMPLATE_NC="${EMISSION_DIR}/emissions-cmip6_CO2_anthro_surface_AERA_T_hist_nird_2015-2019_201401-210112_fv_1.9x2.5.nc"

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

WRAPPER_WORK="${CASEROOT}/AERA_wrapper_work"
LOGDIR="${WRAPPER_WORK}/logs"
MARKER_DIR="${WRAPPER_WORK}/markers"

mkdir -p "${WRAPPER_WORK}" "${LOGDIR}" "${MARKER_DIR}" "${AERA_FUTURE_DIR}" "${EMISSION_DIR}"

RUN_START=$((YEAR_X + 1))
RUN_END=$((YEAR_X + 5))
SEG_START=$((YEAR_X - 4))
SEG_END="${YEAR_X}"
TAG="${RUN_START}-${RUN_END}"

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
echo "[$(timestamp)] Current TAS segment   = ${SEG_START}-${SEG_END}"
echo "[$(timestamp)] Next emission period  = ${RUN_START}-${RUN_END}"
echo "[$(timestamp)] CASEROOT              = ${CASEROOT}"
echo "[$(timestamp)] RUNDIR                = ${RUNDIR}"
echo "[$(timestamp)] ARCHIVE_ROOT          = ${ARCHIVE_ROOT}"
echo "[$(timestamp)] CAM_HIST_DIR          = ${CAM_HIST_DIR}"
echo "[$(timestamp)] AERA_SCRIPT           = ${AERA_SCRIPT}"
echo "[$(timestamp)] AERAID                = ${AERAID}"
echo "[$(timestamp)] INITIAL_STATE_DAT     = ${INITIAL_STATE_DAT}"
echo "[$(timestamp)] HIST_INPUT_CSV        = ${HIST_INPUT_CSV}"
echo "[$(timestamp)] AERA_FUTURE_DIR       = ${AERA_FUTURE_DIR}"
echo "[$(timestamp)] EMISSION_TEMPLATE_NC  = ${EMISSION_TEMPLATE_NC}"
echo "[$(timestamp)] EMISSION_DIR          = ${EMISSION_DIR}"
echo "[$(timestamp)] SKIP_NAMELIST_UPDATE  = ${SKIP_NAMELIST_UPDATE}"
echo "[$(timestamp)] SKIP_PREVIEW          = ${SKIP_PREVIEW}"

# ============================================================
# Sanity checks
# ============================================================
test -d "${CASEROOT}" || die "CASEROOT does not exist: ${CASEROOT}"
test -d "${RUNDIR}" || die "RUNDIR does not exist: ${RUNDIR}"
test -d "${CAM_HIST_DIR}" || die "CAM_HIST_DIR does not exist: ${CAM_HIST_DIR}"

test -f "${AERA_SCRIPT}" || die "AERA script does not exist: ${AERA_SCRIPT}"
test -f "${INITIAL_STATE_DAT}" || die "Initial state dat does not exist: ${INITIAL_STATE_DAT}"
test -f "${HIST_INPUT_CSV}" || die "Historical input CSV does not exist: ${HIST_INPUT_CSV}"
test -f "${EMISSION_TEMPLATE_NC}" || die "Emission template netCDF does not exist: ${EMISSION_TEMPLATE_NC}"

if ! ls "${CAM_HIST_DIR}/${CASE}.cam.h0.${SEG_START}-01"* >/dev/null 2>&1; then
    die "Missing CAM file for ${SEG_START}-01 in ${CAM_HIST_DIR}"
fi

if ! ls "${CAM_HIST_DIR}/${CASE}.cam.h0.${SEG_END}-12"* >/dev/null 2>&1; then
    die "Missing CAM file for ${SEG_END}-12 in ${CAM_HIST_DIR}"
fi

if [[ "${YEAR_X}" -gt 2019 ]]; then
    PREV_YEAR=$((YEAR_X - 5))
    PREV_STATE="${AERA_FUTURE_DIR}/${AERAID}_ann_tas_co2_ems_${PREV_YEAR}.dat"
    test -s "${PREV_STATE}" || die "Previous AERA state is missing: ${PREV_STATE}"
fi

# ============================================================
# Avoid duplicate AERA job for the same stocktake year
# ============================================================
LOCKFILE="${WRAPPER_WORK}/${CASE}.AERA_YEARX${YEAR_X}.lock"
exec 9>"${LOCKFILE}"
flock -n 9 || die "Another AERA job for YEAR_X=${YEAR_X} seems to be running."

# ============================================================
# Run AERA stocktake and create the scaled emission netCDF file
# ============================================================
echo
echo "============================================================"
echo "[$(timestamp)] Running AERA stocktake for YEAR_X=${YEAR_X}"
echo "============================================================"

python -u "${AERA_SCRIPT}" \
    --case "${CASE}" \
    --rundir "${RUNDIR}" \
    --archive-root "${ARCHIVE_ROOT}" \
    --cam-hist-dir "${CAM_HIST_DIR}" \
    --aera-id "${AERAID}" \
    --initial-state-dat "${INITIAL_STATE_DAT}" \
    --future-output-dir "${AERA_FUTURE_DIR}" \
    --hist-input-csv "${HIST_INPUT_CSV}" \
    --emission-template-nc "${EMISSION_TEMPLATE_NC}" \
    --emission-output-dir "${EMISSION_DIR}" \
    --base-pattern-year 2014 \
    --year-x "${YEAR_X}" \
    --temp-target-rel 2.0 \
    --temp-target-type 2 \
    --model-start-year 1850 \
    --tas-mode absolute \
    > "${LOGDIR}/aera_YEARX${YEAR_X}_${SLURM_JOB_ID}.log" 2>&1

echo "[$(timestamp)] AERA finished for YEAR_X=${YEAR_X}"

# ============================================================
# Check AERA outputs
# ============================================================
AERA_PICKLE="${AERA_FUTURE_DIR}/${AERAID}_ann_tas_co2_ems_${YEAR_X}.dat"
AERA_CSV="${AERA_FUTURE_DIR}/${AERAID}_${YEAR_X}.csv"
AERA_NEXT5_CSV="${AERA_FUTURE_DIR}/${AERAID}_${YEAR_X}_next5.csv"
AERA_STATE_CSV="${AERA_FUTURE_DIR}/${AERAID}_state_${YEAR_X}.csv"
AERA_MODEL_TAS_CSV="${AERA_FUTURE_DIR}/${AERAID}_model_tas_${SEG_START}-${SEG_END}.csv"
EMISSION_PATH_TXT="${AERA_FUTURE_DIR}/${AERAID}_${YEAR_X}_emission_nc_path.txt"

test -s "${AERA_PICKLE}" || die "AERA pickle not found: ${AERA_PICKLE}"
test -s "${AERA_CSV}" || die "AERA CSV not found: ${AERA_CSV}"
test -s "${AERA_NEXT5_CSV}" || die "AERA next-5 CSV not found: ${AERA_NEXT5_CSV}"
test -s "${AERA_STATE_CSV}" || die "AERA state CSV not found: ${AERA_STATE_CSV}"
test -s "${AERA_MODEL_TAS_CSV}" || die "AERA model TAS CSV not found: ${AERA_MODEL_TAS_CSV}"
test -s "${EMISSION_PATH_TXT}" || die "Emission path txt not found: ${EMISSION_PATH_TXT}"

EMISSION_NC="$(head -1 "${EMISSION_PATH_TXT}")"
test -s "${EMISSION_NC}" || die "Emission netCDF not found: ${EMISSION_NC}"

echo
echo "[$(timestamp)] AERA output files:"
ls -lh "${AERA_PICKLE}" \
       "${AERA_CSV}" \
       "${AERA_NEXT5_CSV}" \
       "${AERA_STATE_CSV}" \
       "${AERA_MODEL_TAS_CSV}" \
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

# Match any AERA-style CO2 anthropogenic surface emission file.
pattern = re.compile(
    r'[/A-Za-z0-9._-]*emissions-cmip6_CO2_anthro_surface_[A-Za-z0-9._-]+_[0-9]{4}-[0-9]{4}_201401-210112_fv_1\.9x2\.5\.nc'
)

# First replace exact old template path/name if present.
text = text.replace(old_file, new_file)
text = text.replace(Path(old_file).name, new_file)

# Then replace any previous AERA emission filename if present.
text = pattern.sub(new_file, text)

if text == original:
    print("ERROR: No existing CO2 emission filename was found in user_nl_cam.")
    print("You need to check the exact CAM namelist variable once.")
    print("")
    print("Try:")
    print(f"  grep -n 'emissions-cmip6_CO2_anthro_surface' {user_nl_cam}")
    print("")
    print("Also check generated namelists:")
    print(f"  grep -R 'emissions-cmip6_CO2_anthro_surface' {caseroot}/CaseDocs")
    raise SystemExit(1)

backup = user_nl_cam.with_name(user_nl_cam.name + f".bak_YEARX{year_x}_{jobid}")
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
