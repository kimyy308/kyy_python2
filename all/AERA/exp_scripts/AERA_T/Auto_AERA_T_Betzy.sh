#!/bin/bash

set -euo pipefail

# ============================================================
# User settings
# ============================================================

CASE="NSSP126frc2esm_f19_tn14_noresm2.0.9_esm-ssp126-AERA_T_Betzy_20260709"

CASEROOT="/cluster/projects/nn2980k/yongyub/NORESM/NorESM2/cases/${CASE}"
ARCHIVE_ROOT="/cluster/work/users/yongyub/archive"
ARCHIVE_HIST_DIR="${ARCHIVE_ROOT}/${CASE}/atm/hist"

AERA_JOB_SCRIPT="${HOME}/Dropbox/source/python/all/AERA/exp_scripts/AERA_T/Auto_AERA_sub01_submit_AERA_T_Betzy.sh"

# First stocktake year.
# If the model has already completed 2015-2019, this should be 2019.
FIRST_STOCKTAKE_YEAR=2019

# Final model year to reach.
# Example:
#   FINAL_MODEL_YEAR=2029 means:
#     YEAR_X=2019 -> run 2020-2024
#     YEAR_X=2024 -> run 2025-2029
FINAL_MODEL_YEAR=2029

STOCKTAKE_STEP=5

# Extra arguments passed to case.submit.
# Do not pass partition here unless necessary, because case.run and case.st_archive
# may use different partitions from their generated scripts.
CASE_SUBMIT_BATCH_ARGS="-A nn2980k --time=48:00:00"

# Polling intervals in seconds.
POLL_JOB=60
POLL_ARCHIVE=60

# Maximum time to wait for archive files after model queue clears.
# 21600 sec = 6 hours.
ARCHIVE_WAIT_MAX=21600

# Autoscript log directory.
AUTO_LOG_DIR="${CASEROOT}/AERA_wrapper_work/autoscript_logs"
mkdir -p "${AUTO_LOG_DIR}"

AUTO_LOG="${AUTO_LOG_DIR}/auto_AERA_NorESM_loop_$(date +%Y%m%d_%H%M%S).log"

# ============================================================
# Helper functions
# ============================================================

timestamp () {
    date "+%Y-%m-%d %H:%M:%S"
}

log () {
    echo "[$(timestamp)] $*" | tee -a "${AUTO_LOG}"
}

die () {
    echo "[$(timestamp)] ERROR: $*" | tee -a "${AUTO_LOG}" >&2
    exit 1
}

run_cmd () {
    log "CMD: $*"
    "$@" 2>&1 | tee -a "${AUTO_LOG}"
}

job_state () {
    local jobid="$1"
    sacct -j "${jobid}" -X --noheader --format=State 2>/dev/null \
        | awk 'NF {print $1; exit}'
}

job_exitcode () {
    local jobid="$1"
    sacct -j "${jobid}" -X --noheader --format=ExitCode 2>/dev/null \
        | awk 'NF {print $1; exit}'
}

wait_for_job_success () {
    local jobid="$1"
    local label="$2"

    log "Waiting for ${label} job ${jobid}"

    while squeue -j "${jobid}" -h >/dev/null 2>&1; do
        local qline
        qline="$(squeue -j "${jobid}" -h -o "%i %j %T %M %R" || true)"
        if [[ -n "${qline}" ]]; then
            log "Still running/queued: ${qline}"
        fi
        sleep "${POLL_JOB}"
    done

    # Slurm accounting can lag a little.
    local state=""
    local exitcode=""

    for _ in {1..20}; do
        state="$(job_state "${jobid}" || true)"
        exitcode="$(job_exitcode "${jobid}" || true)"

        if [[ -n "${state}" ]]; then
            break
        fi

        sleep 15
    done

    log "${label} job ${jobid} final state: ${state:-UNKNOWN}, exitcode: ${exitcode:-UNKNOWN}"

    if [[ "${state}" != "COMPLETED" ]]; then
        die "${label} job ${jobid} did not complete successfully."
    fi

    if [[ -n "${exitcode}" && "${exitcode}" != "0:0" ]]; then
        die "${label} job ${jobid} has non-zero exit code: ${exitcode}"
    fi
}

extract_case_submit_jobid () {
    local submit_log="$1"

    # Try common Slurm/CIME output patterns.
    grep -Eo 'Submitted batch job [0-9]+' "${submit_log}" \
        | awk '{print $NF}' \
        | tail -1
}

submit_aera_job () {
    local year_x="$1"

    log "Submitting AERA prep job for YEAR_X=${year_x}"

    local jid
    jid="$(sbatch --parsable "${AERA_JOB_SCRIPT}" --year-x "${year_x}")"

    if [[ -z "${jid}" ]]; then
        die "Failed to get AERA job id for YEAR_X=${year_x}"
    fi

    log "Submitted AERA prep job: ${jid}"
    echo "${jid}"
}

submit_case_run () {
    local year_x="$1"
    local run_start="$2"
    local run_end="$3"

    log "Submitting NorESM case.run through case.submit for ${run_start}-${run_end}"

    cd "${CASEROOT}"

    local submit_log
    submit_log="${AUTO_LOG_DIR}/case_submit_YEARX${year_x}_$(date +%Y%m%d_%H%M%S).log"

    set +e
    ./case.submit --batch-args "${CASE_SUBMIT_BATCH_ARGS}" 2>&1 | tee "${submit_log}" | tee -a "${AUTO_LOG}"
    local rc=${PIPESTATUS[0]}
    set -e

    if [[ "${rc}" -ne 0 ]]; then
        die "case.submit failed for YEAR_X=${year_x}. See ${submit_log}"
    fi

    local jid
    jid="$(extract_case_submit_jobid "${submit_log}" || true)"

    if [[ -z "${jid}" ]]; then
        log "Could not parse job id from case.submit output."
        log "case.submit log: ${submit_log}"
        die "Failed to parse NorESM model job id."
    fi

    log "Submitted NorESM model job: ${jid}"
    echo "${jid}"
}

wait_for_archive_files () {
    local run_start="$1"
    local run_end="$2"

    log "Waiting for archived CAM history files for ${run_start}-${run_end}"
    log "Archive hist dir: ${ARCHIVE_HIST_DIR}"

    local waited=0

    while true; do
        local nfiles
        nfiles="$(
            find "${ARCHIVE_HIST_DIR}" -maxdepth 1 -type f \
                -name "${CASE}.cam.h0.*.nc" \
                | awk -v ys="${run_start}" -v ye="${run_end}" '
                    {
                        if (match($0, /[0-9]{4}-[0-9]{2}/)) {
                            ym=substr($0, RSTART, RLENGTH)
                            yy=substr(ym,1,4)+0
                            if (yy >= ys && yy <= ye) print
                        }
                    }
                ' \
                | wc -l
        )"

        local first_ok=0
        local last_ok=0

        if ls "${ARCHIVE_HIST_DIR}/${CASE}.cam.h0.${run_start}-01"* >/dev/null 2>&1; then
            first_ok=1
        fi

        if ls "${ARCHIVE_HIST_DIR}/${CASE}.cam.h0.${run_end}-12"* >/dev/null 2>&1; then
            last_ok=1
        fi

        log "Archive check for ${run_start}-${run_end}: nfiles=${nfiles}, first_ok=${first_ok}, last_ok=${last_ok}"

        # Monthly h0 files should normally be 5 years x 12 months = 60 files.
        if [[ "${nfiles}" -ge 60 && "${first_ok}" -eq 1 && "${last_ok}" -eq 1 ]]; then
            log "Archive files found for ${run_start}-${run_end}"
            ls -lh "${ARCHIVE_HIST_DIR}/${CASE}.cam.h0.${run_start}-01"* \
                   "${ARCHIVE_HIST_DIR}/${CASE}.cam.h0.${run_end}-12"* \
                   2>/dev/null | tee -a "${AUTO_LOG}"
            break
        fi

        if [[ "${waited}" -ge "${ARCHIVE_WAIT_MAX}" ]]; then
            die "Timed out waiting for archive files for ${run_start}-${run_end}"
        fi

        sleep "${POLL_ARCHIVE}"
        waited=$((waited + POLL_ARCHIVE))
    done
}


wait_until_case_queue_quiet () {
    local grace_count=0
    local grace_required=3

    # Use a shortened case prefix because the Slurm job name is often truncated.
    local case_prefix="${CASE:0:14}"

    log "Waiting until all case-related Slurm jobs are gone from the queue"
    log "Case prefix used for queue matching: ${case_prefix}"

    while true; do
        local qlines

        qlines="$(
            squeue -u "${USER}" -h -o "%i %j %T %M %R" \
            | awk -v pfx="${case_prefix}" '
                $2 ~ pfx || $2 ~ /st_archive/ || $2 ~ /archive/ {print}
            ' || true
        )"

        if [[ -z "${qlines}" ]]; then
            grace_count=$((grace_count + 1))
            log "No matching case-related jobs in queue. Grace ${grace_count}/${grace_required}."

            if [[ "${grace_count}" -ge "${grace_required}" ]]; then
                break
            fi
        else
            grace_count=0
            log "Case-related jobs still in queue:"
            echo "${qlines}" | tee -a "${AUTO_LOG}"
        fi

        sleep "${POLL_JOB}"
    done
}

check_aera_ready_marker () {
    local year_x="$1"
    local run_start="$2"
    local run_end="$3"

    local marker="${CASEROOT}/AERA_wrapper_work/markers/AERA_YEARX${year_x}_READY_FOR_CASE_SUBMIT_${run_start}-${run_end}.marker"

    if [[ ! -s "${marker}" ]]; then
        die "AERA ready marker not found: ${marker}"
    fi

    log "AERA ready marker found: ${marker}"
    cat "${marker}" | tee -a "${AUTO_LOG}"
}

# ============================================================
# Main
# ============================================================

log "============================================================"
log "AERA-NorESM autoscript started"
log "CASE=${CASE}"
log "CASEROOT=${CASEROOT}"
log "ARCHIVE_HIST_DIR=${ARCHIVE_HIST_DIR}"
log "AERA_JOB_SCRIPT=${AERA_JOB_SCRIPT}"
log "FIRST_STOCKTAKE_YEAR=${FIRST_STOCKTAKE_YEAR}"
log "FINAL_MODEL_YEAR=${FINAL_MODEL_YEAR}"
log "CASE_SUBMIT_BATCH_ARGS=${CASE_SUBMIT_BATCH_ARGS}"
log "AUTO_LOG=${AUTO_LOG}"
log "============================================================"

test -d "${CASEROOT}" || die "CASEROOT does not exist: ${CASEROOT}"
test -f "${AERA_JOB_SCRIPT}" || die "AERA job script does not exist: ${AERA_JOB_SCRIPT}"

LAST_STOCKTAKE_YEAR=$((FINAL_MODEL_YEAR - STOCKTAKE_STEP))

YEAR_X="${FIRST_STOCKTAKE_YEAR}"

while [[ "${YEAR_X}" -le "${LAST_STOCKTAKE_YEAR}" ]]; do

    RUN_START=$((YEAR_X + 1))
    RUN_END=$((YEAR_X + STOCKTAKE_STEP))

    log "============================================================"
    log "Starting cycle: YEAR_X=${YEAR_X}, next run=${RUN_START}-${RUN_END}"
    log "============================================================"

    # ------------------------------------------------------------
    # 1. Submit AERA preprocessing job
    # ------------------------------------------------------------
    AERA_JOBID="$(submit_aera_job "${YEAR_X}")"
    wait_for_job_success "${AERA_JOBID}" "AERA prep"

    check_aera_ready_marker "${YEAR_X}" "${RUN_START}" "${RUN_END}"

    # ------------------------------------------------------------
    # 2. Submit NorESM model run through CIME case.submit
    # ------------------------------------------------------------
    MODEL_JOBID="$(submit_case_run "${YEAR_X}" "${RUN_START}" "${RUN_END}")"
    wait_for_job_success "${MODEL_JOBID}" "NorESM model"

    # ------------------------------------------------------------
    # 3. Wait for CIME short-term archive job to finish
    # ------------------------------------------------------------
    wait_until_case_queue_quiet

    # Concrete archive check: this is what the next AERA step needs.
    wait_for_archive_files "${RUN_START}" "${RUN_END}"

    log "Cycle completed successfully: YEAR_X=${YEAR_X}, run=${RUN_START}-${RUN_END}"

    YEAR_X=$((YEAR_X + STOCKTAKE_STEP))

done

log "============================================================"
log "All requested AERA-NorESM cycles completed successfully."
log "Final model year reached: ${FINAL_MODEL_YEAR}"
log "============================================================"
