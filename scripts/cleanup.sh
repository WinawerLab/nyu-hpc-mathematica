#!/bin/bash
#PBS -N math_job_cleanup_____FINISHED_JOB_NAME____
#PBS -l walltime=0:30:00
#PBS -l procs=1
#PBS -l mem=128mb
#PBS -o $HOME/.nyu_hpc_math_jobs/____FINISHED_JOB_NAME____/cleanup.log
#PBS -j oe
#PBS -W depend=afteranyarray:____FINISHED_JOB_ID____
#PBS -v FINISHED_JOB_ID=____FINISHED_JOB_ID____
#PBS -v FINISHED_JOB_NAME=____FINISHED_JOB_NAME____

# This script is run as a job that cleans up other Mathematica jobs.
# We can assume, based on the dependency, that the job given in FINISHED_JOB_ID has already been
# completed. Additionally, the FINISHED_JOB_NAME variable gives us the name of the job that just
# finished.
# Author: Noah C. Benson <nben@nyu.edu>

function die {
    \echo "$*" 1>&2
    \exit 1
}

# Sanity Checking...
( [ -d "$SCRATCH/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME" ] \
    && [ -d "$HOME/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME" ] \
    && [ -r "$HOME/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME/status.txt" ] \
    && [ `cat "$HOME/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME/status.txt"` = "Running" ]
) || die "Job status not found or not running!"

# First, we make the directories we're going to need
LOG_DIR="$HOME/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME/outputs"
DET_DIR="$HOME/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME/details"
RES_DIR="$HOME/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME/results"
[ -d "$LOG_DIR" ] || \mkdir -p "$LOG_DIR" || die "Could not create logs directory"
[ -d "$DET_DIR" ] || \mkdir -p "$DET_DIR" || die "Could not create details directory"

# First, we want to move all logs to the home directory
\mv "$SCRATCH/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME"/worker_log.txt-* "$LOG_DIR/"

# Then, we move the scripts to the details dir
\mv "$SCRATCH/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME/worker_log.txt-*" "$DET_DIR/"

# Then we move everything else to the results directory
\mv "$SCRATCH/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME/" "$RES_DIR/" \
    || die "Could not move results to home!"

# Okay, we can fix the status to be complete
\echo "Complete" > "$HOME/.nyu_hpc_math_jobs/$FINISHED_JOB_NAME/status.txt"


