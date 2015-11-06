#! /bin/bash
# This script is designed to make running jobs from Mathematica on the NYU HPC cluster much easier.
# Note that this script should generally be called from the Mathematica HPC scheduler and not
# directly. The Mathematica HPC scheduler edits this script when running jobs.
# Author: Noah C. Benson <nben@nyu.edu>

####################################################################################################
# Definitions

# die
# die <text> prints <text> to stderr then exits with a return code of 1.
function die {
    echo "$*" 1>&2
    exit 1
}

# MATH_CMD defines the command used to launch the wolfram kernel
MATH_CMD="/share/apps/mathematica/10.2.0/Executables/WolframKernel"
MATH_SCRIPT="$HOME/.Mathematica/nyu-hpc-mathematica/scripts/run.m"

# math_run
# math_run <init_filename> <worker_id> runs the mathematica kernel for the given init file and the
# provided worker id.
function math_run {
    local RUNFILE="$HOME/.nyu_hpc_math_jobs/$1/running-${PBS_ARRAYID}.txt"
    local QFILE="$HOME/.nyu_hpc_math_jobs/$1/queued-${PBS_ARRAYID}.txt"
    [ `\cat "$HOME/.nyu_hpc_math_jobs/$1/status.txt"` = "Queued" ] \
        && \echo "Running" > "$HOME/.nyu_hpc_math_jobs/$1/status.txt"
    \date > "$RUNFILE"
    \rm -f "$QFILE"
    "$MATH_CMD" -noprompt -script "$MATH_SCRIPT"
    \rm -f "$RUNFILE"
}

####################################################################################################
# Initialization and Command Line Arguments

# Make sure we've loaded the mathematica module
module list 2>&1 | grep -q athematica || module load "mathematica/10.2.0"

# All we get on the command line are the job and the worker id
JOB="$PBS_JOBNAME"
WORKERID="$PBS_ARRAYID"
( [ -z "$JOB" ] || [ -z "$WORKERID" ] ) && die "Could not get both job name and array id"
# Fix the job name to not include the worker id
JOB=`basename "$JOB" "-$WORKERID"`

# okay, now switch to that directory
cd "$SCRATCH/.nyu_hpc_math_jobs/$JOB"

# We need to start the math kernel and actually run the job
math_run "$JOB"

# That's it!
exit 0
