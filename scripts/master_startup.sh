#!/bin/bash
# This script is designed to be run upon connection to the HPC in order to setup convenient aliases
# and to check for the existance of various directories, etc.
# Note that this file is meant to be sourced and not run, so the exit command should not be used;
# instead the file is wrapped in a while loop and break can be used to escape.
# Author: Noah C. Benson <nben@nyu.edu>

####################################################################################################
# Introduction
# Functions and variables used throughout go here.

function die {
    echo "$*" 1>&2
    break
}



####################################################################################################
# While Loop
# All commands are wrapped in this loop so that a break (i.e., in the die function) will quit
while :
do



####################################################################################################
# Step 1
# Check for the various directories and code.

# 1.1: The ~/.Mathematica Directory ----------------------------------------------------------------
export MMA_DIR="$HOME/.Mathematica"
export MMA_APP_DIR="$HOME/.Mathematica/Applications"

# the directory exists
[ -d "$MMA_APP_DIR" ] || \mkdir -p "$MMA_APP_DIR" || die "Could not mkdir $MMA_APP_DIR" 
\cd "$MMA_DIR"

# the git repository exists
[ -d nyu-hpc-mathematica ] \
    || \git clone https://github.com/WinawerLab/nyu-hpc-mathematica &> /dev/null \
    || die "Could not clone git repository"

# and is up to date
( \cd nyu-hpc-mathematica \
    && \git fetch --all &> /dev/null \
    && \git reset --hard origin/master &> /dev/null \
    && \cd .. 
 ) || die "Could not clean git repository"

# the worker script is executable
[ -x nyu-hpc-mathematica/scripts/run_math_worker.sh ] \
    || \chmod 755 nyu-hpc-mathematica/scripts/run_math_worker.sh \
    || die "Could not chmod 755 worker script"

# the NYUHCPWorker namespace is available to Mathematica
[ -a "$MMA_APP_DIR/NYUHPCWorker.m" ] \
    || ( \pushd "$MMA_APP_DIR" &>/dev/null \
         && \ln -s ../ny-hpc-mathematica/NYUHPCWorker.m . &>/dev/null \
         && popd &>/dev/null ) \
    || die "Could not link NYUHPCWorker.m"

\cd "$HOME"


# 1.2: The math_jobs directories -------------------------------------------------------------------
export NYUHPC_RUN_DIR="$SCRATCH/.nyu_hpc_math_jobs"
export NYUHPC_FIN_DIR="$HOME/.nyu_hpc_math_jobs"
export NYUHPC_ARC_DIR="$ARCHIVE/.nyu_hpc_math_jobs"

[ -d "$NYUHPC_RUN_DIR" ] || \mkdir -p "$NYUHPC_RUN_DIR" || die "Could not mkdir $NYUHPC_RUN_DIR"
[ -d "$NYUHPC_FIN_DIR" ] || \mkdir -p "$NYUHPC_FIN_DIR" || die "Could not mkdir $NYUHPC_FIN_DIR"
[ -d "$NYUHPC_ARC_DIR" ] || \mkdir -p "$NYUHPC_ARC_DIR" || die "Could not mkdir $NYUHPC_ARC_DIR"



####################################################################################################
# Step 2
# Setup aliases used by the NYUHPC front-end.

# 2.1: Function for setting up jobs ----------------------------------------------------------------
function nyuhpc_setup_job {
    local JOB_RUN_DIR="$NYUHPC_RUN_DIR/$1"
    local JOB_FIN_DIR="$NYUHPC_FIN_DIR/$1"
    local JOB_STAT_FL="$NYUHPC_FIN_DIR/$1/status.txt"
    ( [ -d "$JOB_RUN_DIR" ] || \mkdir -p "$JOB_RUN_DIR" &>/dev/null ) \
        && ( [ -d "$JOB_FIN_DIR" ] || \mkdir -p "$JOB_FIN_DIR" &>/dev/null ) \
        && \echo "Queued" > "$JOB_STAT_FL" \
        && \echo OKAY
}



####################################################################################################
# Step 3
# Echo a success status and exit from this file

\cd "$HOME"
\echo SUCCESS
break



####################################################################################################
# End While Loop
done


