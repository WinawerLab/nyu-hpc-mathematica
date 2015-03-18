#! /bin/bash
# This script is designed to make running jobs from Mathematica on the BYU HPC cluster much easier.
# Author: Noah C. Benson <nben@nyu.edu>

####################################################################################################
# Definitions

# die
# die <text> prints <text> to stderr then exits with a return code of 1.
function die {
    echo "$*" 1>&2
    exit 1
}

# JOBS_PATH defines the location that job data is stored while running a job
JOBS_PATH="$SCRATCH/mathematica-jobs"
[ -a "$JOBS_PATH" ] || mkdir -p "$JOBS_PATH"
( [ -x "$JOBS_PATH" ] && [ -r "$JOBS_PATH" ] ) || die "Could not list and/or read $JOBS_PATH"

# JOBS_INSTRUCTIONS defines the location where job instructions go; e.g., the init file and such
JOBS_INSTR_PATH="$HOME/math/jobs"
#JOBS_RESULTS defines the location where the results from jobs go
JOBS_RESULTS="$HOME/math/results"

# MATH_CMD defines the command used to launch the wolfram kernel
MATH_CMD="/share/apps/mathematica/10.0.0/Executables/WolframKernel"

# math_run
# math_run <init_filename> <worker_id> runs the mathematica kernel for the given init file and the
# provided worker id.
function math_run {
    echo '
(* Import Neurotica and define the NYUHPCJob namespace... *)

(* Run the job *)
status = NYUHPC`Private`RunWorker[$WorkerID];

(* That is it! *)
If[status === $Failed,
  JobError["RunWorker yielded $Failed"],
  JobSuccess[status]];
' | HPC_MMA_INIT_DATA="$1" HPC_JOB="$2" HPC_WORKER_ID="$3" HPC_JOBDIR="$JOBDIR" "$MATH_CMD"
}

####################################################################################################
# Initialization and Command Line Arguments

# Make sure we've loaded the mathematica module
module list 2>&1 | grep -q athematica || module load "mathematica/10.0.0"

# All we get on the command line are the job and the worker id
JOB="$PBS_JOBNAME"
WORKERID="$PBS_ARRAYID"
( [ -z "$JOB" ] || [ -z "$WORKERID" ] ) && die "Could not get both job name and array id"

# Fix the job name to not include the worker id
JOB=`basename "$JOB" "-$WORKERID"`

# We have to make sure that the job data exists
( [ -r "$JOBS_INSTR_PATH/$JOB" ] && [ -x "$JOBS_INSTR_PATH/$JOB" ] ) || {
    die "$JOBS_INSTR_PATH has no job \"$JOB\""
}

# make sure we have an init file
[ -r "$JOBS_INSTR_PATH/$JOB/init.m" ] || {
    die "No init.m file found in job directory $JOBS_PATH/$JOB"
}

# we need to make our own scratch directory for this worker
printf -v JOB_HOME "$JOBS_PATH/$JOB/%04d" "$WORKERID"
mkdir -p "$JOB_HOME" || die "mkdir failed: $JOB_HOME"
( [ -r "$JOB_HOME" ] && [ -x "$JOB_HOME" ] ) || die "Could not read/access $JOB_HOME"

# we also want to make a home dir scratch directory for the data
JOB_RESULTS_HOME="$JOBS_RESULTS/$JOB"
# if we can't make this directory, fail now...
printf -v JOB_TEMP_LOC "$JOB_RESULTS_HOME/%04d" "$WORKERID"
mkdir -p "$JOB_TEMP_LOC" || die "Could not make results directory: $JOB_TEMP_LOC"

# okay, now switch to that directory
cd "$JOB_HOME"

# We need to start the math kernel and actually run the job
math_run "$JOBS_INSTR_PATH/$JOB/init.m" "$JOB" "$WORKERID"

# Now, we move all files in the job home back to the user's home directory
rsync -a "$JOB_HOME" "$JOB_RESULTS_HOME" &> "$JOB_TEMP_LOC/rsync.log"

# That's it!
exit 0
