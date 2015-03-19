(* run.m
 *
 * This script is meant to ease the running of Mathematica jobs on NYU's HPC cluster. 
 *)

(* Import the NYUHPCWorker namespace... *)
<<NYUHPCWorker`

(* See if there are dependencies we need to load... *)
Check[
  Map[Get, NYUHPC`Private`$Dependencies],
  JobError["Message raised while loading dependencies!"]];

(* Run the job *)
status = NYUHPC`Private`RunWorker[$WorkerID];

(* That is it! *)
If[status === $Failed,
  JobError["RunWorker yielded $Failed"],
  JobSuccess[status]];

