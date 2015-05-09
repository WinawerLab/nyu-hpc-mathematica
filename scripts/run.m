(* run.m
 *
 * This script is meant to ease the running of Mathematica jobs on NYU's HPC cluster. 
 *)

(* Import the NYUHPCWorker namespace... *)
<<NYUHPCWorker`;

(* import the initialization file *)
$WorkerInitStatus = Check[
  Get[$JobInitFile],
  $Failed];
If[$WorkerInitStatus === $Failed, JobError["Could not Get job init file"]];
Protect[$WorkerInitStatus];

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

