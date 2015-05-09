(* run.m
 *
 * This script is meant to ease the running of Mathematica jobs on NYU's HPC cluster. 
 *)

(* Import the NYUHPCWorker namespace... *)
<<NYUHPCWorker`;

(* See if there are dependencies we need to load... *)
Check[
  Map[Get, NYUHPC`Private`$Dependencies],
  JobError["Message raised while loading dependencies!"]];

(* import the initialization file *)
$WorkerInitStatus = Check[
  ToExpression /@ Reverse[
    StringSplit[
      Import[$JobInitFile, "String"],
      "\n\n"]],
  $Failed];
If[$WorkerInitStatus === $Failed, JobError["Could not Get job init file"]];
Protect[$WorkerInitStatus];

(* Run the job *)
runWorker = ReleaseHold[NYUHPC`Private`RunWorker];
status = runWorker[$WorkerID];

(* That is it! *)
If[status === $Failed,
  JobError["RunWorker yielded $Failed"],
  JobSuccess[status]];

