(*  NYUHPCWorker.m
 *
 *  This file contains code for a Mathematica package that fascilitates the running of jobs on the
 *  NYU HPC cluster.
 *
 *  Copyright (c) 2015 Noah C. Benson
 *  This file is part of the nyu-hpc-mathematica project.
 *
 *  The nyu-hpc-mathematica project is free software: you can redistribute it and/or modify it under
 *  the terms of the MIT Public License. 
 *
 *  You should have received a copy of the MIT Public License along with this program.
 *  If not, see <http://opensource.org/licenses/MIT>.
 *)

BeginPackage["NYUHPCWorker`"];
Unprotect["NYUHPCWorker`*", "NYUHPCWorker`Private`*"];
ClearAll["NYUHPCWorker`*", "NYUHPCWorker`Private`*"];

JobError::usage = "JobError[message...] immediately ends the currently running job instance with the provided error message.";
JobWarning::usage = "JobWarning[message...] ensures that the given message is saved as a warning for the current running job.";
JobMessage::usage = "JobMessage[message...] ensures that the given message is saved as a message for the current running job.";
JobSuccess::usage = "JobSuccess[data] finishes the job and ensures that data is the currently running job's return value.";
JobExport::usage = "JobExport[name, data, args...] is equivalent to Export[name, data, args...] except that it ensures that name will be found by the controlling process when joining the job and can be retreived using HPCExports.";

$WorkerID::usage = "$WorkerID is the current array ID for the running job.";
$WorkerInitStatus::usage = "$WorkerInitStatus is the result of loading the worker's init file.";
$JobName::usage = "$JobName is the name of the currently running job (excluding the -id at the end).";
$JobWorkingDirectory::usage = "$JobWorkingDirectory contains the scratch directory in which the currently running job should write its results.";

Begin["`Private`"];

(* Define a function for handling job errors, messages, warnings *)
JobError[msg___] := (
  Export[
    "error.txt",
    StringJoin[ToString /@ {msg}],
    "Text"];
  Quit[]);
JobMessage[msg___] := PutAppend[
  StringJoin[ToString /@ {msg}],
  "messages.txt"];
JobSuccess[data_] := Block[
  {result = data},
  Save[
    FileNameJoin[{$JobWorkingDirectory, "success.m"}],
    result];
  Quit[]];
JobExport[name_String, data_, args___] := (
  If[!DirectoryQ[FileNameJoin[{$JobWorkingDirectory, "export"}]],
    CreateDirectory[
      FileNameJoin[{$JobWorkingDirectory, "export"}],
      CreateIntermediateDirectories -> True]];
  Export[FileNameJoin[{$JobWorkingDirectory, "export", name}], data, args]);
JobWarning[msg___] := PutAppend[
  StringJoin[ToString /@ {msg}],
  FileNameJoin[{$JobWorkingDirectory, "warnings.txt"}];

(* first, set our basic variables *)
$JobName = Check[
  With[
    {job = Environment["HPC_JOB"]},
    If[job === $Failed, None, ToExpression[job]]],
  $Failed];
$WorkerID = Check[
  With[
    {id = Environment["HPC_JOB"]},
    If[id === $Failed, None, ToExpression[id]]],
  $Failed];
$JobInitFile = Replace[Environment["HPC_MMA_INIT_DATA"], $Failed -> None];

(* make sure those were found... *)
If[$WorkerID === $Failed, JobError["WorkerID not found"]];
If[$JobName === $Failed, JobError["JobName not found"]];
If[$JobInitFile === $Failed, JobError["JobInitFile not found"]];
If[$JobWorkingDirectory === $Failed, JobError["JobWorkingDirectory not found"]];

(* import the initialization file *)
$WorkerInitStatus = Check[Get[$JobInitFile], $Failed];
If[$WorkerInitStatus === $Failed, JobError["Could not Get job init file"]];

(* Protect our definitions... *)
Protect[JobError, JobMessage, JobSuccess, JobWarning, $WorkerID, $JobInitFile, $WorkerInitStatus, $JobName];

End[];
EndPackage[];

