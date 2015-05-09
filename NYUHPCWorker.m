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
$JobInitFile::usage = "$JobInitFile contains the filename of the initialization file for the running job.";

Begin["`Private`"];

(* Define a function for handling job errors, messages, warnings *)
JobError[msg___] := (
  Export[
    FileNameJoin[{$JobWorkingDirectory, "error-"<>IntegerString[$WorkerID, 10, 4]<>".txt"}],
    StringJoin[ToString /@ {msg}],
    "Text"];
  Quit[]);
JobMessage[msg___] := PutAppend[
  StringJoin[ToString /@ {msg}],
  FileNameJoin[{$JobWorkingDirectory, "messages"<>IntegerString[$WorkerID, 10, 4]<>".txt"}]];
JobSuccess[data_] := Block[
  {result = data},
  Save[
    FileNameJoin[{$JobWorkingDirectory, "success-"<>IntegerString[$WorkerID, 10, 4]<>".m"}],
    result];
  Quit[]];
JobExport[name_String, data_, args___] := (
  If[!DirectoryQ[FileNameJoin[{$JobWorkingDirectory, "export-"<>IntegerString[$WorkerID, 10, 4]}]],
    CreateDirectory[
      FileNameJoin[{$JobWorkingDirectory, "export-"<>IntegerString[$WorkerID, 10, 4]}],
      CreateIntermediateDirectories -> True]];
  Export[
    FileNameJoin[{$JobWorkingDirectory, "export-"IntegerString[$WorkerID, 10, 4], name}],
    data,
    args]);
JobWarning[msg___] := PutAppend[
  StringJoin[ToString /@ {msg}],
  FileNameJoin[{$JobWorkingDirectory, "warnings-"<>IntegerString[$WorkerID, 10, 4]<>".txt"}]];

(* first, set our basic variables *)
$WorkerID = Check[
  With[
    {id = Environment["PBS_ARRAYID"]},
    If[id === $Failed, None, ToExpression[id]]],
  $Failed];
$JobName = Check[
  With[
    {job = Environment["PBS_JOBNAME"]},
    If[job === $Failed,
      None,
      StringJoin[Riffle[Most@StringSplit[job, "-"], "-"]]]],
  $Failed];
$JobWorkingDirectory = Check[
  FileNameJoin[
    {Environment["SCRATCH"],
     ".nyu_hpc_math_jobs",
     $JobName}],
  $Failed];
$JobInitFile = Check[FileNameJoin[{$JobWorkingDirectory, "init.m"}], $Failed];

(* We also load the dependencies list *)
$DepsInitStatus = Check[FileNameJoin[{$JobWorkingDirectory, "deps.m"}], $Failed];

(* make sure those were found... *)
If[$JobName === $Failed, JobError["JobName not found"]];
If[$WorkerID === $Failed, JobError["WorkerID not found"]];
If[$JobWorkingDirectory === $Failed, JobError["JobWorkingDirectory not found"]];
If[$JobInitFile === $Failed, JobError["JobInitFile not found"]];

(* Protect our definitions... *)
Protect[JobError, JobMessage, JobSuccess, JobWarning, 
        $WorkerID, $JobInitFile, $JobName, $JobWorkingDirectory];

End[];
EndPackage[];

