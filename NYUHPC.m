(*  NYUHPC.m
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

BeginPackage["NYUHPC`"];
Unprotect["NYUHPC`*", "NYUHPC`Private`*"];
ClearAll["NYUHPC`*", "NYUHPC`Private`*"];

LaunchJob::usage = "LaunchJob[name, n, fn] launches a job that runs the given fn with a single argument, the worker ID number, on n different worker nodes on the NYU HPC. The worker ID passed to the given function fn is always between 1 and n, inclusive. The following options may be given:
  * \"RunAt\" (default: Automatic) specifies the time to run the job, which should be given as a string formatted according to the date_time argument of qsub (see man qsub); Automatic causes the job to run immediately.
  * \"Resources\" (default: Automatic) specifies the resource requirement string for the qsub command (see man qsub, -l option).
  * \"Priority\" (default: 0) specifies the job's priority (must be between -1024 and 1023).
  * \"Overwrite\" (default: False) specifies whether a job with the same name should be overwritten or not.
  * \"Directory\" (default: \"~/math\") specifies the directory where we should place the jobs and results directory for this job.";
LaunchJob::badarg = "Bad argument given to LaunchJob: `1`";

HPCConnection::usage = "HPCConnection[...] is a form used to hold information about a currently open connection to the NYU HPC.";

HPCConnect::usage = "HPCConnect[username] yields an HPCConnection object for the given username.";
HPCConnect::noconn = "HPCConnect was unable to connect to the remote host: `1`";

HPCStatus::usage = "HPCStatus[conn] yields the current status of the given HPCConnection object conn.";

HPCClose::usage = "HPCClose[conn] closes the given HPCConnection and yields True.";
HPCClose::badstatus = "The status given of the process given to HPCClose (`1`) cannot be closed.";

HPCCommand::usage = "HPCCommand[cmd] yields the text results of executing the given command on the HPC.";

$HPCCurrentConnection::usage = "$HPCCurrentConnection is a variable that contains the current HPC connection; this is the last connection opened using HPCConnect[]. Certain HPC functions use this value by default (e.g., HPCRun). The variable is not protected and may be set.";

Begin["`Private`"];

$HPCCurrentConnection = None;

(* #HPCConnection *********************************************************************************)
HPCConnection /: ProcessObject[HPCConnection[u_, ___]] := u;
HPCConnection /: ProcessStatus[HPCConnection[_, s_, ___]] := s;
Protect[HPCConnection];

(* #HPCPassword *)
HPCPassword[username_String] := Block[
  {passwd = ""},
  DialogInput[
    DialogNotebook[
      {TextCell["HPC Password for "<>username<>": "], 
       InputField[Dynamic[passwd], String, FieldMasked -> True],
       DefaultButton["Okay", DialogReturn[passwd]]}]]];
HPCPassword[] := Block[
  {passwd = ""},
  DialogInput[
    DialogNotebook[
      {TextCell["HPC Password: "], 
       InputField[Dynamic[passwd], String, FieldMasked -> True],
       DefaultButton["Okay", DialogReturn[passwd]]}]]];
Protect[HPCPassword];

(* #HPCPrep ***************************************************************************************)
HPCPrep[hpc_HPCConnection] := Check[
  With[
    {proc = ProcessObject[hpc],
     tag = ToString @ Unique["DONE"]},
    WriteString[
      proc,
      (* The goal of this small bit of bash code is to do the following: *)
      StringJoin[
        "{ ",
        Riffle[
          {Riffle[
             {"[ -d ~/.Mathematica/Applications ]",
              "mkdir -p ~/.Mathematica/Applications &> /dev/null",
              "( echo fail_mkdir_1 && exit 1 )"},
             " || "],
           "cd ~/.Mathematica",
           Riffle[
             {"[ -d nyu-hpc-mathematica ]",
              "git clone https://github.com/WinawerLab/nyu-hpc-mathematica &> /dev/null",
              "( echo fail_git_clone && exit 1 )"},
             " || "],
           Riffle[
             {{"( ", Riffle[{"cd nyu-hpc-mathematica",
                             "git fetch --all &>/dev/null",
                             "git reset --hard origin/master &>/dev/null",
                             "cd .."},
                            " && "], " )"},
              "( echo fail_git_pull && exit 1 )"},
             " || "],
           Riffle[
             {"[ -x nyu-hpc-mathematica/scripts/run_math_worker.sh ]",
              "chmod 755 nyu-hpc-mathematica/scripts/run_math_worker.sh &> /dev/null",
              "( echo fail_chmod && exit 1 )"},
             " || "],
           Riffle[
             {"[ -a ~/.Mathematica/Applications/NYUHPCWorker.m ]",
              "ln -s ~/.Mathematica/nyu-hpc-mathematica/NYUHPCWorker.m"
               <> "  ~/.Mathematica/Applications/NYUHPCWorker.m &> /dev/null",
              "( echo fail_ln && exit 1 )"},
             " || "],
           "cd ~",
           Riffle[
             {"[ -d ~/.nyu_hpc_math_jobs ]",
              "mkdir -p ~/.nyu_hpc_math_jobs &> /dev/null",
              "( echo fail_mkdir_2 && exit 1 )"},
             " || "],
           "echo SUCCESS;"},
          "; "],
        " }; ",
        "echo ", tag, ";\n"]];
    StringTrim @ ReadString[proc, tag]],
  "FAIL"];
Protect[HPCPrep];    

(* #HPCConnect ************************************************************************************)
Options[HPCConnect] = {"Host" -> "mercer.es.its.nyu.edu"};
HPCConnect[username_String, OptionsPattern[]] := Check[
  Catch[
    With[
      {host = OptionValue["Host"],
       env = Module[
         {assoc = Association @ DeleteCases[GetEnvironment[], "DISPLAY" -> _]},
         assoc["TERM"] = "dumb";
         assoc["SHELL"] = "/bin/bash";
         assoc],
       tag = Unique["ping"]},
      With[
        {proc = StartProcess[
           {"ssh", "-l", username, "-o", "BatchMode=yes", host,
            "echo "<>ToString[tag]<>" && exec /bin/bash"},
           ProcessEnvironment -> env]},
        (* Now we have to make sure the process starts correctly... *)
        With[
          {str = Quiet@Check[ReadString[proc, ToString[tag]], EndOfFile],
           info = ProcessInformation[proc]},
          If[str === EndOfFile || info["ExitCode"] == 255,
            (* failure to startup; we may need a password... *)
            Throw[
              Message[HPCConnect::noconn, "you may need to setup passwordless login or a tunnel"];
              $Failed],
            (* correct startup... *)
            With[
              {conn = HPCConnection[proc, "OKAY"]},
              If[HPCPrep[conn] != "SUCCESS",
                KillProcess[proc];
                Message[HPCConnect::noconn, "could not initialize/verify remote math setup"];
                Throw[$Failed]];
              $HPCCurrentConnection = conn]]]]]],
  $Failed];
Protect[HPCConnect];

(* #HPCSTATUS *************************************************************************************)
HPCStatus[hpc:HPCConnection[_, status_, ___]] := With[
  {proc = ProcessObject[hpc]},
  With[
    {stat = ProcessStatus[proc]},
    If[stat["ExitCode"] != 0, "ERROR", status]]];
Protect[HPCStatus];    

(* #HPCClose **************************************************************************************)
HPCClose[p:HPCConnection[u_, status_, ___]] := Which[
  status === None, True,
  status == "OKAY", (
    WriteLine[u, "exit"];
    ReadString[u, TimeConstraint -> 10];
    KillProcess[u];
    If[p === $HPCCurrentConnection, $HPCCurrentConnection = None];
    True),
  True, Message[HPCClose::badstatus, status]];
Protect[HPCClose];


(**************************************************************************************************)
(* This section deals with reading to and writing from the HPC                                    *)

(* #HPCReadOutput *)
HPCReadOutput[conn_HPCConnection, stag_, etag_] := With[
  {proc = ProcessObject[conn],
   sof = ToString[stag] <> "\n",
   eof = ToString[etag] <> "\n"},
  ReadString[proc, sof];
  ReadString[proc, eof]];
Protect[HPCReadOutput];

(* #HPCCommand *)
HPCCommand[conn_HPCConnection, args__] := With[
  {cmd = StringJoin[Riffle[ToString /@ {args}, " "]],
   stag = Unique["SOF"],
   etag = Unique["EOF"]},
  WriteLine[
    ProcessObject[conn],
    StringJoin[
      "{ echo ", ToString[stag],
      " && ", cmd,
      " && echo ", ToString[etag], "; }"]];
  HPCReadOutput[conn, stag, etag]];
Protect[HPCCommand];

(*HPCJobStatus[conn_HPCConnection, name_String] := *)

Options[LaunchJob] = {
  "RunAt" -> Automatic,
  "Resources" -> Automatic,
  "Priority" -> 0,
  "Overwrite" -> False,
  "MaxSimultaneousJobs" -> 50,
  "Directory" :> FileNameJoin[{Environment["HOME"], "math"}]};
LaunchJob[name_String, nArg_, code_, OptionsPattern[]] := Catch[
  With[
    {n = nArg,
     runAt = OptionValue["RunAt"],
     resources = OptionValue["Resources"],
     priority = OptionValue["Priority"],
     overwrite = OptionValue["Overwrite"],
     maxSim = OptionValue["MaxSimultaneousJobs"],
     baseDir = OptionValue["Directory"]},
    Check[
      Which[
        !IntegerQ[n] || n < 1, Message[
          LaunchJob::badarg,
          "number of workers must be an integer > 0"],
        !StringQ[runAt] && runAt =!= Automatic, Message[
          LaunchJob::badarg,
          "run-at option must be a string"],
        !StringQ[resources] && resources =!= Automatic,  Message[
          LaunchJob::badarg,
          "resources must be a string"],
        !IntegerQ[priority] || !(-1024 <= priority <= 1023), Message[
          LaunchJob::badarg,
          "priority must be an integer between -1024 and 1023"],
        !IntegerQ[maxSim] || maxSim < 1, Message[
          LaunchJob::badarg,
          "MaxSimultaneousJobs must be an integer > 0"],
        !StringQ[baseDir] || !DirectoryQ[baseDir], Message[
          LaunchJob::badarg,
          "directory must be an existing directory"],
        !TrueQ[overwrite] && DirectoryQ[FileNameJoin[{baseDir, "jobs", name}]], Message[
          LaunchJob::badarg,
          "overwrite set to false and job directory already exists"]],
      Throw[$Failed]];
    (* we can start the job now, but first we need to create the job and result directories *)
    If[$Failed === CreateDirectory[
      FileNameJoin[{baseDir, "jobs", name}],
      CreateIntermediateDirectories -> True],
      Message[LaunchJob::ioerr, "Could not create job init directory"];
      Throw[$Failed]];
    If[$Failed === CreateDirectory[
      FileNameJoin[{baseDir, "results", name}],
      CreateIntermediateDirectories -> True],
      Message[LaunchJob::ioerr, "Could not create job results directory"];
      Throw[$Failed]];
    (* now, we want to put the init.m file in the job directory *)
    Block[
      {RunWorker = code},
      Check[
        Save[
          FileNameJoin[{baseDir, "jobs", name, "init.m"}],
          RunWorker],
        (Message[LaunchJob::ioerr, "could not export function"];
         Throw[$Failed])]];
    (* finally, we run the qsub command itself *)
    Check[
      With[
        {rscArg = If[resources === Automatic,
           Which[
             n > 100, Message[LaunchJob::badarg, "LaunchJob normally requires n <= 100"],
             True, "nodes="<>ToString[n]],
           resources]},
        With[
          {cmd = Flatten@List[
             "/share/apps/admins/torque/qsub.sh",
             "-N", name,
             "-d", FileNameJoin[{baseDir, "jobs", name}],
             "-t", "1-"<>ToString[n]<>If[maxSim < n, "%" <> ToString[maxSim], ""],
             If[StringQ[rscArg], {"-l", rscArg}, {}],
             If[priority === Automatic, {}, {"-p", ToString[priority]}],
             If[runAt === Automatic, {}, {"-a", runAt}],
             Environment["HOME"]<>"/bin/mathJob.sh"]},
          RunProcess[cmd]]],
      Throw[$Failed]]]];
Protect[LaunchJob];

End[];
EndPackage[];



