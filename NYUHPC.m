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

HPCStatus::usage = "HPCStatus[name, n, fn] launches a job that runs the given fn with a single argument, the worker ID number, on n different worker nodes on the NYU HPC. The worker ID passed to the given function fn is always between 1 and n, inclusive. The following options may be given:
  * \"RunAt\" (default: Automatic) specifies the time to run the job, which should be given as a string formatted according to the date_time argument of qsub (see man qsub); Automatic causes the job to run immediately.
  * \"Resources\" (default: Automatic) specifies the resource requirement string for the qsub command (see man qsub, -l option).
  * \"Priority\" (default: 0) specifies the job's priority (must be between -1024 and 1023).
  * \"Overwrite\" (default: False) specifies whether a job with the same name should be overwritten or not.
  * \"Directory\" (default: \"~/math\") specifies the directory where we should place the jobs and results directory for this job.";
HPCStatus::badarg = "Bad argument given to HPCStatus: `1`";

HPCConnection::usage = "HPCConnection[...] is a form used to hold information about a currently open connection to the NYU HPC.";
HPCConnection::noconn = "No current default connection in $HPCCurrentConnection.";

HPCConnect::usage = "HPCConnect[username] yields an HPCConnection object for the given username.";
HPCConnect::noconn = "HPCConnect was unable to connect to the remote host: `1`";

HPCStatus::usage = "HPCStatus[conn] yields the current status of the given HPCConnection object conn.";
HPCStatus::badstatus = "Status of given hpc object (`1`) is invalid for the given operation (`2`).";

HPCUsername::usage = "HPCUsername[conn] yields the username for the given HPCConnection conn.";

HPCClose::usage = "HPCClose[conn] closes the given HPCConnection and yields True.";
HPCClose::badstatus = "The status given of the process given to HPCClose (`1`) cannot be closed.";

HPCCommand::usage = "HPCCommand[cmd] yields the text results of executing the given command on the HPC.";

HPCJobList::usage = "HPCJobList[conn] yields a list of the names of all non-archived jobs currently in residence on the HPC.
HPCJobList[conn, type] yields a list of the jobs of the given type; type may be \"Finished\", \"Running\", or \"Archived\". Additionally, type may be Automatic (finished or running jobs only), All, or an Alternatives form such as \"Running\"|\"Archived\".
HPCJobList[] is equivalend to HPCJobList[$HPCCurrentConnection].
HPCJobList[type] is equivalend to HPCJobList[$HPCCurrentConnection, type].";
HPCJobInfo::usage = "HPCJobInfo[conn, name] yields an Association of data relevant to the job named by the given name string.";

$HPCCurrentConnection::usage = "$HPCCurrentConnection is a variable that contains the current HPC connection; this is the last connection opened using HPCConnect[]. Certain HPC functions use this value by default (e.g., HPCRun). The variable is not protected and may be set.";

Begin["`Private`"];

$HPCCurrentConnection = None;

(* #HPCConnection *********************************************************************************)
HPCConnection /: ProcessObject[HPCConnection[u_, ___]] := u;
HPCConnection /: ProcessStatus[hpc_HPCConnection] := ProcessStatus[ProcessObject[hpc]];
MakeBoxes[hpc_HPCConnection, form_] := RowBox[
  {"HPCConnection","[",
   With[
     {status = HPCStatus[hpc]},
   StyleBox[
     HPCUsername[hpc],
     ",",
     "-"<>status<>"-",
     FontColor -> Which[
       status == "OKAY", Darker[Green, 1/4], 
       status == "DONE", Blue,
       status == "BUSY", Darker[Yellow, 1/4],
       True, Red]]],
   "]"}];
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
      (* The goal of this small bit of bash code is to do the following:
       *  (1) ensure that .Mathematica/nyu-hpc-mathematica exists and contains the git code
       *  (2) ensure the nyu-hpc-mathematica/NYUHPCWorker.m has been linked to the Applications dir
       *  (3) ensure the .nyu_hpc_math_jobs directory has been created in scratch
       *  (4) ensure the .nyu_hpc_math_jobs directory has been created in archive
       *  (5) ensure the .nyu_hpc_math_jobs directory has been created in home
       *)
      StringJoin[
        "{ ",
        Riffle[
          {Riffle[
             {"[ -d ~/.Mathematica/Applications ]",
              "\\mkdir -p ~/.Mathematica/Applications &> /dev/null",
              "( \\echo fail_mkdir_1 && exit 1 )"},
             " || "],
           "\\cd ~/.Mathematica",
           Riffle[
             {"[ -d nyu-hpc-mathematica ]",
              "\\git clone https://github.com/WinawerLab/nyu-hpc-mathematica &> /dev/null",
              "( \\echo fail_git_clone && exit 1 )"},
             " || "],
           Riffle[
             {{"( ", Riffle[{"\\cd nyu-hpc-mathematica",
                             "\\git fetch --all &>/dev/null",
                             "\\git reset --hard origin/master &>/dev/null",
                             "\\cd .."},
                            " && "], " )"},
              "( \\echo fail_git_pull && exit 1 )"},
             " || "],
           Riffle[
             {"[ -x nyu-hpc-mathematica/scripts/run_math_worker.sh ]",
              "\\chmod 755 nyu-hpc-mathematica/scripts/run_math_worker.sh &> /dev/null",
              "( \\echo fail_chmod && exit 1 )"},
             " || "],
           Riffle[
             {"[ -a ~/.Mathematica/Applications/NYUHPCWorker.m ]",
              "\\ln -s ~/.Mathematica/nyu-hpc-mathematica/NYUHPCWorker.m"
               <> " ~/.Mathematica/Applications/NYUHPCWorker.m &> /dev/null",
              "( \\echo fail_ln && exit 1 )"},
             " || "],
           "\\cd ~",
           Riffle[
             {"[ -d ~/.nyu_hpc_math_jobs ]",
              "\\mkdir -p ~/.nyu_hpc_math_jobs &> /dev/null",
              "( \\echo fail_mkdir_2 && exit 1 )"},
             " || "],
           Riffle[
             {"[ -d $SCRATCH/.nyu_hpc_math_jobs ]",
              "\\mkdir -p $SCRATCH/.nyu_hpc_math_jobs &> /dev/null",
              "( \\echo fail_mkdir_3 && exit 1 )"},
             " || "],
           Riffle[
             {"[ -d $ARCHIVE/.nyu_hpc_math_jobs ]",
              "\\mkdir -p $ARCHIVE/.nyu_hpc_math_jobs &> /dev/null",
              "( \\echo fail_mkdir_4 && exit 1 )"},
             " || "],
           "\\echo SUCCESS;"},
          "; "],
        " }; ",
        "\\echo ", tag, ";\n"]];
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
            "\\echo "<>ToString[tag]<>" && \\exec /bin/bash"},
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
              {conn = HPCConnection[proc, "OKAY", username]},
              If[HPCPrep[conn] != "SUCCESS",
                KillProcess[proc];
                Message[HPCConnect::noconn, "could not initialize/verify remote math setup"];
                Throw[$Failed]];
              $HPCCurrentConnection = conn]]]]]],
  $Failed];
Protect[HPCConnect];

(* #HPCStatus *************************************************************************************)
HPCStatus[hpc:HPCConnection[_, status_, ___]] := With[
  {proc = ProcessObject[hpc]},
  With[
    {info = ProcessInformation[proc],
     stat = ProcessStatus[proc]},
    Which[
      info["ExitCode"] != 0, "ERROR",
      stat == "Running", status,
      stat == "Finished", "DONE",
      True, status]]];
Protect[HPCStatus];

(* #HPCUsername ***********************************************************************************)
HPCUsername[hpc:HPCConnection[_, _, uid_, ___]] := uid;
Protect[HPCUsername];

(* #HPCClose **************************************************************************************)
HPCClose[hpc_HPCConnection] := With[
  {status = HPCStatus[hpc],
   proc = ProcessObject[hpc]},
  Which[
    status == "DONE", True,
    status == "OKAY", (
      WriteLine[proc, "exit"];
      ReadString[proc, TimeConstraint -> 10];
      KillProcess[proc];
      If[proc === $HPCCurrentConnection, $HPCCurrentConnection = None];
      True),
    status == "ERROR", Message[HPCClose::badstatus, "cannot close connections in ERROR status"],
    True, Message[HPCClose::badstatus, status]]];
HPCClose[] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCClose[$HPCCurrentConnection]];
Protect[HPCClose];


(* #HPCReadOutput *)
HPCReadOutput[conn_HPCConnection, stag_, etag_] := With[
  {proc = ProcessObject[conn],
   sof = ToString[stag] <> "\n",
   eof = ToString[etag] <> "\n"},
  ReadString[proc, sof];
  ReadString[proc, eof]];
HPCReadOutput[stag_, etag_] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCReadOutput[$HPCCurrentConnection, stag, etag]];
Protect[HPCReadOutput];

(* #HPCCommand *)
HPCCommand[conn_HPCConnection, args__] := With[
  {cmd = StringJoin[Riffle[ToString /@ {args}, " "]],
   stag = Unique["SOF"],
   etag = Unique["EOF"]},
  WriteLine[
    ProcessObject[conn],
    "{ echo " <> ToString[stag] <> " && " <> cmd <> " && echo " <> ToString[etag] <> "; }"];
  HPCReadOutput[conn, stag, etag]];
HPCCommand[arg:Except[_HPCConnection], args___] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCCommand[$HPCCurrentConnection, arg, args]];
Protect[HPCCommand];

(* #HPCJobList ************************************************************************************)
HPCJobList[] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCJobList[$HPCCurrentConnection, Automatic]];
HPCJobList[type:Except[_HPCConnection]] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCJobList[$HPCCurrentConnection, type]];
HPCJobList[hpc_HPCConnection] := HPCJobList[hpc, Automatic];
HPCJobList[hpc_HPCConnection, type_] := If[HPCStatus[hpc] != "OKAY",
  Message[HPCStatus::badstatus, HPCStatus[hpc], "HPCJobList"],
  With[
    {patt = Which[
       type === Automatic, "Finished"|"Running", 
       type === All, "Finished"|"Running"|"Archived",
       True, type]},
    Join[
      If[MatchQ["Running", patt],
        StringSplit[HPCCommand["\\ls $SCRATCH/.nyu_hpc_math_jobs"], "\n"],
        {}],
      If[MatchQ["Finished", patt],
        StringSplit[HPCCommand["\\ls $HOME/.nyu_hpc_math_jobs"], "\n"],
        {}],
      If[MatchQ["Archived", patt],
        StringSplit[HPCCommand["\\ls $ARCHIVE/.nyu_hpc_math_jobs"], "\n"],
        {}]]]];
Protect[HPCJobList];

(* #HPCJobStatus **********************************************************************************)
HPCJobStatus[name_String] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCJobStatus[$HPCCurrentConnection, name]];
HPCJobStatus[conn_HPCConnection, name_String] := If[HPCStatus[hpc] != "OKAY",
  Message[HPCStatus::badstatus, HPCStatus[hpc], "HPCJobList"],
  Indeterminate];
Protect[HPCJobStatus];
  

(* #HPCSubmit *************************************************************************************)
Options[HPCSubmit] = {
  "RunAt" -> Automatic,
  "Resources" -> Automatic,
  "Priority" -> 0,
  "Overwrite" -> False,
  "Walltime" -> "3:00:00",
  "MaxSimultaneousWorkers" -> 50,
  "Directory" :> FileNameJoin[{Environment["HOME"], "math"}]};
HPCSubmit[name_String, n_, code_, opts___] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCSubmit[$HPCCurrentConnection, name, n, code, opts]];
HPCSubmit[hpc_HPCCommection, name_String, n_, code_, OptionsPattern[]] := Catch[
  With[
    {runAt = OptionValue["RunAt"],
     resources = OptionValue["Resources"],
     priority = OptionValue["Priority"],
     overwrite = OptionValue["Overwrite"],
     maxSim = OptionValue["MaxSimultaneousWorkers"],
     baseDir = OptionValue["Directory"],
     proc = ProcessObject[hpc],
     walltime = OptionValue["Walltime"]},
    Check[
      Which[
        HPCStatus[hpc] != "OKAY", Message[HPCStatus::badstatus, HPCStatus[hpc], "HPCSubmit"],
        !overwrite && MemberQ[HPCJobList[hpc, All], name], Message[
          HPCSubmit::badarg,
          "\"Overwrite\" is False, but a job named \""<>name<>"\" already exists"],
        !IntegerQ[n] || n < 1, Message[
          HPCSubmit::badarg,
          "number of workers must be an integer > 0"],
        !StringQ[runAt] && runAt =!= Automatic, Message[
          HPCSubmit::badarg,
          "run-at option must be a string"],
        !StringQ[resources] && resources =!= Automatic,  Message[
          HPCSubmit::badarg,
          "resources must be a string"],
        !IntegerQ[priority] || !(-1024 <= priority <= 1023), Message[
          HPCSubmit::badarg,
          "priority must be an integer between -1024 and 1023"],
        !IntegerQ[maxSim] || maxSim < 1, Message[
          HPCSubmit::badarg,
          "MaxSimultaneousJobs must be an integer > 0"],
        !StringQ[baseDir] || !DirectoryQ[baseDir], Message[
          HPCSubmit::badarg,
          "directory must be an existing directory"],
        !TrueQ[overwrite] && DirectoryQ[FileNameJoin[{baseDir, "jobs", name}]], Message[
          HPCSubmit::badarg,
          "overwrite set to false and job directory already exists"]],
      Throw[$Failed]];
    (* Error checking is done; now we create job directories and a status file *)
    If[
      "OKAY" != HPCCommand[
        hpc,
        StringJoin[
          "\\mkdir -p \"$HOME/.nyu_hpc_math_jobs/", name, "\" &>/dev/null",
          " && \\mkdir \"$SCRATCH/.nyu_hpc_math_jobs/", name, "\" &>/dev/null",
          " && \\echo \"Queued\" > \"$HOME/.nyu_hpc_math_jobs/", name, "/status.txt\"",
          " && \\echo OKAY"]],
      Throw[
        Message[HPCSubmit::ioerr, "Could not create job directories"];
        $Failed]];
    (* Create the init file... *)
    If[
      "OKAY" != HPCCommand[
        hpc,
        StringJoin[
          "\\cat << EOF_EOF_EOF > \"$SCRATCH/.nyu_hpc_math_jobs/", name, "/init.m\" && echo OKAY\n",
          Block[
            {NYUHPC`Private`RunWorker = code},
            Check[
              ToString[FullDefinition[code]],
              Throw[
                Message[HPCSubmit::ioerr, "Could not get FullDefinition of function"],
                $Failed]]],
          "EOF_EOF_EOF\n"]],
      Throw[
        Message[HPCSubmit::ioerr, "Could not create job directories"];
        $Failed]];
    (* Create the script file *)
    If[
      "OKAY" != HPCCommand[
        hpc,
        StringJoin[
          {"\\cat << EOF_EOF_EOF > \"$SCRATCH/.nyu_hpc_math_jobs/", name, "/run.sh\"",
           " && \\cat $HOME/.Mathematica/nyu-hpc-mathematica/scripts/run_math_worker.sh",
           " >> \"$SCRATCH/.nyu_hpc_math_jobs/", name, "/run.sh\""
           " && \\echo OKAY\n"},
          "#! /bin/bash\n",
          "#PBS -N ", name, "\n",
          "#PBS -d $SCRATCH/.nyu_hpc_math_jobs/", name, "\n",
          "#PBS -o $SCRATCH/.nyu_hpc_math_jobs/", name, "/log-${PBS_ARRAYID}.txt\n",
          "#PBS -j oe\n",
          "#PBS -l walltime=", walltime, "\n",
          "#PBS -t 1-", ToString[n], If[maxSim > n, "%"<>ToString[maxSim], ""], "\n",
          If[StringQ[rscArg], "#PBS -l " <> rscArg <> "\n", ""],
          If[priority =!= Automatic, "#PBS -p " <> ToString[priority] <> "\n", ""],
          If[runAt =!= Automatic, "#PBS -a " <> runAt <> "\n", ""],
          "EOF_EOF_EOF\n"]],
      Throw[
        Message[HPCSubmit::ioerr, "Could not create job script"];
        $Failed]];
    (* finally, we run the qsub command itself *)
    (*HPCCommand[
      hpc,
      "/share/apps/admins/torque/qsub.sh \"$SCRATCH/.nyu_hpc_math_jobs/" <> name <> "/run.sh\""]*)
    True]];
Protect[HPCSubmit];

End[];
EndPackage[];



