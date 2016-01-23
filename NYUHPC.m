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

HPCSubmit::usage = "HPCSubmit[name, n, fn] launches a job that runs the given fn with a single argument, the worker ID number, on n different worker nodes on the NYU HPC. The worker ID passed to the given function fn is always between 1 and n, inclusive. The following options may be given:
  * \"RunAt\" (default: Automatic) specifies the time to run the job, which should be given as a string formatted according to the date_time argument of qsub (see man qsub); Automatic causes the job to run immediately.
  * \"Resources\" (default: Automatic) specifies the resource requirement string for the qsub command (see man qsub, -l option).
  * \"Priority\" (default: 0) specifies the job's priority (must be between -1024 and 1023).
  * \"Overwrite\" (default: False) specifies whether a job with the same name should be overwritten or not.
  * \"Directory\" (default: \"~/math\") specifies the directory where we should place the jobs and results directory for this job.";
HPCSubmit::badarg = "Bad argument given to HPCSubmit: `1`";
HCPSubmit::ioerr = "Error during I/O with HPC: `1`";

HPCResult::usage = "HPCResult[conn, name] yields a list of the result of each of the workers for the job submitted to the HPC with the given name.
The HPCConnection object conn may be excluded, in which case $HPCCurrentConnection is used.";
HPCResult::badarg = "Bad argument given to HPCResult: `1`";
HPCResult::werr = "Job worker `1` completed with error: `2`";

HPCConnection::usage = "HPCConnection[...] is a form used to hold information about a currently open connection to the NYU HPC.";
HPCConnection::noconn = "No current default connection in $HPCCurrentConnection.";

HPCConnect::usage = "HPCConnect[username] yields an HPCConnection object for the given username.";
HPCConnect::noconn = "HPCConnect was unable to connect to the remote host: `1`";

HPCStatus::usage = "HPCStatus[conn] yields the current status of the given HPCConnection object conn.";
HPCStatus::badstatus = "Status of given hpc object (`1`) is invalid for the given operation (`2`).";

HPCMetaData::usage = "HPCMetaData[conn] yields an Association of the meta-data associated with the given HPCConnection conn.";
HPCUsername::usage = "HPCUsername[conn] yields the username for the given HPCConnection conn.";
HPCHost::usage = "HPCHost[conn] yields the host name for the given HPCConnection conn.";
HPCEnvironment::usage = "HPCEnvironment[conn] yields the environment Association used to spawn the ssh client for the given HPCConnection conn.";

HPCClose::usage = "HPCClose[conn] closes the given HPCConnection and yields True.";
HPCClose::badstatus = "The status given of the process given to HPCClose (`1`) cannot be closed.";

HPCCommand::usage = "HPCCommand[cmd] yields the text results of executing the given command on the HPC.";

HPCJobList::usage = "HPCJobList[conn] yields a list of the names of all non-archived jobs currently in residence on the HPC.
HPCJobList[conn, type] yields a list of the jobs of the given type; type may be \"Finished\", \"Running\", or \"Archived\". Additionally, type may be Automatic (finished or running jobs only), All, or an Alternatives form such as \"Running\"|\"Archived\".
HPCJobList[] is equivalend to HPCJobList[$HPCCurrentConnection].
HPCJobList[type] is equivalend to HPCJobList[$HPCCurrentConnection, type].";
HPCJobInfo::usage = "HPCJobInfo[conn, name] yields an Association of data relevant to the job named by the given name string.";

HPCJobStatus::usage = "HPCJobStatus[conn, name] yields the status (Queued, Running, Complete, Archived) of each of the job with the given name, using the given HPCConnection conn.
The conn argument may be excluded in which case it is replaced with $HPCCurrentConnection.";

HPCWorkerCount::usage = "HPCWorkerCount[conn, name] yields the number of workers assigned to the job with the given name for the given HPC connection conn. If the job is not found, $Failed is returned.
The argument conn may be excluded in which case it is replaced with $HPCCurrentConnection.";

HPCWorkerStatus::usage = "HPCWorkerStatus[conn, name, id] yields that status (Queued, Running, Success, Error) of the worker identified by the given id for the job with the given name, using the HPCConnection conn.
HPCWorkerStatus[conn, name] yields a list of the worker status for each worker in the job with the given name.
The conn argument may be excluded, in which case it is replaced with $HPCCurrentConnection.";
HPCWorkerStatus::badarg = "Bad argument given to HPCWorkerStatus: `1`";

$HPCCurrentConnection::usage = "$HPCCurrentConnection is a variable that contains the current HPC connection; this is the last connection opened using HPCConnect[]. Certain HPC functions use this value by default (e.g., HPCRun). The variable is not protected and may be set.";

Begin["`Private`"];

$HPCCurrentConnection = None;

(* #HPCConnection *********************************************************************************)
HPCConnection /: ProcessObject[HPCConnection[u_, ___]] := u;
HPCConnection /: ProcessStatus[hpc_HPCConnection] := ProcessStatus[ProcessObject[hpc]];
MakeBoxes[hpc_HPCConnection, form_] := RowBox[
  {"HPCConnection","[",
   HPCUsername[hpc],
   ",",
   With[
     {status = HPCStatus[hpc]},
     StyleBox[
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
       *  (2) source the .Mathematica/nyu-hpc-mathematica/scripts/master_startup.sh script
       *)
      StringJoin[
        "{ ( ",
        Riffle[
          {"[ -d ~/.Mathematica/Applications ]",
           "\\mkdir -p ~/.Mathematica/Applications &> /dev/null",
           "( \\echo fail_mkdir_1 && exit 1 )"},
          " || "],
        " ) && \\cd ~/.Mathematica && ( ",
        Riffle[
          {"[ -d nyu-hpc-mathematica ]",
           "\\git clone https://github.com/WinawerLab/nyu-hpc-mathematica &> /dev/null",
           "( \\echo fail_git_clone && exit 1 )"},
          " || "],
        " ) && ( ",
        Riffle[
          {"[ -r ~/.Mathematica/nyu-hpc-mathematica/scripts/master_startup.sh ]",
           StringJoin @ Riffle[
             {"( \\cd nyu-hpc-mathematica",
              "\\git fetch --all &>/dev/null",
              "\\git reset --hard origin/master &>/dev/null",
              "\\cd .. )"},
             " && "],
           " ( \\echo fail_source_startup && exit 1 )"},
          " || "],
        " ); } && source ~/.Mathematica/nyu-hpc-mathematica/scripts/master_startup.sh",
        "; \\echo ", tag, ";\n"]];
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
          If[str === EndOfFile || (IntegerQ@info["ExitCode"] && info["ExitCode"] == 255),
            (* failure to startup; we may need a password... *)
            Throw[
              Message[HPCConnect::noconn, "you may need to setup passwordless login or a tunnel"];
              $Failed],
            (* correct startup... *)
            With[
              {conn = HPCConnection[
                 proc,
                 "OKAY",
                 <|"Username" -> username, "Host" -> host, "Environment" -> env|>]},
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
      IntegerQ@info["ExitCode"] && info["ExitCode"] != 0, "ERROR",
      stat == "Running", status,
      stat == "Finished", "DONE",
      True, status]]];
Protect[HPCStatus];

(* #HPCUsername ***********************************************************************************)
HPCUsername[hpc_HPCConnection] := HPCMetaData[hpc]["Username"];
Protect[HPCUsername];

(* #HPCHost ***************************************************************************************)
HPCHost[hpc_HPCConnection] := HPCMetaData[hpc]["Host"];
Protect[HPCHost];

(* #HPCEnvironment ********************************************************************************)
HPCEnvironment[hpc_HPCConnection] := HPCMetaData[hpc]["Environment"];
Protect[HPCEnvironment];

(* #HPCMetaData ***********************************************************************************)
HPCMetaData[hpc:HPCConnection[_, _, data_, ___]] := data;
Protect[HPCMetaData];

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
Options[HPCCommand] = {Epilog -> None};
HPCCommand[conn_HPCConnection, cmd_, OptionsPattern[]] := With[
  {stag = Unique["SOF"],
   etag = Unique["EOF"],
   epi = OptionValue[Epilog]},
  WriteLine[
    ProcessObject[conn],
    StringJoin[
      "{ echo " <> ToString[stag] <> " && " <> cmd <> " && echo " <> ToString[etag],
      If[epi === None, "", "\n"<>epi],
      "\n}\n"]];
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
    Union @ Join[
      If[MatchQ["Running", patt],
        StringSplit[HPCCommand[hpc, "\\ls $SCRATCH/.nyu_hpc_math_jobs"], "\n"],
        {}],
      If[MatchQ["Finished", patt],
        StringSplit[HPCCommand[hpc, "\\ls $HOME/.nyu_hpc_math_jobs"], "\n"],
        {}],
      If[MatchQ["Archived", patt],
        StringSplit[HPCCommand[hpc, "\\ls $ARCHIVE/.nyu_hpc_math_jobs"], "\n"],
        {}]]]];
Protect[HPCJobList];

(* #HPCWorkerCount ********************************************************************************)
HPCWorkerCount[name_String] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCWorkerCount[$HPCCurrentConnection, name]];
HPCWorkerCount[conn_HPCConnection, name_String] := If[HPCStatus[conn] != "OKAY",
  Message[HPCStatus::badstatus, HPCStatus[conn], "HPCWorkerCount"],
  Switch[
    HPCJobStatus[conn, name],
    "Nonexistant", $Failed,
    "Archived", ToExpression @ HPCCommand[
      conn,
      With[
        {arch = "\"$ARCHIVE/.nyu_hpc_math_jobs/" <> name <> ".tar.gz\""},
        StringJoin[
          "( [ -r ", arch, " ] && \\tar zxfO ", arch, " \"", name, "/worker_count.txt\" )",
          " || \\echo \"\$Failed\""]]],
    _, ToExpression @ HPCCommand[
      conn,
      StringJoin[
        "( [ -r \"$HOME/.nyu_hpc_math_jobs/", name, "/worker_count.txt\" ]",
        "  && \\cat \"$HOME/.nyu_hpc_math_jobs/", name, "/worker_count.txt\" )",
        " || \\echo \"\$Failed\""]]]];

(* #HPCJobStatus **********************************************************************************)
HPCJobStatus[name_String] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCJobStatus[$HPCCurrentConnection, name]];
HPCJobStatus[conn_HPCConnection, name_String] := If[HPCStatus[conn] != "OKAY",
  Message[HPCStatus::badstatus, HPCStatus[conn], "HPCJobList"],
  StringTrim @ HPCCommand[
    conn,
    With[
      {arch = "\"$ARCHIVE/.nyu_hpc_math_jobs/" <> name <> ".tar.gz\"",
       stat = "\"$HOME/.nyu_hpc_math_jobs/" <> name <> "/status.txt\""},
    StringJoin[
      "( [ -a ", arch, " ] && echo Archived )",
      " || ( [ -r ", stat, " ] && cat ", stat, " )",
      " || echo Nonexistant"]]]];
Protect[HPCJobStatus];

(* #HPCArchivedWorkerStatus ***********************************************************************)
HPCArchivedWorkerStatus[conn_HPCConnection, name_String] := If[HPCStatus[conn] != "OKAY",
  Message[HPCStatus::badstatus, HPCStatus[conn], "HPCJobList"],
  Check[
    With[
      {jobStatus = HPCJobStatus[conn, name],
       n = HPCWorkerCount[conn, name],
       file = "\"$ARCHIVE/.nyu_hpc_math_jobs/"<>name<>".tar.gz\""},
      If[jobStatus != "Archived", 
        $Failed,
        With[
          {arch = "\"$ARCHIVE/.nyu_hpc_math_jobs/" <> name <> ".tar.gz\""},
          (* we need to look through the archived directory *)
          Part[
            SortBy[
              Map[
                Function[
                  {If[StringMatchQ[#, __ ~~ "/results/success-" ~~ DigitCharacter .. ~~ ".m"], 
                     "OKAY",
                     "ERROR"],
                   ToExpression[StringTake[#, {-6, -3}]]}],
                Select[
                  StringSplit[
                    HPCCommand[conn, "\\tar ztf " <> arch],
                    "\n"],
                  Function @ StringMatchQ[
                    #,
                    (name<>"/results/") ~~ {
                       "success-" ~~ (DigitCharacter..) ~~ ".m",
                       "error-" ~~ (DigitCharacter..) ~~ ".txt"}]]],
              Last],
            All, 1]]]],
    $Failed]];
Protect[HPCArchivedWorkerStatus];

(* $HPCWorkerCheckStatus **************************************************************************)
HPCHomeFile[s___] := StringJoin["\"$HOME/.nyu_hpc_math_jobs/", s, "\""];
HPCScratchFile[s___] := StringJoin["\"$SCRATCH/.nyu_hpc_math_jobs/", s, "\""];
HPCWorkerCheckStatus[conn_HPCConnection, name_String, id_Integer] := With[
  {res = StringTrim @ HPCCommand[
     conn,
     StringJoin[
       "( [ -r ", HPCHomeFile[name, "/queued-", ToString[id], ".txt"], " ]",
       "  && \\echo Queued",
       ") || (",
       "  [ -r ", HPCHomeFile[name, "/running-", ToString[id], ".txt"], " ]",
       "  && \\echo Running ",
       ") || (",
       "  [ -r ", HPCHomeFile[name, "/results/success-", IntegerString[id,10,4], ".m"], " ]",
       "  && \\echo OKAY ",
       ") || (",
       "  [ -r ", HPCHomeFile[name, "/results/error-", IntegerString[id,10,4], ".txt"], " ]",
       "  && \\echo ERROR ",
       ") || (",
       "  [ -r ", HPCScratchFile[name, "/success-", IntegerString[id,10,4], ".m"], " ]",
       "  && \\echo OKAY ",
       ") || (",
       "  [ -r ", HPCScratchFile[name, "/error-", IntegerString[id,10,4], ".txt"], " ]",
       "  && \\echo ERROR ",
       ") || \\echo \\$Failed"]]},
  If[res == "$Failed", $Failed, res]];
Protect[HPCHomeFile, HPCScratchFile, HPCWorkerCheckStatus];

(* #HPCWorkerStatus *******************************************************************************)
HPCWorkerStatus[name_String] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCWorkerStatus[$HPCCurrentConnection, name]];
HPCWorkerStatus[name_String, id_Integer] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCWorkerStatus[$HPCCurrentConnection, name, id]];
HPCWorkerStatus[conn_HPCConnection, name_String, id_Integer] := If[HPCStatus[conn] != "OKAY",
  Message[HPCStatus::badstatus, HPCStatus[conn], "HPCJobList"],
  Check[
    With[
      {jobStatus = HPCJobStatus[conn, name],
       n = HPCWorkerCount[conn, name]},
      If[id <= 0 || id > n,
        Message[HPCWorkerStatus::badarg, "id must be between 1 and the number of workers"]];
      Switch[
        jobStatus,
        "Nonexistant", $Failed,
        "Archived", HPCArchivedWorkerStatus[conn, name][[id]],
        _, HPCWorkerCheckStatus[conn, name, id]]],
    $Failed]];
HPCWorkerStatus[conn_HPCConnection, name_String] := If[HPCStatus[conn] != "OKAY",
  Message[HPCStatus::badstatus, HPCStatus[conn], "HPCJobList"],
  Check[
    With[
      {jobStatus = HPCJobStatus[conn, name],
       n = HPCWorkerCount[conn, name]},
      Switch[
        jobStatus,
        "Nonexistant", $Failed,
        "Archived", HPCArchivedWorkerStatus[conn, name],
        _, HPCWorkerCheckStatus[conn, name, #]& /@ Range[n]]],
    $Failed]];
Protect[HPCWorkerStatus];

(* #HPCSubmit *************************************************************************************)
Options[HPCSubmit] = {
  "Cleanup" -> True,
  "RunAt" -> Automatic,
  "Resources" -> Automatic,
  "Priority" -> 0,
  "Overwrite" -> False,
  "Walltime" -> "3:00:00",
  "MaxSimultaneousWorkers" -> 50,
  "Directory" :> FileNameJoin[{Environment["HOME"], "math"}],
  "Dependencies" -> None};
HPCSubmit[name_String, n_, code_, opts___] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCSubmit[$HPCCurrentConnection, name, n, code, opts]];
HPCSubmit[hpc_HPCConnection, name_String, n_, code_, OptionsPattern[]] := Catch[
  With[
    {runAt = OptionValue["RunAt"],
     resources = OptionValue["Resources"],
     priority = OptionValue["Priority"],
     overwrite = OptionValue["Overwrite"],
     maxSim = OptionValue["MaxSimultaneousWorkers"],
     baseDir = OptionValue["Directory"],
     proc = ProcessObject[hpc],
     walltime = OptionValue["Walltime"],
     deps = OptionValue["Dependencies"] /. None -> {},
     makeClean = OptionValue["Cleanup"]},
    Check[
      Which[
        HPCStatus[hpc] != "OKAY", Message[HPCStatus::badstatus, HPCStatus[hpc], "HPCSubmit"],
        !MatchQ[deps, {_String...}], Message[
          HPCStatus::badarg,
          "Dependencies must be a list of strings"],
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
        !TrueQ[overwrite] && DirectoryQ[FileNameJoin[{baseDir, "jobs", name}]], Message[
          HPCSubmit::badarg,
          "overwrite set to false and job directory already exists"]],
      Throw[$Failed]];
    (* Error checking is done; now we create job directories and a status file *)
    If["OKAY" != StringTrim@HPCCommand[hpc, "nyuhpc_setup_job \""<>name<>"\" "<>ToString[n]],
      Throw[
        Message[HPCSubmit::ioerr, "Could not create job directories"];
        $Failed]];
    (* Create the init files... *)
    With[
      {tmpdir = CreateDirectory[]},
      (* save the data... *)
      Block[
        {NYUHPC`Private`RunWorker = code,
         NYUHPC`Private`$Dependencies = deps},
        Save[
          FileNameJoin[{tmpdir, "deps.m"}],
          NYUHPC`Private`$Dependencies];
        Save[
          FileNameJoin[{tmpdir, "init.m"}],
          NYUHPC`Private`RunWorker]];
      (* scp it to the remote host *)
      With[
        {res1 = RunProcess[
           {"scp", "-q", "-C", FileNameJoin[{tmpdir, "init.m"}],
            StringJoin[
              HPCUsername[hpc], "@", HPCHost[hpc], 
              ":/scratch/", HPCUsername[hpc], "/.nyu_hpc_math_jobs/", name, "/init.m"]}],
         res2 = RunProcess[
           {"scp", "-q", "-C", FileNameJoin[{tmpdir, "deps.m"}],
            StringJoin[
              HPCUsername[hpc], "@", HPCHost[hpc], 
              ":/scratch/", HPCUsername[hpc], "/.nyu_hpc_math_jobs/", name, "/deps.m"]}]},
        If[res1["ExitCode"] == 0 && res2["ExitCode"] == 0,
          (DeleteFile[FileNameJoin[{tmpdir, "init.m"}]];
           DeleteDirectory[tmpdir, DeleteContents -> True]),
          Throw[
            Message[HPCSubmit::ioerr, "Could not scp init.m or deps.m file"];
            $Failed]]]];
    (* Create the script file *)
    If[
      "OKAY" != StringTrim@HPCCommand[
        hpc,
        StringJoin[
          "\\cat << EOF_EOF_EOF > \"$SCRATCH/.nyu_hpc_math_jobs/", name, "/run.sh\"",
          " && \\cat $HOME/.Mathematica/nyu-hpc-mathematica/scripts/run_math_worker.sh",
          " >> \"$SCRATCH/.nyu_hpc_math_jobs/", name, "/run.sh\"",
          " && \\echo OKAY"],
        Epilog -> StringJoin[
          "#! /bin/bash\n",
          "#PBS -N ", name, "\n",
          "#PBS -d $SCRATCH/.nyu_hpc_math_jobs/", name, "\n",
          "#PBS -o $SCRATCH/.nyu_hpc_math_jobs/", name, "/worker_log.txt\n",
          "#PBS -j oe\n",
          "#PBS -l walltime=", walltime, "\n",
          "#PBS -t 1-", ToString[n], If[maxSim < n, "%"<>ToString[maxSim], ""], "\n",
          If[StringQ[resources], "#PBS -l " <> resources <> "\n", ""],
          If[priority =!= Automatic, "#PBS -p " <> ToString[priority] <> "\n", ""],
          If[runAt =!= Automatic, "#PBS -a " <> runAt <> "\n", ""],
          "\n",
          "EOF_EOF_EOF\n"]],
      Throw[
        Message[HPCSubmit::ioerr, "Could not create job script"];
        $Failed]];
    (* okay, now, we run the qsub command itself *)
    With[
      {idstr = StringTrim @ HPCCommand[
         hpc,
         "/share/apps/admins/torque/qsub.sh \"$SCRATCH/.nyu_hpc_math_jobs/" <> name <> "/run.sh\""]},
      If[StringLength[idstr] < 3 || StringTake[idstr, {-2,-1}] != "[]",
        $Failed -> idstr,
        (* we need to submit one more job that cleans this job up... first, make a cleanup script *)
        With[
          {cleaner = HPCHomeFile[name, "/cleanup.sh"]},
          With[
            {cleanres = If[makeClean,
               StringTrim @ HPCCommand[
                 hpc,
                 StringJoin[
                   "( \\sed 's/____FINISHED_JOB_ID____/", idstr, "/g'",
                   "    \"$HOME/.Mathematica/nyu-hpc-mathematica/scripts/cleanup.sh\"",
                   "  | \\sed 's/____FINISHED_JOB_NAME____/", name, "/g'",
                   "  > ", cleaner, 
                   "  && \\chmod 755 ", cleaner, " && \\echo OKAY ",
                   ") || \\echo ERROR"]],
               None]},
          If[cleanres =!= None && cleanres != "OKAY",
            Message[HPCSubmit::ioerr, "Could create cleanup job"],
            StringTrim @ HPCCommand[
              hpc,
              StringJoin[
                "( /share/apps/admins/torque/qsub.sh ", cleaner, " &>/dev/null && \\echo OKAY )",
                " || \\echo ERROR"]]]]]]]]];
Protect[HPCSubmit];

(* #HPCResult *************************************************************************************)
HPCResult[name_String] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCResult[$HPCCurrentConnection, name]];
HPCResult[name_String, id_Integer] := If[$HPCCurrentConnection === None,
  Message[HPCConnection::noconn],
  HPCResult[$HPCCurrentConnection, name, id]];
HPCResult[conn_HPCConnection, name_String, id_Integer] := If[HPCStatus[conn] != "OKAY",
  Message[HPCResult::badstatus, HPCStatus[conn], "HPCJobList"],
  Check[
    With[
      {jobStatus = HPCJobStatus[conn, name],
       n = HPCWorkerCount[conn, name]},
      If[id <= 0 || id > n,
        Message[HPCResult::badarg, "id must be between 1 and the number of workers"]];
      Switch[
        jobStatus,
        "Nonexistant", $Failed,
        "Queued", $Failed,
        "Running", $Failed,
        "Complete", With[
          {wstat = HPCWorkerStatus[conn, name, id]},
          Which[
            wstat == "ERROR", With[
              {msg = StringTrim @ HPCCommand[
                 conn,
                 "\\cat "<>HPCHomeFile[name, "/results/error-", IntegerString[id, 10, 4], ".txt"]]},
              Message[HPCResult::werr, id, msg];
              $Failed],
            wstat == "OKAY", With[
              {dir = CreateDirectory[],
               absname = HPCCommand[
                 conn,
                 "\\ls "<>HPCHomeFile[name, "/results/success-", IntegerString[id, 10, 4], ".m"]]},
              Block[
                {NYUHPCWorker`Private`result},
                With[
                  {procRes = RunProcess[
                     {"scp", "-q", "-C", 
                      HPCUsername[conn] <> "@" <> HPCHost[conn] <> ":" <> absname,
                      FileNameJoin[{dir, "import.m"}]}]},
                  If[procRes["ExitCode"] == 0,
                    Get[FileNameJoin[{dir, "import.m"}]],
                    NYUHPCWorker`Private`result = $Failed];
                  DeleteDirectory[dir, DeleteContents -> True];
                  NYUHPCWorker`Private`result]]],
            True, Indeterminate]],
        "Archived", With[
          {wstat = HPCWorkerStatus[conn, name, id]},
          Which[
            wstat == "ERROR", With[
              {msg = StringTrim @ HPCCommand[
                 conn,
                 StringJoin[
                   "\\tar zxfO \"$ARCHIVE/.nyu_hpc_math_jobs/" <> name <>".tar.gz\" \"",
                   name, "/results/error-", IntegerString[id, 10, 4], ".txt\""]]},
              Message[HPCResult::werr, id, msg];
              $Failed],
            wstat == "OKAY", With[
              {absname = StringTrim @ HPCCommand[
                 conn,
                 "\\ls \"$ARCHIVE/.nyu_hpc_math_jobs/" <> name <>".tar.gz\""]},
              Block[
                {NYUHPCWorker`Private`result = $Failed},
                With[
                  {proc = StartProcess[
                     {"ssh", "-q", "-C", HPCUsername[conn] <> "@" <> HPCHost[conn],
                      StringJoin[
                        "{ \\tar zxfO \"", absname, 
                        "\" \"", name, "/results/success-", IntegerString[id, 10, 4], ".m\"; }"]}]},
                  If[proc =!= $Failed,
                    (Get[ProcessConnection[proc, "StandardOutput"]];
                     KillProcess[proc]),
                    NYUHPCWorker`Private`result = $Failed];
                  NYUHPCWorker`Private`result]]],
        st_, Message[HPCResult::badarg, "Job has unknown status: " <> ToString[st]]]]]],
    $Failed]];
HPCResult[conn_HPCConnection, name_String] := If[HPCStatus[conn] != "OKAY",
  Message[HPCResult::badstatus, HPCStatus[conn], "HPCJobList"],
  HPCResult[conn, name, #]& /@ Range[HPCWorkerCount[conn, name]]];
Protect[HPCResult];

End[];
EndPackage[];

