# nyu-hpc-mathematica ##########################################################

This package manages communication with and queuing on the NYU high performance
computing cluster from Mathematica. Note that currently this is not intended as
a stand-alone drop-in solution to the NYU HPC system generically; rather it is
designed to ease most job submission and monitoring.


## Author ######################################################################

Primary Author: Noah C. Benson <[nben@nyu,edu](mailto:nben@nyu.edu)>

For help installing this package for use with Mathematica, please contact Noah.


## Usage #######################################################################

This toolbox consists of the following functions, each of which is also
documented in Mathematica (i.e., one may evaluate ?HPCConnect or ?HPCSubmit in
Mathematica to see its documentation).

  * HPCConnect\[username\] yields a connection object to the HPC
  * HPCStatus\[connection\] yields a status indication for the connection
  * HPCJobList\[connection\] yields a list of the job names; note that this only
    yields jobs that have been submitted using HPCSubmit
  * HPCSubmit\[connection, name, workerCount, function\] submits an array job
    with the given name consisting of workerCount workers, each of which runs
    the given function with a single argument (the worker ID, which is in the
    range 1 to workerCount, inclusive).
  * HPCJobStatus\[connection, name\] yields the status of the given job name.
  * HPCWorkerStatus\[connection, name\] yields the status of the individual
    workers in the job with the given name.
  * HPCResult\[connection, name\] yields a list of the result of each worker,
    assuming that job has completed.

For an example of the usage of the toolbox, please see the Mathematica notebook
usage.nb in the repository's root.

#### Security Note #############################################################

This toolbox is not designed to be secure. While the connection to the server is
performed with the ssh program exclusively, this toolbox happily reads and
executes code generated on the server by your workers. It does not check to make
sure that this code is not malicious, and someone with access to your HPC
account could, for example, easily hijack your desktop from which you submitted
the job. Be careful, and do not share your passwords.

## License #####################################################################

Copyright (c) 2015 Noah C. Benson

This README file is part of the nyu-hpc-mathematica project.

The nyu-hpc-mathematica project is free software: you can redistribute it and/or
modify it under the terms of the MIT Public License. 

You should have received a copy of the MIT Public License along with this
program. If not, see [<http://opensource.org/licenses/MIT>](http://opensource.org/licenses/MIT).



