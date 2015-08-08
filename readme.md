** Introduction:

This module helps running informatica cloud jobs from unix shell or windows shell. I have been using informatica cloud for years and always had 
problems integrating it to other applications for on-demand cloud workflow execution, so I have decided to write this python module for the purpose 
mentioned. 

You can execute this python script as pre-session command or command task from informatica, you can call this from a shell script or power shell, which 
opens a lot of possibilities for informatica cloud.

usage:

** usage: runInfaCloudTask.py [-h] [-v] -c CREDFILE -j JOBFILE -w WAITTIME

Argument parser for informatica cloud run job... runInfaCloudTask

*optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         allows prograss messages to be displayed
  
*required arguments:
  -c CREDFILE, --credFile CREDFILE
                        specifies credentails file name
  -j JOBFILE, --jobFile JOBFILE
                        specifies job information file
  -w WAITTIME, --waitTime WAITTIME
                        how many secs to wait while checking task status
                        recursively

*See creadentials.properties for sample credentials file
See sample_job.properties for sample job that you need to execute

** Please uncomment code that is below the comment "Uncomment this code for using proxy information" for using it with proxy settings