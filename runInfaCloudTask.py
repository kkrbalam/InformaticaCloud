#!/usr/bin/python2

import json          # import json module to work with json response 
import urllib        # import urllib to convert arguments to desired format
import urllib2       # import urllib2 to call REST API
import os            # import for operating system commands
import sys           # import system commands
import time as Time  # import time functions
import argparse      # use argparse to parse arguments
import logging       # standard library for logging
from pprint import pprint # standarad library for pretty print
from jq import jq    # jq for querying json
from operator import itemgetter # for iterating through json list returned for activity log
from datetime import * # For converting timezones as informatica cloud works in PST
import dateutil.parser as dateparser # Date parser for converting the string returned in json to date
import pytz # Converting timezones
"""Module docstring.

Name: runInfaCloudTask.py
Description: This python script takes informatica cloud job takes two file names one containing
             informatica cloud credentials and one file containing job information that needs to be
			 executed. This uses informatica cloud rest API version one. 
Arguments:   file1 = full path to file that contains credentials
             file2 = full path to file that contains job details
             IMPORTANT NOTE: Save these two files in the same directory as script
Created by:  Krishna Balam
Date:        08/04/2015

Change log:

Date              Name                  Purpose
------------------------------------------------------------------------
08/04/2015        Krishna Balam         Initial Creation

"""

################################################################################
def ParseCommandLine():
    """  
    Name: ParseCommandLine() Function                                     
    Desc: Process and Validate the command line arguments                 
    use Python Standard Library module argparse                            
    Input: none                                                           
    Actions:                                                              
    Uses the standard library argparse to process the command line        
    establishes a global variable gl_args where any of the functions can  
    obtain argument information   
    
    """  
    # define an object of type parser 
    parser = argparse.ArgumentParser(description="Argument parser for informatica cloud run job... runInfaCloudTask")
    
    # add argument verbose to the parser to give user ability to see verbose execution messages
    parser.add_argument('-v','--verbose', action='store_true', help='allows prograss messages to be displayed')
    # add creadentials file option 
    parser.add_argument('-c','--credFile', required=True, help='specifies credentails file name')
    # add job file option
    parser.add_argument('-j','--jobFile',  required=True, help='specifies job information file')
    # add wait time option
    parser.add_argument('-w','--waitTime',  required=True, type=int, help='how many secs to wait while checking task status recursively')   
    
    # create global object that can hold all valid arguments and make it avialable to all functions
    global gl_args    
    
    # save the arguments gl_args
    gl_args = parser.parse_args()
    
    DisplayMessage("Command Line processing finished: successfully")
    
    return
################################################################################
def DisplayMessage(msg):
    """
    Name: DisplayMessage() Function 
    Desc: Displays the message if the verbose command line option is present 
    Input: message type string 
    Actions: Uses the standard library print function to display the message 
    
    """
    if gl_args.verbose:
        pprint(msg)
################################################################################
def ReadFileToDict(fileName):
    """
    Name: ReadFileToDict() Function 
    Desc: Reads credentials from the supplied file and returns dictionary 
    Input: file name that must be read 
    Actions: Uses the standard library file open function to read data 
    
    """    
    # define the dictionary
    keyStore = {}
    
    DisplayMessage("Reading keys and values from the file " + fileName)
        
    try:
        with open(fileName, "rb") as fileData:
            for line in fileData:
                key,value = line.strip().split(':')
                keyStore[key] = value
        fileData.close()
        DisplayMessage("Reading keys and values from the file " + fileName + " Successful")
        return keyStore
    except Exception, e:
        logging.error("open / read the file provided failed with error " + str(e))
        DisplayMessage("open / read the file provided failed with error " + str(e))
        raise
################################################################################

def InfaCloudLogin(url, payload):
    """
    Name: InfaCloudLogin() Function 
    Desc: Takes payload and url and calls login method in Informatica Rest API
    Input: base URL to be used for logging in and the credentails payload data
    Actions: takes url and payload and uses urllib and urllib2 for calling APIs
    Output: Session Id for informatica session
    
    """
    DisplayMessage("Starting informatica Cloud Login module")
    
    try:
        # encode the dictionay values in payload to url type arguments using urllib
        payloadEncoded = urllib.urlencode(payload)
        DisplayMessage("encoded arguments are: " + payloadEncoded)
        
        # use urllib2 to form the request
        loginRequest = urllib2.Request(url, payloadEncoded)
        # open a handle for the API using urllib2 urlopen method
        loginHandle = urllib2.urlopen(loginRequest)
        
        # The above handle returns a json message which we use to get session id
        for loginLine in loginHandle:
            loginResponse = json.loads(loginLine)
        
        # close login Handle
        loginHandle.close()
        
        DisplayMessage("informatica Cloud Login module successfull with Session id: " + loginResponse["sessionId"])
        
        # return session ID output        
        return loginResponse["sessionId"]
    
    except Exception, e:
        logging.error("informatica login module failed with error" + str(e))
        DisplayMessage("informatica login module failed with error" + str(e))
        raise    
        
################################################################################
def InfaRunJob(url, payload):
    """
    Name: InfaRunJob() Function 
    Desc: Takes sessionid and url and calls runjob method in Informatica Rest API
    Input: base URL to be used for running job and the job properties payload data
    Actions: takes url, sessioniD and payload and uses urllib and urllib2 for calling APIs
    Output: success or failure for informatica job submission
    
    """
    DisplayMessage("Starting informatica Cloud run job module")
    
    try:
        # encode the dictionay values in payload to url type arguments using urllib
        payloadEncoded = urllib.urlencode(payload)
        DisplayMessage("encoded arguments are: " + payloadEncoded)
        
        # use urllib2 to form the request
        jobRequest = urllib2.Request(url, payloadEncoded)
        
        # Uncomment this code for using proxy information
        #opener = urllib2.build_opener(gl_proxy)
        #urllib2.install_opener(opener) 
        
        # Save job kick off time to a global variable for later use        
        kickoffTime = datetime.now(pytz.timezone('US/Pacific'))
        
        global gl_KickoffTime
        # delete timezone info to make it convenient for time arthmatic
        gl_KickoffTime = kickoffTime.replace(tzinfo=None)
        
        # let it sleep for couple of secs to make it easier for aritmetic
        Time.sleep(2)
        
        # open a handle for the API using urllib2 urlopen method
        jobHandle = urllib2.urlopen(jobRequest)
         
        
        # The above handle returns a json message which we use to get session id
        for jobLine in jobHandle:
            jobResponse = json.loads(jobLine)
        
        # close login Handle
        jobHandle.close()
        
        DisplayMessage("informatica Cloud Login module successfull with Session id: " + str(jobResponse["success"]))
        
        # return session ID output        
        return jobResponse["success"]
    
    except Exception, e:
        logging.error("informatica run job module failed with error" + str(e))
        DisplayMessage("informatica run job module failed with error" + str(e))
        raise    
################################################################################
def InfaJobStatus(url, payload, jobName):
    """
    Name: InfaJobStatus() Function 
    Desc: Takes payload and url and calls activity log method in Informatica Rest API
    Input: base URL to be used for activitylog and the payload data
    Actions: takes url, sessioniD and payload and uses urllib and urllib2 for calling APIs
    Output: success or failure for informatica current informatica job
    
    """
    DisplayMessage("Starting informatica Cloud activity log module")
    
    try:
        # encode the dictionay values in payload to url type arguments using urllib
        payloadEncoded = urllib.urlencode(payload)
        DisplayMessage("encoded arguments are: " + payloadEncoded)
        
        
        # since this is get type url, we need to build url first
        logUrl = url + '?' + payloadEncoded
        
        while True:
            
            # Uncomment this code for using proxy information
            #opener = urllib2.build_opener(gl_proxy)
            #urllib2.install_opener(opener)           
            
            # open a handle for the API using urllib2 urlopen method
             
            logHandle = urllib2.urlopen(logUrl)
            
            # The above handle returns a json message which contains last 20 log results
            # we save the message to logData        
            logData = logHandle.read()
            
            # close login Handle
            logHandle.close()
            
            # Now use JQ to convert json into a list of dictionaries with only columns needed
            logDataJson = (jq('.entries[] | {objectName, runId, startTime, endTime, success}').transform(json.loads(logData), multiple_output=True))
            
            #Filter the list logDataJson for only the jobName that we have selected to verify
            keyValList = [jobName]
            logDataForJob = [d for d in logDataJson if d['objectName'] in keyValList]
            
            # Sort the filtered list by startTime to get latest run        
            logDataForJobTop = sorted(logDataForJob, key=itemgetter('startTime'), reverse=True)
            
            # Select the top row from sorted list
            jobResult = logDataForJobTop[0]
            
            #select start time of the job result and convert it into time compare it to the job kick off time and see if it is greater or lesser 
            jobStartTime = dateparser.parse(jobResult["startTime"], ignoretz=True)
            
            days, secs = (jobStartTime - gl_KickoffTime).days, (jobStartTime-gl_KickoffTime).seconds
            
            DisplayMessage('Difference between start time of last successful run and noted kickoff time is ' + str(days) +' days and ' + str(secs) + ' seconds')
            logging.info('Difference between start time of last successful run and noted kickoff time is ' + str(days) +' days and ' + str(secs) + ' seconds')
            
            if (days >= 0 and secs > 0):
                if (jobResult["success"] == 3):
                    DisplayMessage(jobName + ' is successful')
                    logging.info(jobName + ' is successful')
                    returnValue = 0
                    break
                else:
                    DisplayMessage(jobName + ' is failed')
                    logging.info(jobName + ' is failed')
                    logging.error(jobName + ' is failed')
                    returnValue = -1
                    break
            else:
                DisplayMessage(jobName + ' is still waiting for completion')
                Time.sleep(gl_args.waitTime)
                continue
 
        return returnValue
    except Exception, e:
        logging.error("informatica job status module failed with error" + str(e))
        DisplayMessage("informatica job status module failed with error" + str(e))
        raise    
################################################################################


def main():
    
    # define run job version constant. change when version changes
    RUN_INFA_CLOUD_JOB_VERSION = '1.0'

    # turn on logging
    logging.basicConfig(filename="run_infa_cloud_job.log", level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(message)s', filemode="w")
    
    # Record Starting Time
    startTime = Time.time()
    
    # Record welcome message
    logging.info('')
    logging.info('Welcome to run job version ' + RUN_INFA_CLOUD_JOB_VERSION + ' ...')
    logging.info('New job run started...')
    logging.info('')
    
    # Record some information regarding system
    logging.info('System: ' + sys.platform)
    logging.info('Version: ' + sys.version)
    
    
    #Parse arguments using the function defined
    ParseCommandLine()
    
    logging.info('Define global variables for URLs')
    # define global variables for urls that will be used for API calls
    global gl_login_url 
    global gl_runjob_url
    global gl_status_url
    # uncomment this statement for using proxy information
    #global gl_proxy
    
    gl_login_url = "https://app.informaticaondemand.com/saas/api/1/login"
    gl_runjob_url = "https://app.informaticaondemand.com/saas/api/1/runjob"
    gl_status_url = "https://app.informaticaondemand.com/saas/api/1/activitylog"
    
    # uncomment this block for using proxy information
    # gl_proxy = {'http': '127.0.0.1'}
    # add any other attributes needed for proxy like port
    
    # Read credentails from credentials file from arguments parsed
    logging.info('Get Credentails')
    logging.info('Credentials file is ' + gl_args.credFile)
    credentials = ReadFileToDict(gl_args.credFile)
    
    logging.info('Get informatica login session id')
    sessionId = InfaCloudLogin(gl_login_url,credentials)
    
    logging.info('informatica login session id is ' + sessionId)  
    
    # Read job properties from job file from arguments parsed
    logging.info('Get Job properties')
    logging.info('Job Properties file is ' + gl_args.jobFile)
    jobProps = ReadFileToDict(gl_args.jobFile)    
    
    # add session id to payload
    jobProps["icSessionId"] = sessionId
    
    logging.info("running job")
    logging.info(jobProps)
    
    # run the job wait get the submit status
    submitStatus = InfaRunJob(gl_runjob_url, jobProps)
    
    logging.info("Submitting of the job is successful? " + str(submitStatus))
    
    if  submitStatus:
        # Now build url and the parameters for checking activity log for completion of this job
        
        DisplayMessage("job submission was successful, now checking activity log for job completion...")
        logging.info("job submission was successful, now checking activity log for job completion...")
        activityKeyStore = {}
        activityKeyStore["icSessionId"] = sessionId
        activityKeyStore["rowLimit"] = 20
        activityKeyStore["responseType"] = "json"
        retValue = InfaJobStatus(gl_status_url, activityKeyStore, jobProps["jobName"] )
    else:
        DisplayMessage("job submission failed")
        logging.error("job submission failed")
        retValue = -1
    
    # Record end Time
    endTime = Time.time()    
    duration = endTime - startTime
    
    DisplayMessage("Elapsed time is " + str(duration) + " Seconds")
    
    if  retValue == 0:
        DisplayMessage("Execution was successful")
        logging.info("Execution was successful")
        sys.exit(0)
    else:
        DisplayMessage("Execution failed")
        logging.info("Execution failed")
        sys.exit(-1)
    
################################################################################       
        
if __name__ == "__main__":
    main()
