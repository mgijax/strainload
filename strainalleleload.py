#!/usr/local/bin/python

#
# Program: strainalleleload.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	To load new data into:
#
#	PRB_Strain_Marker
#
# Requirements Satisfied by This Program:
#
# Usage:
#	strainalleleload.py
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1: Strain ID (JRS, MGI, ...)
#		field 2: MGI Allele ID
#		field 3: Qualifier
#		field 4: Created By
#
# Outputs:
#
#       1 BCP files:
#
#       PRB_Strain_Marker.bcp
#
#       Diagnostics file of all input parameters and SQL commands
#       Error file
#
# Exit Codes:
#
# Assumes:
#
#	That no one else is adding records to the database.
#
# Bugs:
#
# Implementation:
#
# History
#
# 02/09/2006	lec
#	- new, for JRS cutover; uses JRS format (for now)
#

import sys
import os
import string
import db
import mgi_utils
import loadlib

#globals

user = os.environ['MGD_DBUSER']
passwordFileName = os.environ['MGD_DBPASSWORDFILE']
mode = os.environ['STRAINMODE']
inputFileName = os.environ['STRAININPUTFILE']

DEBUG = 0		# if 0, not in debug mode
TAB = '\t'		# tab
CRT = '\n'		# carriage return/newline

bcpon = 1		# can the bcp files be bcp-ed into the database?  default is yes.

diagFile = ''		# diagnostic file descriptor
errorFile = ''		# error file descriptor
inputFile = ''		# file descriptor
strainFile = ''         # file descriptor

strainTable = 'PRB_Strain_Marker'

strainFileName = strainTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

strainalleleKey = 0           # PRB_Strain._Strain_key

strainTypeKey = 10	# ACC_MGIType._MGIType_key for Strains
alleleTypeKey = 11	# ACC_MGIType._MGIType_key for Allele
markerTypeKey = 2       # ACC_MGIType._MGIType_key for Marker

qualifiersDict = {}    # dictionary of types for quick lookup

loaddate = loadlib.loaddate

def exit(status, message = None):
        # requires: status, the numeric exit status (integer)
        #           message (string)
        #
        # effects:
        # Print message to stderr and exits
        #
        # returns:
        #

    if message is not None:
        sys.stderr.write('\n' + str(message) + '\n')
 
    try:
        diagFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
        errorFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
        diagFile.close()
        errorFile.close()
	inputFile.close()
    except:
        pass

    db.useOneConnection(0)
    sys.exit(status)
 
def init():
        # requires: 
        #
        # effects: 
        # 1. Processes command line options
        # 2. Initializes local DBMS parameters
        # 3. Initializes global file descriptors/file names
        # 4. Initializes global keys
        #
        # returns:
        #

    global diagFile, errorFile, inputFile, errorFileName, diagFileName
    global strainFile
 
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)
 
    fdate = mgi_utils.date('%m%d%Y')	# current date
    head, tail = os.path.split(inputFileName) 
    diagFileName = tail + '.' + fdate + '.diagnostics'
    errorFileName = tail + '.' + fdate + '.error'

    try:
        diagFile = open(diagFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % diagFileName)
		
    try:
        errorFile = open(errorFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % errorFileName)
		
    try:
        inputFile = open(inputFileName, 'r')
    except:
        exit(1, 'Could not open file %s\n' % inputFileName)

    try:
        strainFile = open(strainFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % strainFileName)

    # Log all SQL
    db.set_sqlLogFunction(db.sqlLogAll)

    # Set Log File Descriptor
    db.set_sqlLogFD(diagFile)

    diagFile.write('Start Date/Time: %s\n' % (mgi_utils.date()))
    diagFile.write('Server: %s\n' % (db.get_sqlServer()))
    diagFile.write('Database: %s\n' % (db.get_sqlDatabase()))

    errorFile.write('Start Date/Time: %s\n\n' % (mgi_utils.date()))

    return

def verifyMode():
        # requires:
        #
        # effects:
        #       Verifies the processing mode is valid.  If it is not valid,
        #       the program is aborted.
        #       Sets globals based on processing mode.
        #       Deletes data based on processing mode.
        #
        # returns:
        #       nothing
        #

    global DEBUG

    if mode == 'preview':
        DEBUG = 1
        bcpon = 0
    elif mode != 'load':
        exit(1, 'Invalid Processing Mode:  %s\n' % (mode))

def verifyQualifier(qualifier, lineNum):
        # requires:
        #       qualifier - the Qualifier
        #       lineNum - the line number of the record from the input file
        #
        # effects:
        #       verifies that:
        #               the Qualifier exists
        #       writes to the error file if the Qualifier is invalid
        #
        # returns:
        #       0 if the Qualifier is invalid
        #       Qualifier Key if the Qualifier valid
        #

        qualifierKey = 0

        if qualifiersDict.has_key(qualifier):
                qualifierKey = qualifiersDict[qualifier]
        else:
                errorFile.write('Invalid Qualifier (%d) %s\n' % (lineNum, qualifier))
                qualifierKey = 0

        return(qualifierKey)

def loadDictionaries():
        # requires:
        #
        # effects:
        #       loads global dictionaries for quicker lookup
        #
        # returns:
        #       nothing

        global qualifiersDict

        results = db.sql('select _Term_key, term from VOC_Term where _Vocab_key = 31', 'auto')
        for r in results:
		qualifiersDict[r['term']] = r['_Term_key']

def setPrimaryKeys():
        # requires:
        #
        # effects:
        #       Sets the global primary keys values needed for the load
        #
        # returns:
        #       nothing
        #

    global strainalleleKey

    results = db.sql('select maxKey = max(_StrainMarker_key) + 1 from PRB_Strain_Marker', 'auto')
    strainalleleKey = results[0]['maxKey']

def bcpFiles():
        # requires:
        #
        # effects:
        #       BCPs the data into the database
        #
        # returns:
        #       nothing
        #

    bcpdelim = "|"

    if DEBUG or not bcpon:
        return

    strainFile.close()

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())

    bcp1 = '%s%s in %s %s' % (bcpI, strainTable, strainFileName, bcpII)

    for bcpCmd in [bcp1]:
	diagFile.write('%s\n' % bcpCmd)
	os.system(bcpCmd)

    return

def processFile():
        # requires:
        #
        # effects:
        #       Reads input file
        #       Verifies and Processes each line in the input file
        #
        # returns:
        #       nothing
        #

    global strainalleleKey

    lineNum = 0
    notDeleted = 1

    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    strainID = tokens[0]
	    alleleID = tokens[1]
	    qualifier = tokens[2]
	    createdBy = tokens[3]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	if len(strainID) == 4:
	    strainID = '00' + strainID
	if len(strainID) == 3:
	    strainID = '000' + strainID
	if len(strainID) == 2:
	    strainID = '0000' + strainID
	if len(strainID) == 1:
	    strainID = '00000' + strainID

	strainKey = loadlib.verifyObject(strainID, strainTypeKey, None, lineNum, errorFile)

	# this could generate an error because the ID is a marker, not an allele
	# just ignore the error in the error file if it gets resolved later
	alleleKey = loadlib.verifyObject(alleleID, alleleTypeKey, None, lineNum, errorFile)
	markerKey = 0

	if alleleKey == 0:
	    markerKey = loadlib.verifyObject(alleleID, markerTypeKey, None, lineNum, errorFile)

	qualifierKey = verifyQualifier(qualifier, lineNum)
	createdByKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

	if notDeleted:
	    db.sql('delete PRB_Strain_Marker where _CreatedBy_key = %s' % (createdByKey), None)
	    notDeleted = 0

	# if Allele found, resolve to Marker

	if alleleKey > 0:
	    results = db.sql('select _Marker_key from ALL_Allele where _Allele_key = %s' % (alleleKey),  'auto')
	    if len(results) > 0:
		markerKey = results[0]['_Marker_key']

        elif markerKey == 0:
	    errorFile.write('Invalid Allele (%s): %s\n' % (lineNum, alleleID))
	    error = 1

        if strainKey == 0 or markerKey == 0 or qualifierKey == 0:
            # set error flag to true
            error = 1

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process

	if alleleKey == 0:
	    alleleKey = ''

        strainFile.write('%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
            % (strainalleleKey, strainKey, markerKey, alleleKey, qualifierKey, createdByKey, createdByKey, loaddate, loaddate))

        strainalleleKey = strainalleleKey + 1

    #	end of "for line in inputFile.readlines():"

#
# Main
#

init()
verifyMode()
setPrimaryKeys()
loadDictionaries()
processFile()
bcpFiles()
exit(0)

