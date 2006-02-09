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
#	PRB_Strain_Allele
#
# Requirements Satisfied by This Program:
#
# Usage:
#	program.py
#	-S = database server
#	-D = database
#	-U = user
#	-P = password file
#	-M = mode
#	-I = input file
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
#	- new, for JRS cutover
#

import sys
import os
import string
import getopt
import db
import mgi_utils
import accessionlib
import loadlib

#globals

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
passwordFileName = ''	# password file name

mode = ''		# processing mode (load, preview)
strainalleleKey = 0           # PRB_Strain._Strain_key

strainTypeKey = 10	# ACC_MGIType._MGIType_key for Strains
alleleTypeKey = 11	# ACC_MGIType._MGIType_key for Allele

qualifiersDict = {}    # dictionary of types for quick lookup

loaddate = loadlib.loaddate

# Purpose: displays correct usage of this program
# Returns: nothing
# Assumes: nothing
# Effects: exits with status of 1
# Throws: nothing
 
def showUsage():
    usage = 'usage: %s -S server\n' % sys.argv[0] + \
        '-D database\n' + \
        '-U user\n' + \
        '-P password file\n' + \
        '-M mode\n' + \
	'-I input file\n'

    exit(1, usage)
 
# Purpose: prints error message and exits
# Returns: nothing
# Assumes: nothing
# Effects: exits with exit status
# Throws: nothing

def exit(
    status,          # numeric exit status (integer)
    message = None   # exit message (string)
    ):

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
 
# Purpose: process command line options
# Returns: nothing
# Assumes: nothing
# Effects: initializes global variables
#          calls showUsage() if usage error
#          exits if files cannot be opened
# Throws: nothing

def init():
    global diagFile, errorFile, inputFile, errorFileName, diagFileName, passwordFileName
    global mode
    global strainFile
 
    try:
        optlist, args = getopt.getopt(sys.argv[1:], 'S:D:U:P:M:I:')
    except:
        showUsage()
 
    #
    # Set server, database, user, passwords depending on options specified
    #
 
    server = ''
    database = ''
    user = ''
    password = ''
 
    for opt in optlist:
        if opt[0] == '-S':
            server = opt[1]
        elif opt[0] == '-D':
            database = opt[1]
        elif opt[0] == '-U':
            user = opt[1]
        elif opt[0] == '-P':
            passwordFileName = opt[1]
        elif opt[0] == '-M':
            mode = opt[1]
        elif opt[0] == '-I':
            inputFileName = opt[1]
        else:
            showUsage()

    # User must specify Server, Database, User and Password
    password = string.strip(open(passwordFileName, 'r').readline())
    if server == '' or database == '' or user == '' or password == '' \
	or mode == '' or inputFileName == '':
        showUsage()

    # Initialize db.py DBMS parameters
    db.set_sqlLogin(user, password, server, database)
    db.useOneConnection(1)
 
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
    diagFile.write('Server: %s\n' % (server))
    diagFile.write('Database: %s\n' % (database))
    diagFile.write('User: %s\n' % (user))

    errorFile.write('Start Date/Time: %s\n\n' % (mgi_utils.date()))

    return

# Purpose: verify processing mode
# Returns: nothing
# Assumes: nothing
# Effects: if the processing mode is not valid, exits.
#	   else, sets global variables
# Throws:  nothing

def verifyMode():

    global DEBUG

    if mode == 'preview':
        DEBUG = 1
        bcpon = 0
    elif mode != 'load':
        exit(1, 'Invalid Processing Mode:  %s\n' % (mode))


# Purpose:  verify Qualifier
# Returns:  Qualifier Key if Qualifier is valid, else 0
# Assumes:  nothing
# Effects:  verifies that the Qualifier exists either in the Qualifier dictionary or the database
#	writes to the error file if the Qualifier is invalid
#	adds the Qualifier and key to the Qualifier dictionary if the Qualifier is valid
# Throws:  nothing

def verifyQualifier(
    qualifier, 	# Qualifier (string)
    lineNum	# line number (integer)
    ):

    global qualifiersDict

    if len(qualifiersDict) == 0:
        results = db.sql('select _Term_key, term from VOC_Term where _Vocab_key = 31', 'auto')

        for r in results:
	    qualifiersDict[r['term']] = r['_Qualifier_key']

    if qualifiersDict.has_key(qualifier):
            qualifierKey = qualifiersDict[qualifier]
    else:
            errorFile.write('Invalid Qualifier (%d) %s\n' % (lineNum, qualifier))
            qualifierKey = 0

    return qualifierKey

# Purpose:  sets global primary key variables
# Returns:  nothing
# Assumes:  nothing
# Effects:  sets global primary key variables
# Throws:   nothing

def setPrimaryKeys():

    global strainalleleKey

    results = db.sql('select maxKey = max(_StrainMarker_key) + 1 from PRB_Strain_Marker', 'auto')
    strainalleleKey = results[0]['maxKey']

# Purpose:  BCPs the data into the database
# Returns:  nothing
# Assumes:  nothing
# Effects:  BCPs the data into the database
# Throws:   nothing

def bcpFiles():

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

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global strainalleleKey

    lineNum = 0
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
	    user = tokens[3]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	strainKey = accessionlib.get_Object_key(strainID, _MGIType_key = strainTypeKey)
	alleleKey = accessionlib.get_Object_key(alleleID, _MGIType_key = alleleTypeKey)
	qualifierKey = verifyQualifier(qualifier, lineNum)
	userKey = loadlib.verifyUser(user, lineNum, errorFile)

	markerKey = 0
	if alleleKey > 0:
	    results = db.sql("select _Marker_key from ALL_Allele where _Allele_key = %s' % (alleleKey),  'auto')
	    if len(results) > 0:
		markerKey = results[0]['_Marker_key']

        if strainKey == 0 or alleleKey == 0 or markerKey == 0 or qualifierKey == 0:
            # set error flag to true
            error = 1

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process

        strainFile.write('%d|%d|%d|%d|%d|%s|%s|%s|%s\n' \
            % (strainalleleKey, strainKey, markerKey, alleleKey, qualifierKey, userKey, userKey, loaddate, loaddate))

        strainalleleKey = strainalleleKey + 1

    #	end of "for line in inputFile.readlines():"

#
# Main
#

init()
verifyMode()
setPrimaryKeys()
processFile()
bcpFiles()
exit(0)

# $Log$
