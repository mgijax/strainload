#!/usr/local/bin/python

# $Header$
# $Name$

#
# Program: strainload.py
#
# Original Author: Lori Corbani
#
# Purpose:
#
#	To load new Strains into:
#
#	PRB_Strain
#	MLP_Strain
#	MLP_StrainTypes
#	MLP_Extra
#	ACC_Accession
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
#		field 1: Strain Name
#		field 2: Strain Type
#		field 3: Strain Species
#		field 4: Standard (1/0)
#		field 5: Note 1
#		field 6: Created By
#
# Outputs:
#
#       5 BCP files:
#
#       PRB_Strain.bcp                  master Strain records
#	MLP_Strain.bcp			MLP Strain records
#	MLP_StrainTypes.bcp		Strain Types
#	MLP_Extra.bcp			
#       ACC_Accession.bcp               Accession records
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

import sys
import os
import string
import getopt
import db
import mgi_utils
import accessionlib

#globals

DEBUG = 0		# if 0, not in debug mode
TAB = '\t'		# tab
CRT = '\n'		# carriage return/newline

bcpon = 1		# can the bcp files be bcp-ed into the database?  default is yes.

diagFile = ''		# diagnostic file descriptor
errorFile = ''		# error file descriptor
inputFile = ''		# file descriptor
strainFile = ''         # file descriptor
mlpFile = ''		# file descriptor
typeFile = ''     	# file descriptor
extraFile = ''		# file descriptor
accFile = ''            # file descriptor

strainTable = 'PRB_Strain'
mlpTable = 'MLP_Strain'
typeTable = 'MLP_StrainTypes'
extraTable = 'MLP_Extra'
accTable = 'ACC_Accession'

strainFileName = strainTable + '.bcp'
mlpFileName = mlpTable + '.bcp'
typeFileName = typeTable + '.bcp'
extraFileName = extraTable + '.bcp'
accFileName = accTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name
passwordFileName = ''	# password file name

mode = ''		# processing mode (load, preview)
strainKey = 0           # PRB_Strain._Strain_key
accKey = 0              # ACC_Accession._Accession_key
mgiKey = 0              # ACC_AccessionMax.maxNumericPart

needsReview = 0
isPrivate = 0
NULL = ''

mgiTypeKey = 10		# Strains
mgiPrefix = "MGI:"

strainTypesDict = {}      	# dictionary of types for quick lookup
speciesDict = {}      	# dictionary of species for quick lookup

cdate = mgi_utils.date('%m/%d/%Y')	# current date

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
    global strainFile, mlpFile, typeFile, extraFile, accFile
 
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

    try:
        mlpFile = open(mlpFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % mlpFileName)

    try:
        typeFile = open(typeFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % typeFileName)

    try:
        extraFile = open(extraFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % extraFileName)

    try:
        accFile = open(accFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % accFileName)

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


# Purpose:  verify Species
# Returns:  Species Key if Species is valid, else 0
# Assumes:  nothing
# Effects:  verifies that the Species exists either in the Species dictionary or the database
#	writes to the error file if the Species is invalid
#	adds the Species and key to the Species dictionary if the Species is valid
# Throws:  nothing

def verifySpecies(
    species, 	# Species (string)
    lineNum	# line number (integer)
    ):

    global speciesDict

    if len(speciesDict) == 0:
        results = db.sql('select _Species_key, species from MLP_Species', 'auto')

        for r in results:
	    speciesDict[r['species']] = r['_Species_key']

    if speciesDict.has_key(species):
            speciesKey = speciesDict[species]
    else:
            errorFile.write('Invalid Species (%d) %s\n' % (lineNum, species))
            speciesKey = 0

    return speciesKey

# Purpose:  verify Strain Type
# Returns:  Strain Type Key if Strain Type is valid, else 0
# Assumes:  nothing
# Effects:  verifies that the Strain Type exists either in the Strain Type dictionary or the database
#	writes to the error file if the Strain Type is invalid
#	adds the Strain Type and key to the Strain Type dictionary if the Strain Type is valid
# Throws:  nothing

def verifyStrainType(
    strainType, 	# Strain Type (string)
    lineNum		# line number (integer)
    ):

    global strainTypesDict

    if len(strainTypesDict) == 0:
        results = db.sql('select _StrainType_key, strainType from MLP_StrainType', 'auto')

        for r in results:
	    strainTypesDict[r['strainType']] = r['_StrainType_key']

    if strainTypesDict.has_key(strainType):
            strainTypeKey = strainTypesDict[strainType]
    else:
            errorFile.write('Invalid Strain Type (%d) %s\n' % (lineNum, strainType))
            strainTypeKey = 0

    return strainTypeKey

# Purpose:  sets global primary key variables
# Returns:  nothing
# Assumes:  nothing
# Effects:  sets global primary key variables
# Throws:   nothing

def setPrimaryKeys():

    global strainKey, accKey, mgiKey

    results = db.sql('select maxKey = max(_Strain_key) + 1 from PRB_Strain', 'auto')
    strainKey = results[0]['maxKey']

    results = db.sql('select maxKey = max(_Accession_key) + 1 from ACC_Accession', 'auto')
    accKey = results[0]['maxKey']

    results = db.sql('select maxKey = maxNumericPart + 1 from ACC_AccessionMax ' + \
        'where prefixPart = "%s"' % (mgiPrefix), 'auto')
    mgiKey = results[0]['maxKey']

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
    mlpFile.close()
    typeFile.close()
    extraFile.close()
    accFile.close()

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())
    truncateDB = 'dump transaction %s with truncate_only' % (db.get_sqlDatabase())

    bcp1 = '%s%s in %s %s' % (bcpI, strainTable, strainFileName, bcpII)
    bcp2 = '%s%s in %s %s' % (bcpI, mlpTable, mlpFileName, bcpII)
    bcp3 = '%s%s in %s %s' % (bcpI, typeTable, typeFileName, bcpII)
    bcp4 = '%s%s in %s %s' % (bcpI, extraTable, extraFileName, bcpII)
    bcp5 = '%s%s in %s %s' % (bcpI, accTable, accFileName, bcpII)

    for bcpCmd in [bcp1, bcp2, bcp3, bcp4, bcp5]:
	diagFile.write('%s\n' % bcpCmd)
	os.system(bcpCmd)
	db.sql(truncateDB, None)

    return

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global strainKey, accKey, mgiKey

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    name = tokens[0]
	    strainType = tokens[1]
	    species = tokens[2]
	    isStandard = tokens[3]
	    note1 = tokens[4]
	    createdBy = tokens[5]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	strainTypeKey = verifyStrainType(strainType, lineNum)
	speciesKey = verifySpecies(species, lineNum)

	if isStandard == 'y':
		isStandard = '1'
	else:
		isStandard = '0'

        if strainTypeKey == 0 or speciesKey == 0:
            # set error flag to true
            error = 1

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process

        strainFile.write('%d|%s|%s|%s|%s|%s|%s\n' \
            % (strainKey, name, isStandard, needsReview, isPrivate, cdate, cdate))

        mlpFile.write('%d|%s|%s|%s|%s|%s\n' % (strainKey, speciesKey, NULL, NULL, cdate, cdate))

        typeFile.write('%d|%s|%s|%s\n' % (strainKey, strainTypeKey, cdate, cdate))

        extraFile.write('%d|%s|%s|%s|%s|%s|%s\n' % (strainKey, NULL, NULL, note1, NULL, cdate, cdate))

        # MGI Accession ID for the strain

	if isStandard == '1':
          accFile.write('%d|%s%d|%s|%s|1|%d|%d|0|1|%s|%s|%s\n' \
              % (accKey, mgiPrefix, mgiKey, mgiPrefix, mgiKey, strainKey, mgiTypeKey, cdate, cdate, cdate))

          accKey = accKey + 1
          mgiKey = mgiKey + 1

        strainKey = strainKey + 1

    #	end of "for line in inputFile.readlines():"

    #
    # Update the AccessionMax value
    #

    if not DEBUG:
        db.sql('exec ACC_setMax %d' % (lineNum), None)

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
