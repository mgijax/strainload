#!/usr/local/bin/python

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
#	PRB_Strain_Marker
#	ACC_Accession
#
# Requirements Satisfied by This Program:
#
# Usage:
#	strainload.py
#
# Envvars:
#
# Inputs:
#
#	A tab-delimited file in the format:
#		field 1: Strain id
#		field 2: Strain Name
#		field 3: MGI Allele ID
#		field 4: Strain Type
#		field 5: Strain Species
#		field 6: Standard (1/0)
#		field 7: Note 1
#		field 8: External Logical DB key
#		field 9: External MGI Type key
#		field 10: Created By
#
# Outputs:
#
#       3 BCP files:
#
#       PRB_Strain.bcp                  master Strain records
#       PRB_Strain_Marker.bcp                  master Strain records
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
markerFile = ''         # file descriptor
accFile = ''            # file descriptor

strainTable = 'PRB_Strain'
markerTable = 'PRB_Strain_Marker'
accTable = 'ACC_Accession'

strainFileName = strainTable + '.bcp'
markerFileName = markerTable + '.bcp'
accFileName = accTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

strainKey = 0           # PRB_Strain._Strain_key
strainmarkerKey = 0	# PRB_Strain_Marker._StrainMarker_key
accKey = 0              # ACC_Accession._Accession_key
mgiKey = 0              # ACC_AccessionMax.maxNumericPart

isPrivate = 0
NULL = ''

mgiTypeKey = 10		# ACC_MGIType._MGIType_key for Strains
mgiPrefix = "MGI:"
alleleTypeKey = 11	# ACC_MGIType._MGIType_key for Allele
markerTypeKey = 2       # ACC_MGIType._MGIType_key for Marker

qualifierKey = 615427	# nomenclature

strainDict = {}      	# dictionary of types for quick lookup
strainTypesDict = {}    # dictionary of types for quick lookup
speciesDict = {}      	# dictionary of species for quick lookup

cdate = mgi_utils.date('%m/%d/%Y')	# current date
 
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
    global diagFile, errorFile, inputFile, errorFileName, diagFileName
    global strainFile, markerFile, accFile
 
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

    try:
        markerFile = open(markerFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % markerFileName)

    try:
        accFile = open(accFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % accFileName)

    # Log all SQL
    db.set_sqlLogFunction(db.sqlLogAll)

    # Set Log File Descriptor
    db.set_sqlLogFD(diagFile)

    diagFile.write('Start Date/Time: %s\n' % (mgi_utils.date()))
    diagFile.write('Server: %s\n' % (db.get_sqlServer()))
    diagFile.write('Database: %s\n' % (db.get_sqlDatabase()))

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
        results = db.sql('select _Term_key, term from VOC_Term where _Vocab_key = 26', 'auto')

        for r in results:
	    speciesDict[r['term']] = r['_Term_key']

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
        results = db.sql('select _Term_key, term from VOC_Term where _Vocab_key = 27', 'auto')

        for r in results:
	    strainTypesDict[r['term']] = r['_Term_key']

    if strainTypesDict.has_key(strainType):
            strainTypeKey = strainTypesDict[strainType]
    else:
            errorFile.write('Invalid Strain Type (%d) %s\n' % (lineNum, strainType))
            strainTypeKey = 0

    return strainTypeKey

# Purpose:  verify Strain
# Returns:  Strain Key if Strain is valid, else 0
# Assumes:  nothing
# Effects:  verifies that the Strain exists either in the Strain dictionary or the database
#	writes to the error file if the Strain is invalid
#	adds the Strain and key to the Strain dictionary if the Strain Type is valid
# Throws:  nothing

def verifyStrain(
    strain, 	# Strain (string)
    lineNum	# line number (integer)
    ):

    global strainDict

    results = db.sql('select _Strain_key, strain from PRB_Strain where strain = "%s"' % (strain), 'auto')

    for r in results:
        strainDict[r['strain']] = r['_Strain_key']

    if strainDict.has_key(strain):
            strainExistKey = strainDict[strain]
            errorFile.write('Strain Already Exists (%d) %s\n' % (lineNum, strain))
    else:
            #errorFile.write('Invalid Strain (%d) %s\n' % (lineNum, strain))
            strainExistKey = 0

    return strainExistKey

# Purpose:  sets global primary key variables
# Returns:  nothing
# Assumes:  nothing
# Effects:  sets global primary key variables
# Throws:   nothing

def setPrimaryKeys():

    global strainKey, strainmarkerKey, accKey, mgiKey

    results = db.sql('select maxKey = max(_Strain_key) + 1 from PRB_Strain', 'auto')
    strainKey = results[0]['maxKey']

    results = db.sql('select maxKey = max(_StrainMarker_key) + 1 from PRB_Strain_Marker', 'auto')
    strainmarkerKey = results[0]['maxKey']

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
    markerFile.close()
    accFile.close()

    bcpI = 'cat %s | bcp %s..' % (passwordFileName, db.get_sqlDatabase())
    bcpII = '-c -t\"|" -S%s -U%s' % (db.get_sqlServer(), db.get_sqlUser())
    truncateDB = 'dump transaction %s with truncate_only' % (db.get_sqlDatabase())

    bcp1 = '%s%s in %s %s' % (bcpI, strainTable, strainFileName, bcpII)
    bcp2 = '%s%s in %s %s' % (bcpI, markerTable, markerFileName, bcpII)
    bcp3 = '%s%s in %s %s' % (bcpI, accTable, accFileName, bcpII)

    for bcpCmd in [bcp1, bcp2, bcp3]:
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

    global strainKey, strainmarkerKey, accKey, mgiKey

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = string.split(line[:-1], '\t')

        try:
	    id = tokens[0]
	    (externalPrefix, externalNumeric) = string.split(id, ':')
	    name = tokens[1]
	    alleleID = tokens[2]
	    strainType = tokens[3]
	    species = tokens[4]
	    isStandard = tokens[5]
	    note1 = tokens[6]
	    externalLDB = tokens[7]
            externalTypeKey = tokens[8]
	    createdBy = tokens[9]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	strainExistKey = verifyStrain(name, lineNum)
	alleleKey = loadlib.verifyObject(alleleID, alleleTypeKey, None, lineNum, errorFile)
	strainTypeKey = verifyStrainType(strainType, lineNum)
	speciesKey = verifySpecies(species, lineNum)
	createdByKey = loadlib.verifyUser(createdBy, 0, errorFile)

	if isStandard == 'y':
		isStandard = '1'
	else:
		isStandard = '0'

	# if Allele found, resolve to Marker

	if alleleKey > 0:
	    results = db.sql('select _Marker_key from ALL_Allele where _Allele_key = %s' % (alleleKey),  'auto')
	    if len(results) > 0:
		markerKey = results[0]['_Marker_key']

        if strainExistKey > 0 or markerKey == 0 or strainTypeKey == 0 or speciesKey == 0 or createdByKey == 0:
            # set error flag to true
            error = 1

        # if errors, continue to next record
        if error:
            continue

        # if no errors, process

        strainFile.write('%d|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
            % (strainKey, speciesKey, strainTypeKey, name, isStandard, isPrivate, 
	       createdByKey, createdByKey, cdate, cdate))

	markerFile.write('%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
	    % (strainmarkerKey, strainKey, markerKey, alleleKey, qualifierKey, 
	       createdByKey, createdByKey, cdate, cdate))

        # MGI Accession ID for the strain

        accFile.write('%d|%s%d|%s|%s|1|%d|%d|0|1|%s|%s|%s|%s\n' \
          % (accKey, mgiPrefix, mgiKey, mgiPrefix, mgiKey, strainKey, mgiTypeKey, 
	     createdByKey, createdByKey, cdate, cdate))
        accKey = accKey + 1

        # external accession id

        accFile.write('%d|%s|%s|%s|%s|%s|%s|0|1|%s|%s|%s|%s\n' \
          % (accKey, id, externalPrefix, externalNumeric, externalLDB, strainKey, externalTypeKey, 
	     createdByKey, createdByKey, cdate, cdate))
        accKey = accKey + 1

        mgiKey = mgiKey + 1

        strainKey = strainKey + 1
	strainmarkerKey = strainmarkerKey + 1

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

