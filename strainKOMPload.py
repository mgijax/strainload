#!/usr/local/bin/python

#
# Program: strainKOKMPload.py
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
#	VOC_Annot
#	MGI_Note/MGI_NoteChunk
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
#		field 1:  Strain id
#		field 2:  Strain Name
#		field 3:  MGI Allele ID
#		field 4:  Strain Type (ex. 'coisogenic', 'congenic', 'conplastic')
#		field 5:  Strain Species (ex. 'laboratory mouse')
#		field 6:  Standard (1/0)
#		field 7:  Strain of Origin Note
#		field 8:  Mutant Cell Line of Origin Note
#		field 9:  External Logical DB key
#		field 10  External MGI Type key
#		field 11: Strain Attributes (xxxxx|xxxxx) (ex. 'chromosome aberration', 'closed colony')
#		field 12: Allele Symbol Contains Criteria (xxxx)
#		field 13: Created By
#
#
# ES Cell line (field 8) and Allele Symbol Contains Criteria (field 12)
# will return one Allele results.  Use this Allele ID for field 3.
#
# For example:
# ES Cell Line: 10871A-A7
# query: "tm1(KOMP)"
# query: "tm1.1(KOMP)"
#
# Outputs:
#
#       4 BCP files:
#
#       PRB_Strain.bcp                  master Strain records
#       PRB_Strain_Marker.bcp                  master Strain records
#       ACC_Accession.bcp               Accession records
#       VOC_Annot.bcp
#       MGI_Note/MGI_NoteChunk          strain of origin notes
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
# History
#
# lec	03/26/2012
#	- TR11015/Gensat
#

import sys
import os
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
annotFile = ''          # file descriptor
noteFile = ''           # file descriptor
noteChunkFile = ''      # file descriptor

strainTable = 'PRB_Strain'
markerTable = 'PRB_Strain_Marker'
accTable = 'ACC_Accession'
annotTable = 'VOC_Annot'
noteTable = 'MGI_Note'
noteChunkTable = 'MGI_NoteChunk'

strainFileName = strainTable + '.bcp'
markerFileName = markerTable + '.bcp'
accFileName = accTable + '.bcp'
annotFileName = annotTable + '.bcp'
noteFileName = noteTable + '.bcp'
noteChunkFileName = noteChunkTable + '.bcp'

diagFileName = ''	# diagnostic file name
errorFileName = ''	# error file name

strainKey = 0           # PRB_Strain._Strain_key
strainmarkerKey = 0	# PRB_Strain_Marker._StrainMarker_key
accKey = 0              # ACC_Accession._Accession_key
mgiKey = 0              # ACC_AccessionMax.maxNumericPart
annotKey = 0
noteKey = 0             # MGI_Note._Note_key

isPrivate = 0
isGeneticBackground = 0
NULL = ''

mgiTypeKey = 10		# ACC_MGIType._MGIType_key for Strains
alleleTypeKey = 11	# ACC_MGIType._MGIType_key for Allele
markerTypeKey = 2       # ACC_MGIType._MGIType_key for Marker
mgiNoteObjectKey = 10   # MGI_Note._MGIType_key
mgiStrainOriginTypeKey = 1011   # MGI_Note._NoteType_key
mgiMutantOriginTypeKey = 1038   # MGI_Note._NoteType_key

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
    global strainFile, markerFile, accFile, annotFile
    global noteFile, noteChunkFile
 
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)
 
    head, tail = os.path.split(inputFileName) 
    diagFileName = tail + '.diagnostics'
    errorFileName = tail + '.error'

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

    try:
        noteFile = open(noteFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % noteFileName)

    try:
        noteChunkFile = open(noteChunkFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % noteChunkFileName)

    try:
        annotFile = open(annotFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % annotFileName)

    # Log all SQL
    db.set_sqlLogFunction(db.sqlLogAll)

    # Set Log File Descriptor
    #db.set_sqlLogFD(diagFile)

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
        results = db.sql('select _Term_key, term from VOC_Term where _Vocab_key = 55', 'auto')

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

    results = db.sql('select _Strain_key, strain from PRB_Strain where strain = \'%s\'' % (strain), 'auto')

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

    global strainKey, strainmarkerKey, accKey, mgiKey, annotKey, noteKey

    results = db.sql('select max(_Strain_key) + 1 as maxKey from PRB_Strain', 'auto')
    strainKey = results[0]['maxKey']

    results = db.sql('select max(_StrainMarker_key) + 1 as maxKey from PRB_Strain_Marker', 'auto')
    strainmarkerKey = results[0]['maxKey']

    results = db.sql('select max(_Accession_key) + 1 as maxKey from ACC_Accession', 'auto')
    accKey = results[0]['maxKey']

    results = db.sql('select maxNumericPart + 1 as maxKey from ACC_AccessionMax where prefixPart = \'MGI:\'', 'auto')
    mgiKey = results[0]['maxKey']

    results = db.sql('select max(_Annot_key) + 1 as maxKey from VOC_Annot', 'auto')
    annotKey = results[0]['maxKey']

    results = db.sql('select max(_Note_key) + 1 as maxKey from MGI_Note', 'auto')
    noteKey = results[0]['maxKey']

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing

def processFile():

    global strainKey, strainmarkerKey, accKey, mgiKey, annotKey, noteKey

    lineNum = 0
    # For each line in the input file

    for line in inputFile.readlines():

        error = 0
        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = line[:-1].split('\t')

        try:
	    id = tokens[0]
	    name = tokens[1]
	    alleleID = tokens[2]
	    strainType = tokens[3]
	    species = tokens[4]
	    isStandard = tokens[5]
	    sooNote = tokens[6]
	    mutantNote = tokens[7]
	    externalLDB = tokens[8]
            externalTypeKey = tokens[9]
	    annotations = tokens[10].split('|')
	    queryCriteria = tokens[11]
	    createdBy = tokens[12]
        except:
            exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

	strainExistKey = verifyStrain(name, lineNum)
	strainTypeKey = verifyStrainType(strainType, lineNum)
	speciesKey = verifySpecies(species, lineNum)
	createdByKey = loadlib.verifyUser(createdBy, 0, errorFile)

	# if Allele found, resolve to Marker

	if len(alleleID) > 0:
	    alleleKey = loadlib.verifyObject(alleleID, alleleTypeKey, None, lineNum, errorFile)
	else:
	    alleleKey = 0
	markerKey = 0

	if alleleKey == 0:
	    queryCriteria = '%' + queryCriteria + '>'
	    querySQL = '''
		select a._Allele_key, a._Marker_key
		from ALL_Allele_CellLine aa, ALL_CellLine c, ALL_Allele a
		where aa._MutantCellLine_key = c._CellLine_key
		and c.cellLine = '%s'
		and aa._Allele_key = a._Allele_key
		and a.symbol like '%s'
		''' % (mutantNote, queryCriteria)
	    print querySQL
	    results = db.sql(querySQL, 'auto')
	    if len(results) == 1:
		alleleKey = results[0]['_Allele_key']
		markerKey = results[0]['_Marker_key']
	    else:
		error = 1
        else:
	    results = db.sql('select _Marker_key from ALL_Allele where _Allele_key = %s' % (alleleKey),  'auto')
	    if len(results) > 0:
		markerKey = results[0]['_Marker_key']
  
        if strainExistKey > 0 or markerKey == 0 or strainTypeKey == 0 or speciesKey == 0 or createdByKey == 0:
            # set error flag to true
            error = 1

        # if errors, continue to next record
        if error:
	    errorFile.write(querySQL)
            continue

        # if no errors, process

        strainFile.write('%d|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
            % (strainKey, speciesKey, strainTypeKey, name, isStandard, isPrivate, isGeneticBackground,
	       createdByKey, createdByKey, cdate, cdate))

	markerFile.write('%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
	    % (strainmarkerKey, strainKey, markerKey, alleleKey, qualifierKey, 
	       createdByKey, createdByKey, cdate, cdate))

        # MGI Accession ID for the strain

        accFile.write('%d|MGI:%d|MGI:|%s|1|%d|%d|0|1|%s|%s|%s|%s\n' \
          % (accKey, mgiKey, mgiKey, strainKey, mgiTypeKey, 
	     createdByKey, createdByKey, cdate, cdate))
        accKey = accKey + 1

        # external accession id
	# for ids that contain prefix:numeric
          #% (accKey, id, externalPrefix, externalNumeric, externalLDB, strainKey, externalTypeKey, 

        accFile.write('%d|%s|%s|%s|%s|%s|%s|0|1|%s|%s|%s|%s\n' \
          % (accKey, id, '', id, externalLDB, strainKey, externalTypeKey, 
	     createdByKey, createdByKey, cdate, cdate))
        accKey = accKey + 1

        # storing data in MGI_Note/MGI_NoteChunk
        # Strain of Origin Note

	# this stuff will convert the carriage returns coorectly
        #noteTokens = mutantNote.split('\\n')
        #newNotes = ''
        #for n in noteTokens:
        #    newNotes = newNotes + n + chr(10)
	#mutantNote = newNotes

        if len(sooNote) > 0:

            noteFile.write('%s|%s|%s|%s|%s|%s|%s|%s\n' \
                % (noteKey, strainKey, mgiNoteObjectKey, mgiStrainOriginTypeKey, \
                   createdByKey, createdByKey, cdate, cdate))

            noteChunkFile.write('%s|%s|%s|%s|%s|%s|%s\n' \
                % (noteKey, 1, sooNote, createdByKey, createdByKey, cdate, cdate))

            noteKey = noteKey + 1

        if len(mutantNote) > 0:

            noteFile.write('%s|%s|%s|%s|%s|%s|%s|%s\n' \
                % (noteKey, strainKey, mgiNoteObjectKey, mgiMutantOriginTypeKey, \
                   createdByKey, createdByKey, cdate, cdate))

            noteChunkFile.write('%s|%s|%s|%s|%s|%s|%s\n' \
                % (noteKey, 1, mutantNote, createdByKey, createdByKey, cdate, cdate))

            noteKey = noteKey + 1

	#
        # Annotations
        #
	# _AnnotType_key = 1009
	# _Qualifier_ke = 1614158
	#

	for a in annotations:

	    # strain annotation type
	    annotTypeKey = 1009

	    # this is a null qualifier key
	    annotQualifierKey = 1614158

	    annotTermKey = loadlib.verifyTerm('', 27, a, lineNum, errorFile)
	    if annotTermKey == 0:
		continue

            annotFile.write('%s|%s|%s|%s|%s|%s|%s\n' \
              % (annotKey, annotTypeKey, strainKey, annotTermKey, annotQualifierKey, cdate, cdate))
            annotKey = annotKey + 1

        mgiKey = mgiKey + 1
        strainKey = strainKey + 1
	strainmarkerKey = strainmarkerKey + 1

    #	end of "for line in inputFile.readlines():"

    #
    # Update the AccessionMax value
    #

    if not DEBUG:
        db.sql('select * from ACC_setMax (%d);' % (lineNum), None)
    	db.commit()

#
# Main
#

init()
verifyMode()
setPrimaryKeys()
processFile()
exit(0)

