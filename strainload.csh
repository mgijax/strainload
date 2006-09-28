#!/bin/csh -f

#
# Wrapper script to create & load new strains
#
# Usage:  strainload.csh
#

setenv CONFIGFILE $1

cd `dirname $0` && source ${CONFIGFILE}

setenv STRAINLOG	$0.log
rm -rf ${STRAINLOG}
touch ${STRAINLOG}

date >& ${STRAINLOG}

${STRAINLOAD}/strainload.py >>& ${STRAINLOG}

date >>& ${STRAINLOG}

