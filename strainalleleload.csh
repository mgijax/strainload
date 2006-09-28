#!/bin/csh -f

#
# Wrapper script to create & load new strain/marker associations
#
# Usage:  strainalleleload.csh
#

setenv CONFIGFILE $1

source ${CONFIGFILE}

rm -rf ${STRAINLOG}
touch ${STRAINLOG}

date >& ${STRAINLOG}

${STRAINLOAD}/strainalleleload.py >>& ${STRAINLOG}

date >>& ${STRAINLOG}

