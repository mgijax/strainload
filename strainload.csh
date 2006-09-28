#!/bin/csh -f

#
# Wrapper script to create & load new strains
#
# Usage:  strainload.csh
#

setenv CONFIGFILE $1

source ${CONFIGFILE}

rm -rf ${STRAINLOG}
touch ${STRAINLOG}

date >& ${STRAINLOG}

${STRAINLOAD}/strainload.py >>& ${STRAINLOG}

date >>& ${STRAINLOG}

