#!/bin/csh -f

#
# Wrapper script to create & load new strains
#
# Usage:  strainKOMPload.csh
#

setenv CONFIGFILE $1

source ${CONFIGFILE}

rm -rf ${STRAINLOG}
touch ${STRAINLOG}

date >& ${STRAINLOG}

${STRAINLOAD}/strainKOMPload.py >>& ${STRAINLOG}

date >>& ${STRAINLOG}

