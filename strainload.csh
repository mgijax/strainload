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

date | tee -a ${STRAINLOG}

rm -rf *.bcp

${STRAINLOAD}/strainload.py | tee -a ${STRAINLOG}

${ALLCACHELOAD}/allstrain.csh | tee -a ${STRAINLOG}
${PG_MGD_DBSCHEMADIR}/test/findmgi.csh | tee -a ${STRAINLOG}

date | tee -a ${STRAINLOG}

