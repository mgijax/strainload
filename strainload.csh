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

${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} PRB_Strain . PRB_Strain.bcp ${COLDELIM} ${LINEDELIM} mgd | tee -a ${STRAINLOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} PRB_Strain_Marker . PRB_Strain_Marker.bcp ${COLDELIM} ${LINEDELIM} mgd | tee -a ${STRAINLOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ACC_Accession . ACC_Accession.bcp ${COLDELIM} ${LINEDELIM} mgd | tee -a ${STRAINLOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} VOC_Annot . VOC_Annot.bcp ${COLDELIM} ${LINEDELIM} mgd | tee -a ${STRAINLOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} MGI_Note . MGI_Note.bcp ${COLDELIM} ${LINEDELIM} mgd | tee -a ${STRAINLOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} MGI_NoteChunk . MGI_NoteChunk.bcp ${COLDELIM} ${LINEDELIM} mgd | tee -a ${STRAINLOG}

date >>& ${STRAINLOG}

