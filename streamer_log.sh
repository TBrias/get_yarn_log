#!/bin/bash

echo Récupération des logs du dernier job Spark du user $1 sur l environnement $2

# Init Kerberos
echo kinit -kt /home/$1/$1.keytab $1
kinit -kt /home/$1/$1.keytab $1

echo $(yarn application -appStates FINISHED,FAILED,KILLED -list | grep my_job_name-"$2" | sort | tail -1 | grep -E -o application_[0-9,_]* | tail -1)
APP_ID=$(yarn application -appStates FINISHED,FAILED,KILLED -list | grep my_job_name-"$2" | sort | tail -1 | grep -E -o application_[0-9,_]* | tail -1)

# Suppression du fichier temporaire s'il existe encore
rm -f /tmp/TMP_OUTPUT

echo '####################################################################################################################' >> /tmp/TMP_OUTPUT
echo Script joué à $(date +"%Y/%m/%d %r") >> /tmp/TMP_OUTPUT

APP_STATUS=$(yarn application -status "$APP_ID")

VAR_TO_KEEP="Application-Id|Start-Time|Finish-Time|Progress|State|Diagnostics "
APP_STATUS_CLEAN=$(echo "$APP_STATUS" | grep -E "$VAR_TO_KEEP")
APP_STATUS_CLEAN="1. Logs de status: \n $APP_STATUS_CLEAN"

echo -e "$APP_STATUS_CLEAN" >> /tmp/TMP_OUTPUT

ERROR_LOG=$(echo "$APP_STATUS" | grep -P "(ERROR|^\tat |Exception|^Caused by: |\t... \d+ more)"  )

if [ -n "$ERROR_LOG" ]
then
        ERROR_LOG="\nPrésence d erreur dans le status: \n$ERROR_LOG"
        echo -e "$ERROR_LOG" >> /tmp/TMP_OUTPUT
fi

# Calcul de la durée du traitement
START_TIME=$(echo "$APP_STATUS" | grep Start-Time | grep -o '[^, ]\+$')
FINISH_TIME=$(echo "$APP_STATUS" | grep Finish-Time | grep -o '[^, ]\+$')

# Fonction secondes -> heure:minute:seconde format
hms()
{
  local SECONDS H M S MM H_TAG M_TAG S_TAG
  SECONDS=${1:-0}
  let S=${SECONDS}%60
  let MM=${SECONDS}/60 # Total number of minutes
  let M=${MM}%60
  let H=${MM}/60

  # Display "01h02m03s" format
  [ "$H" -gt "0" ] && printf "%02d%s" $H "h"
  [ "$M" -gt "0" ] && printf "%02d%s" $M "m"
  printf "%02d%s\n" $S "s"
}

DURATION_MILLIS=$(( (${FINISH_TIME} - ${START_TIME}) / 1000))
DURATION=`hms $DURATION_MILLIS`
DURATION="Durée du traitement:  ${DURATION} \n"

echo -e $DURATION >> /tmp/TMP_OUTPUT

# Début traitement des logs Yarn
echo -e "\n2. Logs fonctionnelles: \n \tLogs Yarn" >> /tmp/TMP_OUTPUT

# Récupération des logs fonctionnelles
YARN_LOG=$(yarn logs --applicationId $APP_ID | grep -oP "my_job_name: \K.*")
echo "$YARN_LOG" >> /tmp/TMP_OUTPUT

# Récupération des éventuelles erreurs
YARN_ERROR_LOG=$(yarn logs --applicationId $APP_ID | grep -P "(ERROR|^\tat |Exception|^Caused by: |\t... \d+ more)"  )

if [ -n "$YARN_ERROR_LOG" ]
then
        ERROR_LOG="\tPrésence d erreur dans la log Yarn: \n\t$YARN_ERROR_LOG"
        echo -e "$ERROR_LOG" >> /tmp/TMP_OUTPUT
fi

# Ecriture des logs
cat /tmp/TMP_OUTPUT >> /tmp/spark_job.log
rm /tmp/TMP_OUTPUT

echo FIN du traitement de récupération des logs
