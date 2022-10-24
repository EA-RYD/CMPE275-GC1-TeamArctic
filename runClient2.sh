#!/bin/bash
#

if [ "$#" -ne 1 ]; then
   echo -e "\nUsage: $0 <config file>\n"
   exit
fi

export SVR_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

JAVA_MAIN=' gash.grpc.route.client.Client'
JAVA_ARGS="$1"
echo -e "\n** config ${JAVA_ARGS} **\n"
JAVA_TUNE='-client -Xms96m -Xmx512m'

java ${JAVA_TUNE} -cp .:${SVR_HOME}/lib/'*':${SVR_HOME}/lib-ref2/'*':${SVR_HOME}/classes ${JAVA_MAIN} ${JAVA_ARGS} 
