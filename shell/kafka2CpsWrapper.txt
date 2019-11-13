#!bin/bash
## kafka2CpsWrapper.sh

export logFilePath=/path/to/log
export logTs=`date +%H%M%S`
export logFile=${logFilePath}/log_"${logTs}".log

exec 2>&1 ${logFile}


echo "## *********************************************************************LOG Starts Here********************************************************************* ##"

## Check no. of Arguments
if [[ $# -ne 4 ]]
    then
       echo -e "\n### ERROR : Illegal number of parameters. Please provide valid number of parameters. ###"
       exit 1
fi


export brokers=$1
export groupId=$2
export topic=$3
export cpsTopic=$4
export jsonFile=/path/to/jsonFile
export jarPath=/path/to/jar

echo -e "\n#### Input Parameters ####"
echo -e "\nKafka Broker List : ${brokers}"
echo -e "\nKafka Consumer GroupId : ${groupId}"
echo -e "\nKafka Topic : ${topic}"
echo -e "\nPubSUB Topic : ${cpsTopic}"
echo -e "\n#########################\n"

echo -e "\n### INFO : Authenticating Keytab Account ###"
kinit -kt -- set keytab account
            if [[ $! -eq 0 ]]
                       then
                       echo "### INFO : Keytab Authentication Successful. ###"
            else
                       echo "### ERROR : Keytab Authentication Failed. ### "
                       
            fi


echo -e "\n### INFO : Authenticating Google Credentials ###"
export GOOGLE_APPLICATION_CREDENTIALS=${jsonFile}
            if [[ $! -eq 0 ]]
                       then
                       echo "### INFO : Google Credentials Authentication Successful. ###"
            else
                       echo "### ERROR : Google Credentials Authentication Failed. ### "
                       
            fi

			
echo -e "\n### INFO : Execution Started. Ctrl+C or Kill Process to Stop Execution###"
	
java -jar jarPath ${brokers} ${groupId} ${topic} ${cpsTopic}

            if [[ $! -eq 0 ]]
                       then
                       echo "### INFO : Execution Successful. ###"
            else
                       echo "### ERROR : Execution Failed. ###"
                       
            fi
			
echo "## ************************************************************************************************************************************************************** ##"