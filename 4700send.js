const datagram = require("dgram");
const fs = require("fs");

const udpSocket = datagram.createSocket("udp4");
let host = null;
let port = null;

let dataToSend = new Map();
let successDataSoFar = [];
let dataInHasStopped = false;
let sequenceNumberToSend = 0;
let notAckMap = new Map();
let windowSize = 2;
let stringToSend = "";
let ssthresh = 20;

const old_rttMultiplier = 0.875;
const new_rttMultiplier = 0.125;
let old_rtt = 0;
let rto = 1267;

function checksumHelper(data) {
    let total = 0;
    const dataArray = data.split("");
    for(const eachChar of dataArray) {
        const charEncoded = eachChar.charCodeAt(0);
        const multipliedBy3 = charEncoded * 3;
        total += multipliedBy3;
    }
    return total;
}

function increaseWindowSizeBy1() {
    if(windowSize < ssthresh) {
        windowSize++;
    }
    else {
        windowSize = windowSize + 1 / windowSize;
    }
}

function prepareDataToSend(stringToSend) {
    const checknumCalculate = checksumHelper(stringToSend);
    const packetToStore = {
        sequenceNumber: dataToSend.size,
        info: stringToSend,
        checksum: checknumCalculate
    };
    stringToSend = "";
    //store it
    dataToSend.set(dataToSend.size, packetToStore);

    
}

function appendDataHelper(chunk) {
    if(stringToSend == "") {
        stringToSend = chunk;
    }
    else {
        stringToSend += chunk;
    }
    while(stringToSend.length > 1375) {
        let actualToSend = stringToSend.slice(0, 1375);
        prepareDataToSend(actualToSend);
        //update stringToSend to leftover
        stringToSend = stringToSend.slice(1375);
    }
}

function sendDataHelper() {
    const dataStored = dataToSend.get(sequenceNumberToSend);
    const checksumCalculate = checksumHelper(dataStored.info);
    const objectToSend = JSON.stringify({
        sequenceNumber: sequenceNumberToSend,
        info: dataStored.info,
        checksum: checksumCalculate
    });
    udpSocket.send(objectToSend, port, host);
    const currentTime = Date.now();
    notAckMap.set(sequenceNumberToSend, 
        {dataStored: dataStored, 
        createTime: currentTime
        });
    sequenceNumberToSend++;
}

function updateRTTHelper(createTime) {
    const totalTime = Date.now() - createTime;
    let currentNew_rtt;
    //for first time there is no measurement so use total time as new rtt
    if(old_rtt === 0) {
        currentNew_rtt = totalTime;
    }
    else {
        currentNew_rtt  = old_rtt * old_rttMultiplier + totalTime * new_rttMultiplier;
    }
    return {currentNew_rtt};
}

function fromReceiverHandler(msg, rinfo) {
    try {
        const dataFromReceiver = JSON.parse(msg);

        //check if checksumi is not int, neededSN is not int, or receiveSN is not int
        if(!Number.isInteger(dataFromReceiver.checksum) || !Number.isInteger(dataFromReceiver.NeededSN) || !Number.isInteger(dataFromReceiver.receiveSN)) {
            throw new Error("some of the fields are not correct types");
        }

        const receivedChecksum = dataFromReceiver.checksum;
        const receivedNeededSN = dataFromReceiver.NeededSN;
        const receivedReceiveSN = dataFromReceiver.receiveSN;

        const {dataStored, createTime} = notAckMap.get(receivedReceiveSN) || {};
        if(!dataStored) {
            throw new Error("no data for this sequence number");
        }

        if(receivedNeededSN > sequenceNumberToSend) {
            throw new Error("needed SN is greater than sequence number to send");
        }

        if(receivedReceiveSN > sequenceNumberToSend) {
            throw new Error("receive SN is greater than sequence number to send");
        }

        //updatge the round trip time estimation
        const {currentNew_rtt} = updateRTTHelper(createTime);
        old_rtt = currentNew_rtt;
        rto = currentNew_rtt  * 2;

        const calculatedChecksum = checksumHelper(dataStored.info);
        if(receivedChecksum !== calculatedChecksum) {
            throw new Error("checksum is not correct");
        }

        notAckMap.delete(receivedReceiveSN);
        successDataSoFar.push(receivedReceiveSN);

        //increase window size by 1
        increaseWindowSizeBy1();

        //check if can remove more from notAckMap
        const keysSmallerThanReceivedSN = Array.from(notAckMap.keys()).filter(
            key => key < receivedNeededSN);

        for(const key of keysSmallerThanReceivedSN) {
            notAckMap.delete(key);
            successDataSoFar.push(key);
            //increase window size by 1
            increaseWindowSizeBy1();
        }

        while(notAckMap.size < windowSize && sequenceNumberToSend < dataToSend.size) {
            sendDataHelper();
        }

        //check if all acks are received
        if(dataInHasStopped && successDataSoFar.length == dataToSend.size) {
            process.stderr.write("All done!\n");
            process.exit(0);
        }

    } catch (error) {
        process.stderr.write(`Error: ${error.message}\n`);
    }

}

function resendNotAckData(sequenceNumber, dataStored, currentTime) {
    const objectToResend = JSON.stringify({
        sequenceNumber: sequenceNumber,
        info: dataStored.info,
        checksum: checksumHelper(dataStored.info)
    });
    udpSocket.send(objectToResend, port, host);
}

function timeoutHelper() {
    ssthresh = Math.max(2, Math.floor(windowSize / 2));
    windowSize = ssthresh;
}

function resendDataHelper() {
    const entriesOfMap = notAckMap.entries();
    let timeoutOrNot = false;

    for(const [sequenceNumber, {dataStored, createTime}] of entriesOfMap) {
        
        const updatedTime = createTime + rto;
        if(updatedTime < Date.now()) {
            //resend the data
            resendNotAckData(sequenceNumber, dataStored, createTime);
            timeoutOrNot = true;

            const currentTime = Date.now();
            notAckMap.set(sequenceNumber, 
                {
                    dataStored: dataStored, 
                    createTime: currentTime
                });
        }
    }
    //reset the ssthresh and window size cuz of timeout
    if(timeoutOrNot) {
        timeoutHelper();
    }
}

function main() {

    const commandLineArgs = process.argv.slice(2);
    host = commandLineArgs[0];
    port = Number(commandLineArgs[1]);

    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
        appendDataHelper(chunk);
        // if possible to send then send
        while(notAckMap.size < windowSize && sequenceNumberToSend < dataToSend.size) {
            sendDataHelper();
        }
    });


    process.stdin.on("end", () => {
        //check if there is data left to send
        if(stringToSend.length > 0) {
            prepareDataToSend(stringToSend);
            stringToSend = "";
        }
        dataInHasStopped = true;
        while(notAckMap.size < windowSize && sequenceNumberToSend < dataToSend.size) {
            sendDataHelper();
        }
        //check if all acks 
        if(successDataSoFar.length == dataToSend.size) {
            process.stderr.write("All done!\n");
            process.exit(0);
        }
    });

    udpSocket.on("message", (msg, rinfo) => {
        fromReceiverHandler(msg, rinfo);
    });

    //set the interval to resend
    setInterval(resendDataHelper,67);
}

main();