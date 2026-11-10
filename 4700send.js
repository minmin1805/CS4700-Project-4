//these are the libs that i use
const datagram = require("dgram");


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

//this is a helper function to calculate the checksum
function checksumHelper(data) {
    let total = 0;
    const dataArray = data.split("");
    for(const eachChar of dataArray) {
        const charEncoded = eachChar.charCodeAt(0);
        // i times each char encoded by 3 and add to total
        const multipliedBy3 = charEncoded * 3;
        total += multipliedBy3;
    }
    return total;
}

//this is a helper function to increase the window size by 1
function increaseWindowSizeBy1() {
    //if window size < ssthresh then increase size by 1
    if(windowSize < ssthresh) {
        windowSize++;
    }
    //if not then increase by 1 / window size
    else {
        windowSize = windowSize + 1 / windowSize;
    }
}

// helper function to put data into an object to prepare to send
function prepareDataToSend(stringToSend) {
    const checknumCalculate = checksumHelper(stringToSend);
    const packetToStore = {
        sequenceNumber: dataToSend.size,
        info: stringToSend,
        checksum: checknumCalculate
    };
    stringToSend = "";
    //store it in the map
    dataToSend.set(dataToSend.size, packetToStore);
    
}

//helper function to make 1 string of size 1400 so that it can be sent
function appendDataHelper(chunk) {
    if(stringToSend == "") {
        stringToSend = chunk;
    }
    else {
        stringToSend += chunk;
    }
    //if more than 1400 then partition it and make leftover the new stringToSend
    while(stringToSend.length > 1400) {
        let actualToSend = stringToSend.slice(0, 1400);
        prepareDataToSend(actualToSend);
        //update stringToSend to leftover
        stringToSend = stringToSend.slice(1400);
    }
}

//helper function to send data to the receiver
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
    //store the data in the notAckMap to mark them not ack yet
    notAckMap.set(sequenceNumberToSend, 
        {dataStored: dataStored, 
        createTime: currentTime
        });
    sequenceNumberToSend++;
}

//this is helper function to update the 
function roundtripUpdater(createTime) {
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

//this is helper function to handle data from receiver
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

        //updatge the round trip time with the new rtt
        const {currentNew_rtt} = roundtripUpdater(createTime);
        old_rtt = currentNew_rtt;
        rto = currentNew_rtt  * 2;

        //check if the checksum is correct
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
        
        //remove the data from notAckMap + increase window size by 1
        for(const key of keysSmallerThanReceivedSN) {
            notAckMap.delete(key);
            successDataSoFar.push(key);
            //increase window size by 1
            increaseWindowSizeBy1();
        }

        //send more data if possible
        while(notAckMap.size < windowSize && sequenceNumberToSend < dataToSend.size) {
            sendDataHelper();
        }

        //check if all acks are received
        if(dataInHasStopped && successDataSoFar.length == dataToSend.size) {
            process.stderr.write("all acks are received yayy!\n");
            process.exit(0);
        }

    } catch (error) {
        process.stderr.write(`error: ${error.message}\n`);
    }

}

//helper function to resend the data that is not ack yet (use to resend specific data in the notAckMap when timeout happen)
function resendNotAckData(sequenceNumber, dataStored, currentTime) {
    const objectToResend = JSON.stringify({
        sequenceNumber: sequenceNumber,
        info: dataStored.info,
        checksum: checksumHelper(dataStored.info)
    });
    udpSocket.send(objectToResend, port, host);
}

//helper function to reset ssthresh + window size when timeout happen
function timeoutHelper() {
    ssthresh = Math.max(2, Math.floor(windowSize / 2));
    windowSize = ssthresh;
}

//helper function to resend the data that is not ack yet
function resendDataHelper() {
    const entriesOfMap = notAckMap.entries();
    let timeoutOrNot = false;

    for(const [sequenceNumber, {dataStored, createTime}] of entriesOfMap) {
        //calculate the time and check for timeout
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

//main function this is the function that runs first when program start
function main() {

    const commandLineArgs = process.argv.slice(2);
    host = commandLineArgs[0];
    port = Number(commandLineArgs[1]);

    //get data from stdin
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
        appendDataHelper(chunk);
        // if possible to send then send
        while(notAckMap.size < windowSize && sequenceNumberToSend < dataToSend.size) {
            sendDataHelper();
        }
    });

    //"end" is when data from stdin is done
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

    //this is triggered when got data from receiver
    udpSocket.on("message", (msg, rinfo) => {
        fromReceiverHandler(msg, rinfo);
    });

    //set the interval to resend the data in the notAckMap 
    setInterval(resendDataHelper,67);
}

main();