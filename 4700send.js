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
let windowSize = 4;
let stringToSend = "";

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
    notAckMap.set(sequenceNumberToSend, dataStored);
    sequenceNumberToSend++;
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

        const stringForThisSeq = notAckMap.get(receivedReceiveSN);
        if(!stringForThisSeq) {
            throw new Error("no data for this sequence number");
        }

        if(receivedNeededSN > sequenceNumberToSend) {
            throw new Error("needed SN is greater than sequence number to send");
        }

        if(receivedReceiveSN > sequenceNumberToSend) {
            throw new Error("receive SN is greater than sequence number to send");
        }

        const calculatedChecksum = checksumHelper(stringForThisSeq.info);
        if(receivedChecksum !== calculatedChecksum) {
            throw new Error("checksum is not correct");
        }

        notAckMap.delete(receivedReceiveSN);
        successDataSoFar.push(receivedReceiveSN);

        //check if can remove more from notAckMap
        const keysSmallerThanReceivedSN = Array.from(notAckMap.keys()).filter(
            key => key < receivedNeededSN);

        for(const key of keysSmallerThanReceivedSN) {
            notAckMap.delete(key);
            successDataSoFar.push(key);
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
}

main();