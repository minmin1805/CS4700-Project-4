const datagram = require("dgram");
const fs = require("fs");

const udpSocket = datagram.createSocket("udp4");
const host = null;
const port = null;

let dataToSend = new Map();
let successDataSoFar = [];
let dataInHasStopped = false;
let sequenceNumberToSend = 0;
let notAckMap = new Map();
let windowSize = 2;
let stringToSend = "";

function checksumHelper(data) {
    const total = 0;
    const dataArray = data.split("");
    for(const eachChar of dataArray) {
        const charEncoded = eachChar.charCodeAt(0);
    }
    return total;
}
function prepareDataToSend(stringToSend) {
    const checknumCalculate = checksumHelper(data);
    const dataToSend = JSON.stringify({
        sequenceNumber: sequenceNumberToSend,
        info: stringToSend,
        checksum: checknumCalculate
    });
    stringToSend = "";
    //store it
    dataToSend.set(sequenceNumberToSend, dataToSend);

    
}

function appendDataHelper(chunk) {
    if(stringToSend == "") {
        stringToSend = chunk;
    }
    else {
        stringToSend += chunk;
    }
    if(stringToSend.length > 1375) {
        let actualToSend = stringToSend.slice(0, 1375);
        prepareDataToSend(actualToSend);
        //update stringToSend to leftover
        stringToSend = stringToSend.slice(1375);
    }
}

function sendDataHelper() {
    const dataToSend = dataToSend.get(sequenceNumberToSend);
    const checksumCalculate = checksumHelper(dataToSend.info);
    const objectToSend = JSON.stringify({
        sequenceNumber: sequenceNumberToSend,
        info: dataToSend.info,
        checksum: checksumCalculate
    });
    udpSocket.send(objectToSend, port, host);
    notAckMap.set(sequenceNumberToSend, dataToSend);
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
            key => key < receivedReceiveSN);

        for(const key of keysSmallerThanReceivedSN) {
            notAckMap.delete(key);
        }

        while(notAckMap.size < windowSize && stringToSend.length == 1375) {
            sendDataHelper();
            stringToSend = "";
        }

        //check if all acks are received
        if(successDataSoFar.length == dataToSend.size) {
            process.stdout.write("All done!\n");
            process.exit(0);
        }

    } catch (error) {
        process.stderr.write(`Error: ${error.message}\n`);
    }

}

function main() {

    const commandLineArgs = process.argv.slice(2);
    const host = commandLineArgs[2];
    const port = commandLineArgs[3];

    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
        appendDataHelper(chunk);
        // if possible to send then send
        while(notAckMap.size < windowSize && stringToSend.length == 1375) {
            sendDataHelper();
            stringToSend = "";
        }
    });


    process.stdin.on("end", () => {
        //check if there is data left to send
        if(stringToSend.length > 0) {
            sendDataHelper();
            stringToSend = "";
        }
        dataInHasStopped = true;
        //check if all acks 
        if(successDataSoFar.length == dataToSend.size) {
            process.stdout.write("All done!\n");
            process.exit(0);
        }
    });

    udpSocket.on("message", (msg, rinfo) => {
        fromReceiverHandler(msg, rinfo);
    });
}

main();