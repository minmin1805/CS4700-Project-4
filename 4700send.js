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

function main() {

    const commandLineArgs = process.argv.slice(2);
    const host = commandLineArgs[2];
    const port = commandLineArgs[3];

    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
        appendDataHelper(chunk);
        // if possible to send then send
        while(notAckMap.size < windowSize) {
            sendDataHelper();
        }
    });


    process.stdin.on("end", () => {
        //check if there is data left to send
        if(stringToSend.length > 0) {
            sendDataHelper();
        }
        dataInHasStopped = true;
        //check if all acks 
        if(successDataSoFar.length == dataToSend.size) {
            process.stdout.write("All done!\n");
            process.exit(0);
        }
    });
}

main();