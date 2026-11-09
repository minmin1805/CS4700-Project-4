const fs = require("fs");

const datagram = require("dgram");

const udpSocket = datagram.createSocket("udp4");

let senderAddress = null;
let senderPort = null;
let sequenceNumberWaitingFor = 0;

const receivedEarlyMap = new Map();

function sendListeningMessage(port) {
    process.stderr.write(`Bound to port ${port}\n`);
}

function checkSender(address, port) {
    return address === senderAddress && port === senderPort;
}

function checksumHelper(info) {
    let total = 0;
    const infoArray = info.split("");
    for(const eachChar of infoArray) {
        const charEncoded = eachChar.charCodeAt(0);
        const multipliedBy3 = charEncoded * 3;
        total += multipliedBy3;
    }
    return total;
}

function printDataStdout(info) {
    fs.writeSync(1, info);
}

function checkForEarlyData() {
    
const keysFromMap = Array.from(receivedEarlyMap.keys()).sort((a, b) => 
    a - b);

for(const key of keysFromMap) {
    if(key == sequenceNumberWaitingFor) {
        //print out stdout
        printDataStdout(receivedEarlyMap.get(key));
        //remove from map
        receivedEarlyMap.delete(key);
        //now we need the next one
        sequenceNumberWaitingFor++;
    }
}
}

function sendAckHelper(datagram) {
    const sequenceNumToReport = datagram.sequenceNumber;
    const checksumToSend = checksumHelper(datagram.info)
    const ackToSend = JSON.stringify({ receiveSN: sequenceNumToReport, NeededSN: sequenceNumberWaitingFor, checksum: checksumToSend });
    udpSocket.send(ackToSend, senderPort, senderAddress);
}

function processDatagram(newDatagram, rinfo) {
    try {
        const currentSequenceNumber = newDatagram.sequenceNumber;
        const info = newDatagram.info;

        const originalChecksum = newDatagram.checksum;

        const comparedChecksum = checksumHelper(info);

        if(originalChecksum !== comparedChecksum) {
            return;
        }

        //assign sender address
        if(senderAddress === null) {
            senderAddress = rinfo.address;
            senderPort = rinfo.port;
        }
        else if(!checkSender(rinfo.address, rinfo.port)) {
            return;
        }

        if(currentSequenceNumber > sequenceNumberWaitingFor) {
            receivedEarlyMap.set(currentSequenceNumber, info);
            //send ack for the early packet too
            sendAckHelper(newDatagram);
        }
        else if(currentSequenceNumber < sequenceNumberWaitingFor) {
            sendAckHelper(newDatagram);
            return;
            //send ack even for duplicate data
        }
        else {
            //print out stdout
            printDataStdout(info);
            //increment sequence number waiting for
            sequenceNumberWaitingFor++;
            //send ack
            sendAckHelper(newDatagram);

            //check map for data that was received early
            checkForEarlyData();
        }



    } catch (error) {
        
    }


}


function main() {
    udpSocket.on("message", (msg, rinfo) => {
        let newDatagram;
        try {
            newDatagram = JSON.parse(msg.toString());
        } catch (error) {
            return;
        }
        processDatagram(newDatagram, rinfo);
    });

    udpSocket.on("listening", () => {
        sendListeningMessage(udpSocket.address().port);
    });

    udpSocket.bind(0, "0.0.0.0");
}

main();