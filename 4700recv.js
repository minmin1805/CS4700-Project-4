const fs = require("fs");

const datagram = require("dgram");

const udpSocket = datagram.createSocket("udp4");

const senderAddress = null;
const senderPort = null;
const sequenceNumberWaitingFor = 0;

const receivedEarlyMap = new Map();

function sendListeningMessage(port) {
    process.stderr.write(`Bound to port ${port}\n`);
}

function checkSender(address, port) {
    return address === senderAddress && port === senderPort;
}

function checkForEarlyData() {
    
const keysFromMap = Array.from(receivedEarlyMap.keys());

for(const key of keysFromMap) {
    if(key == sequenceNumberWaitingFor) {
        //print out stdout
        printDataStdout(receivedEarlyMap.get(key));
    }
}
}

function sendAckHelper(datagram) {
    const sequenceNumToReport = datagram.sequenceNumber;
    const checksumToSend = checksumHelper(datagram.info)
    const ackToSend = JSON.stringify({ receiveSN: sequenceNumToReport, sendSN: sequenceNumberWaitingFor, checksum: checksumToSend });
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
            receivedEarlyMap.set(sequenceNumber, info);
        }
        else if(currentSequenceNumber < sequenceNumberWaitingFor) {
            return;
            //send ack even for duplicate data
            sendAckHelper(newDatagram);
        }
        else {
            //print out stdout
            printDataStdout(info);
            //increment sequence number waiting for
            sequenceNumberWaitingFor++;
            //send ack
            sendAckHelper(currentSequenceNumber);

            //check map for data that was received early
            checkForEarlyData();
        }



    } catch (error) {
        
    }


}


function main() {
    udpSocket.on("message", (msg, rinfo) => {
        const newDatagram = JSON.parse(msg.toString());
        processDatagram(newDatagram, rinfo);
    });

    udpSocket.on("listening", () => {
        sendListeningMessage(udpSocket.address().port);
    });

    udpSocket.bind(0, "0.0.0.0");
}

main();