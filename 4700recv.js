//this is the libs that i use for this file
const fs = require("fs");

const datagram = require("dgram");

const udpSocket = datagram.createSocket("udp4");

let senderAddress = null;
let senderPort = null;
let sequenceNumberWaitingFor = 0;

const receivedEarlyMap = new Map();

//this is helper function to print listening message to stderr
function sendListeningMessage(port) {
    process.stderr.write(`Bound to port ${port}\n`);
}

//helper function to calculate the checksum
function checksumHelper(info) {
    let total = 0;
    const infoArray = info.split("");
    for(const eachChar of infoArray) {
        const charEncoded = eachChar.charCodeAt(0);
        // i times each char encoded by 3 and add to total
        const multipliedBy3 = charEncoded * 3;
        total += multipliedBy3;
    }
    return total;
}

//helper function to print data to stdout
function printDataStdout(info) {
    fs.writeSync(1, info);
}

//helper function to print the data if they are received ealier and with the new sequence number received we can print them
function checkForEarlyData() {
    
//get the sequences from the map that can be printed
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

//this is helper function to send the ack back to sender
function sendAckHelper(datagram) {
    const sequenceNumToReport = datagram.sequenceNumber;
    const checksumToSend = checksumHelper(datagram.info)
    const ackToSend = JSON.stringify({ receiveSN: sequenceNumToReport, NeededSN: sequenceNumberWaitingFor, checksum: checksumToSend });
    udpSocket.send(ackToSend, senderPort, senderAddress);
}

//this is helper function to process the data from the sender
function processDatagram(newDatagram, rinfo) {
    try {
        //pull out the fields of the newDatagram
        const currentSequenceNumber = newDatagram.sequenceNumber;
        const info = newDatagram.info;

        const originalChecksum = newDatagram.checksum;
        //calculate the checksum
        const comparedChecksum = checksumHelper(info);

        if(originalChecksum !== comparedChecksum) {
            return;
        }

        //assign sender address
        if(senderAddress === null) {
            senderAddress = rinfo.address;
            senderPort = rinfo.port;
        }

        if(currentSequenceNumber > sequenceNumberWaitingFor) {
            receivedEarlyMap.set(currentSequenceNumber, info);
            //send ack for the early packet too
            sendAckHelper(newDatagram);
        }
        //this is when data is duplicate, ack them back
        else if(currentSequenceNumber < sequenceNumberWaitingFor) {
            sendAckHelper(newDatagram);
            return;
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

//this is the main function to run the program
function main() {
    //this triggers when the sender send the data over
    udpSocket.on("message", (msg, rinfo) => {
        let newDatagram;
        try {
            newDatagram = JSON.parse(msg.toString());
        } catch (error) {
            return;
        }
        processDatagram(newDatagram, rinfo);
    });

    //triggers this when the socket is listening
    udpSocket.on("listening", () => {
        sendListeningMessage(udpSocket.address().port);
    });

    udpSocket.bind(0, "0.0.0.0");
}

main();