// create an empty modbus client
const ModbusRTU = require("modbus-serial");
const client = new ModbusRTU();
var buf = new ArrayBuffer(1);
// Create a data view of it
var view = new DataView(buf);

var VryV;
var VybV;
var VbrV;
var VLN1;
var VLN2;
var VLN3;
var VLL1;
var VLL2;
var VLL3;
var Ir;
var Iy;
var Ib;
var Pf;
var Freq;
var Kw;
var Kwh;
var Kvah;
var MD;

var globalID = 1;
var prevID = 1;
var curID = 1;
////////////////////////// TCP Client strt //////////////////

var net = require('net');

client.connectRTUBuffered("/dev/ttyUSB0", { baudRate: 9600 });
////////////////////////// TCP Client Start //////////////////


var client2 = new net.Socket();

function connectToServer() {
    client2.connect(5000, '192.168.0.137', function () {
        console.log('Connected to server');
    });

    client2.on('data', function (data) {
        // Handle incoming data from server
        console.log('Received data from server:', data.toString());
    });

    client2.on('close', function () {
        console.log('Server connection closed. Attempting to reconnect...');
        reconnectServer();
    });

    client2.on('error', function (err) {
        console.error('TCP client error:', err.message);
        if (err.code === 'ECONNREFUSED') {
            console.log('Connection refused. Reconnecting...');
            reconnectServer();
        }
    });
}

function reconnectServer() {
    setTimeout(() => {
        console.log('Attempting to reconnect to the server...');
        client2.connect(5000, '192.168.0.214', function () {
            console.log('Reconnected to server');
        });
    }, 5000); // Retry every 5 seconds
}

// Initial connection to the server
connectToServer();

////////////////////////// TCP Client End //////////////////

///////////////////////////// TCP Client end /////////////////


///////////// simulating KWH for 27 MFM ////////////////

// setInterval(() => {

//   var tempNum = (Math.random() * (2.120 - 2.0200) + 0.1200).toFixed(4)
//   Kwh = parseFloat(Kwh) + parseFloat(tempNum);

//   var tempNum = (Math.random() * (2.120 - 2.0200) + 0.0200).toFixed(4)
//   Kvah = parseFloat(Kvah) + parseFloat(tempNum);
//  // console.log('KWH',live_KWh_M1_value.toFixed(2))
// }, 1000);
/////////////// simulating KWH for 27 MFM end /////////////



setInterval(function () {
    // for(var i=0;i<readvalue.length;i++){

    client.setID(globalID);
    console.log('seting ID', globalID);

    console.log('prevID ID', prevID);
    console.log('curID ID', curID);
    //curID = pararead();
    if (prevID > globalID) {
        prevID = 1;
        curID = 1;
    }
    pararead();
    setTimeout(() => {
        console.log("curID", curID);
        if (prevID < curID) {
            prevID = curID;
        }
        else {
            if (globalID == 21) {
                globalID = 1;
                prevID = 1;
                curID = 1;
            }

            else if (prevID == curID) {
                var deacmeter = globalID.toString();
                client2.write('Deactive|' + deacmeter);
                globalID++;
            }
        }
    }, 700);

    //}
}, 1000);

function pararead() {

    console.log('reading ID', globalID);
    client.readHoldingRegisters(3109, 2, function (err, data) {
        if (err) {
            console.log("error");
        }
        // console.log('ooooooooooooooooo')
        var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
        Freq = currCount;
        console.log("freq value:", Freq)
        // Freq =  data.data[1]

        //***** VryV *****//
        setTimeout(() => {
            client.readHoldingRegisters(3019, 2, function (err, data) {
                // console.log('Vry',  data.data[1])
                // VryV = data.data[1]
                var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                VryV = currCount;
                console.log("Vry value:", VryV)

                //***** VbrV *****//
                setTimeout(() => {
                    client.readHoldingRegisters(3021, 2, function (err, data) {
                        // console.log('Vbr',  data.data[1])
                        // VbrV = data.data[1];
                        var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                        VbrV = currCount;
                        console.log("VbrV value:", VbrV)

                        //***** VybV *****//
                        setTimeout(() => {
                            client.readHoldingRegisters(3023, 2, function (err, data) {
                                // console.log('Vyb',  data.data[1])
                                // VybV = data.data[1];
                                var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                VybV = currCount;
                                console.log("VybV value:", VybV)

                                //***** VLN1 *****//
                                setTimeout(() => {
                                    client.readHoldingRegisters(3027, 2, function (err, data) {
                                        // console.log('VLN1',  data.data[1])
                                        // VLN1 = data.data[1]
                                        var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                        VLN1 = currCount;
                                        console.log("VLN1 value:", VLN1)

                                        //***** VLN2 *****//
                                        setTimeout(() => {
                                            client.readHoldingRegisters(3029, 2, function (err, data) {
                                                // console.log('VLN2',  data.data[1])
                                                // VLN2 = data.data[1]
                                                var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                VLN2 = currCount;
                                                console.log("VLN2 value:", VLN2)

                                                //***** VLN3 *****//
                                                setTimeout(() => {
                                                    client.readHoldingRegisters(3031, 2, function (err, data) {
                                                        // console.log('VLN3',  data.data[1])
                                                        // VLN3 = data.data[1]
                                                        var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                        VLN3 = currCount;
                                                        console.log("VLN3 value:", VLN3)

                                                        //***** VLL1 *****//
                                                        setTimeout(() => {
                                                            client.readHoldingRegisters(3033, 2, function (err, data) {
                                                                // console.log('VLL1',  data.data[1])
                                                                // VLL1 = data.data[1]
                                                                var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                VLL1 = currCount;
                                                                console.log("VLL1 value:", VLL1)

                                                                //***** VLL2 *****//
                                                                setTimeout(() => {
                                                                    client.readHoldingRegisters(3035, 2, function (err, data) {
                                                                        // console.log('VLL2',  data.data[1])
                                                                        // VLL2 = data.data[1]
                                                                        var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                        VLL2 = currCount;
                                                                        console.log("VLL2 value:", VLL2)

                                                                        //***** VLL3 *****//
                                                                        setTimeout(() => {
                                                                            client.readHoldingRegisters(3037, 2, function (err, data) {
                                                                                // console.log('VLL3',  data.data[1])
                                                                                // VLL3 = data.data[1]
                                                                                var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                                VLL3 = currCount;
                                                                                console.log("VLL3 value:", VLL3)

                                                                                //***** Ir *****//
                                                                                setTimeout(() => {
                                                                                    client.readHoldingRegisters(2999, 2, function (err, data) {
                                                                                        //console.log('Ir',  data.data[1])
                                                                                        //Ir = data.data[1];
                                                                                        var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                                        Ir = currCount;
                                                                                        console.log("Ir value:", Ir)

                                                                                        //***** Iy *****//
                                                                                        setTimeout(() => {
                                                                                            client.readHoldingRegisters(3001, 2, function (err, data) {
                                                                                                //console.log('Iy',  data.data[1])
                                                                                                //Iy = data.data[1];
                                                                                                var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                                                Iy = currCount;
                                                                                                console.log("Iy value:", Iy)

                                                                                                //***** Ib *****//
                                                                                                setTimeout(() => {
                                                                                                    client.readHoldingRegisters(3003, 2, function (err, data) {
                                                                                                        //console.log('ib',  data.data[1])
                                                                                                        //Ib = data.data[1];
                                                                                                        var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                                                        Ib = currCount;
                                                                                                        console.log("Ib value:", Ib)

                                                                                                        //***** Pf *****//
                                                                                                        setTimeout(() => {
                                                                                                            client.readHoldingRegisters(3193, 2, function (err, data) {
                                                                                                                //console.log('pf',  data.data[1])
                                                                                                                // Pf = data.data[1];
                                                                                                                var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                                                                Pf = currCount;
                                                                                                                console.log("Pf value:", Pf)

                                                                                                                //***** Kw *****//
                                                                                                                setTimeout(() => {
                                                                                                                    client.readHoldingRegisters(3059, 2, function (err, data) {
                                                                                                                        // console.log('Kw',  data.data[1])
                                                                                                                        // Kw = data.data[1];
                                                                                                                        var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                                                                        Kw = currCount;
                                                                                                                        console.log("Kw value:", Kw)

                                                                                                                        //***** Kwh *****//
                                                                                                                        setTimeout(() => {
                                                                                                                            client.readHoldingRegisters(2699, 2, function (err, data) {
                                                                                                                                //console.log('kwh',  data.data[1])
                                                                                                                                //Kwh = data.data[1];
                                                                                                                                var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                                                                                Kwh = currCount;
                                                                                                                                console.log("Kwh value:", Kwh)
                                                                                                                                                      
                                                                                                                                setTimeout(() => {
                                                                                                                                    client.readHoldingRegisters(3769, 2, function (err, data) {
                                                                                                                                        //console.log('kwh',  data.data[1])
                                                                                                                                        //Kwh = data.data[1];
                                                                                                                                        var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                                                                                        MD = currCount;
                                                                                                                                        console.log("MD value:", MD)

                                                                                                                                //***** MD *****//
                                                                                                                                // setTimeout(() => {
                                                                                                                                //     client.readHoldingRegisters(3769, 2, function (err, data) {
                                                                                                                                //         if (err) {
                                                                                                                                //             console.log("Error reading MD:", err);
                                                                                                                                //         } else {
                                                                                                                                //             var v1 = data.data[0];
                                                                                                                                //             view.setUint16(0, v1);
                                                                                                                                //             view.setUint16(2, v1);
                                                                                                                                //             var num = view.getFloat32(0);
                                                                                                                                //             var currCount = num.toFixed(1);
                                                                                                                                //             MD = currCount;
                                                                                                                                //             console.log("MD value:", MD);

                                                                                                                                //             // Include MD in the EMS array
                                                                                                                                //             EMS.push(MD);

                                                                                                                                //             client2.write(EMS.toString());
                                                                                                                                //             globalID++;
                                                                                                                                //             curID = globalID;
                                                                                                                                //             console.log('reading completed & now ++', globalID);
                                                                                                                                //             if (globalID == 2) {
                                                                                                                                //                 globalID = 1;
                                                                                                                                //                 return globalID;
                                                                                                                                //             } else {
                                                                                                                                //                 return globalID;
                                                                                                                                //             }
                                                                                                                                //         }
                                                                                                                                //     });
                                                                                                                                // }, 5);


                                                                                                                                //***** Kvah *****//
                                                                                                                                setTimeout(() => {
                                                                                                                                    client.readHoldingRegisters(2715, 2, function (err, data) {
                                                                                                                                        //console.log('kvah',  data.data[1])
                                                                                                                                        //Kvah = data.data[1];
                                                                                                                                        var v1 = data.data[0]; view.setUint16(0, v1); view.setUint16(2, v1); var num = view.getFloat32(0); var currCount = num.toFixed(1);
                                                                                                                                        Kvah = currCount;
                                                                                                                                        console.log("Kvah value:", Kvah)

                                                                                                                                        //  io.emit("sendvalue",VryV,VybV,VbrV,Ir,Iy,Ib,Pf,Freq,Kwh,Kvah)
                                                                                                                                        /// sending data to server
                                                                                                                                        var EMS = [];
                                                                                                                                        var temp;
                                                                                                                                        //simulation of 10 meters
                                                                                                                                        if (globalID == 1) {
                                                                                                                                            temp = "1";
                                                                                                                                        }
                                                                                                                                        else if (globalID == 2) {
                                                                                                                                          temp = "2";
                                                                                                                                        } 
                                                                                                                                        else if (globalID == 3) {
                                                                                                                                          temp = "3";
                                                                                                                                        } else if (globalID == 4) {
                                                                                                                                          temp = "4";
                                                                                                                                        } else if (globalID == 5) {
                                                                                                                                          temp = "5";
                                                                                                                                        } else if (globalID == 6) {
                                                                                                                                          temp = "6";
                                                                                                                                        } else if (globalID == 7) {
                                                                                                                                          temp = "7";
                                                                                                                                        } else if (globalID == 8) {
                                                                                                                                          temp = "8";
                                                                                                                                        } else if (globalID == 9) {
                                                                                                                                          temp = "9";
                                                                                                                                        } else if (globalID == 10) {
                                                                                                                                          temp = "10";
                                                                                                                                        } else if (globalID == 11) {
                                                                                                                                          temp = "11";
                                                                                                                                        } 
                                                                                                                                     else if (globalID == 11) {
                                                                                                                                        temp = "12";
                                                                                                                                      } else if (globalID == 11) {
                                                                                                                                        temp = "13";
                                                                                                                                      } else if (globalID == 11) {
                                                                                                                                        temp = "14";
                                                                                                                                      } else if (globalID == 11) {
                                                                                                                                        temp = "15";
                                                                                                                                      } else if (globalID == 11) {
                                                                                                                                        temp = "16";
                                                                                                                                      } else if (globalID == 11) {
                                                                                                                                        temp = "17";
                                                                                                                                      } else if (globalID == 11) {
                                                                                                                                        temp = "18";
                                                                                                                                      } else if (globalID == 11) {
                                                                                                                                        temp = "19";
                                                                                                                                      } else if (globalID == 11) {
                                                                                                                                        temp = "20";
                                                                                                                                      } 

                                                                                                                                        console.log('temp', temp)
                                                                                                                                        EMS.push(temp);
                                                                                                                                        EMS.push(VryV);
                                                                                                                                        EMS.push(VybV);
                                                                                                                                        EMS.push(VbrV);
                                                                                                                                        EMS.push(VLN1);
                                                                                                                                        EMS.push(VLN2);
                                                                                                                                        EMS.push(VLN3);
                                                                                                                                        EMS.push(VLL1);
                                                                                                                                        EMS.push(VLL2);
                                                                                                                                        EMS.push(VLL3);
                                                                                                                                        EMS.push(Ir);
                                                                                                                                        EMS.push(Iy);
                                                                                                                                        EMS.push(Ib);
                                                                                                                                        EMS.push(Pf);
                                                                                                                                        EMS.push(Freq);
                                                                                                                                        EMS.push(Kw);
                                                                                                                                        EMS.push(Kwh);
                                                                                                                                        EMS.push(Kvah);

                                                                                                                                        // EMS.push('420.5');
                                                                                                                                        // EMS.push('414.5');
                                                                                                                                        // EMS.push('418.5');
                                                                                                                                        // EMS.push('0');
                                                                                                                                        // EMS.push('0');
                                                                                                                                        // EMS.push('0');
                                                                                                                                        // EMS.push('-0.1');
                                                                                                                                        // EMS.push('5.1');
                                                                                                                                        // EMS.push(Kwh);
                                                                                                                                        // EMS.push(Kvah);
                                                                                                                                        // console.log('simulated value Kwh',Kwh)
                                                                                                                                        // console.log('simulated value Kvah',Kvah)
                                                                                                                                        var i = 0;
                                                                                                                                        client2.write(EMS.toString());
                                                                                                                                        globalID++;
                                                                                                                                        curID = globalID;
                                                                                                                                        // console.log("tttt", globalID);
                                                                                                                                        console.log('reading completed & now ++', globalID);
                                                                                                                                        if (globalID == 21) {
                                                                                                                                            globalID = 1;
                                                                                                                                            //curID = globalID;
                                                                                                                                            return globalID;
                                                                                                                                        }
                                                                                                                                        else {
                                                                                                                                            return globalID;
                                                                                                                                        }
                                                                                                                                        /// sending data end
                                                                                                                                    });
                                                                                                                                }, 1);
                                                                                                                            });
                                                                                                                        }, 1);

                                                                                                                            });
                                                                                                                        }, 1);

                                                                                                                    });
                                                                                                                }, 1);

                                                                                                            });
                                                                                                        }, 1);
                                                                                                    });
                                                                                                }, 1);

                                                                                            });
                                                                                        }, 1);

                                                                                    });
                                                                                }, 1);

                                                                            });
                                                                        }, 1);
                                                                    });
                                                                }, 1);
                                                            });
                                                        }, 1);
                                                    });
                                                }, 1);
                                            });
                                        }, 1);
                                    });
                                }, 1);
                            });
                        }, 1);

                    });
                }, 1);

            });
        }, 1);

    });
}