const async = require('async')
    , _ = require('lodash')
    , noble = require('noble')
    , util = require('util')
    , log = require('npmlog-ts')
    , EventEmitter = require('events').EventEmitter
;

log.timestamp = true;
log.level     = 'verbose';

const BLE = "BLE"
;

const SAMPLINGRATE = 500
    , XDK_CHARACTERISTIC_CONTROL_NODE_START_SAMPLING       = '55b741d17ada11e482f80800200c9a66'
    , XDK_CHARACTERISTIC_CONTROL_NODE_CHANGE_SAMPLING_RATE = '55b741d27ada11e482f80800200c9a66'
    , XDK_CHARACTERISTIC_CONTROL_NODE_REBOOT               = '55b741d37ada11e482f80800200c9a66'
    , XDK_CHARACTERISTIC_CONTROL_NODE_GET_FW_VERSION       = '55b741d47ada11e482f80800200c9a66'
    , XDK_CHARACTERISTIC_CONTROL_NODE_USE_SENSOR_FUSION    = '55b741d57ada11e482f80800200c9a66'
    , READER1    = '5a211d41716611e482f80800200c9a66'
    , READER2    = '5a211d42716611e482f80800200c9a66'
    , READER3    = '5a211d43716611e482f80800200c9a66'
    , READER4    = 'aca96a4174a411e482f80800200c9a66'
    , READER5    = 'aca96a4274a411e482f80800200c9a66'
    , READER6    = 'aca96a4374a411e482f80800200c9a66'
    , READER7    = '38eb02c1754011e482f80800200c9a66'
    , READER8    = '01033831754c11e482f80800200c9a66'
    , READER9    = '651f4c01757911e482f80800200c9a66'
    , READER10    = '651f4c02757911e482f80800200c9a66'
    , READER11    = '651f4c03757911e482f80800200c9a66'
    , READER12    = '651f4c04757911e482f80800200c9a66'
    , READER13    = '92dab061763411e482f80800200c9a66'
    , READER14    = '92dab062763411e482f80800200c9a66'
    , READER15    = '92dab063763411e482f80800200c9a66'

    , READER16    = 'c29672117ba411e482f80800200c9a66'
    , READER17    = 'c29672127ba411e482f80800200c9a66'
    , POWEREDON  = 'poweredOn'
    , POWEREDOFF = 'poweredOff'
;

var XDKID     = _.noop()
  , XDK       = _.noop()
  , MAINLOOP  = _.noop()
  , BLESTATUS = POWEREDOFF
  , SCANNING    = false
  , SCANTIMEOUT = _.noop()
  , SCANTIMER   = _.noop()
  , AUTOCONNECT = false
;

var self;

var accelParser = (data) => {
  if (!data) return;
  var word = new Buffer(2);
  data.copy(word, 0, 0, 2);
  var Accel_X = word.readInt16LE();
  data.copy(word, 0, 2, 4);
  var Accel_Y = word.readInt16LE();
  data.copy(word, 0, 4, 6);
  var Accel_Z = word.readInt16LE();
  data.copy(word, 0, 6, 8);
  var Gyro_X = word.readInt16LE();
  data.copy(word, 0, 8, 10);
  var Gyro_Y = word.readInt16LE();
  data.copy(word, 0, 10, 12);
  var Gyro_Z = word.readInt16LE();
  var payload = {
    accelerometer: {
      x: Accel_X,
      y: Accel_Y,
      z: Accel_Z
    },
    gyrometer: {
      x: Gyro_X,
      y: Gyro_Y,
      z: Gyro_Z
    }
  }
  self.emit('data', payload);
};

var sensorsParser = (data) => {
  if (!data) return;
  var id = data.readUInt8(0);
  var word = new Buffer(2);
  var doubleword = new Buffer(4);
  var Light, Noise, Pressure, Temp, RH, Mag_X, Mag_Y, Mag_Z;
  var payload = {};
  if (id == 1) {
    data.copy(doubleword, 0, 1, 5);
    Light = Math.round(doubleword.readInt32LE() / 1000);
    Noise = data.readUInt8(5);
    data.copy(doubleword, 0, 6, 10);
    Pressure = doubleword.readInt32LE();
    data.copy(doubleword, 0, 10, 14);
    Temp = Number(Math.round((doubleword.readInt32LE() / 1000)+'e2')+'e-2');
    data.copy(doubleword, 0, 14, 18);
    RH = doubleword.readInt32LE();
    payload.light       = Light;
    payload.noise       = Noise;
    payload.pressure    = Pressure;
    payload.temperature = Temp;
    payload.humidity    = RH;
  } else {
    data.copy(word, 0, 1, 3);
    var Mag_X = word.readInt16LE();
    data.copy(word, 0, 3, 5);
    var Mag_Y = word.readInt16LE();
    data.copy(word, 0, 5, 7);
    var Mag_Z = word.readInt16LE();
    data.copy(word, 0, 7, 9);
    var Mag_R = word.readInt16LE();
    payload.magneticfield = {
      x: Mag_X,
      y: Mag_Y,
      z: Mag_Z,
      r: Mag_R
    }
  }
  self.emit('data', payload);
};

var dummyReader = (data) => {
  console.log("Data!");
}

/**
var READERS = [ { characteristic: READER1, parser: accelParser },
                { characteristic: READER2, parser: sensorsParser }
  ]
**/
var READERS = [ { characteristic: READER1, parser: dummyReader },
                { characteristic: READER2, parser: dummyReader },
                { characteristic: READER3, parser: dummyReader },
                { characteristic: READER4, parser: dummyReader },
                { characteristic: READER5, parser: dummyReader },
                { characteristic: READER6, parser: dummyReader },
                { characteristic: READER7, parser: dummyReader },
                { characteristic: READER8, parser: dummyReader },
                { characteristic: READER9, parser: dummyReader },
                { characteristic: READER10, parser: dummyReader },
                { characteristic: READER11, parser: dummyReader },
                { characteristic: READER12, parser: dummyReader },
                { characteristic: READER13, parser: dummyReader },
                { characteristic: READER14, parser: dummyReader },
                { characteristic: READER15, parser: dummyReader },
                { characteristic: READER16, parser: dummyReader },
                { characteristic: READER17, parser: dummyReader }
  ]
  , WRITERS = []
;

class XdkNodeUtils extends EventEmitter {

  constructor() {
    super();
    EventEmitter.defaultMaxListeners = 20;
    self = this;
  }

  connect() {
    return new Promise((resolve, reject) => {
      log.verbose(BLE,"XDK connecting...");
      if (!XDK || !XDK.connect) reject("Cannot connect. XDK not discovered");
      XDK.connect((err) => {
        log.verbose(BLE,"XDK connected!");
        if (err) {
          log.error(BLE, "Error trying to connect to XDK");
          reject(err);
          return;
        }
        log.verbose(BLE,"Discovering services...");
        XDK.discoverServices([], (error, services) => { // Grab characteristics
          log.verbose(BLE,"Services discovered");
          if (error) {
            log.error(BLE, "Error discovering services:");
            log.error(BLE, error);
            return;
          }
          async.eachSeries(services, (service, nextService) => {
            service.discoverCharacteristics([], (err, characteristics) => {
              async.eachSeries(characteristics, (characteristic, nextCharacteristic) => {
                log.verbose(BLE,"Characteristic: UUID: %s, properties: %j", characteristic.uuid, characteristic.properties);
                if (_.includes(characteristic.properties, 'write')) {
                  // We want all WRITERS available, as each one has its own task
                  WRITERS.push({ characteristic: characteristic.uuid, c: characteristic});
                } else {
                  var READER = _.find(READERS, { characteristic: characteristic.uuid } );
                  if ( READER ) {
                    log.verbose(BLE, "Subscribing to characteristic '%s'", READER.characteristic);
                    READER.c = characteristic;
                    characteristic.notify(true, function(err) {
                      if (err) {
                        nextCharacteristic(err);
                        return;
                      } else {
                        characteristic.on('read', (data, isNotification) => {
                          console.log('data');
                          READER.parser(data);
                        });
                      }
                    });
                  }
                }
                nextCharacteristic();
              }, (err) => {
                nextService(err);
              });
            });
          }, (err) => {
            if (err) {
              reject(err);
              return;
            }
            // Do not use Sensor Fusion
            w = _.find(WRITERS, { characteristic: XDK_CHARACTERISTIC_CONTROL_NODE_USE_SENSOR_FUSION } );
            var b = new Buffer(1);
            b.writeUInt8(0x00, 0);
            if (w) {
              log.verbose(BLE, "Request do not use Sensor Fusion");
              w.c.write(b, false, function(err) {
                if (err) {
                  reject(err);
                }
              });
            }
            // Set sampling rate
            w = _.find(WRITERS, { characteristic: XDK_CHARACTERISTIC_CONTROL_NODE_CHANGE_SAMPLING_RATE } );
            var b = new Buffer(4);
            b.writeInt32LE(SAMPLINGRATE, 0);
            if (w) {
              log.verbose(BLE, "Set sampling rate to %d milliseconds", SAMPLINGRATE);
              w.c.write(b, false, function(err) {
                if (err) {
                  reject(err);
                }
              });
            }
            // Start sampling
            var w = _.find(WRITERS, { characteristic: XDK_CHARACTERISTIC_CONTROL_NODE_START_SAMPLING } );
            if (w) {
              log.verbose(BLE, "Request start sampling");

              var b = new Buffer(1);
              b.writeUInt8(0x01, 0);

              w.c.write(b, false, function(err) {
                if (err) {
                  reject(err);
                }
              });
            }
            log.verbose(BLE, "Start Reading data");
            MAINLOOP = setInterval(function() {
              _.forEach(READERS, (r) => {
                if (r.c) {
                  r.c.read(function(err) {
                    if (err) {
                      log.error(BLE, err);
                    }
                  });
                }
              });
            }, SAMPLINGRATE);
          }, (err) => {
            if (err) reject(err);
            reseolve();
          });
        });
      });
    });
  }

  scan(xdkId, timeout, autoconnect) {
    return new Promise((resolve, reject) => {
      if (SCANNING) reject("Scanning already ongoing");
      if (!xdkId) reject("Missing XDK ID to look for");
      if (!noble) reject("NOBLE not available!");
      if (BLESTATUS !== POWEREDON) reject ("Cannot scan yet as BLE is not ON");
      if (SCANTIMER) {
        clearTimeout(SCANTIMER);
        SCANTIMER = _.noop();
      }
      XDKID = xdkId;
      SCANTIMEOUT = (timeout && Number.isInteger(timeout)) ? timeout : _.noop();
      AUTOCONNECT = ( autoconnect && (typeof(autoconnect) === "boolean")) ? autoconnect : false;
      log.verbose(BLE, "Start scanning");
      noble.startScanning([], false);
      resolve();
    })
  }


}

noble.on('stateChange', function(state) {
  log.verbose(BLE, "BTLE State changed: "+state);
  if (state === 'poweredOn') {
    BLESTATUS = POWEREDON;
    if (self) self.emit('on');
  } else {
    BLESTATUS = POWEREDOFF;
    log.verbose(BLE, "BLE no longer powered on: %s", state);
    log.verbose(BLE, "Stop scanning");
    noble.stopScanning();
  }
});

var peripheralConnected = () => {
  log.verbose(BLE, "XDK connected!");
}

var peripheralDisconnected = () => {
  log.verbose(BLE, "XDK disconnected!");
  // Reset everything and start over
  clearInterval(MAINLOOP);
  _.forEach(READERS, (r) => {
    if (r.c) {
      r.c.removeAllListeners();
      delete r.c;
    }
  });
  // Empty the arrays
  WRITERS.length = 0;
  XDK.removeAllListeners();
  XDK = _.noop();
  log.verbose(BLE, "Start scanning again");
  noble.startScanning([], false);
}

noble.on('discover', function(peripheral) {
  var id = peripheral.advertisement.serviceUuids;
  var manufacturerData = peripheral.advertisement.manufacturerData;

  if (peripheral.id == XDKID) {
    log.verbose(BLE, "XDK found (%s)", manufacturerData);
//    console.log(util.inspect(peripheral, true, null));
    noble.stopScanning();
    XDK = peripheral;
    XDK.once('connect', peripheralConnected);
    XDK.once('disconnect', peripheralDisconnected);
    self.emit('discovered');
    if (AUTOCONNECT) {
      connect();
    }
  } else {
    log.verbose(BLE, "Ignoring device id '%s' (%s)", peripheral.id, (peripheral.advertisement) ? peripheral.advertisement.localName : 'unknown');
  }
});

noble.on('scanStart', () => {
  log.verbose(BLE, "Scanning started");
  SCANNING = true;
  if (SCANTIMEOUT) {
    SCANTIMER = setTimeout(() => {
      log.verbose(BLE, "Stop scanning by timeout");
      SCANTIMER = _.noop();
      noble.stopScanning();
    }, SCANTIMEOUT);
  }
});

noble.on('scanStop', () => {
  log.verbose(BLE, "Scanning stopped");
  SCANNING = false;
});

// Exported methods

module.exports = XdkNodeUtils;
