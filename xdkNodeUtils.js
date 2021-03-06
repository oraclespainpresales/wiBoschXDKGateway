const async = require('async')
    , _ = require('lodash')
    , noble = require('noble')
    , util = require('util')
    , log = require('npmlog-ts')
    , EventEmitter = require('events').EventEmitter
;

log.timestamp = true;

const BLE = "BLE"
;

const SAMPLINGRATE = 500
    , XDK_CHARACTERISTIC_CONTROL_NODE_START_SAMPLING       = '55b741d17ada11e482f80800200c9a66'
    , XDK_CHARACTERISTIC_CONTROL_NODE_CHANGE_SAMPLING_RATE = '55b741d27ada11e482f80800200c9a66'
    , XDK_CHARACTERISTIC_CONTROL_NODE_REBOOT               = '55b741d37ada11e482f80800200c9a66'
    , XDK_CHARACTERISTIC_CONTROL_NODE_GET_FW_VERSION       = '55b741d47ada11e482f80800200c9a66'
    , XDK_CHARACTERISTIC_CONTROL_NODE_USE_SENSOR_FUSION    = '55b741d57ada11e482f80800200c9a66'
    , READER1    = 'c29672117ba411e482f80800200c9a66'
    , READER2    = 'c29672127ba411e482f80800200c9a66'
    , POWEREDON  = 'poweredOn'
    , POWEREDOFF = 'poweredOff'
;

var XDKID       = _.noop()
  , XDK         = _.noop()
  , MAINLOOP    = _.noop()
  , BLESTATUS   = POWEREDOFF
  , SCANNING    = false
  , SCANTIMEOUT = _.noop()
  , SCANTIMER   = _.noop()
  , AUTOCONNECT = false
  , SAMPLING    = false
  , TIMER       = {}
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

var READERS = [ { characteristic: READER1, parser: accelParser },
                { characteristic: READER2, parser: sensorsParser }
  ]
  , WRITERS = []
;

class XdkNodeUtils extends EventEmitter {

  constructor(level) {
    super();
    EventEmitter.defaultMaxListeners = 20;
    self = this;
    log.level = level;
    SAMPLING = false;
  }

  connect() {
    return new Promise((resolve, reject) => {
      log.verbose(BLE,"XDK connecting...");
      if (!XDK || !XDK.connect) reject("Cannot connect. XDK not discovered");
      XDK.connect((err) => {
        if (err) {
          log.error(BLE, "Error trying to connect to XDK");
          reject(err);
          return;
        }
        XDK.discoverServices([], (error, services) => { // Grab characteristics
          if (error) {
            log.error(BLE, "Error discovering services:");
            log.error(BLE, error);
            return;
          }
          async.eachSeries(services, (service, nextService) => {
            service.discoverCharacteristics([], (err, characteristics) => {
              async.eachSeries(characteristics, (characteristic, nextCharacteristic) => {
                if (_.includes(characteristic.properties, 'write')) {
                  // We want all WRITERS available, as each one has its own task
                  WRITERS.push({ characteristic: characteristic.uuid, c: characteristic});
                } else {
                  var READER = _.find(READERS, { characteristic: characteristic.uuid } );
                  if ( READER ) {
                    READER.c = characteristic;
                    // NOBLE on RPi does not need to invoke characteristic.notify()
                    READER.c.on('read', (data, isNotification) => {
                      READER.parser(data);
                    });
/**
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
**/
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
            var w = _.find(WRITERS, { characteristic: XDK_CHARACTERISTIC_CONTROL_NODE_USE_SENSOR_FUSION } );
            var b = new Buffer(1);
            b.writeUInt8(0x00, 0);
            if (w) {
              log.verbose(BLE, "Request not to use Sensor Fusion");
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
            XDK.ready = true;
            resolve();
        }/**, (err) => {
            if (err) reject(err);
            resolve();
          }**/);
        });
      });
    });
  }

  sampling(mode, timer) {
    return new Promise((resolve, reject) => {
      if (mode.toUpperCase() === "START") {
        if (SAMPLING === true) {
          reject("Sampling already ongoing");
          return;
        }
        if (!XDK || !XDK.connect) reject("Cannot start sampling. XDK not discovered");
        if (!XDK.ready) reject("Cannot start sampling. XDK not ready");

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
            SAMPLING = true;
          });
        }
        log.info(BLE, "Start Reading data");
        MAINLOOP = setInterval(function() {
          _.forEach(READERS, (r) => {
            r.c.read(function(err) {
              if (err) {
                log.error(BLE, err);
              }
            });
          });
        }, SAMPLINGRATE);

        if (timer) {
          if (!isNaN(timer)) {
            timer = parseInt(timer) * 1000;
            if (timer > 0) {
              if (TIMER.timer) {
                log.verbose(BLE, "Aborting previous Data pump timer");
                clearTimeout(TIMER.timer);
                TIMER = {};
              }
              log.info(BLE, "Data pump enabled for %d milliseconds", timer);
              TIMER.time = timer;
              TIMER.timer = setTimeout(() => {
                log.verbose(BLE, "Data pump timer reached!");
                self.sampling("stop");
              }, TIMER.time);
            }
          }
        }

        resolve();
      } else if (mode.toUpperCase() === "STOP") {
        if (SAMPLING === false) {
          reject("Sampling already stopped");
          return;
        }
        if (!XDK || !XDK.connect) reject("Cannot stop sampling. XDK not discovered");
        if (!XDK.ready) reject("Cannot stop sampling. XDK not ready");

        log.info(BLE, "Stop Reading data");
        clearInterval(MAINLOOP);
        if (TIMER.timer) {
          clearTimeout(TIMER.timer);
          log.verbose(BLE, "Data pump timeout cleared");
          TIMER = {};
        }

        // Stop sampling
        var w = _.find(WRITERS, { characteristic: XDK_CHARACTERISTIC_CONTROL_NODE_START_SAMPLING } );
        if (w) {
          log.verbose(BLE, "Request stop sampling");

          var b = new Buffer(1);
          b.writeUInt8(0x00, 0);

          w.c.write(b, false, function(err) {
            if (err) {
              reject(err);
            }
            SAMPLING = false;
          });
        }
        resolve();
      } else {
        reject("Unknown mode: " + mode);
      }
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
  log.info(BLE, "XDK connected");
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

//  var found = (peripheral.id == XDKID);
  var found = (String(peripheral.advertisement.localName) === "XDK_Virtual_Sensor");

  var peripheralName = new String(peripheral.advertisement.localName.toString().replace(/\0/g, ''));
  var y = new String("XDK_Virtual_Sensor");

  var a = Buffer.from(peripheralName, 'utf8');
  var b = Buffer.from(y, 'utf8');

  console.log(Buffer.from(peripheralName, 'utf8'));
  console.log(Buffer.from(y, 'utf8'));

  console.log("Result: " + Buffer.compare(a, b));

  if (Buffer.compare(a, b) == 0) {
    log.info(BLE, "XDK found (%s)", peripheral.advertisement.localName);
    noble.stopScanning();
    XDK = peripheral;
    XDK.ready = false;
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
