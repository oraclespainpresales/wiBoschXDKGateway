'use strict'

const async = require('async')
    , _ = require('lodash')
    , log = require('npmlog-ts')
    , util = require('util')
    , restify = require('restify-clients')
    , kafka = require('kafka-node')
    , queue = require('block-queue')
    , isOnline = require('is-online')
    , fs = require('fs-extra')
    , glob = require("glob")
    , child_process = require('child_process')
    , commandLineArgs = require('command-line-args')
    , getUsage = require('command-line-usage')
    , uuidv4 = require('uuid/v4')
;

var XdkNodeUtils = _.noop()
  , xdkNodeUtils = _.noop()
;

log.timestamp = true;

// In MAC:
//const XDKID = "09c2b4046299459b8475b237d200eac4"

// In RPi (built-in BLE)
//const XDKID = "fcd6bd100551"
var XDKID = _.noop()
//      XDKID = "fcd6bd1037b2"
;

const PROCESSNAME = "WEDO Industry - Bosch XDK Gateway"
    , VERSION = "v1.0"
    , AUTHOR  = "Carlos Casares <carlos.casares@oracle.com>"
    , DEMO    = 'WEDOINDUSTRY'
    , PROCESS = 'PROCESS'
    , BLE     = "BLE"
    , IOTCS   = "IOTCS"
    , REST    = "REST"
    , QUEUE   = "QUEUE"
    , DATA    = "DATA"
    , DB      = "DB"
    , KAFKA   = "KAFKA"
    , MQTT    = "MQTT"
    , ALERT   = "ALERT"
    , XDK     = "XDK"
;

const
//      DBHOST              = "https://apex.wedoteam.io"
      DBHOST              = "https://apex.wedoteam.io"
    , DBURI               = '/ords/pdb1/wedoindustry'
    , COMMONURI           = '/ords/pdb1/wedo/common'
    , DBDEMOZONE          = '/setup/demozone/{demozone}'
    , DBIOTCSSETUP        = '/setup/iot/{demozone}/am'
    , EVENTHUBSETUP       = '/setup/eventhub'
    , DBDEVICEDATA        = '/device'
    , MQTTSETUPURI        = "/setup/mqtt"
    , DEVICESETUPURI      = "/devices/{demo}/{demozone}"
    , XDKTRUCKSDEVICEDATA = '/iot/xdk/{demozone}'
    , TIMEOUT             = 2000
    , EVENT               = 'XDK'
    , CONNECTED           = "CONNECTED"
    , DISCONNECTED        = "DISCONNECTED"
    , IOTAPI              = '/iot/api/v2'
    , IOTGETDEVICESCOUNT  = '/devices/count'
    , IOTDEVICE           = '/devices'
    , IOTPROVISION        = '/provisioner/device'
    , DEVICEFILE          = 'device.conf'
    , PASSWORD            = 'Welcome1'
    , DEMOZONEFILE        = '/demozone.dat'
    , CONSUMERGROUPFILEID = '/home/pi/temp/kafka-consumergroup.id'
    , DEFAULTDEMOZONE     = 'MADRID'
;

// MQTT related stuff
const ENABLEMQTTFILE = '/home/pi/temp/USEMQTT'
;

var useMQTT = fs.existsSync(ENABLEMQTTFILE);

log.info(MQTT, "MQTT enabled mode: " + useMQTT);

if (useMQTT) {
  var mqtt       = require('mqtt')
    , mqttClient = _.noop()
    , mqttTopic  = _.noop()
    , xdkDevices = []
    , MQTTBROKER
    , MQTTUSERNAME
    , MQTTPASSWORD
    , MQTTRECONNECTPERIOD
    , MQTTCONNECTTIMEOUT
  ;
}
// MQTT stuff end

// Initialize input arguments
const optionDefinitions = [
  { name: 'help', alias: 'h', type: Boolean },
  { name: 'verbose', alias: 'v', type: Boolean, defaultOption: false }
];
const sections = [
  {
    header: PROCESSNAME,
    content: 'Gateway to send Bosch XDK sensor data to IoTCS'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'verbose',
        alias: 'v',
        description: 'Enable verbose logging.'
      },
      {
        name: 'help',
        alias: 'h',
        description: 'Print this usage guide.'
      }
    ]
  }
];
const options = commandLineArgs(optionDefinitions);
if (options.help) {
  console.log(getUsage(sections));
  process.exit(-1);
}

log.level = (options.verbose) ? 'verbose' : 'info';

// Main handlers registration - BEGIN
// Main error handler
process.on('uncaughtException', function (err) {
  console.log("Uncaught Exception: " + err);
  console.log("Uncaught Exception: " + err.stack);
});
process.on('SIGINT', function() {
  log.info(PROCESS, "Caught interrupt signal");
  log.info(PROCESS, "Exiting gracefully");
  process.removeAllListeners()
  if (typeof err != 'undefined')
    log.error(PROCESS, err)
  process.exit(2);
});
// Main handlers registration - END

var DEMOZONE = _.noop()
;

// IoTCS stuff BEGIN
var setupDemozone    = _.noop()
  , iotsettings      = _.noop()
  , iotClient        = _.noop()
  , iotClientStr     = _.noop()
  , demozoneData     = _.noop()
  , newDeviceId      = _.noop()
  , activationId     = _.noop()
  , provisioningData = _.noop()
  , isNewDevice      = false
  , devices          = []
  , dcl              = _.noop()
  , Device           = require('./device')
  , xdkTrucks        = _.noop()
  , storeFile        = _.noop()
;

var urn = [
  'urn:oracle:wedo:industry:bosch:xdk'
];

var STREAM1 = _.noop()
  , STREAM2 = _.noop()
  , STREAM3 = _.noop()
;

function getModel(device, urn, callback) {
  device.getDeviceModel(urn, function (response, error) {
    if (error) {
      callback(error);
    }
    callback(null, response);
  });
}
// IoTCS stuff END

// Initializing QUEUE variables BEGIN
var q = _.noop();
var queueConcurrency = 1;
// Initializing QUEUE variables END

// Initializing KAFKA BEGIN
var Producer       = kafka.Producer
  , Consumer       = kafka.Consumer
  , kafkaClient    = _.noop()
  , kafkaProducer  = _.noop()
  , kafkaConsumer  = _.noop()
  , kafkaCnxStatus = DISCONNECTED;
;
// Initializing KAFKA END

var dbClient = restify.createJsonClient({
  url: DBHOST,
  connectTimeout: 10000,
  requestTimeout: 10000,
  retry: false,
  rejectUnauthorized: false,
  headers: {
    "content-type": "application/json",
    "accept": "application/json"
  }
});

var currentTruckId = _.noop();

// Helpers BEGIN
function IsJsonString(item) {
  item = typeof item !== "string"
    ? JSON.stringify(item)
    : item;
  try {
    item = JSON.parse(item);
  } catch (e) {
    return false;
  }
  return (typeof item === "object" && item !== null);
}

function validate(payload) {
  if (!payload.demozone || !payload.component || !payload.action) {
    log.verbose(KAFKA, "Ignoring invalid JSON");
    return false;
  }
  if ( payload.demozone.toUpperCase() !== DEMOZONE) {
    log.verbose(KAFKA, "Ignoring action for other demozone (%s vs %s)", payload.demozone, DEMOZONE);
    return false;
  }
  if ( payload.component.toUpperCase() !== XDK) {
    log.verbose(KAFKA, "Ignoring action for invalid component (%s)", payload.component);
    return false;
  }
  if (!_.includes(ACTIONS, payload.action.toUpperCase())) {
    log.verbose(KAFKA, "Ignoring invalid action (%s)", payload.component);
    return false;
  }
  return true;
}
// Helper END

// KAFKA BEGIN

const ACTIONS = [ "START", "STOP" ];
var kafkaSetup = {};

function getUuid() {
  var uuid;
  try {
    uuid = fs.readFileSync(CONSUMERGROUPFILEID, { encoding: 'utf8' });
  } catch (e) {
    uuid = uuidv4();
    fs.writeFileSync(CONSUMERGROUPFILEID, uuid);
  }
  return uuid;
}

function startKafka(cb) {

  var options = {
    host: kafkaSetup.zookeeper,
    groupId: getUuid(),
    sessionTimeout: 15000
  };

  log.verbose(KAFKA, "Starting consumer group with id: '%s' on topic: '%s' in zookeper host: '%s'", options.groupId, kafkaSetup.actiontopic, options.host);

  kafkaConsumer = new kafka.ConsumerGroup(options, kafkaSetup.actiontopic);

  kafkaConsumer.on('connect', () => {
    log.verbose(KAFKA, "Consumer group connected. Member id: '%s'", kafkaConsumer.memberId);
  });

  kafkaConsumer.on('message', (data) => {
    log.verbose(KAFKA, "Incoming message on topic '%s', payload: %s", data.topic, data.value);
    // Start validation
    if (!IsJsonString(data.value)) {
      log.verbose(KAFKA, "Ignoring invalid JSON string");
      return;
    }
    var payload = JSON.parse(data.value);
    if (!validate(payload)) {
      return;
    }
    if (payload.truckid) {
      currentTruckId = XDK + payload.truckid.toUpperCase();
    }

    var xdkDevice = _.noop();
    if (useMQTT) {
      xdkDevice = _.find(xdkDevices, { name: currentTruckId });
    } else {
      xdkDevice = _.find(devices, (d) => { return d.getName() === currentTruckId });
    }
    if (!xdkDevice) {
      log.verbose(IOTCS, "Ignoring '%s' command as no device found for requested truck id '%s'", payload.action, payload.truckid);
      return;
    }

    xdkNodeUtils.sampling(payload.action, payload.timer).catch((err) => log.error(XDK, err));

  });

  kafkaConsumer.on('ready', () => {
    log.verbose(KAFKA, "Consumer ready at topic '%s'", kafkaSetup.actiontopic);
  });

  kafkaConsumer.on('error', (err) => {
    log.error(KAFKA, "Error initializing KAFKA consumer: " + err.message);
  });

  kafkaConsumer.on('disconnected', () => {
    kafkaCnxStatus = DISCONNECTED;
    log.verbose(KAFKA, "Consumer disconnected!");
  });

  kafkaClient = new kafka.Client(kafkaSetup.zookeeper, "RETAIL", {sessionTimeout: 1000});
  kafkaClient.zk.client.on('connected', () => {
    kafkaCnxStatus = CONNECTED;
    log.verbose(KAFKA, "Client connected!");
  });

/**
    // CONSUMER
    log.verbose(KAFKA, "Starting consumer on topic: '%s'", kafkaSetup.actiontopic);
    kafkaConsumer = new Consumer(
      kafkaClient, [ { topic: kafkaSetup.actiontopic, partition: 0 } ], { autoCommit: true }
    );

    kafkaConsumer.on('message', (data) => {
      log.verbose(KAFKA, "Incoming message on topic '%s', payload: %s", data.topic, data.value);
      // Start validation
      if (!IsJsonString(data.value)) {
        log.verbose(KAFKA, "Ignoring invalid JSON string");
        return;
      }
      var payload = JSON.parse(data.value);
      if (!validate(payload)) {
        return;
      }
      if (payload.truckid) {
        currentTruckId = XDK + payload.truckid.toUpperCase();
      }

      var xdkDevice = _.find(devices, (d) => { return d.getName() === currentTruckId });
      if (!xdkDevice) {
        log.verbose(IOTCS, "Ignoring '%s' command as no device found for requested truck id '%s'", payload.action, payload.truckid);
        return;
      }

      xdkNodeUtils.sampling(payload.action, payload.timer).catch((err) => log.error(XDK, err));

    });

    kafkaConsumer.on('ready', () => {
      log.verbose(KAFKA, "Consumer ready at topic '%s'", kafkaSetup.actiontopic);
    });

    kafkaConsumer.on('error', (err) => {
      log.error(KAFKA, "Error initializing KAFKA consumer: " + err.message);
    });

  });
**/
  kafkaClient.zk.client.on('disconnected', () => {
    kafkaCnxStatus = DISCONNECTED;
    log.verbose(KAFKA, "Client disconnected!");
  });
  kafkaClient.zk.client.on('expired', () => {
    kafkaCnxStatus = DISCONNECTED;
    log.verbose(KAFKA, "Client disconnected!");
  });
  // PRODUCER
  kafkaProducer = new Producer(kafkaClient);
  kafkaProducer.on('ready', () => {
    log.info(KAFKA, "Producer ready");
  });
  kafkaProducer.on('error', (err) => {
    log.error(KAFKA, "Error initializing KAFKA producer: " + err.message);
  });
  if (typeof(cb) == 'function') cb();
}

function stopKafka(cb) {
  if (kafkaClient) {
    kafkaClient.close(() => {
      cb();
    });
  } else {
    cb();
  }
}
// KAFKA END

async.series([
  function(next) {
    log.info(PROCESS, "%s - %s", PROCESSNAME, VERSION);
    log.info(PROCESS, "Author - %s", AUTHOR);
    next();
  },
  function(next) {
    // Get demozone Data
    DEMOZONE = DEFAULTDEMOZONE;
    fs.readFile(DEMOZONEFILE,'utf8').then((data)=>{DEMOZONE=data.trim();next();log.info(PROCESS, 'Working for demozone: %s', DEMOZONE);}).catch(() => {});
  },
  function(next) {
    // Try to identify and use the dongle BLE if exists
    try {
      var bledevices = child_process.execSync('hcitool dev').toString().split('\n');
      bledevices.shift();
      bledevices.pop();
      //bledevices.length should have the # of BLE devices available in the Pi
      var BLE_ = _.noop()
        , BUILTIN = _.noop()
      ;
      _.forEach(bledevices, (b) => {
        var s = b.split('\t');
        s.shift();
        // something like: [ 'hci1', 'B8:27:EB:D4:07:48' ]
        if (!s[1].toLowerCase().startsWith('b8')) {
          // Built-in Bluetooth mac address always starts with B8
          BLE_ = parseInt(s[0].replace('hci',''));
        } else {
          BUILTIN = parseInt(s[0].replace('hci',''));
        }
      });
      process.env.NOBLE_HCI_DEVICE_ID = (BLE_ !== undefined) ? BLE_ : BUILTIN;
    } catch(e) {
      process.env.NOBLE_HCI_DEVICE_ID = 0;
    }
    log.info(BLE, "Using BLE device id: " + process.env.NOBLE_HCI_DEVICE_ID);
    next();
  },
  function(next) {
    // Check for internet connectivity; and wait forever if neccessary until it's available
    var internet = false;
    async.whilst(
      function() { return !internet },
      function(c) {
        log.info(PROCESS, "Waiting for internet availability...");
        isOnline({timeout: TIMEOUT}).then(online => { internet = online; c(null, online) });
      },
      function(err, result) {
        log.info(PROCESS, "Internet seems to be available...");
        next();
      }
    );
  },
  function(next) {
    // Retrieve DEMOZONE settings from DB
    log.info(DB, "Retrieving DEMOZONE settings for demozone %s", DEMOZONE);
    dbClient.get(DBURI + DBDEMOZONE.replace('{demozone}', DEMOZONE), (err, req, res, data) => {
      if (err) {
        log.error(DB,"Error from DB call: " + err.statusCode);
        next(err);
        return;
      }
      if (data.items.length === 0) {
        next(new Error("No data found for demozone: " + DEMOZONE));
        return;
      }
      setupDemozone = data.items[0];
      next();
    });
  },
  function(next) {
    if (!setupDemozone || !setupDemozone.setup || setupDemozone.setup.indexOf(XDK) == -1) {
      // Demozone doesn't have XDK setup. Simply abort!!
      log.error(PROCESS, "Demozone '%s' doesn't have XDK enabled. Aborting!", DEMOZONE);
      log.info(PROCESS, "Exiting gracefully");
      process.removeAllListeners();
      process.exit(0);
    } else {
      if (!setupDemozone.xdkmac) {
        // Demozone doesn't have XDK setup. Simply abort!!
        log.error(PROCESS, "Demozone '%s' doesn't have XDK MAC setup. Aborting!", DEMOZONE);
        log.info(PROCESS, "Exiting gracefully");
        process.removeAllListeners();
        process.exit(0);
      } else {
        XDKID = setupDemozone.xdkmac.toLowerCase();
        next();
      }
    }
  },
  function(next) {
    if (!useMQTT) {
      // Retrieve IoTCS settings from DB
      log.info(DB, "Retrieving IoTCS settings for demozone %s", DEMOZONE);
      dbClient.get(DBURI + DBIOTCSSETUP.replace('{demozone}', DEMOZONE), (err, req, res, data) => {
        if (res.statusCode === 404) {
          next(new Error("No data found for demozone: " + DEMOZONE));
          return;
        }
        if (err) {
          log.error(DB,"Error from DB call: " + err.statusCode);
          next(err);
          return;
        }
        iotsettings = _.clone(data);
        log.verbose(DB, "IoTCS settings:");
        log.verbose(DB, "Hostname:    %s", iotsettings.url);
        log.verbose(DB, "Credentials: %s / %s", iotsettings.username, iotsettings.password);
        next();
      });
    } else {
      // Retrieve MQTT settings from DB
      log.info(DB, "Retrieving MQTT settings");
      dbClient.get(COMMONURI + MQTTSETUPURI, (err, req, res, data) => {
        if (res.statusCode === 404) {
          next(new Error("No data found!!!"));
          return;
        }
        if (err) {
          log.error(DB,"Error from DB call: " + err.statusCode);
          next(err);
          return;
        }
        MQTTBROKER = data.broker;
        MQTTUSERNAME = data.username;
        MQTTPASSWORD = data.password;
        MQTTRECONNECTPERIOD = data.reconnectperiod;
        MQTTCONNECTTIMEOUT = data.connecttimeout;
        next();
      });
    }
  },
  function(next) {
    if (useMQTT) {
      log.info(MQTT, "Retrieving device settings for demo '%s' and demozone '%s'", DEMO, DEMOZONE);
        dbClient.get(COMMONURI + DEVICESETUPURI.replace('{demo}', DEMO).replace('{demozone}', DEMOZONE), (err, req, res, data) => {
        if (res.statusCode === 404) {
          next(new Error("No data found!!!"));
          return;
        }
        if (err) {
          log.error(DB,"Error from DB call: " + err.statusCode);
          next(err);
          return;
        }
        _.forEach(data.items, (d) => {
          if (d.devicename.startsWith('XDK')) {
            xdkDevices.push({
              name: d.devicename,
              deviceid: d.deviceid,
              urn: JSON.parse(d.urns)[0],
              mqtttopic: d.mqtttopic
            });
          }
        });
        if (xdkDevices.length == 0) {
          next(new Error("No data found!!!"));
          return;
        }
        next();
      });
    } else {
      next();
    }
  },
  function(next) {
    if (useMQTT) {
      // Initialize mqtt
      log.info(MQTT, "Connecting to MQTT broker at %s", MQTTBROKER);
      mqttClient  = mqtt.connect(MQTTBROKER, { username: MQTTUSERNAME, password: MQTTPASSWORD, reconnectPeriod: MQTTRECONNECTPERIOD, connectTimeout: MQTTCONNECTTIMEOUT });
      mqttClient.connected = false;

      // Common event handlers
      mqttClient.on('connect', () => {
        log.info(MQTT, "Successfully connected to MQTT broker at %s", MQTTBROKER);
        mqttClient.connected = true;
      });

      mqttClient.on('error', err => {
        log.error(MQTT, "Error: ", err);
      });

      mqttClient.on('reconnect', () => {
        log.verbose(MQTT, "Client trying to reconnect...");
      });

      mqttClient.on('offline', () => {
        mqttClient.connected = false;
        log.warn(MQTT, "Client went offline!");
      });

      mqttClient.on('end', () => {
        mqttClient.connected = false;
        log.info(MQTT, "Client ended");
      });
      next();
    } else {
      next();
    }
  },
  function(next) {
    log.verbose(PROCESS, "Retrieving EventHub setup");
    dbClient.get(DBURI + EVENTHUBSETUP, function(err, req, res, obj) {
      if (err) {
        next(err.message);
      }
      var jBody = JSON.parse(res.body);
      kafkaSetup.zookeeper   = jBody.zookeeperhost;
      kafkaSetup.eventtopic  = jBody.eventtopic.replace('{demozone}', DEMOZONE.toLowerCase());
      kafkaSetup.actiontopic = jBody.actiontopic.replace('{demozone}', DEMOZONE.toLowerCase());
      next();
    });
  },
  function(next) {
    if (!useMQTT) {
      // "Ping" IoTCS by calling the "Get Devices Count" simple API
      iotClient = restify.createJsonClient({
        url: iotsettings.url,
        contentType: 'application/json',
        accept: 'application/json',
        connectTimeout: TIMEOUT,
        requestTimeout: TIMEOUT,
        retry: {
          'retries': 0
        },
        rejectUnauthorized: false
      });
      iotClient.basicAuth(iotsettings.username, iotsettings.password);
      // Create an String one to download the provisioning file
      iotClientStr = restify.createStringClient({
        url: iotsettings.url,
        contentType: 'application/json',
        accept: 'text/plain',
        connectTimeout: TIMEOUT,
        requestTimeout: TIMEOUT,
        retry: {
          'retries': 0
        },
        rejectUnauthorized: false
      });
      iotClientStr.basicAuth(iotsettings.username, iotsettings.password);

      log.info(IOTCS, "Pinging IoTCS instance at %s...", iotsettings.url)
      iotClient.get(IOTAPI + IOTGETDEVICESCOUNT, (err, req, res, data) => {
        if (err) {
          if (err.statusCode) {
            log.error(IOTCS,"Error pinging IoTCS instance: " + err.statusCode);
          }
          next(err);
          return;
        }
        if (res.statusCode !== 200) {
          next(new Error("Unexpected HTTP return code when pinging IoTCS instance: " + res.statusCode));
          return;
        }
        log.info(IOTCS, "IoTCS instance at %s successfully ping'ed", iotsettings.url)
        next();
      });
    } else {
      next();
    }
  },
  function(next) {
    if (!useMQTT) {
      // Retrieve XDK TRUCKS provisioning data
      log.info(DB, "Retrieving XDK TRUCKS device provisioning data for demozone %s", DEMOZONE);
      dbClient.get(DBURI + XDKTRUCKSDEVICEDATA.replace('{demozone}', DEMOZONE), (err, req, res, data) => {
        if (err) {
          log.error(DB,"Error from DB call: " + err.statusCode);
          next(err);
          return;
        }
        if (data.items.length === 0) {
          next(new Error("No data found for demozone: " + DEMOZONE));
          return;
        }
        xdkTrucks = data.items;
        next();
      });
    } else {
      next();
    }
  },
  function(next) {
    if (!useMQTT) {
      // Remove any existing "*.conf" file
      glob('*.conf', (er, files) => {
        _.forEach(files, (f) => {
          fs.removeSync(f);
        });
        next();
      });
    } else {
      next();
    }
  },
  function(next) {
    if (!useMQTT) {
      // Create conf files based on data retrieved from DB and create as many XDK devices as needed
      dcl = require('./device-library.node');
      dcl = dcl({debug: false});
      async.eachSeries( xdkTrucks, (xdkTruck, nextXdk) => {
        log.verbose(IOTCS, "Retrieving provisioning data for device '%s'", xdkTruck.deviceid);
        // We have the device ID and the provisioning data. Create the provisioning file
        var file = xdkTruck.deviceid.toUpperCase() + '.conf';
        fs.outputFileSync(file, xdkTruck.provisiondata);
        // Create and init Device object and push it to the array
        var xdkDevice = new Device(xdkTruck.deviceid.toUpperCase(), log);
        xdkDevice.setStoreFile(file, PASSWORD);
        xdkDevice.setUrn(urn);
        devices.push(xdkDevice);
        log.verbose(IOTCS, "Device successfully registered: %s", xdkTruck.deviceid.toUpperCase());
        nextXdk();
      }, (err) => {
        next(err);
      });
    } else {
      next();
    }
  },
  function(next) {
    if (!useMQTT) {
      log.info(IOTCS, "Initializing IoTCS devices");
      log.info(IOTCS, "Using IoTCS JavaScript Libraries v" + dcl.version);
      async.eachSeries( devices, function(d, callbackEachSeries) {
        async.series( [
          function(callbackSeries) {
            // Initialize Device
            log.info(IOTCS, "Initializing IoT device '" + d.getName() + "'");
            d.setIotDcd(new dcl.device.DirectlyConnectedDevice(d.getIotStoreFile(), d.getIotStorePassword()));
            callbackSeries(null);
          },
          function(callbackSeries) {
            // Check if already activated. If not, activate it
            if (!d.getIotDcd().isActivated()) {
              log.verbose(IOTCS, "Activating IoT device '" + d.getName() + "'");
              d.getIotDcd().activate(d.getUrn(), function (device, error) {
                if (error) {
                  log.error(IOTCS, "Error in activating '" + d.getName() + "' device (" + d.getUrn() + "). Error: " + error.message);
                  callbackSeries(error);
                }
                d.setIotDcd(device);
                if (!d.getIotDcd().isActivated()) {
                  log.error(IOTCS, "Device '" + d.getName() + "' successfully activated, but not marked as Active (?). Aborting.");
                  callbackSeries("ERROR: Successfully activated but not marked as Active");
                }
                callbackSeries(null);
              });
            } else {
              log.verbose(IOTCS, "'" + d.getName() + "' device is already activated");
              callbackSeries(null);
            }
          },
          function(callbackSeries) {
            // When here, the device should be activated. Get device models, one per URN registered
            async.eachSeries(d.getUrn(), function(urn, callbackEachSeriesUrn) {
              getModel(d.getIotDcd(), urn, (function (error, model) {
                if (error !== null) {
                  log.error(IOTCS, "Error in retrieving '" + urn + "' model. Error: " + error.message);
                  callbackEachSeriesUrn(error);
                } else {
                  d.setIotVd(urn, model, d.getIotDcd().createVirtualDevice(d.getIotDcd().getEndpointId(), model));
                  log.verbose(IOTCS, "'" + urn + "' intialized successfully");
                }
                callbackEachSeriesUrn(null);
              }).bind(this));
            }, function(err) {
              if (err) {
                callbackSeries(err);
              } else {
                callbackSeries(null, true);
              }
            });
          }
        ], function(err, results) {
          callbackEachSeries(err);
        });
      }, function(err) {
        if (err) {
          next(err);
        } else {
          log.info(IOTCS, "IoTCS device initialized successfully");
          next(null);
        }
      });
    } else {
      next();
    }
  },
  function(next) {
    if (!useMQTT) {
      // If new device, get updated provisioning file contents and update DB
      if (isNewDevice) {
        var body = {
          deviceid: newDeviceId,
          data: provisioningData
        }
        log.verbose(DB, "Upserting device data in DB for demozone %s", DEMOZONE);
        dbClient.post(DBURI + DBDEVICEDATA + '/' + DEMOZONE, body, (err, req, res, data) => {
          if (err) {
            log.error(DB,"Error from DB call: " + err.statusCode);
            n(err);
            return;
          }
          log.verbose(DB, "Device data in DB for demozone %s successfully upserted", DEMOZONE);
          next(null);
        });
      } else {
        next(null);
      }
    } else {
      next();
    }
  },
  function(next) {
    // Initialize Queue system
    log.info(QUEUE, "Initializing QUEUE system");
    q = queue(queueConcurrency, (task, done) => {
      // Get device based on currentTruckId

      if (_.has(task.data, 'accelerometer')) {
        STREAM1 = task.data;
      }
      if (_.has(task.data, 'magneticfield')) {
        STREAM2 = task.data;
      }
      if (_.has(task.data, 'light')) {
        STREAM3 = task.data;
      }

      if (STREAM1 && STREAM2 && STREAM3) {
        // Send all data in one single stream
        var payload = {};
        payload.accelX      = Number(STREAM1.accelerometer.x);
        payload.accelY      = Number(STREAM1.accelerometer.y);
        payload.accelZ      = Number(STREAM1.accelerometer.z);
        payload.gyroX       = Number(STREAM1.gyrometer.x);
        payload.gyroY       = Number(STREAM1.gyrometer.y);
        payload.gyroZ       = Number(STREAM1.gyrometer.z);
        payload.magX        = Number(STREAM2.magneticfield.x);
        payload.magY        = Number(STREAM2.magneticfield.y);
        payload.magZ        = Number(STREAM2.magneticfield.z);
        payload.magR        = Number(STREAM2.magneticfield.r);
        payload.light       = Number(STREAM3.light);
        payload.noise       = Number(STREAM3.noise);
        payload.pressure    = Number(STREAM3.pressure);
        payload.temperature = Number(STREAM3.temperature);
        payload.humidity    = Number(STREAM3.humidity);
        if (!useMQTT) {
          var xdkDevice = _.find(devices, (d) => { return d.getName() === currentTruckId });
          var vd = xdkDevice.getIotVd(urn[0]);
          if (vd) {
            log.verbose(IOTCS, "Updating data: %j", payload);
            vd.update(payload);
          }
        } else {
          // Send through MQTT
          // currentTruckId contains MADX52
          let d = _.find(xdkDevices, { name: currentTruckId });
          if (!d) {
            log.error(MQTT, "Current truck with id '%s' not found in MQTT settings!", currentTruckId);
          } else {
            let mqttTopic =   d.mqtttopic + '/' + d.deviceid;
            let body = {
              type: "data",
              urn: d.urn,
              payload: payload
            }
            log.info(MQTT, "Publishing to topic '%s': %j", mqttTopic, body);
            mqttClient.publish(mqttTopic, JSON.stringify(body));
          }
        }
        // If KAFKA is available, send event
        if (kafkaProducer) {
          var kafkaMessage = {
            demozone: DEMOZONE,
            eventname: XDK,
            payload: payload
          };
          kafkaProducer.send([{ topic: kafkaSetup.eventtopic, messages: JSON.stringify(kafkaMessage), partition: 0 }], (err, data) => {
            if (err) {
              log.error(KAFKA, err);
            } else {
              log.verbose(KAFKA, "Message sent to topic %s, partition %s and id %d", Object.keys(data)[0], Object.keys(Object.keys(data)[0])[0], data[Object.keys(data)[0]][Object.keys(Object.keys(data)[0])[0]]);
            }
          });
        }
        STREAM1 = STREAM2 = STREAM3 = _.noop();
      }
      done(); // Let queue handle next task
    });
    log.info(QUEUE, "QUEUE system initialized successfully");
    next(null);
  },
  function(next) {
    // Start Kafka client
//    log.verbose(KAFKA, "Connecting to Zookeper host at %s...", kafkaSetup.zookeeper);
    startKafka(next);
  },
  function(next) {
    XdkNodeUtils = require('./xdkNodeUtils')
    xdkNodeUtils = new XdkNodeUtils(log.level);
    xdkNodeUtils.on('on', () => {
      log.verbose(PROCESS,"BLE on, scanning for XDK device with MAC address: '%s'...", XDKID);
      xdkNodeUtils.scan(XDKID)
        .catch((err) => log.error(PROCESS, err));
    });

    xdkNodeUtils.on('discovered', () => {
      log.verbose(PROCESS,"XDK discovered, trying to connect...");
      xdkNodeUtils.connect()
        .catch((err) => log.error(PROCESS, err));
    });

    xdkNodeUtils.on('data', data => {
      q.push({
        type: DATA,
        data: data
      });
    });
    next();
  },
  function(next) {
  }
/**
  function(next) {
    // Complete the REST setup and listen to POST commands only when everything else is up & running
    router.post(sendDataURI, function(req, res) {
      var urn = req.params.urn;
      var body = req.body;
      log.verbose(REST, "Send '" + DATA + "' method invoked for URN '" + urn + "' with data: %j", body);
      q.push({
        type: DATA,
        urn: urn,
        data: body
      });
      res.send({result:"Message queued for processing"});
    });
    router.post(sendAlertURI, function(req, res) {
      var urn = req.params.urn;
      var body = req.body;
      log.verbose(REST, "Send '" + ALERT + "' method invoked for URN '" + urn + "' with data: %j", body);
      q.push({
        type: ALERT,
        urn: urn,
        data: body
      });
      res.send({result:"Message queued for processing"});
    });
    next(null);
  }
**/
], function(err, results) {
  if (err) {
    log.error(PROCESS, err.message);
    process.exit(2);
  }
  log.info(PROCESS, "Initialization done");
});
