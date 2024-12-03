// Mqtt broker for local network.
// Sits on internal framework code with a few hacks
// Has Serial2 link to hive mqtt bridge.
// Has a tinyRTC on I2C as a system master clock to synch ESP32 time and serve time synch requests
// Uses PICOMQTT as an mqtt server on fixed IP (nominally x.x.x.200)
// Has a basic dns to map mqttid to ip
// Has topic conventions:
// - any external topic from Serial2 published locally.
// - any local topic starting 'mb/f/' sent to Serial2 less the 'mb/f/'
// - any local topic starting 'mb/s/' is a periodic synch request for time and dns
// note: mb is the properties mqttid for the broker

// Hive bridge in separate ESP32 to give reliable service

// Not really much to it. All Mqtt client code commented out


const char* module = "mqttbroker-1";
const char* buildDate = __DATE__  "  "  __TIME__;

// issued: sort mqttin / mqttsend in showng zero

// ----------- start properties include 1 --------------------
#include <SPIFFS.h>
#include <ArduinoJson.h>
String propFile = "/props.properties";   // prop file name
#define PROPSSIZE 40
String propNames[PROPSSIZE];
int propNamesSz = 0;
#define BUFFLEN 200
char buffer[BUFFLEN];
int bufPtr = 0;
#include <esp_task_wdt.h>
#include <EEPROM.h>
// ----------- end properties include 1 --------------------

// ----------- start WiFi & Mqtt & Telnet include 1 ---------------------
#define HAVEWIFI 1
// sort of a DNS
#define DNSSIZE 20
#define SYNCHINTERVAL 30
// wifi, time, mqtt
#include <ESP32Time.h>
ESP32Time rtc;
#include <WiFi.h>
#include "ESPTelnet.h"
#include "EscapeCodes.h"
#include <PicoMQTT.h>
ESPTelnet telnet;
IPAddress ip;
// PicoMQTT::Client mqttClient;  // broker only

// ----------- end  WiFi & Mqtt & Telnet include 1 ---------------------

// ---------- start custom ------------------------
// broker only
PicoMQTT::Server mqttBroker;
// serial2
#define RXD2 16
#define TXD2 17
#define BRATE 115200
// tiny RTC
#include <RTClib.h>
#include <RTClib.h>
RTC_DS1307 tinyrtc;

// ---------- end custom ------------------------

// --------- standard properties ------------------
int logLevel = 2;
String logLevelN = "logLevel";
int eeWriteLimit = 100;
String eeWriteLimitN = "eeWriteLimit";
String wifiSsid = "<ssid>";
String wifiSsidN = "wifiSsid";        
String wifiPwd = "<pwd>";
String wifiPwdN = "wifiPwd";
byte wifiIp4 = 0;   // > 0 for fixed ip
String wifiIp4N = "wifiIp4";
//byte mqttIp4 = 200;
//String mqttIp4N = "mqttIp4";
//int mqttPort = 1883;
//String mqttPortN = "mqttPort";   
int telnetPort = 23;
String telnetPortN = "telnetport";   
String mqttId = "xx";          // is username-N and token xx/n/.. from unitId
String mqttIdN = "mqttId";   
int unitId  = 9;                  // uniquness of mqtt id
String unitIdN = "unitId";
int wdTimeout = 30;
String wdTimeoutN = "wdTimeout";
// generic way of setting property as 2 property setting operations
String propNameA = "";
String propNameN = "propName";
String propValue = "";
String propValueN = "propValue";
// these used to apply adjustment via props system
String restartN = "restart";
String writePropsN = "writeProps";

// ------- custom properties -----------
// setting tiny rtc
String hourN = "hour";
String minN = "min";
String secN = "sec";
String dayN = "day";
String monthN = "month";
String yearN = "year";
// ------- end custom properties -----------

// ----------- start properties include 2 --------------------

bool mountSpiffs()
{
   if(!SPIFFS.begin(true))
  {
    log(1, "SPIFFS Mount Failed");
    return false;
  }
  else
  {
    log(1, "SPIFFS formatted");
  }
  return true;
}

// checks a property name in json doc and keeps a list in propNames
bool checkProp(JsonDocument &doc, String propName, bool reportMissing)
{
  if (propNamesSz >= PROPSSIZE)
  {
    log(0, "!! props names limit");
  }
  else
  {
    propNames[propNamesSz++] = propName;
  }
  if (doc.containsKey(propName))
  {
    String val = doc[propName].as<String>();
    log(0, propName + "=" + val);
    return true;
  }
  if (reportMissing)
  {
    log(0, propName + " missing");
  }
  return false;
}

bool readProps()
{
  log(0, "Reading file " + propFile);
  File file = SPIFFS.open(propFile);
  if(!file || file.isDirectory())
  {
    log(0, "− failed to open file for reading");
    return false;
  }
  JsonDocument doc;
  DeserializationError error = deserializeJson(doc, file);
  if (error) 
  {
    log(0, "deserializeJson() failed: ");
    log(0, error.f_str());
    return false;
  }
  extractProps(doc, true);
  return true;
}

// writes/displays the props
bool writeProps(bool noWrite)
{
  JsonDocument doc;
  addProps(doc);
  String s;
  serializeJsonPretty(doc, s);
  s = s.substring(3,s.length()-2);
  log(0, s);
  if (noWrite)
  {
    return true;
  }
  log(0, "Writing file:" + propFile);
  File file = SPIFFS.open(propFile, FILE_WRITE);
  if(!file)
  {
    log(0, "− failed to open file for write");
    return false;
  }
  serializeJson(doc, file);
  file.close();
  return true;
}

// is expected to be form 'name=value' or 'name value' and can be a comma sep list
// name can be case insensitve match on first unique characters..
// converted to Json to update
void adjustProp(String s)
{
  String ss = s;
  while (true)
  {
    int p1 = ss.indexOf(',');
    if (p1 < 0)
    {
      adjustProp2(ss);
      return;
    }
    String s1 = ss.substring(0, p1);
    adjustProp2(s1);
    ss = ss.substring(p1+1);
  }
}
void adjustProp2(String s)
{
  int p1 = s.indexOf('=');
  if (p1 < 0)
  {
    p1 = s.indexOf(' ');
  }
  if (p1 < 0)
  {
    log(0, "no = or space found");
    return;
  }
  String p = s.substring(0,p1);
  String pl = p;
  pl.toLowerCase();
  String v = s.substring(p1+1);
  int ip;
  int m = 0;
  for (int ix = 0; ix < propNamesSz; ix++)
  {
    if (propNames[ix].length() >= pl.length())
    {
      String pn = propNames[ix].substring(0, pl.length());
      pn.toLowerCase();
      if (pl == pn)
      {
        if (m == 0)
        {
          ip = ix;
          m++;
        }
        else
        {
          if (m == 1)
          {
            log(0, "duplicate match " + p + " " + propNames[ip]);
          }
          m++;
          log(0, "duplicate match " + p + " " + propNames[ix]);
        }
      }
    }
  }
  if (m > 1)
  {
    return;
  }
  else if (m==0)
  {
    log(0, "no match " + p);
    return;
  }
  s = "{\"" + propNames[ip] + "\":\"" + v + "\"}";
 
  JsonDocument doc;
  DeserializationError error = deserializeJson(doc, s);
  if (error) 
  {
    log(0, "deserializeJson() failed: ");
    log(0, error.f_str());
    return;
  }
  extractProps(doc, false);
}

// logger
void log(int level, String s)
{
  if (level > logLevel) return;
  Serial.println(s);
  #if HAVEWIFI
  telnet.println(s);
  #endif
}
void log(int level)
{
  if (level > logLevel) return;
  Serial.println();
  #if HAVEWIFI
  telnet.println();
  #endif
}
void loga(int level, String s)
{
  if (level > logLevel) return;
  Serial.print(s);
  #if HAVEWIFI
  telnet.print(s);
  #endif
}

void checkSerial()
{
  while (Serial.available())
  {
    char c = Serial.read();
    switch (c)
    {
      case 0:
        break;
      case '\r':
        break;
      case '\n':
        buffer[bufPtr++] = 0;
        processCommandLine(String(buffer));
        bufPtr = 0;
        break;
      default:
        if (bufPtr < BUFFLEN -1)
        {
          buffer[bufPtr++] = c;
        }
        break;
    }
  } 
}
// for counting restarts - write to eprom
struct eeStruct
{
  unsigned int writes = 0;
  unsigned int wdtRestart = 0;
  unsigned int isrRestart = 0;
  unsigned int panicRestart = 0;
  unsigned int otherRestart = 0;
};

eeStruct eeData;

// for reliability stats from each restart
int getGatewayCount = 0;
int getWifiCount = 0;
int reConnWifiCount = 0;
unsigned long mqttDiscMs = 0;
int mqttConnCount = 0;
int mqttDiscCount = 0;
int mqttConnFailCount = 0;
int mqttConnTimeMs = 0;
int mqttSendCount = 0;
int mqttInCount = 0;

void eeDataReset()
{
  eeData.writes = 0;
  eeData.isrRestart = 0;
  eeData.wdtRestart = 0;
  eeData.panicRestart = 0;
  eeData.otherRestart = 0;
}

void eepromInit()
{
  int eeSize = sizeof(eeData);
  EEPROM.begin(eeSize);
  log(0, "ee size "+ String(eeSize));
}
void eepromWrite()
{
  eeData.writes++;
  if (eeData.writes > eeWriteLimit)
  {
    log(0, "eeprop Write limit..");
    return;
  }
  EEPROM.put(0, eeData);
  EEPROM.commit();
  log(0, "eewrite:" + String(eeData.writes));
}
void eepromRead()
{
  EEPROM.get(0, eeData);
  log(0, "eeWrites: " + String(eeData.writes));
}
void checkRestartReason()
{
  eepromRead();
  int resetReason = esp_reset_reason();
  log(0, "ResetReason: " + String(resetReason));
  switch (resetReason)
  {
    case ESP_RST_POWERON:
      return;// ok
    case ESP_RST_PANIC:
      eeData.panicRestart++;
      break;
    case ESP_RST_INT_WDT:
      eeData.isrRestart++;
      break;
    case ESP_RST_TASK_WDT:
      eeData.wdtRestart++;
      break;
    default:
      eeData.otherRestart++;
      break;
  }
  eepromWrite();
  logResetStats();
}

void logResetStats()
{
  log(0, "eeWrites: " + String(eeData.writes));
  log(0, "panic: " + String(eeData.panicRestart));
  log(0, "taskwd: " + String(eeData.wdtRestart));
  log(0, "irswd: " + String(eeData.isrRestart));
  log(0, "other: " + String(eeData.otherRestart));
  #if HAVEWIFI
  log(0, "getGateway: " + String(getGatewayCount));
  log(0, "getWifi: " + String(getWifiCount));
  log(0, "reconnWifi: " + String(reConnWifiCount));
  log(0, "mqttConn: " + String(mqttConnCount));
  log(0, "mqttConnT: " + String(mqttConnTimeMs));
  log(0, "mqttDisc: " + String(mqttDiscCount));
  log(0, "mqttFail: " + String(mqttConnFailCount));
  log(0, "mqttSend: " + String(mqttSendCount));
  log(0, "mqttIn: " + String(mqttInCount));
  log(0, "wfChannel: " + String(WiFi.channel()));
  log(0, "wfRSSI: " + String(WiFi.RSSI()));
  log(0, "wfPower: " + String(WiFi.getTxPower()));
  #endif
}
// ----------- end properties include 2 --------------------

// ----------- custom properties modify section  --------------------
// extract and add properties to json doc
// customize this for props expected and data types - watch with bools

void extractProps(JsonDocument &doc, bool reportMissing)
{
  propNamesSz = 0;
  log(0, "setting properties:");
  String propName;
  propName = logLevelN; if (checkProp(doc, propName, reportMissing)) logLevel = doc[propName].as<int>();
  propName = eeWriteLimitN; if (checkProp(doc, propName, reportMissing)) eeWriteLimit = doc[propName].as<int>();
  propName = wifiSsidN; if (checkProp(doc, propName, reportMissing)) wifiSsid = doc[propName].as<String>();
  propName = wifiPwdN;  if (checkProp(doc, propName, reportMissing)) wifiPwd = doc[propName].as<String>();
  propName = wifiIp4N;  if (checkProp(doc, propName, reportMissing)) wifiIp4 = doc[propName].as<byte>();
  //propName = mqttPortN; if (checkProp(doc, propName, reportMissing)) mqttPort = doc[propName].as<int>();
  //propName = mqttIp4N;  if (checkProp(doc, propName, reportMissing)) mqttIp4 = doc[propName].as<byte>();
  propName = telnetPortN;if (checkProp(doc, propName, reportMissing)) telnetPort = doc[propName].as<int>();
  propName = mqttIdN;   if (checkProp(doc, propName, reportMissing)) mqttId = doc[propName].as<String>();
  propName = unitIdN;   if (checkProp(doc, propName, reportMissing)) unitId = doc[propName].as<int>();
  propName = wdTimeoutN;if (checkProp(doc, propName, reportMissing)) wdTimeout = max(doc[propName].as<int>(),30);
  // these just for adjustment
  propName = restartN; if (checkProp(doc, propName, false)) ESP.restart();
  propName = writePropsN; if (checkProp(doc, propName, false)) writeProps(false);
  propName = propNameN; if (checkProp(doc, propName, false)) propNameA = doc[propName].as<String>();
  propName = propValueN;if (checkProp(doc, propName, false)) propValue = doc[propName].as<String>();  // picked up in checkState()

  // ----- start custom extract -----
  propName = yearN; if (checkProp(doc, propName, false)) setTinyRtc(doc[propName].as<int>(),propName);
  propName = monthN; if (checkProp(doc, propName, false)) setTinyRtc(doc[propName].as<int>(),propName);
  propName = dayN; if (checkProp(doc, propName, false)) setTinyRtc(doc[propName].as<int>(),propName);
  propName = hourN; if (checkProp(doc, propName, false)) setTinyRtc(doc[propName].as<int>(),propName);
  propName = minN; if (checkProp(doc, propName, false)) setTinyRtc(doc[propName].as<int>(),propName);
  propName = secN; if (checkProp(doc, propName, false)) setTinyRtc(doc[propName].as<int>(),propName);
  // ----- end custom extract -----
}

// adds props for props write - customize
void addProps(JsonDocument &doc)
{
  doc[logLevelN] = logLevel;
  doc[eeWriteLimitN] = eeWriteLimit;
  doc[wifiSsidN] = wifiSsid;
  doc[wifiPwdN] = wifiPwd;
  doc[wifiIp4N] = wifiIp4;
  //doc[mqttIp4N] = mqttIp4;
  doc[telnetPortN] = telnetPort;
  //doc[mqttPortN] = mqttPort;
  doc[mqttIdN] = mqttId;
  doc[unitIdN] = unitId;
  doc[wdTimeoutN] = wdTimeout;

  // ----- start custom add -----
  // ----- end custom add -----
}

// custom modified section for props control and general commands
void processCommandLine(String cmdLine)
{
  if (cmdLine.length() == 0)
  {
    return;
  }
  
  switch (cmdLine[0])
  {
    case 'h':
    case '?':
      log(0, "v:version, w:writeprops, d:dispprops, l:loadprops p<prop>=<val>: change prop, r:restart");
      log(0, "s:showstats, z:zerostats, n:dns, 0,1,2:loglevel = " + String(logLevel));
      return;
    case 'w':
      writeProps(false);
      return;
    case 'd':
      writeProps(true);
      return;
    case 'l':
      readProps();
      return;
    case 'p':
      adjustProp(cmdLine.substring(1));
      return;
    case 'r':
      ESP.restart();
      return;
    case 'v':
      loga(0, module);
      loga(0, " ");
      log(0, buildDate);
      return;
    case 'z':
      eeDataReset();
      return;
    case 's':
      logResetStats();
      return;
    case 't':
      {
        tm now = rtc.getTimeStruct();
        log(0, "ESP time: " + dateTimeIso(now));
        DateTime tinyNow = tinyrtc.now();
        log(0, "tiny time: " + dateTimeIsoTiny(tinyNow));
        return;
      }
    case 'n':
      logDns();
      break;
    case '0':
      logLevel = 0;
      log(0, " loglevel=" + String(logLevel));
      return;
    case '1':
      logLevel = 1;
      log(0, " loglevel=" + String(logLevel));
      return;
    case '2':
      logLevel = 2;
      log(0, " loglevel=" + String(logLevel));
      return;

  // ----- start custom cmd -----

  // ----- end custom cmd -----
    default:
      log(0, "????");
      return;
  }
}
// ----------- end custom properties modify section  --------------------

// ------------ start wifi and mqtt include section 2 ---------------
IPAddress localIp;
IPAddress gatewayIp;
IPAddress primaryDNSIp;
String mqttMoniker;

int recoveries = 0;
unsigned long seconds = 0;
unsigned long lastSecondMs = 0;
unsigned long startRetryDelay = 0;
int long retryDelayTime = 10;  // seconds
bool retryDelay = false;


// state engine
#define START 0
#define STARTGETGATEWAY 1
#define WAITGETGATEWAY 2
#define STARTCONNECTWIFI 3
#define WAITCONNECTWIFI 4
#define ALLOK 5

String stateS(int state)
{
  if (state == START) return "start";
  if (state == STARTGETGATEWAY) return "startgetgateway";
  if (state == WAITGETGATEWAY) return "waitgetgateway";
  if (state == STARTCONNECTWIFI) return "startconnectwifi";
  if (state == WAITCONNECTWIFI) return "waitconnectwifi";
  if (state == ALLOK) return "allok";
  return "????";
}
int state = START;

unsigned long startWaitWifi;

bool startGetGateway()
{
  getGatewayCount++;
  WiFi.disconnect();
  delay (500);
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(false);
  log(1, "Start wifi dhcp" + wifiSsid + " " + wifiPwd);
  WiFi.begin(wifiSsid, wifiPwd);
  startWaitWifi = millis();
  return true;
}
bool startWifi()
{
  getWifiCount++;
  WiFi.disconnect();
  WiFi.setAutoReconnect(true);
  delay (500);
  unsigned long startWaitWifi = millis();
  WiFi.mode(WIFI_STA);
  if (wifiIp4 == 0)
  {
    log(1, "Start wifi dhcp: " + wifiSsid + " " + wifiPwd);
  }
  else
  {
    IPAddress subnet(255, 255, 0, 0);
    IPAddress fixedIp = localIp;
    fixedIp[3] = wifiIp4;
    if (!WiFi.config(fixedIp, gatewayIp, subnet, primaryDNSIp, primaryDNSIp)) 
    {
      log(1, "STA Failed to configure");
      return false;
    }
    log(1, "Start wifi fixip: " + wifiSsid + " " + wifiPwd);
  }
  WiFi.begin(wifiSsid, wifiPwd);
  return true;
}

int waitWifi()
{
  // 0=waiting, <0=fail, >0=ok
  unsigned long et = millis() - startWaitWifi;
  if (WiFi.status() == WL_CONNECTED)
  {
    localIp = WiFi.localIP();
    gatewayIp = WiFi.gatewayIP();
    primaryDNSIp = WiFi.dnsIP();
    log(1, "connected, t=" + String(et) + ", local=" + localIp.toString() + " gateway=" + gatewayIp.toString() + " dns=" + primaryDNSIp.toString());
    reConnWifiCount--;
    return 1;
  }
  
  if (et > 30000)
  {
    log(1, "... fail wifi connection timeout");
    return -1;
  }
  return 0;
}


// dns support
struct dnsIsh
{
  bool used = false;
  String name;
  String ip;
  int timeout;
};
dnsIsh dnsList[DNSSIZE];
unsigned long dnsVersion = 0;
unsigned long lastSynchTime = 0;

void logDns()
{
  log(0, "dns v=" + String(dnsVersion));
  for (int ix = 0; ix < DNSSIZE; ix++)
  {
    if (dnsList[ix].used && dnsList[ix].timeout > 0)
    {
      log(0, String(ix) + " " + dnsList[ix].name + " " + dnsList[ix].ip);
    }
  }
}

String dnsGetIp(String name)
{
  for (int ix = 0; ix < DNSSIZE; ix++)
  {
    if (dnsList[ix].used && dnsList[ix].name.startsWith(name))
    {
      return dnsList[ix].ip;
    }
  }
  return "";
}

// ESP32 Time
String formatd2(int i)
{
  if (i < 10)
  {
    return "0" + String(i);
  }
  return String(i);
}
String dateTimeIso(tm d)
{
  return String(d.tm_year+1900)+"-"+formatd2(d.tm_mon+1)+"-"+formatd2(d.tm_mday)+"T"+formatd2(d.tm_hour)+":"+formatd2(d.tm_min)+":"+formatd2(d.tm_sec);
}

#if 1
void synchCheck()
{
}
#else
// time and dns synch
void sendSynch()
{
  // will get updates if not in synch
  JsonDocument doc;
  doc["r"] = mqttMoniker + "/c/s";    // reply token
  doc["n"] = mqttId + String(unitId);
  doc["i"] = localIp.toString();
  doc["e"] = rtc.getEpoch();
  doc["v"] = dnsVersion;
  mqttSend("mb/s", doc);
}

void synchCheck()
{
  if (seconds - lastSynchTime > SYNCHINTERVAL/2)
  {
    lastSynchTime = seconds;
    sendSynch();
  }
}

void processSynch(JsonDocument &doc)
{
  unsigned long epoch = doc["e"].as<unsigned long>();
  if (epoch > 0)
  {
    rtc.setTime(epoch);
    tm now = rtc.getTimeStruct();
    log(2, "espTimeSet: " + dateTimeIso(now));
  }
  else
  {
    int timeAdjust = doc["t"].as<int>();
    if (timeAdjust != 0)
    {
      rtc.setTime(rtc.getEpoch() + timeAdjust);
      log(2, "espTimeAdjust: " + String(timeAdjust));
    }
  }
  long newDnsVersion = doc["v"].as<long>();
  if (newDnsVersion != 0)
  {
    dnsVersion  = newDnsVersion;
    log(2, "dns version: " + String(dnsVersion));
    for (int ix = 0; ix < DNSSIZE; ix++)
    {
      dnsList[ix].used = false;
    }
    for (int ix = 0; ix < DNSSIZE; ix++)
    {
      if (doc.containsKey("n" + String(ix)))
      {
        dnsList[ix].name = doc["n" + String(ix)].as<String>();
        dnsList[ix].ip = doc["i" + String(ix)].as<String>();
        dnsList[ix].used = true;
        dnsList[ix].timeout = 1;   // for consistency with dnsLog
        log(2, ".. " + dnsList[ix].name + " " + dnsList[ix].ip);
      }
      else
      {
        break;
      }
    }
  }
}
#endif



// ------------- mqtt section -----------------
// start mqtt client skipped
#if 0
// mqtt 
void setupMqttClient()
{
  IPAddress fixedIp = localIp;
  fixedIp[3] = mqttIp4;
  String server = fixedIp.toString();
  mqttClient.host = server;
  mqttClient.port = mqttPort;
  mqttMoniker = mqttId + "/" + String(unitId);
  mqttClient.client_id = mqttMoniker;
  String topic = mqttMoniker + "/c/#";
  mqttClient.subscribe(topic, &mqttMessageHandler);
  mqttSubscribeAdd();
  mqttClient.connected_callback = [] {mqttConnHandler();};
  mqttClient.disconnected_callback = [] {mqttDiscHandler();};
  mqttClient.connection_failure_callback = [] {mqttFailHandler();};
  mqttClient.begin();
}

// mqtt handlers
void mqttConnHandler()
{
  log(0, "MQTT connected: " + String(millis() - mqttDiscMs));
  sendSynch();
  mqttConnCount++;
}
void mqttDiscHandler()
{
  log(0, "MQTT disconnected");
  mqttDiscCount++;
  mqttDiscMs = millis();
}
void mqttFailHandler()
{
  log(0, "MQTT CONN FAIL");
  if (WiFi.isConnected())
  {
    mqttConnFailCount++;
  }
  mqttDiscMs = millis();
}
void mqttMessageHandler(const char * topicC, Stream & stream)
{
  mqttInCount++;
  String topic = String(topicC);
  JsonDocument doc;
  DeserializationError error = deserializeJson(doc, stream);
  if (error) 
  {
    log(2, "deserialize fail: " +  String(error.f_str()) + " " + topic);
    return;
  }
  if (logLevel >=2)
  {
    String s;
    serializeJson(doc, s);
    log(2, "in:" + topic + " " + s);
   }
  if (topic.endsWith("/p"))
  {   
    // its a property setting
    adjustProp(doc["p"].as<String>());
  }
  else if (topic.endsWith("/s"))
  {   
    // its a synch response message
    processSynch(doc);
  }
  else
  {
    handleIncoming(topic, doc);
  }
}

void mqttSend(String topic, JsonDocument &doc)
{
  if (logLevel >=2)
  {
    String s;
    serializeJson(doc, s);
    log(2, "out: " + topic + " " + s);
  }
  if (WiFi.isConnected() && mqttClient.connected())
  {
    // publish using begin_publish()/send() API
    auto publish = mqttClient.begin_publish(topic, measureJson(doc));
    serializeJson(doc, publish);
    publish.send();
    mqttSendCount++;
  }
}
#endif
// end mqtt client skipped
// ------------ telnet --------------
void setupTelnet(int port) 
{  
  telnet.stop();
  // passing on functions for various telnet events
  telnet.onConnect(onTelnetConnect);
  telnet.onDisconnect(onTelnetDisconnect);
  telnet.onConnectionAttempt(onTelnetConnectionAttempt);
  telnet.onReconnect(onTelnetReconnect);
  telnet.onInputReceived(onTelnetInput);

  if (telnet.begin(port)) 
  {
    log(1, "telnet running");
  } 
  else 
  {
    log(1, "telnet start fail");
  }
}

void onTelnetConnect(String ip) 
{
  Serial.println("telnet connected");
  telnet.println("hello..");
}
void onTelnetDisconnect(String ip) 
{
  Serial.println("telnet disconnected");
}

void onTelnetReconnect(String ip) 
{
  Serial.println("telnet reconnected");
}

void onTelnetConnectionAttempt(String ip) 
{
  Serial.println("another telnet tried to connected - disconnecting");
  telnet.println("Another session trying. disconnecting you..");
  telnet.disconnectClient();
}

void onTelnetInput(String str) 
{
  processCommandLine(str);
}

void setRetryDelay()
{
  startRetryDelay = seconds;
  retryDelay = true;
  log(1, "retry delay....");
  recoveries++;
}
bool lastWifiState = false;
// state engine manager
void checkState()
{
  if (propValue != "")
  {
    adjustProp(propNameA + "=" + propValue);
    propValue = "";
  }  
  unsigned long nowMs = millis();
  while (nowMs - lastSecondMs > 1000)
  {
    seconds++;
    lastSecondMs+= 1000;
  }
  synchCheck();
  bool thisWifiState = WiFi.isConnected();
  if (thisWifiState != lastWifiState)
  {
    if (thisWifiState)
    {
      log(0, "WiFi Connected..");
      reConnWifiCount++;
    }
    else
    {
      log(0, "WiFi Disconnected..");
    }
    lastWifiState = thisWifiState;
  }
 
  if (retryDelay)
  {
    if (seconds - startRetryDelay < (retryDelayTime))
    {
      return; // retry wait
    }
    else
    {
      retryDelay = false;
    }
  }

  int res;
  switch (state)
  {
    case START:
      if (wifiIp4 == 0)
      {
        state = STARTCONNECTWIFI;    // dhcp ip
      }
      else
      {
        state = STARTGETGATEWAY;
      }
      return;
    case STARTGETGATEWAY:
      // only get gateway for fixed ip
      if (!startGetGateway())
      {
        setRetryDelay();
        return;
      }
      state = WAITGETGATEWAY;
      return;
    case WAITGETGATEWAY:
      res = waitWifi();
      if (res == 0)
      {
        return;
      }
      if (res < 0)
      {
        setRetryDelay();
        state = STARTGETGATEWAY;
        return;
      }
    case STARTCONNECTWIFI:
      if (!startWifi())
      {
        setRetryDelay();
        return;
      }
      state = WAITCONNECTWIFI;
      return;
    case WAITCONNECTWIFI:
      // mandatory we get connected once before proceeding
      res = waitWifi();
      if (res == 0)
      {
        return;
      }
      if (res < 0)
      {
        setRetryDelay();
        state = STARTCONNECTWIFI;
        return;
      }
      setupTelnet(telnetPort);
      //setupMqttClient();   // dont use client
      setupBroker();
      state = ALLOK;

      return;

    case ALLOK:
      return;
  }
}
// ------------ end wifi and mqtt include section 2 ---------------

// ------------ start wifi and mqtt custom section 2 ---------------

void mqttSubscribeAdd()
{
  // use standard or custom handler
  // start subscribe add
  
  // end subscribe add
}
void handleIncoming(String topic, JsonDocument &doc)
{
  // start custom additional incoming
  
  // end custom additional incoming
}

// ------------ end wifi and mqtt custom section 2 ---------------

// ---- start custom code --------------

// -------------start  specific code for mqtt broker ------------
// tiny RTC master clock

String dateTimeIsoTiny(DateTime d)
{
  return String(d.year())+"-"+formatd2(d.month())+"-"+formatd2(d.day())+"T"+formatd2(d.hour())+":"+formatd2(d.minute())+":"+formatd2(d.second());
}

void setTinyRtc(int val, String propName)
{
  DateTime tinyNow = tinyrtc.now();
  int year = tinyNow.year();
  int month = tinyNow.month();
  int day = tinyNow.day();
  int hour = tinyNow.hour();
  int min = tinyNow.minute();
  int sec = tinyNow.second();
  if (propName == yearN) year = val;
  if (propName == monthN) month = val;
  if (propName == dayN) day = val;
  if (propName == hourN) hour = val;
  if (propName == minN) min = val;
  if (propName == secN) sec = val;

  tinyrtc.adjust(DateTime(year, month, day, hour, min, sec));
  tinyNow = tinyrtc.now();
  log(0, "tiny RTC: " + dateTimeIsoTiny(tinyNow));
  espTimeSynch();
}

void espTimeSynch()
{ 
  DateTime tinyNow = tinyrtc.now();
  rtc.setTime(tinyNow.second(),tinyNow.minute(),tinyNow.hour(),tinyNow.day(),tinyNow.month(),tinyNow.year());
}

// dns manager - broker only

unsigned long lastDnsJanitor = 0;

void dnsJanitor()
{
  if (seconds - lastDnsJanitor > 0)
  {
    lastDnsJanitor = seconds;
    for (int ix = 0; ix < DNSSIZE; ix++)
    {
      if (dnsList[ix].timeout > 0)
      {
        dnsList[ix].timeout--;
        if (dnsList[ix].timeout == 0 && logLevel >=2)
        {
          log(2, "drop dns[" + String(ix) + "]: " + dnsList[ix].name + " " + dnsList[ix].ip);
          dnsVersion++; 
        }
      }
    }
  }
}
void registerDns(String name, String ip)
{
  for (int ix = 0; ix < DNSSIZE; ix++)
  {
    if (dnsList[ix].used && dnsList[ix].name == name)
    {
      dnsList[ix].timeout = SYNCHINTERVAL+60;
      if (dnsList[ix].ip != ip)
      {
        dnsList[ix].ip = ip;
      }
      return;
    }
  }
  for (int ix = 0; ix < DNSSIZE; ix++)
  {
    if (!dnsList[ix].used)
    {
      dnsList[ix].used = true;
      dnsList[ix].timeout = SYNCHINTERVAL+60;
      dnsList[ix].ip = ip;
      dnsList[ix].name = name;
      if (logLevel >=2)
      {
        log(2, "add dns[" + String(ix) + "]: " + dnsList[ix].name + " " + dnsList[ix].ip);
      }
      dnsVersion++;
      return;
    }
  }
  log(0, "! no room in dns list!");
}

void selfRegister()
{
  registerDns(mqttId + String(unitId), localIp.toString());
}



void processSynchServer(JsonDocument &doc, bool toSerial)
{
  String name = doc["n"].as<String>();
  String ip = doc["i"].as<String>();
  registerDns(name, ip);
  unsigned long clientEpoch = doc["e"].as<unsigned long>();
  unsigned long espEpoch = rtc.getEpoch();
  int timeDiff = 0;
  int timeAdjust = 0;
  unsigned long epochReset = 0;
  bool sendUpdate = false;
  if (clientEpoch > espEpoch)
  {
    timeDiff = clientEpoch - espEpoch;
    if (timeDiff > 2)
    {
      timeAdjust = -1;
      sendUpdate = true;
    }
  }
  else if (espEpoch > clientEpoch)
  {
    timeDiff = espEpoch - clientEpoch;
    if (timeDiff > 2)
    {
      timeAdjust = 1;
      sendUpdate = true;
    }
  }
  if (timeDiff > 20)
  {
    timeAdjust = 0;
    epochReset = espEpoch;
    sendUpdate = true;
  }
  unsigned long clientDnsVersion = doc["v"].as<unsigned long>();
  if (clientDnsVersion != dnsVersion)
  {
    sendUpdate = true;
  }
  if (!sendUpdate)
  {
    return;    // nowt to do
  }
  String replyTo = doc["r"].as<String>();
  doc.clear();
  doc["e"] = epochReset;
  doc["t"] = timeAdjust;
  if (clientDnsVersion == dnsVersion)
  {
    doc["v"] = 0;
  }
  else
  {
    int count = 0;
    for (int ix = 0; ix < DNSSIZE; ix++)
    {
      if (dnsList[ix].used && dnsList[ix].timeout > 0)
      {
        doc["n" + String(count)] = dnsList[ix].name;
        doc["i" + String(count)] = dnsList[ix].ip;
        count++;
      }
    }
    doc["v"] = dnsVersion;
  }
  if (toSerial)
  {
    serial2Send(replyTo, doc);
  }
  else
  {
    brokerSend(replyTo, doc);
  }
}

// broker
void brokerSend(String topic, JsonDocument doc)
{
  // publish using begin_publish()/send() API
  auto publish = mqttBroker.begin_publish(topic, measureJson(doc));
  serializeJson(doc, publish);
  publish.send();
  if (logLevel >=2)
  {
    String s;
    serializeJson(doc, s);
    log(2, "broker out: " + topic + " " + s); 
  }
  mqttSendCount++;
}

void serial2Send(String topic, JsonDocument doc)
{
  Serial2.print(topic);
  serializeJson(doc, Serial2);
  Serial2.print(char(0));
  if (logLevel >=2)
  {
    String s;
    serializeJson(doc, s);
    log(2, "to bridge: " + topic + " " + s); 
  }
}

void setupBroker()
{
  tinyrtc.begin();
  espTimeSynch();
  selfRegister();
  mqttBroker.begin();
  mqttBroker.subscribe(mqttId + "/#", &brokerCallback);
  log(1, "mqttBroker started");

  Serial2.begin(BRATE, SERIAL_8N1, RXD2, TXD2);    // open the floodgates..
}

void brokerCallback(const char * topic, const char * message)
{
  String topicS(topic);
  if (topicS.startsWith(mqttId + "/f/"))
  {
    topicS = topicS.substring(mqttId.length() + 3);
    if (logLevel >=2)
    {
      log(2, "to bridge: " + topicS + " " + String(message));
    }
    //push to Serial2 as is with null terminator 
    mqttInCount++;
    Serial2.print(topicS);
    Serial2.print(message);
    Serial2.print(char(0));
    return;
  }
  else if (topicS == mqttId + "/s"))
  {
    JsonDocument doc;
    deserializeJson(doc, message);
    processSynchServer(doc, false);
  }
 
}

// Serial 2 input handling
bool topicBit = true;
#define TBLEN 128
char tb[TBLEN];    // for topic
int tbPtr = 0;
#define MBLEN 1024
char mb[MBLEN];    // for message
int mbPtr = 0;

void brokerLoop()
{
  if (seconds - lastSynchTime > SYNCHINTERVAL)
  {
    lastSynchTime = seconds;
    espTimeSynch();
    selfRegister();
  }

  dnsJanitor();

  mqttBroker.loop();

  // check serial and publish
  while (Serial2.available())
  {
    char c = Serial2.read();
    if (topicBit)
    {
      if (c=='{')
      {
        tb[tbPtr++] = 0;
        topicBit = false;
        mbPtr = 0;
        tbPtr = 0;
        mb[mbPtr++]= c;
      }
      else
      {
        while (tbPtr >= TBLEN-2)
        {
          tbPtr--;
        }
        tb[tbPtr++] = c;
      }
    }
    else
    {
      if (c==0)
      {
        mb[mbPtr++] = 0;
        topicBit = true;
        mbPtr = 0;
        tbPtr = 0;
        String topic = String(tb);
         if (topic == mqttId + "/s")
        {
          // intercept synch from bridge
          JsonDocument doc;
          deserializeJson (doc, mb);
          processSynchServer(doc, true);
        }
        else
        {
          mqttBroker.publish(tb, mb);
          mqttSendCount++;
        }
        if (logLevel >=2)
        {
          log(2, "from bridge: " + topic + " " + String(mb)); 
        }
      }
      else
      {
        while (mbPtr >= MBLEN-2)
        {
          mbPtr--;
        }
        mb[mbPtr++] = c;
      }
    }
  }
}
// -------------end  specific code for mqtt router ------------

// ---- end custom code --------------

void setup()
{
  loga(1, module);
  loga(1, " ");
  log(1, buildDate);
  Serial.begin(115200);
  checkRestartReason();
  mountSpiffs();
  readProps();
  esp_task_wdt_config_t config;
  int wdt = max(wdTimeout*1000,2000);
  config.timeout_ms = max(wdTimeout*1000,2000);;
  config.idle_core_mask = 3; //both cores
  config.trigger_panic = true;
  esp_task_wdt_reconfigure(&config);
  esp_task_wdt_add(NULL);
}


void loop()
{
  esp_task_wdt_reset();
  telnet.loop();
  checkSerial();
  checkState();
  brokerLoop();

  delay(1);
}

