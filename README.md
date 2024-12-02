# MQTT Broker
Mqtt Broker for local network.

After several versions this is the new one built on the base framework.

It uses PicoMQTT which is simple to work with and has proved stable. I don't have massive throughput needs.

It also provides the master clock via TinyRTC, supports a sort of DNS and resonds to time synch topics to run DNS and synch time to the individual ESP32 clocks. As we know ESP time drifts a bit (few seconds a day) and can't survive power loss.

There is a Serial2 link to forward messages to another ESP32 serving as an MQTT router to secure Hive MQTT in the cloud. Only essential low frequency topics sent up to Hive.

Well that's it really. Enjoy
