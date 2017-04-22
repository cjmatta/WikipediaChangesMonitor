FROM confluentinc/cp-base:latest

ADD changesmonitor-3.2.0-standalone.jar /root/
CMD ["java", "-cp", "/root/changesmonitor-3.2.0-standalone.jar", "org.cmatta.kafka.streams.wikipedia.MessageMonitor"]

