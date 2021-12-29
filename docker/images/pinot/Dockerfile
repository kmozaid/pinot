FROM openjdk:11

ARG PINOT_VERSION=0.9.0

COPY apache-pinot-${PINOT_VERSION}-bin.tar.gz /
COPY hadoop-2.7.0.tar /
COPY agent.tar /
COPY conf.tar /
RUN mkdir /opt/cisco && \
    mkdir /var/log/pinot && \
    mkdir -p /vdb/tmp && \
    mkdir -p /var/log/pinot/minion && \
    mkdir -p /var/log/pinot/server && \
    mkdir -p /var/log/pinot/controller && \
    mkdir -p /var/log/pinot/broker && \
    mkdir -p /vdb/pinot/controller && \
    mkdir -p /vdb/pinot/broker && \
    mkdir -p /vdb/pinot/server/data && \
    mkdir -p /vdb/pinot/server/segments && \
    echo '* hard nofile 102400' >> /etc/security/limits.conf && \
    echo '* soft nofile 102400' >> /etc/security/limits.conf && \
    tar xvf /apache-pinot-${PINOT_VERSION}-bin.tar.gz -C /opt/cisco && \
    tar xvf /hadoop-2.7.0.tar -C /opt/cisco && \
    tar xvf /agent.tar -C /opt/cisco/apache-pinot-${PINOT_VERSION}-bin && \
    rm -rf /opt/cisco/apache-pinot-${PINOT_VERSION}-bin/conf && \
    tar xvf /conf.tar -C /opt/cisco/apache-pinot-${PINOT_VERSION}-bin && \
    rm /apache-pinot-${PINOT_VERSION}-bin.tar.gz && \
    rm /hadoop-2.7.0.tar && \
    rm /agent.tar && \
    rm /conf.tar && \
    apt-get update -y && \
    apt-get install vim -y && \
    apt-get install net-tools -y && \
    apt-get install telnet -y

ENV HADOOP_HOME /opt/cisco/hadoop-2.7.0
ENV HADOOP_VERSION 2.7.0
ENV CLASSPATH_PREFIX ${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-annotations-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-auth-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/lib/guava-11.0.2.jar:${HADOOP_HOME}/share/hadoop/common/lib/gson-2.2.4.jar:${HADOOP_HOME}/share/hadoop/hdfs/lib/htrace-core-3.1.0-incubating.jar
ENV JAVA_OPTS ${JAVA_OPTS}
ENV PINOT_HOST_ID ${PINOT_HOST_ID}
ENV PINOT_VERSION ${PINOT_VERSION}

# expose ports for controller/broker/server/minion/admin
# controller 9000, controller jmx 9066
# broker 8099, broker jmx 9067
# server 8098, controller jmx 9068
# minion 8097, minion jmx 9069, minion adminapi 6500
EXPOSE 9000 9066 8099 9067 8098 9068 8097 9069 6500 8096

ENTRYPOINT ["sh", "-c", "/opt/cisco/apache-pinot-${PINOT_VERSION}-bin/bin/pinot-admin.sh ${Component} -configFileName ${CONFIG_FILE} ${ZK}"]