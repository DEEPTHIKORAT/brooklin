package com.linkedin.datastream.testutil;

/*
 * Copyright 2015 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;


/**
 * Provides a testing cluster with Zookeeper and Kafka together.
 */
public class EmbeddedKafkaCluster {
  private final List<Integer> _ports;
  private final String _zkConnection;
  private final Properties _baseProperties;

  private final String _brokers;

  private final List<KafkaServer> _brokerList;
  private final List<File> _logDirs;

  public EmbeddedKafkaCluster(String zkConnection) {
    this(zkConnection, new Properties());
  }

  public EmbeddedKafkaCluster(String zkConnection, Properties baseProperties) {
    this(zkConnection, baseProperties, Collections.singletonList(-1));
  }

  public EmbeddedKafkaCluster(String zkConnection, Properties baseProperties, List<Integer> ports) {
    this._zkConnection = zkConnection;
    this._ports = resolvePorts(ports);
    this._baseProperties = baseProperties;

    this._brokerList = new ArrayList<>();
    this._logDirs = new ArrayList<>();

    this._brokers = constructBrokerList(this._ports);
  }

  private List<Integer> resolvePorts(List<Integer> ports) {
    List<Integer> resolvedPorts = new ArrayList<Integer>();
    for (Integer port : ports) {
      resolvedPorts.add(resolvePort(port));
    }
    return resolvedPorts;
  }

  private int resolvePort(int port) {
    if (port == -1) {
      return TestUtils.getAvailablePort();
    }
    return port;
  }

  private String constructBrokerList(List<Integer> ports) {
    StringBuilder sb = new StringBuilder();
    for (Integer port : ports) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append("localhost:").append(port);
    }
    return sb.toString();
  }

  public void startup() {
    for (int i = 0; i < _ports.size(); i++) {
      Integer port = _ports.get(i);
      File logDir = TestUtils.constructTempDir("kafka-local-" + port);

      Properties properties = new Properties();
      properties.putAll(_baseProperties);
      properties.setProperty("zookeeper.connect", _zkConnection);
      properties.setProperty("broker.id", String.valueOf(i + 1));
      properties.setProperty("host.name", "localhost");
      properties.setProperty("port", Integer.toString(port));
      properties.setProperty("log.dir", logDir.getAbsolutePath());
      properties.setProperty("log.flush.interval.messages", String.valueOf(1));

      KafkaServer broker = startBroker(properties);

      _brokerList.add(broker);
      _logDirs.add(logDir);
    }
  }

  static class SystemTime implements Time {
    public long milliseconds() {
      return System.currentTimeMillis();
    }

    public long nanoseconds() {
      return System.nanoTime();
    }

    public void sleep(long ms) {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        // Ignore
      }
    }
  }

  private KafkaServer startBroker(Properties props) {
    KafkaServer server = new KafkaServer(KafkaConfig.fromProps(props), new SystemTime(), scala.Option.apply(""));
    server.startup();
    return server;
  }

  public Properties getProps() {
    Properties props = new Properties();
    props.putAll(_baseProperties);
    props.put("metadata.broker.list", _brokers);
    props.put("zookeeper.connect", _zkConnection);
    return props;
  }

  public String getBrokers() {
    return _brokers;
  }

  public List<Integer> getPorts() {
    return _ports;
  }

  public String getZkConnection() {
    return _zkConnection;
  }

  public void shutdown() {
    for (KafkaServer broker : _brokerList) {
      try {
        broker.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    for (File logDir : _logDirs) {
      try {
        TestUtils.deleteFile(logDir);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("EmbeddedKafkaCluster{");
    sb.append("_brokers='").append(_brokers).append('\'');
    sb.append('}');
    return sb.toString();
  }
}