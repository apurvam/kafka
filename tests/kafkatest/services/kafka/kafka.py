# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import json
import os.path
import re
import signal
import subprocess
import time

from ducktape.services.service import Service
from ducktape.utils.util import wait_until

from config import KafkaConfig
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka import config_property
from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.security.minikdc import MiniKdc
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import TRUNK

Port = collections.namedtuple('Port', ['name', 'number', 'open'])


class KafkaService(KafkaPathResolverMixin, JmxMixin, Service):

    PERSISTENT_ROOT = "/mnt"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "server-start-stdout-stderr.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "kafka-log4j.properties")
    # Logs such as controller.log, server.log, etc all go here
    OPERATIONAL_LOG_DIR = os.path.join(PERSISTENT_ROOT, "kafka-operational-logs")
    OPERATIONAL_LOG_INFO_DIR = os.path.join(OPERATIONAL_LOG_DIR, "info")
    OPERATIONAL_LOG_DEBUG_DIR = os.path.join(OPERATIONAL_LOG_DIR, "debug")
    # Kafka log segments etc go here
    DATA_LOG_DIR = os.path.join(PERSISTENT_ROOT, "kafka-data-logs")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "kafka.properties")
    # Kafka Authorizer
    SIMPLE_AUTHORIZER = "kafka.security.auth.SimpleAclAuthorizer"

    logs = {
        "kafka_server_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True},
        "kafka_operational_logs_info": {
            "path": OPERATIONAL_LOG_INFO_DIR,
            "collect_default": True},
        "kafka_operational_logs_debug": {
            "path": OPERATIONAL_LOG_DEBUG_DIR,
            "collect_default": False},
        "kafka_data": {
            "path": DATA_LOG_DIR,
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, zk, security_protocol=SecurityConfig.PLAINTEXT, interbroker_security_protocol=SecurityConfig.PLAINTEXT,
                 client_sasl_mechanism=SecurityConfig.SASL_MECHANISM_GSSAPI, interbroker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_GSSAPI,
                 authorizer_class_name=None, topics=None, version=TRUNK, quota_config=None, jmx_object_names=None,
                 jmx_attributes=[], zk_connect_timeout=5000, zk_session_timeout=6000):
        """
        :type context
        :type zk: ZookeeperService
        :type topics: dict
        """
        Service.__init__(self, context, num_nodes)
        JmxMixin.__init__(self, num_nodes, jmx_object_names, jmx_attributes)

        self.zk = zk
        self.quota_config = quota_config

        self.security_protocol = security_protocol
        self.interbroker_security_protocol = interbroker_security_protocol
        self.client_sasl_mechanism = client_sasl_mechanism
        self.interbroker_sasl_mechanism = interbroker_sasl_mechanism
        self.topics = topics
        self.minikdc = None
        self.authorizer_class_name = authorizer_class_name
        self.zk_set_acl = False

        #
        # In a heavily loaded and not very fast machine, it is
        # sometimes necessary to give more time for the zk client
        # to have its session established, especially if the client
        # is authenticating and waiting for the SaslAuthenticated
        # in addition to the SyncConnected event.
        #
        # The defaut value for zookeeper.connect.timeout.ms is
        # 2 seconds and here we increase it to 5 seconds, but
        # it can be overriden by setting the corresponding parameter
        # for this constructor.
        self.zk_connect_timeout = zk_connect_timeout

        # Also allow the session timeout to be provided explicitly,
        # primarily so that test cases can depend on it when waiting
        # e.g. brokers to deregister after a hard kill.
        self.zk_session_timeout = zk_session_timeout

        self.port_mappings = {
            'PLAINTEXT': Port('PLAINTEXT', 9092, False),
            'SSL': Port('SSL', 9093, False),
            'SASL_PLAINTEXT': Port('SASL_PLAINTEXT', 9094, False),
            'SASL_SSL': Port('SASL_SSL', 9095, False)
        }

        for node in self.nodes:
            node.version = version
            node_config = {}
            node_config[config_property.ENABLE_UNCLEAN_LEADER_ELECTION] = 'false'
            node_config[config_property.BROKER_ID] = self.idx(node)
            node.config = KafkaConfig(**node_config)

    @property
    def security_config(self):
        return SecurityConfig(self.security_protocol, self.interbroker_security_protocol,
                              zk_sasl = self.zk.zk_sasl,
                              client_sasl_mechanism=self.client_sasl_mechanism, interbroker_sasl_mechanism=self.interbroker_sasl_mechanism)

    def open_port(self, protocol):
        self.port_mappings[protocol] = self.port_mappings[protocol]._replace(open=True)

    def close_port(self, protocol):
        self.port_mappings[protocol] = self.port_mappings[protocol]._replace(open=False)

    def start_minikdc(self, add_principals=""):
        if self.security_config.has_sasl:
            if self.minikdc is None:
                self.minikdc = MiniKdc(self.context, self.nodes, extra_principals = add_principals)
                self.minikdc.start()
        else:
            self.minikdc = None

    def start_some(self, nodes_to_start, create_topics=True, add_principals=""):
        """Starts a subset of alloted nodes.

        Keyword arguments:
            nodes_to_start -- a list of nodes to start. Node ids range from
                [0 .. num_nodes)
            create_topics -- whether or not to create the configured topics.
         """
        self.open_port(self.security_protocol)
        self.open_port(self.interbroker_security_protocol)
        self.start_minikdc(add_principals)
        for node_id in nodes_to_start:
            node = self.nodes[node_id]
            # Added precaution - kill running processes, clean persistent files
            # try/except for each step, since each of these steps may fail if
            # there are no processes to kill or no files to remove
            try:
                self.stop_node(node)
            except:
                pass

            try:
                self.clean_node(node)
            except:
                pass

            self.logger.debug("%s: starting node" % self.who_am_i(node))
            self.start_node(node)

        # Create topics if necessary
        if create_topics is True:
            if self.topics is not None:
                for topic, topic_cfg in self.topics.items():
                    if topic_cfg is None:
                        topic_cfg = {}

                    topic_cfg["topic"] = topic
                    self.create_topic(topic_cfg)

    def start(self, add_principals=""):
        self.open_port(self.security_protocol)
        self.open_port(self.interbroker_security_protocol)

        self.start_minikdc(add_principals)
        Service.start(self)

        # Create topics if necessary
        if self.topics is not None:
            for topic, topic_cfg in self.topics.items():
                if topic_cfg is None:
                    topic_cfg = {}

                topic_cfg["topic"] = topic
                self.create_topic(topic_cfg)

    def set_protocol_and_port(self, node):
        listeners = []
        advertised_listeners = []

        for protocol in self.port_mappings:
            port = self.port_mappings[protocol]
            if port.open:
                listeners.append(port.name + "://:" + str(port.number))
                advertised_listeners.append(port.name + "://" +  node.account.hostname + ":" + str(port.number))

        self.listeners = ','.join(listeners)
        self.advertised_listeners = ','.join(advertised_listeners)

    def prop_file(self, node):
        cfg = KafkaConfig(**node.config)
        cfg[config_property.ADVERTISED_HOSTNAME] = node.account.hostname
        cfg[config_property.ZOOKEEPER_CONNECT] = self.zk.connect_setting()

        self.set_protocol_and_port(node)

        # TODO - clean up duplicate configuration logic
        prop_file = cfg.render()
        prop_file += self.render('kafka.properties', node=node, broker_id=self.idx(node),
                                 security_config=self.security_config)
        return prop_file

    def start_cmd(self, node):
        cmd = "export JMX_PORT=%d; " % self.jmx_port
        cmd += "export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % self.LOG4J_CONFIG
        cmd += "export KAFKA_OPTS=%s; " % self.security_config.kafka_opts
        cmd += "%s %s 1>> %s 2>> %s &" % \
               (self.path.script("kafka-server-start.sh", node),
                KafkaService.CONFIG_FILE,
                KafkaService.STDOUT_STDERR_CAPTURE,
                KafkaService.STDOUT_STDERR_CAPTURE)
        return cmd

    def start_node(self, node):
        prop_file = self.prop_file(node)
        self.logger.info("kafka.properties:")
        self.logger.info(prop_file)
        node.account.create_file(KafkaService.CONFIG_FILE, prop_file)
        node.account.create_file(self.LOG4J_CONFIG, self.render('log4j.properties', log_dir=KafkaService.OPERATIONAL_LOG_DIR))

        self.security_config.setup_node(node)

        cmd = self.start_cmd(node)
        self.logger.debug("Attempting to start KafkaService on %s with command: %s" % (str(node.account), cmd))
        with node.account.monitor_log(KafkaService.STDOUT_STDERR_CAPTURE) as monitor:
            node.account.ssh(cmd)
            monitor.wait_until("Kafka Server.*started", timeout_sec=30, err_msg="Kafka server didn't finish startup")

        self.start_jmx_tool(self.idx(node), node)
        if len(self.pids(node)) == 0:
            raise Exception("No process ids recorded on node %s" % str(node))

    def pids(self, node):
        """Return process ids associated with running processes on the given node."""
        try:
            cmd = "ps ax | grep -i kafka | grep java | grep -v grep | awk '{print $1}'"

            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (subprocess.CalledProcessError, ValueError) as e:
            return []

    def signal_node(self, node, sig=signal.SIGTERM):
        pids = self.pids(node)
        for pid in pids:
            node.account.signal(pid, sig)

    def signal_leader(self, topic, partition=0, sig=signal.SIGTERM):
        leader = self.leader(topic, partition)
        self.signal_node(leader, sig)

    def stop_node(self, node, clean_shutdown=True):
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL

        for pid in pids:
            node.account.signal(pid, sig, allow_fail=False)
        wait_until(lambda: len(self.pids(node)) == 0, timeout_sec=60, err_msg="Kafka node failed to stop")

    def clean_node(self, node):
        JmxMixin.clean_node(self, node)
        self.security_config.clean_node(node)
        node.account.kill_process("kafka", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf /mnt/*", allow_fail=False)

    def create_topic(self, topic_cfg, node=None):
        """Run the admin tool create topic command.
        Specifying node is optional, and may be done if for different kafka nodes have different versions,
        and we care where command gets run.

        If the node is not specified, run the command from self.nodes[0]
        """
        if node is None:
            node = self.nodes[0]
        self.logger.info("Creating topic %s with settings %s",
                         topic_cfg["topic"], topic_cfg)
        kafka_topic_script = self.path.script("kafka-topics.sh", node)

        cmd = kafka_topic_script + " "
        cmd += "--zookeeper %(zk_connect)s --create --topic %(topic)s " % {
                'zk_connect': self.zk.connect_setting(),
                'topic': topic_cfg.get("topic"),
           }
        if 'replica-assignment' in topic_cfg:
            cmd += " --replica-assignment %(replica-assignment)s" % {
                'replica-assignment': topic_cfg.get('replica-assignment')
            }
        else:
            cmd += " --partitions %(partitions)d --replication-factor %(replication-factor)d" % {
                'partitions': topic_cfg.get('partitions', 1),
                'replication-factor': topic_cfg.get('replication-factor', 1)
            }

        if "configs" in topic_cfg.keys() and topic_cfg["configs"] is not None:
            for config_name, config_value in topic_cfg["configs"].items():
                cmd += " --config %s=%s" % (config_name, str(config_value))

        self.logger.info("Running topic creation command...\n%s" % cmd)
        node.account.ssh(cmd)

        time.sleep(1)
        self.logger.info("Checking to see if topic was properly created...\n%s" % cmd)
        for line in self.describe_topic(topic_cfg["topic"]).split("\n"):
            self.logger.info(line)

    def describe_topic(self, topic, node=None):
        if node is None:
            node = self.nodes[0]
        cmd = "%s --zookeeper %s --topic %s --describe" % \
              (self.path.script("kafka-topics.sh", node), self.zk.connect_setting(), topic)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line
        return output

    def alter_message_format(self, topic, msg_format_version, node=None):
        if node is None:
            node = self.nodes[0]
        self.logger.info("Altering message format version for topic %s with format %s", topic, msg_format_version)
        cmd = "%s --zookeeper %s --entity-name %s --entity-type topics --alter --add-config message.format.version=%s" % \
              (self.path.script("kafka-configs.sh", node), self.zk.connect_setting(), topic, msg_format_version)
        self.logger.info("Running alter message format command...\n%s" % cmd)
        node.account.ssh(cmd)

    def parse_describe_topic(self, topic_description):
        """Parse output of kafka-topics.sh --describe (or describe_topic() method above), which is a string of form
        PartitionCount:2\tReplicationFactor:2\tConfigs:
            Topic: test_topic\ttPartition: 0\tLeader: 3\tReplicas: 3,1\tIsr: 3,1
            Topic: test_topic\tPartition: 1\tLeader: 1\tReplicas: 1,2\tIsr: 1,2
        into a dictionary structure appropriate for use with reassign-partitions tool:
        {
            "partitions": [
                {"topic": "test_topic", "partition": 0, "replicas": [3, 1]},
                {"topic": "test_topic", "partition": 1, "replicas": [1, 2]}
            ]
        }
        """
        lines = map(lambda x: x.strip(), topic_description.split("\n"))
        partitions = []
        for line in lines:
            m = re.match(".*Leader:.*", line)
            if m is None:
                continue

            fields = line.split("\t")
            # ["Partition: 4", "Leader: 0"] -> ["4", "0"]
            fields = map(lambda x: x.split(" ")[1], fields)
            partitions.append(
                {"topic": fields[0],
                 "partition": int(fields[1]),
                 "replicas": map(int, fields[3].split(','))})
        return {"partitions": partitions}

    def verify_reassign_partitions(self, reassignment, node=None):
        """Run the reassign partitions admin tool in "verify" mode
        """
        if node is None:
            node = self.nodes[0]

        json_file = "/tmp/%s_reassign.json" % str(time.time())

        # reassignment to json
        json_str = json.dumps(reassignment)
        json_str = json.dumps(json_str)

        # create command
        cmd = "echo %s > %s && " % (json_str, json_file)
        cmd += "%s " % self.path.script("kafka-reassign-partitions.sh", node)
        cmd += "--zookeeper %s " % self.zk.connect_setting()
        cmd += "--reassignment-json-file %s " % json_file
        cmd += "--verify "
        cmd += "&& sleep 1 && rm -f %s" % json_file

        # send command
        self.logger.info("Verifying parition reassignment...")
        self.logger.debug(cmd)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line

        self.logger.debug(output)

        if re.match(".*Reassignment of partition.*failed.*",
                    output.replace('\n', '')) is not None:
            return False

        if re.match(".*is still in progress.*",
                    output.replace('\n', '')) is not None:
            return False

        return True

    def execute_reassign_partitions(self, reassignment, node=None,
                                    throttle=None):
        """Run the reassign partitions admin tool in "verify" mode
        """
        if node is None:
            node = self.nodes[0]
        json_file = "/tmp/%s_reassign.json" % str(time.time())

        # reassignment to json
        json_str = json.dumps(reassignment)
        json_str = json.dumps(json_str)

        # create command
        cmd = "echo %s > %s && " % (json_str, json_file)
        cmd += "%s " % self.path.script( "kafka-reassign-partitions.sh", node)
        cmd += "--zookeeper %s " % self.zk.connect_setting()
        cmd += "--reassignment-json-file %s " % json_file
        cmd += "--execute"
        if throttle is not None:
            cmd += " --throttle %d" % throttle
        cmd += " && sleep 1 && rm -f %s" % json_file

        # send command
        self.logger.info("Executing parition reassignment...")
        self.logger.debug(cmd)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line

        self.logger.debug("Verify partition reassignment:")
        self.logger.debug(output)

    def search_data_files(self, topic, messages):
        """Check if a set of messages made it into the Kakfa data files. Note that
        this method takes no account of replication. It simply looks for the
        payload in all the partition files of the specified topic. 'messages' should be
        an array of numbers. The list of missing messages is returned.
        """
        payload_match = "payload: " + "$|payload: ".join(str(x) for x in messages) + "$"
        found = set([])
        self.logger.debug("number of unique missing messages we will search for: %d",
                          len(messages))
        for node in self.nodes:
            # Grab all .log files in directories prefixed with this topic
            files = node.account.ssh_capture("find %s -regex  '.*/%s-.*/[^/]*.log'" % (KafkaService.DATA_LOG_DIR, topic))

            # Check each data file to see if it contains the messages we want
            for log in files:
                cmd = "%s kafka.tools.DumpLogSegments --print-data-log --files %s | grep -E \"%s\"" % \
                      (self.path.script("kafka-run-class.sh", node), log.strip(), payload_match)

                for line in node.account.ssh_capture(cmd, allow_fail=True):
                    for val in messages:
                        if line.strip().endswith("payload: "+str(val)):
                            self.logger.debug("Found %s in data-file [%s] in line: [%s]" % (val, log.strip(), line.strip()))
                            found.add(val)

        self.logger.debug("Number of unique messages found in the log: %d",
                          len(found))
        missing = list(set(messages) - found)

        if len(missing) > 0:
            self.logger.warn("The following values were not found in the data files: " + str(missing))

        return missing

    def restart_node(self, node, clean_shutdown=True):
        """Restart the given node."""
        self.stop_node(node, clean_shutdown)
        self.start_node(node)

    def leader(self, topic, partition=0):
        """ Get the leader replica for the given topic and partition.
        """
        self.logger.debug("Querying zookeeper to find leader replica for topic: \n%s" % (topic))
        zk_path = "/brokers/topics/%s/partitions/%d/state" % (topic, partition)
        partition_state = self.zk.query(zk_path)

        if partition_state is None:
            raise Exception("Error finding partition state for topic %s and partition %d." % (topic, partition))

        partition_state = json.loads(partition_state)
        self.logger.info(partition_state)

        leader_idx = int(partition_state["leader"])
        self.logger.info("Leader for topic %s and partition %d is now: %d" % (topic, partition, leader_idx))
        return self.get_node(leader_idx)

    def list_consumer_groups(self, node=None, new_consumer=False, command_config=None):
        """ Get list of consumer groups.
        """
        if node is None:
            node = self.nodes[0]
        consumer_group_script = self.path.script("kafka-consumer-groups.sh", node)

        if command_config is None:
            command_config = ""
        else:
            command_config = "--command-config " + command_config

        if new_consumer:
            cmd = "%s --new-consumer --bootstrap-server %s %s --list" % \
                  (consumer_group_script,
                   self.bootstrap_servers(self.security_protocol),
                   command_config)
        else:
            cmd = "%s --zookeeper %s %s --list" % (consumer_group_script, self.zk.connect_setting(), command_config)
        output = ""
        self.logger.debug(cmd)
        for line in node.account.ssh_capture(cmd):
            if not line.startswith("SLF4J"):
                output += line
        self.logger.debug(output)
        return output

    def describe_consumer_group(self, group, node=None, new_consumer=False, command_config=None):
        """ Describe a consumer group.
        """
        if node is None:
            node = self.nodes[0]
        consumer_group_script = self.path.script("kafka-consumer-groups.sh", node)

        if command_config is None:
            command_config = ""
        else:
            command_config = "--command-config " + command_config

        if new_consumer:
            cmd = "%s --new-consumer --bootstrap-server %s %s --group %s --describe" % \
                  (consumer_group_script, self.bootstrap_servers(self.security_protocol), command_config, group)
        else:
            cmd = "%s --zookeeper %s %s --group %s --describe" % \
                  (consumer_group_script, self.zk.connect_setting(), command_config, group)
        output = ""
        self.logger.debug(cmd)
        for line in node.account.ssh_capture(cmd):
            if not (line.startswith("SLF4J") or line.startswith("GROUP") or line.startswith("Could not fetch offset")):
                output += line
        self.logger.debug(output)
        return output

    def bootstrap_servers(self, protocol='PLAINTEXT'):
        """Return comma-delimited list of brokers in this cluster formatted as HOSTNAME1:PORT1,HOSTNAME:PORT2,...

        This is the format expected by many config files.
        """
        port_mapping = self.port_mappings[protocol]
        self.logger.info("Bootstrap client port is: " + str(port_mapping.number))

        if not port_mapping.open:
            raise ValueError("We are retrieving bootstrap servers for the port: %s which is not currently open. - " % str(port_mapping))

        return ','.join([node.account.hostname + ":" + str(port_mapping.number) for node in self.nodes])

    def controller(self):
        """ Get the controller node
        """
        self.logger.debug("Querying zookeeper to find controller broker")
        controller_info = self.zk.query("/controller")

        if controller_info is None:
            raise Exception("Error finding controller info")

        controller_info = json.loads(controller_info)
        self.logger.debug(controller_info)

        controller_idx = int(controller_info["brokerid"])
        self.logger.info("Controller's ID: %d" % (controller_idx))
        return self.get_node(controller_idx)

    def is_registered(self, node):
        """
        Check whether a broker is registered in Zookeeper
        """
        self.logger.debug("Querying zookeeper to see if broker %s is registered", node)
        broker_info = self.zk.query("/brokers/ids/%s" % self.idx(node))
        self.logger.debug("Broker info: %s", broker_info)
        return broker_info is not None

    def get_offset_shell(self, topic, partitions, max_wait_ms, offsets, time):
        node = self.nodes[0]

        cmd = self.path.script("kafka-run-class.sh", node)
        cmd += " kafka.tools.GetOffsetShell"
        cmd += " --topic %s --broker-list %s --max-wait-ms %s --offsets %s --time %s" % (topic, self.bootstrap_servers(self.security_protocol), max_wait_ms, offsets, time)

        if partitions:
            cmd += '  --partitions %s' % partitions

        cmd += " 2>> /mnt/get_offset_shell.log | tee -a /mnt/get_offset_shell.log &"
        output = ""
        self.logger.debug(cmd)
        for line in node.account.ssh_capture(cmd):
            output += line
        self.logger.debug(output)
        return output
