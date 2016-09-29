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

import time
import math
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.utils import is_int


class ThrottlingTest(ProduceConsumeValidateTest):
    """Tests throttled partition reassignment. This is essentially similar
    to the reassign_partitions_test, except that we throttle the reassignment
    and verify that it takes a sensible amount of time given the throttle
    and the amount of data being moved.

    Since the correctness is time dependent, this test also simplifies the
    cluster topology. We have 4 brokers, 1 topic with 4 partitions, and a
    replication-factor of 1. The reassignment moves every partition. Hence
    the data transfer in and out of each broker is fixed, and we can make
    very accurate predicitons about whether throttling is working or not.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ThrottlingTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        # Because we are starting the producer/consumer/validate cycle _after_
        # seeding the cluster with big data (to test throttling), we need to
        # Start the consumer from the end of the stream. further, we need to
        # ensure that the consumer is fully started before the producer starts
        # so that we don't miss any messages. This delay ensures the sufficient
        # condition.
        self.delay_between_consumer_and_producer_start_sec = 10
        self.num_brokers = 4
        self.num_partitions = 4
        self.kafka = KafkaService(test_context,
                                  num_nodes=self.num_brokers,
                                  zk=self.zk,
                                  topics={
                                      self.topic: {
                                          "partitions": self.num_partitions,
                                          "replication-factor": 1,
                                          "configs": {
                                              "segment.bytes": 64 * 1024 * 1024
                                          }
                                      }
                                  })
        self.producer_throughput = 1000
        self.timeout_sec = 400
        self.num_records = 5000
        self.record_size = 4096 * 100  # 400 KB
        # 1 MB per partition on average.
        self.partition_size = (self.num_records * self.record_size) / self.num_partitions
        self.num_producers = 2
        self.num_consumers = 1
        self.throttle = 4 * 1024 * 1024  # 2 MB/s

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(ThrottlingTest, self).min_cluster_size() +\
            self.num_producers + self.num_consumers

    def clean_bounce_some_brokers(self):
        """Bounce every other broker"""
        for node in self.kafka.nodes[::2]:
            self.kafka.restart_node(node, clean_shutdown=True)

    def reassign_partitions(self, bounce_brokers, throttle):
        partition_info = self.kafka.parse_describe_topic(
            self.kafka.describe_topic(self.topic))
        self.logger.debug("Partitions before reassignment:" +
                          str(partition_info))

        for i in range(0, self.num_partitions):
            partition_info["partitions"][i]["partition"] =\
                (i+1) % self.num_partitions
        self.logger.debug("Jumbled partitions: " + str(partition_info))
        # send reassign partitions command

        self.kafka.execute_reassign_partitions(partition_info,
                                               throttle=throttle)
        start = time.time()
        if bounce_brokers:
            # bounce a few brokers at the same time
            self.clean_bounce_some_brokers()

        # Wait until finished or timeout
        size_per_broker = self.partition_size
        self.logger.debug("Amount of data transfer per broker: %fb",
                          size_per_broker)
        estimated_throttled_time = math.ceil(float(size_per_broker) /
                                             self.throttle)
        self.logger.debug("Waiting %ds for the reassignment to complete",
                          estimated_throttled_time * 2)
        wait_until(lambda: self.kafka.verify_reassign_partitions(partition_info),
                   timeout_sec=estimated_throttled_time * 2, backoff_sec=.5)
        stop = time.time()
        time_taken = stop - start
        self.logger.debug("Transfer took %d second. Estimated time : %ds",
                          time_taken,
                          estimated_throttled_time)
        assert time_taken >= estimated_throttled_time, \
            ("Expected rebalance to take at least %ds, but it took %ds" % (
                estimated_throttled_time,
                time_taken))

    @parametrize(bounce_brokers=False, new_consumer=True)
    @parametrize(bounce_brokers=False, new_consumer=False)
    def test_throttled_reassignment(self, bounce_brokers, new_consumer):
        """Tests throttled partition reassignment. This is essentially similar
        to the reassign_partitions_test, except that we throttle the reassignment
        and verify that it takes a sensible amount of time given the throttle
        and the amount of data being moved.

        Since the correctness is time dependent, this test also simplifies the
        cluster topology. We have 4 brokers, 1 topic with 4 partitions, and a
        replication-factor of 1. The reassignment moves every partition. Hence
        the data transfer in and out of each broker is fixed, and we can make
        very accurate predicitons about whether throttling is working or not.
        """

        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol

        producer_id = 'bulk_producer'
        bulk_producer = ProducerPerformanceService(
            context=self.test_context, num_nodes=1, kafka=self.kafka,
            topic=self.topic, num_records=self.num_records,
            record_size=self.record_size, throughput=-1, client_id=producer_id,
            jmx_object_names=['kafka.producer:type=producer-metrics,client-id=%s' % producer_id],
            jmx_attributes=['outgoing-byte-rate'])


        self.producer = VerifiableProducer(context=self.test_context,
                                           num_nodes=1,
                                           kafka=self.kafka, topic=self.topic,
                                           message_validator=is_int,
                                           throughput=self.producer_throughput)

        self.consumer = ConsoleConsumer(self.test_context,
                                        self.num_consumers,
                                        self.kafka,
                                        self.topic,
                                        new_consumer=new_consumer,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int,
                                        from_beginning=False)

        self.kafka.start()
        bulk_producer.run()
        self.run_produce_consume_validate(core_test_action=
                                          lambda: self.reassign_partitions(bounce_brokers, self.throttle))
