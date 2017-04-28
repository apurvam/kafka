/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.api

import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.{After, Before, Ignore, Test}
import org.junit.Assert._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class TransactionsTest extends KafkaServerTestHarness {
  val numServers = 3
  val topic1 = "topic1"
  val topic2 = "topic2"

  override def generateConfigs : Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(numServers, zkConnect, true).map(KafkaConfig.fromProps(_, serverProps()))
  }

  @Before
  override def setUp(): Unit = {
    super.setUp()
    val numPartitions = 3
    val topicConfig = new Properties();
    topicConfig.put(KafkaConfig.MinInSyncReplicasProp, 2.toString)
    TestUtils.createTopic(zkUtils, topic1, numPartitions, numServers, servers, topicConfig)
    TestUtils.createTopic(zkUtils, topic2, numPartitions, numServers, servers, topicConfig)
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
  }

  @Test
  def basicTransactionsTest() = {
    error("starting test")
    val producer = transactionalProducer("my-hello-world-transactional-id")
    val consumer = transactionalConsumer()
    try {
      error("initing transactions")
      producer.initTransactions()
      error("inited transactions")
      
      producer.beginTransaction()
      producer.send(producerRecord(topic1, "1", "1", willBeCommitted = true))
      producer.send(producerRecord(topic2, "3", "3", willBeCommitted = true))
      producer.commitTransaction()

      producer.beginTransaction()
      producer.send(producerRecord(topic2, "2", "2", willBeCommitted = false))
      producer.send(producerRecord(topic1, "4", "4", willBeCommitted = false))
      producer.abortTransaction()

      consumer.subscribe(List(topic1, topic2))

      val records = pollUntilNumRecords(consumer, 3)
      records.zipWithIndex.foreach { case (record, i) =>
        assertTrue(record.value().endsWith("committed"))
      }
    } catch {
      case e @ (_ : KafkaException | _ : ProducerFencedException) =>
        fail("Did not expect exception", e)
    } finally {
      consumer.close()
      producer.close()
    }
  }

  private def producerRecord(topic: String, key: String, value: String, willBeCommitted: Boolean) = {
    val suffixedValue = if (willBeCommitted)
      value + "-committed"
    else
      value + "-aborted"
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, suffixedValue.getBytes)
  }


  private def serverProps() = {
    val serverProps = new Properties()
    serverProps.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
    // Set a smaller value for the number of partitions for the offset commit topic (__consumer_offset topic)
    // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
    serverProps.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)
    serverProps.put(KafkaConfig.TransactionsTopicPartitionsProp, 3.toString)
    serverProps.put(KafkaConfig.ControlledShutdownEnableProp, true.toString)
    serverProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, false.toString)
    serverProps.put(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)
    serverProps
  }

  private def transactionalProducer(transactionalId: String) = {
    val props = new Properties()
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers), retries = Integer.MAX_VALUE, acks = -1, props = Some(props))
  }

  private def transactionalConsumer() = {
    val props = new Properties()
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers),
      securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(props))
  }

  private def pollUntilNumRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int) : Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val records = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]()
    TestUtils.waitUntilTrue(() => {
      records ++= consumer.poll(50)
      records.size == numRecords
    }, s"Consumed ${records.size} records until timeout, but expected $numRecords records.")
    records
  }
}
