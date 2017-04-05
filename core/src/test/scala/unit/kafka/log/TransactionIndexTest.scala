/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._

class TransactionIndexTest extends JUnitSuite {

  @Test
  def testCollectAbortedTransactions(): Unit = {
    val file = TestUtils.tempFile()
    val index = new TransactionIndex(file)

    val abortedTxns = List(
      new AbortedTxn(producerId = 0L, firstOffset = 0, lastOffset = 10, lastStableOffset = 2),
      new AbortedTxn(producerId = 1L, firstOffset = 5, lastOffset = 15, lastStableOffset = 16),
      new AbortedTxn(producerId = 2L, firstOffset = 18, lastOffset = 35, lastStableOffset = 25),
      new AbortedTxn(producerId = 3L, firstOffset = 32, lastOffset = 50, lastStableOffset = 40))

    abortedTxns.foreach(index.append)
    assertEquals(abortedTxns, index.collect(0L, 100L))
    assertEquals(abortedTxns.take(3), index.collect(0L, 35))
  }

  @Test
  def testAbortedTxnSerde(): Unit = {
    val pid = 983493L
    val firstOffset = 137L
    val lastOffset = 299L
    val lastStableOffset = 200L

    val abortedTxn = new AbortedTxn(pid, firstOffset, lastOffset, lastStableOffset)
    assertEquals(AbortedTxn.CurrentVersion, abortedTxn.version)
    assertEquals(pid, abortedTxn.producerId)
    assertEquals(firstOffset, abortedTxn.firstOffset)
    assertEquals(lastOffset, abortedTxn.lastOffset)
    assertEquals(lastStableOffset, abortedTxn.lastStableOffset)
  }

}
