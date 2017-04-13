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

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable.ListBuffer

private[log] class TransactionIndex(val file: File) extends Logging {
    val channel: FileChannel = new RandomAccessFile(file, "rw").getChannel

  private val lock = new ReentrantLock

  def append(abortedTxn: AbortedTxn): Unit = Utils.writeFully(channel, abortedTxn.buffer.duplicate())

  def flush(): Unit = channel.force(true)

  def delete(): Boolean = file.delete()

  def truncate() = Unit //  don't truncate for now.. channel.truncate(0)

  def truncateTo(offset: Long): Unit = {
    inLock(lock) {
      var position = 0
      val lastPosition = channel.position()
      while (lastPosition - position >= AbortedTxn.TotalSize) {
        val buffer = ByteBuffer.allocate(AbortedTxn.TotalSize)
        Utils.readFully(channel, buffer, position)
        buffer.flip()

        val abortedTxn = new AbortedTxn(buffer)
        if (abortedTxn.version > AbortedTxn.CurrentVersion)
          trace(s"Reading aborted txn entry with version ${abortedTxn.version} as current " +
            s"version ${AbortedTxn.CurrentVersion}")

        if (abortedTxn.lastOffset >= offset) {
          channel.truncate(position)
          return
        }
        position += AbortedTxn.TotalSize
      }
    }
  }

  def collect(fetchOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    val abortedTransactions = ListBuffer[AbortedTxn]()
    var position = 0
    val lastPosition = channel.position()

    while (lastPosition - position >= AbortedTxn.TotalSize) {
      val buffer = ByteBuffer.allocate(AbortedTxn.TotalSize)
      Utils.readFully(channel, buffer, position)
      buffer.flip()

      val abortedTxn = new AbortedTxn(buffer)
      if (abortedTxn.version > AbortedTxn.CurrentVersion)
        trace(s"Reading aborted txn entry with version ${abortedTxn.version} as current " +
          s"version ${AbortedTxn.CurrentVersion}")

      if (abortedTxn.lastOffset >= fetchOffset) {
        if (abortedTxn.lastStableOffset >= upperBoundOffset)
          return abortedTransactions.toList

        abortedTransactions += abortedTxn
      }
      position += AbortedTxn.TotalSize
    }

    abortedTransactions.toList
  }
}

private[log] object AbortedTxn {
  val VersionOffset = 0
  val VersionSize = 2
  val ProducerIdOffset = VersionOffset + VersionSize
  val ProducerIdSize = 8
  val FirstOffsetOffset = ProducerIdOffset + ProducerIdSize
  val FirstOffsetSize = 8
  val LastOffsetOffset = FirstOffsetOffset + FirstOffsetSize
  val LastOffsetSize = 8
  val LastStableOffsetOffset = LastOffsetOffset + LastOffsetSize
  val LastStableOffsetSize = 8
  val TotalSize = LastStableOffsetOffset + LastStableOffsetSize

  val CurrentVersion: Short = 0
}

private[log] class AbortedTxn(val buffer: ByteBuffer) {
  import AbortedTxn._

  def this(producerId: Long,
           firstOffset: Long,
           lastOffset: Long,
           lastStableOffset: Long) = {
    this(ByteBuffer.allocate(AbortedTxn.TotalSize))
    buffer.putShort(CurrentVersion)
    buffer.putLong(producerId)
    buffer.putLong(firstOffset)
    buffer.putLong(lastOffset)
    buffer.putLong(lastStableOffset)
    buffer.flip()
  }

  def version: Short = buffer.get(VersionOffset)

  def producerId: Long = buffer.getLong(ProducerIdOffset)

  def firstOffset: Long = buffer.getLong(FirstOffsetOffset)

  def lastOffset: Long = buffer.getLong(LastOffsetOffset)

  def lastStableOffset: Long = buffer.getLong(LastStableOffsetOffset)

  override def toString: String =
    s"AbortedTxn(version=$version, producerId=$producerId, firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, lastStableOffset=$lastStableOffset)"

  override def equals(any: Any): Boolean = {
    any match {
      case that: AbortedTxn => this.buffer.equals(that.buffer)
      case _ => false
    }
  }

  override def hashCode(): Int = buffer.hashCode
}
