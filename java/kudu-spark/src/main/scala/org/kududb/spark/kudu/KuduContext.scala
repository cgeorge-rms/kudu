/*
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

package org.kududb.spark.kudu

import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.kududb.annotations.InterfaceStability
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.client._
import scala.collection.JavaConverters._


/**
  * KuduContext is a serializable container for Kudu client connections.
  *
  * If a Kudu client connection is needed as part of a Spark application, a
  * [[KuduContext]] should used as a broadcast variable in the job in order to
  * share connections among the tasks in a JVM.
  */
@InterfaceStability.Unstable
class KuduContext(kuduMaster: String) extends Serializable {

  /**
    * Set to
    * [[org.apache.spark.util.ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY]].
    * The client instances are closed through the JVM shutdown hook
    * mechanism in order to make sure that any unflushed writes are cleaned up
    * properly. Spark has no shutdown notifications.
    */
  private val ShutdownHookPriority = 100

  @transient lazy val syncClient = {
    val syncClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    ShutdownHookManager.get().addShutdownHook(new Runnable {
      override def run() = syncClient.close()
    }, ShutdownHookPriority)
    syncClient
  }

  @transient lazy val asyncClient = {
    val asyncClient = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMaster).build()
    ShutdownHookManager.get().addShutdownHook(
      new Runnable {
        override def run() = asyncClient.close()
      }, ShutdownHookPriority)
    asyncClient
  }

  /**
    * Create an RDD from a Kudu table.
    *
    * @param tableName          table to read from
    * @param columnProjection   list of columns to read. Not specifying this at all
    *                           (i.e. setting to null) or setting to the special
    *                           string '*' means to project all columns.
    * @return a new RDD that maps over the given table for the selected columns
    */
  def kuduRDD(sc: SparkContext,
              tableName: String,
              columnProjection: Seq[String] = Nil): RDD[Row] = {
    new KuduRDD(kuduMaster, 1024*1024*20, columnProjection.toArray, columnProjection.toArray, Array(),
                syncClient.openTable(tableName), this, sc)
  }

  /**
    * Saves partitions of a dataframe into kudu
    * @param rows rows to insert or update
    * @param tableName table to insert or update on
    */
  def savePartition(rows: Iterator[Row], tableName: String, performAsUpdate : Boolean = false, errorsCallback: (scala.collection.mutable.Buffer[OperationResponse]) => Unit = null): Unit = {
    val table: KuduTable = syncClient.openTable(tableName)
    val session: KuduSession = syncClient.newSession
    val columns = (table.getSchema.getColumns.asScala).map(_.getName()) // gets the columns in order so the primary key is first
    while (rows.hasNext) {
      val dfRow = rows.next()
      try {
        session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

        var operation: Operation = table.newInsert()
        if (performAsUpdate) {
          operation = table.newUpdate()
        }
        val kuduRow: PartialRow = operation.getRow
        for (field <- columns) {
          val dt: DataType = dfRow.schema.fields(dfRow.schema.fieldIndex(field)).dataType
          val fieldNames = dfRow.schema.fieldNames
          if (dfRow.isNullAt(dfRow.schema.fieldIndex(field))) {

          } else dt match {
            case DataTypes.StringType => kuduRow.addString(field, dfRow.getAs[String](field))
            case DataTypes.BinaryType => kuduRow.addBinary(field, dfRow.getAs[Array[Byte]](field))
            case DataTypes.BooleanType => kuduRow.addBoolean(field, dfRow.getAs[Boolean](field))
            case DataTypes.ByteType => kuduRow.addInt(field, dfRow.getAs[Byte](field))
            case DataTypes.ShortType => kuduRow.addShort(field, dfRow.getAs[Short](field))
            case DataTypes.IntegerType => kuduRow.addInt(field, dfRow.getAs[Int](field))
            case DataTypes.LongType => kuduRow.addLong(field, dfRow.getAs[Long](field))
            case DataTypes.FloatType => kuduRow.addFloat(field, dfRow.getAs[Float](field))
            case DataTypes.DoubleType => kuduRow.addDouble(field, dfRow.getAs[Double](field))
            case _ => throw new RuntimeException(s"No support for Spark SQL type $dt")
          }
        }
        session.apply(operation)
      } finally {
        val operationResponse = session.close()
        if (operationResponse!=null && operationResponse.size()>0 && errorsCallback!=null) {
          val errors = operationResponse.asScala.filter( operation => operation.hasRowError)
          errorsCallback(errors)
        }
      }
    }
  }

}
