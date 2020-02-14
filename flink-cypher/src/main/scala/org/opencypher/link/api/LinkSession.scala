/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.link.api

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.opencypher.link.api.io.LinkElementTableFactory
import org.opencypher.link.impl.graph.LinkGraphFactory
import org.opencypher.link.impl.table.LinkCypherTable.FlinkTable
import org.opencypher.link.impl.{LinkRecords, LinkRecordsFactory}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.graph.RelationalCypherSession
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult

sealed class LinkSession private(
  val env: ExecutionEnvironment,
  val tableEnv: BatchTableEnvironment
) extends RelationalCypherSession[FlinkTable] with Serializable {

  override type Result = RelationalCypherResult[FlinkTable]

  override type Records = LinkRecords

  implicit val session: LinkSession = this

  override val records: LinkRecordsFactory = LinkRecordsFactory()

  override val graphs: LinkGraphFactory = LinkGraphFactory()

  override val elementTables: LinkElementTableFactory = LinkElementTableFactory(session)

  def sql(query: String): LinkRecords = {
    records.wrap(tableEnv.sqlQuery(query))
  }
}

object LinkSession extends Serializable {

  def create(implicit env: ExecutionEnvironment): LinkSession = {
    new LinkSession(env, TableEnvironment.getTableEnvironment(env))
  }

  def local(): LinkSession = create(ExecutionEnvironment.getExecutionEnvironment)

  implicit class RecordsAsTable(val records: CypherRecords) extends AnyVal {

    def asTable: Table = records match {
      case link: LinkRecords => link.table.table
      case _ => throw UnsupportedOperationException(s"can only handle Link records, got $records")
    }

    def asDataSet: DataSet[CypherMap] = records match {
      case link: LinkRecords => link.toCypherMaps
      case _ => throw UnsupportedOperationException(s"can only handle Link records, got $records")
    }
  }
}