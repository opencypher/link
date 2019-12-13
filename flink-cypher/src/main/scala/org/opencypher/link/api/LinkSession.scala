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