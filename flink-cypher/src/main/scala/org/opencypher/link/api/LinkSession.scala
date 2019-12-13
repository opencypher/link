package org.opencypher.link.api

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.BatchTableEnvironment
import org.opencypher.link.impl.table.LinkCypherTable.LinkTable
import org.opencypher.okapi.relational.api.graph.RelationalCypherSession

sealed class LinkSession private(
  val env: ExecutionEnvironment,
  val tableEnv: BatchTableEnvironment
) extends RelationalCypherSession[LinkTable] with Serializable {

  override type Result = this.type

  override type Records = this.type

  override private[opencypher] def records = ???

  override private[opencypher] def graphs = ???

  override private[opencypher] def elementTables = ???
}
