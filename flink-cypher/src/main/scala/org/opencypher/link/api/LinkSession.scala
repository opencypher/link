package org.opencypher.link.api

import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{BatchTableEnvironment, TableEnvironment}
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.opencypher.link.impl.table.LinkCypherTable.LinkTable
import org.opencypher.okapi.relational.api.graph.RelationalCypherSession

import scala.collection.JavaConverters._

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

object LinkSession extends Serializable {

  def create(implicit env: ExecutionEnvironment): LinkSession = {
    new LinkSession(env, TableEnvironment.getTableEnvironment(env))
  }

  def local(): LinkSession = create(ExecutionEnvironment.getExecutionEnvironment)
}
