package org.opencypher.link.impl.table

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table
import org.opencypher.link.api.LinkSession
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.relational.api.table.{Table => RelationalTable}
import org.opencypher.okapi.relational.impl.planning.{JoinType, Order}
import org.opencypher.okapi.relational.impl.table.RecordHeader
import TableOperations._
import org.apache.flink.table.expressions.UnresolvedFieldReference

object LinkCypherTable {

  implicit class FlinkTable(val table: Table)(implicit val session: LinkSession) extends RelationalTable[FlinkTable] {

    override def physicalColumns: Seq[String] = table.getSchema.getFieldNames

    override def columnType: Map[String, CypherType] = physicalColumns.map(c => c -> table.cypherTypeForColumn(c)).toMap

    override def rows: Iterator[String => CypherValue.CypherValue] = ???

    override def size: Long = ???

    override def select(col: (String, String), cols: (String, String)*): FlinkTable = {
      val columns = col +: cols
      if (table.columns == columns.map { case (_, alias) => alias }) {
        table
      } else {
        table.select(columns.map { case (colName, alias) => UnresolvedFieldReference(colName) as Symbol(alias) }: _*)
      }
    }

    override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): FlinkTable = ???

    override def drop(cols: String*): FlinkTable = ???

    override def join(other: FlinkTable, joinType: JoinType, joinCols: (String, String)*): FlinkTable = ???

    override def unionAll(other: FlinkTable): FlinkTable = ???

    override def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): FlinkTable = ???

    override def skip(n: Long): FlinkTable = ???

    override def limit(n: Long): FlinkTable = ???

    override def distinct: FlinkTable = ???

    override def distinct(cols: String*): FlinkTable = ???

    override def group(by: Set[Var], aggregations: Map[String, Aggregator])(implicit header: RecordHeader, parameters: CypherValue.CypherMap): FlinkTable = ???

    override def withColumns(columns: (Expr, String)*)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): FlinkTable = ???

    override def show(rows: Int): Unit = ???
  }
}
