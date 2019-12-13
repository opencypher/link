package org.opencypher.link.impl.table

import org.apache.flink.table.api.Table
import org.opencypher.link.api.LinkSession
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.relational.api.table.{Table => RelationalTable}
import org.opencypher.okapi.relational.impl.planning.{JoinType, Order}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object LinkCypherTable {

  implicit class LinkTable(val table: Table)(implicit val session: LinkSession) extends RelationalTable[LinkTable] {
    override def physicalColumns: Seq[String] = ???

    override def columnType: Map[String, CypherType] = ???

    override def rows: Iterator[String => CypherValue.CypherValue] = ???

    override def size: Long = ???

    override def select(col: (String, String), cols: (String, String)*): LinkTable = ???

    override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LinkTable = ???

    override def drop(cols: String*): LinkTable = ???

    override def join(other: LinkTable, joinType: JoinType, joinCols: (String, String)*): LinkTable = ???

    override def unionAll(other: LinkTable): LinkTable = ???

    override def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LinkTable = ???

    override def skip(n: Long): LinkTable = ???

    override def limit(n: Long): LinkTable = ???

    override def distinct: LinkTable = ???

    override def distinct(cols: String*): LinkTable = ???

    override def group(by: Set[Var], aggregations: Map[String, Aggregator])(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LinkTable = ???

    override def withColumns(columns: (Expr, String)*)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LinkTable = ???

    override def show(rows: Int): Unit = ???
  }

}
