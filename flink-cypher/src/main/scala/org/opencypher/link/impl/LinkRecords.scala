package org.opencypher.link.impl

import org.apache.flink.api.scala.DataSet
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row
import org.opencypher.link.api.LinkSession
import org.opencypher.link.impl.table.LinkCypherTable.LinkTable
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

case class EmptyRow()

case class LinkRecords(
  header: RecordHeader,
  table: LinkTable,
  override val logicalColumns: Option[Seq[String]]= None
)(implicit val link: LinkSession) extends RelationalCypherRecords[LinkTable] with RecordBehaviour {

  override type Records = LinkRecords

  override implicit val session: LinkSession = link

  def flinkTable: Table = table.table

  override def cache(): LinkRecords = throw UnsupportedOperationException("cache()")

  override def toString: String = {
    if (header.isEmpty) {
      s"LinkRecords.empty"
    } else {
      s"LinkRecords(header: $header)"
    }
  }

}

trait RecordBehaviour extends RelationalCypherRecords[LinkTable] {

  implicit val session: LinkSession

  override lazy val columnType: Map[String, CypherType] = table.table.columnType

  override def rows: Iterator[String => CypherValue] = table.table.rows

  override def iterator: Iterator[CypherMap] = toCypherMaps.collect().iterator

  def toLocalIterator: Iterator[CypherMap] = iterator

  override def collect: Array[CypherMap] = toCypherMaps.collect().toArray

  def toCypherMaps: DataSet[CypherMap] = ???
}
