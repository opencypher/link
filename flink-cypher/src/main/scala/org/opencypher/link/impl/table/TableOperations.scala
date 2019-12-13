package org.opencypher.link.impl.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableSchema, Types}
import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.apache.flink.types.Row
import org.opencypher.link.api.LinkSession
import org.opencypher.link.impl.convert.FlinkConversions.supportedTypes
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.link.impl.convert.FlinkConversions._

object TableOperations {

  implicit class TableTransformation(val table: Table) extends AnyVal {
    def withCypherCompatibleTypes: Table = {
      val castExprs = table.getSchema.getFieldNames.zip(table.getSchema.getFieldTypes).map {
        case (fieldName, fieldType) =>
          Seq(
            UnresolvedFieldReference(fieldName).cast(fieldType.cypherCompatibleDataType.getOrElse(
              throw IllegalArgumentException(
                s"a Flink type supported by Cypher: ${supportedTypes.mkString("[", ", ", "]")}",
                s"type $fieldType of field $fieldName"
              )
            )) as Symbol(fieldName))
      }.reduce(_ ++ _)

      table.select(castExprs: _*)
    }
  }

  implicit class TableOperators(val table: Table) extends AnyVal {

    def cypherTypeForColumn(columnName: String): CypherType = {
      val compatibleCypherType = table.getSchema.getFieldType(columnName).get.cypherCompatibleDataType.flatMap(_.toCypherType())
      compatibleCypherType.getOrElse(
        throw IllegalArgumentException("a supported Flink Type that can be converted to CypherType", table.getSchema.getFieldType(columnName)))
    }

    def columns: Seq[String] = table.getSchema.getFieldNames

    def cross(other: Table)(implicit session: LinkSession): Table = {

      val crossedTableNames = table.columns.map(UnresolvedFieldReference) ++
        other.columns.map(UnresolvedFieldReference)
      val crossedTableTypes = table.getSchema.getFieldTypes.toSeq ++
        other.getSchema.getFieldTypes.toSeq

      val crossedDataSet = table.toDataSet[Row].cross(other).map { rowTuple =>
        rowTuple match {
          case (r1: Row, r2: Row) =>
            val r1Fields = Range(0, r1.getArity).map(r1.getField)
            val r2Fields = Range(0, r2.getArity).map(r2.getField)
            Row.of(r1Fields ++ r2Fields: _*)
        }
      }(Types.ROW(crossedTableTypes: _*), null)

      crossedDataSet.toTable(session.tableEnv, crossedTableNames: _*)
    }
  }

  implicit class RichTableSchema(val schema: TableSchema) extends AnyVal {
    def columnNameToIndex: Map[String, Int] = schema.getFieldNames.zipWithIndex.toMap
  }

}
