package org.opencypher.link.impl.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions.UnresolvedFieldReference
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
  }

}
