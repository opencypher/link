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
