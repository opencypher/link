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
package org.opencypher.link.api.io

import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.opencypher.link.api.LinkSession
import org.opencypher.link.impl.{LinkRecords, RecordBehaviour}
import org.opencypher.link.impl.table.LinkCypherTable.FlinkTable
import org.opencypher.link.impl.table.LinkCypherTable._
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.types.CTInteger
import org.opencypher.okapi.relational.api.io.ElementTable
import org.opencypher.okapi.relational.api.table.RelationalElementTableFactory
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.link.impl.table.TableOperations._

case class LinkElementTableFactory(session: LinkSession) extends RelationalElementTableFactory[FlinkTable] {
  override def elementTable(
    nodeMapping: ElementMapping,
    table: FlinkTable
  ): ElementTable[FlinkTable] = {
    LinkElementTable.create(nodeMapping, table)(session)
  }
}

case class LinkElementTable private[link](
  override val mapping: ElementMapping,
  override val table: FlinkTable
)(implicit link: LinkSession) extends ElementTable[FlinkTable] with RecordBehaviour {
  override type Records = LinkElementTable

  override implicit val session: LinkSession = link

  private[link] def records(implicit session: LinkSession): LinkRecords = session.records.fromElementTable(elementTable =  this)

  override def cache(): LinkElementTable.this.type = {
    table.cache()
    this
  }

  override protected def verify(): Unit = {
    mapping.idKeys.values.toSeq.flatten.foreach {
      case (_, column) => table.verifyColumnType(column, CTInteger, "id key")
    }
  }
}

object LinkElementTable {
  def create(mapping: ElementMapping, table: FlinkTable)(implicit session: LinkSession): LinkElementTable = {
    val sourceIdColumns = mapping.allSourceIdKeys
    val idCols = sourceIdColumns.map(UnresolvedFieldReference)
    val remainingCols = mapping.allSourcePropertyKeys.map(UnresolvedFieldReference)
    val colsToSelect = idCols ++ remainingCols

    LinkElementTable(mapping, table.table.select(colsToSelect: _*))
  }
}

object LinkNodeTable {

  /**
   * Creates a node table from the given [[Table]]. By convention, there needs to be one column storing node
   * identifiers and named after [[GraphElement.sourceIdKey]]. All remaining columns are interpreted as node property columns, the column name is used as property
   * key.
   *
   * @param impliedLabels  implied node labels
   * @param nodeTable         node data
   * @return a node table with inferred node mapping
   */
  def apply(impliedLabels: Set[String], nodeTable: Table)(implicit session: LinkSession): LinkElementTable = {
    val propertyColumnNames = nodeTable.columns.filter(_ != GraphElement.sourceIdKey).toSet
    val propertyKeyMapping = propertyColumnNames.map(p => p.toProperty -> p)

    val mapping = NodeMappingBuilder
      .on(GraphElement.sourceIdKey)
      .withImpliedLabels(impliedLabels.toSeq: _*)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build

    LinkElementTable.create(mapping, nodeTable)
  }
}

object LinkRelationshipTable {

  /**
   * Creates a relationship table from the given [[Table]]. By convention, there needs to be one column storing
   * relationship identifiers and named after [[GraphElement.sourceIdKey]], one column storing source node identifiers
   * and named after [[Relationship.sourceStartNodeKey]] and one column storing target node identifiers and named after
   * [[Relationship.sourceEndNodeKey]]. All remaining columns are interpreted as relationship property columns, the
   * column name is used as property key.
   *
   * Column names prefixed with `property#` are decoded by [[org.opencypher.okapi.impl.util.StringEncodingUtilities]] to
   * recover the original property name.
   *
   * @param relationshipType  relationship type
   * @param relationshipTable relationship data
   * @return a relationship table with inferred relationship mapping
   */
  def apply(relationshipType: String, relationshipTable: Table)(implicit session: LinkSession) = {
    val propertyColumnNames = relationshipTable.columns.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet
    val propertyKeyMapping = propertyColumnNames.map(p => p.toProperty -> p)

    val mapping = RelationshipMappingBuilder
      .on(GraphElement.sourceIdKey)
      .from(Relationship.sourceStartNodeKey)
      .to(Relationship.sourceEndNodeKey)
      .withRelType(relationshipType)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build

    LinkElementTable.create(mapping, relationshipTable)
  }
}