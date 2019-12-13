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
package org.opencypher.link.testing.support.creation.graphs

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.expressions.{ResolvedFieldReference, UnresolvedFieldReference}
import org.apache.flink.types.Row
import org.opencypher.link.api.LinkSession
import org.opencypher.link.api.io.LinkElementTable
import org.opencypher.link.impl.convert.FlinkConversions._
import org.opencypher.link.impl.table.LinkCypherTable.FlinkTable
import org.opencypher.link.schema.LinkSchema._
import org.opencypher.link.testing.support.ElementTableCreationSupport
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.{CypherValue, Element}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.relational.impl.graph.ScanGraph
import org.opencypher.okapi.testing.propertygraph.{InMemoryTestGraph, InMemoryTestNode, InMemoryTestRelationship}

object ScanGraphFactory extends TestGraphFactory with ElementTableCreationSupport {

  override def apply(propertyGraph: InMemoryTestGraph, additionalPatterns: Seq[Pattern])
    (implicit morpheus: LinkSession): ScanGraph[FlinkTable] = {

    val schema = computeSchema(propertyGraph).asLink

    val nodePatterns = schema.labelCombinations.combos.map(labels => NodePattern(CTNode(labels)))
    val relPatterns = schema.relationshipTypes.map(typ => RelationshipPattern(CTRelationship(typ)))

    val scans = (nodePatterns ++ relPatterns ++ additionalPatterns).map { pattern =>
      val data = extractEmbeddings(pattern, propertyGraph, schema)
      createElementTable(pattern, data, schema)
    }

    new ScanGraph(scans.toSeq, schema)
  }

  override def name: String = "ScanGraphFactory"

  private def extractEmbeddings(pattern: Pattern, graph: InMemoryTestGraph, schema: PropertyGraphSchema)
    (implicit morpheus: LinkSession): Seq[Map[PatternElement, Element[Long]]] = {

    val candidates = pattern.elements.map { element =>
      element.cypherType match {
        case CTNode(labels, _) =>
          element -> graph.nodes.filter(_.labels == labels)
        case CTRelationship(types, _) =>
          element -> graph.relationships.filter(rel => types.contains(rel.relType))
        case other => throw IllegalArgumentException("Node or Relationship type", other)
      }
    }.toMap

    val unitEmbedding = Seq(
      Map.empty[PatternElement, Element[Long]]
    )
    val initialEmbeddings = pattern.elements.foldLeft(unitEmbedding) {
      case (acc, patternElement) =>
        val elementCandidates = candidates(patternElement)

        for {
          row <- acc
          elementCandidate <- elementCandidates
        } yield row.updated(patternElement, elementCandidate)
    }

    pattern.topology.foldLeft(initialEmbeddings) {
      case (acc, (relElement, connection)) =>
        connection match {
          case Connection(Some(sourceNode), None, _) => acc.filter { row =>
            row(sourceNode).id == row(relElement).asInstanceOf[InMemoryTestRelationship].startId
          }

          case Connection(None, Some(targetElement), _) => acc.filter { row =>
            row(targetElement).id == row(relElement).asInstanceOf[InMemoryTestRelationship].endId
          }

          case Connection(Some(sourceNode), Some(targetElement), _) => acc.filter { row =>
            val rel = row(relElement).asInstanceOf[InMemoryTestRelationship]
            row(sourceNode).id == rel.startId && row(targetElement).id == rel.endId
          }

          case Connection(None, None, _) => throw IllegalStateException("Connection without source or target node")
        }
    }
  }

  private def createElementTable(
    pattern: Pattern,
    embeddings: Seq[Map[PatternElement, Element[Long]]],
    schema: PropertyGraphSchema
  )(implicit session: LinkSession): LinkElementTable = {

    val unitData: Seq[Seq[Any]] = Seq(embeddings.indices.map(_ => Seq.empty[Any]): _*)

    val (columns, data) = pattern.elements.foldLeft(Seq.empty[ResolvedFieldReference] -> unitData) {
      case ((accColumns, accData), element) =>

        element.cypherType match {
          case CTNode(labels, _) =>
            val propertyKeys = schema.nodePropertyKeys(labels)
            val propertyFields = getPropertyStructFields(element, propertyKeys)

            val nodeData = embeddings.map { embedding =>
              val node = embedding(element).asInstanceOf[InMemoryTestNode]

              val propertyValues = propertyKeys.keySet.toSeq.map(p => node.properties.get(p).map(toFlinkValue).orNull)
              Seq(node.id) ++ propertyValues
            }

            val newData = accData.zip(nodeData).map { case (l, r) => l ++ r }
            val newColumns = accColumns ++ Seq(ResolvedFieldReference(s"${element.name.encodeSpecialCharacters}_id", Types.LONG)) ++ propertyFields

            newColumns -> newData


          case CTRelationship(types, _) =>
            val propertyKeys = schema.relationshipPropertyKeys(types.head)
            val propertyFields = getPropertyStructFields(element, propertyKeys)

            val relData = embeddings.map { embedding =>
              val rel = embedding(element).asInstanceOf[InMemoryTestRelationship]
              val propertyValues = propertyKeys.keySet.toSeq.map(p => rel.properties.get(p).map(toFlinkValue).orNull)
              Seq(rel.id, rel.startId, rel.endId) ++ propertyValues
            }

            val newData = accData.zip(relData).map { case (l, r) => l ++ r }
            val newColumns = accColumns ++
              Seq(
                ResolvedFieldReference(s"${element.name.encodeSpecialCharacters}_id", Types.LONG),
                ResolvedFieldReference(s"${element.name.encodeSpecialCharacters}_source", Types.LONG),
                ResolvedFieldReference(s"${element.name.encodeSpecialCharacters}_target", Types.LONG)
              ) ++
              propertyFields

            newColumns -> newData

          case other => throw IllegalArgumentException("Node or Relationship type", other)
        }
    }

    val dataAsRows = data.map { row =>
      Row.of(row.map(_.asInstanceOf[AnyRef]): _*)
    }

    implicit val rowTypeInfo = new RowTypeInfo(columns.map(_.resultType): _*)

    val table = session.tableEnv.fromDataSet(
      session.env.fromCollection(
        dataAsRows
      ),
      columns.map(ref => UnresolvedFieldReference(ref.name)): _*
    )

    constructElementTable(pattern, table)
  }

  protected def getPropertyStructFields(patternElement: PatternElement, propKeys: PropertyKeys): Seq[ResolvedFieldReference] = {
    propKeys.foldLeft(Seq.empty[ResolvedFieldReference]) { case (fields, key) =>
      fields :+ ResolvedFieldReference(s"${patternElement.name}_${key._1.encodeSpecialCharacters}_property", key._2.getFlinkType)
    }
  }

  private def toFlinkValue(v: CypherValue): Any = {
    v.getValue match {
      case Some(l: List[_]) => l.collect { case c: CypherValue => toFlinkValue(c) }
      case Some(other) => other
      case None => null
    }
  }
}
