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
package org.opencypher.link.impl

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.opencypher.link.api.io.LinkElementTable
import org.opencypher.link.impl.table.LinkCypherTable.FlinkTable
import org.opencypher.link.testing.LinkTestSuite
import org.opencypher.link.testing.fixture.{GraphConstructionFixture, RecordsVerificationFixture, TeamDataFixture}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.operators.Start
import org.opencypher.okapi.testing.Bag

import scala.reflect.runtime.universe

abstract class LinkGraphTest extends LinkTestSuite
  with GraphConstructionFixture
  with RecordsVerificationFixture
  with TeamDataFixture {

  object LinkGraphTest {
    implicit class RecordOps(records: RelationalCypherRecords[FlinkTable]) {
      def planStart: Start[FlinkTable] = {
        implicit val tableTypeTag: universe.TypeTag[FlinkTable] = session.tableTypeTag
        implicit val context: RelationalRuntimeContext[FlinkTable] = session.basicRuntimeContext()
        Start.fromEmptyGraph(records)
      }
    }
  }

  it("should return only nodes with that exact label (single label)") {
    val graph = initGraph(dataFixtureWithoutArrays)
    val nodes = graph.nodes("n", CTNode("Person"), exactLabelMatch = true)
    val cols = Seq(
      n,
      nHasLabelPerson,
      nHasPropertyLuckyNumber,
      nHasPropertyName
    )
    verify(nodes, cols, Bag(Row.of(4L: java.lang.Long, true: java.lang.Boolean, 8L: java.lang.Long, "Donald": java.lang.String)))
  }

  it("should return only nodes with that exact label (multiple labels)") {
    val graph = initGraph(dataFixtureWithoutArrays)
    val nodes = graph.nodes("n", CTNode("Person", "German"), exactLabelMatch = true)
    val cols = Seq(
      n,
      nHasLabelGerman,
      nHasLabelPerson,
      nHasPropertyLuckyNumber,
      nHasPropertyName
    )
    val data = Bag(
      Row.of(2L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, 1337L: java.lang.Long, "Martin": java.lang.String),
      Row.of(3L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, 8L: java.lang.Long, "Max": java.lang.String),
      Row.of(0L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, 42L: java.lang.Long, "Stefan": java.lang.String)
    )
    verify(nodes, cols, data)
  }

  it("should support the same node label from multiple node tables") {
    // this creates additional :Person nodes
    val personsPart2 = session.tableEnv.fromDataSet(
      session.env.fromCollection(
        Seq(
          (5L: java.lang.Long, false: java.lang.Boolean, "Soeren": java.lang.String, 23L: java.lang.Long),
          (6L: java.lang.Long, false: java.lang.Boolean, "Hannes": java.lang.String, 42L: java.lang.Long)
        )
      ), 'ID, 'IS_SWEDE, 'NAME, 'NUM)

    val personTable2 = LinkElementTable.create(personTable.mapping, personsPart2)

    val graph = session.graphs.create(personTable, personTable2)
    graph.nodes("n").size shouldBe 6
  }

  it("should support the same relationship type from multiple relationship tables") {
    // this creates additional :KNOWS relationships
    val knowsParts2 = session.tableEnv.fromDataSet(
      session.env.fromCollection(
      Seq(
        (1L: java.lang.Long, 7L: java.lang.Long, 2L: java.lang.Long, 2017L: java.lang.Long),
        (1L: java.lang.Long, 8L: java.lang.Long, 3L: java.lang.Long, 2016L: java.lang.Long)
      )
    ), 'SRC, 'ID, 'DST, 'SINCE)

    val knowsTable2 = LinkElementTable.create(knowsTable.mapping, knowsParts2)

    val graph = session.graphs.create(personTable, knowsTable, knowsTable2)
    graph.relationships("r").size shouldBe 8
  }

  it("should return an empty result for non-present types") {
    val graph = session.graphs.create(personTable, knowsTable)
    graph.nodes("n", CTNode("BAR")).size shouldBe 0
    graph.relationships("r", CTRelationship("FOO")).size shouldBe 0
  }
}
