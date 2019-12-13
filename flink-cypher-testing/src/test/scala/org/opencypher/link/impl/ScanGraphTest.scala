///*
// * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// * Attribution Notice under the terms of the Apache License 2.0
// *
// * This work was created by the collective efforts of the openCypher community.
// * Without limiting the terms of Section 6, any Derivative Work that is not
// * approved by the public consensus process of the openCypher Implementers Group
// * should not be described as “Cypher” (and Cypher® is a registered trademark of
// * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
// * proposals for change that have been documented or implemented should only be
// * described as "implementation extensions to Cypher" or as "proposed changes to
// * Cypher that are not yet approved by the openCypher community".
// */
//package org.opencypher.link.impl
//
//import org.apache.flink.api.scala._
//import org.apache.flink.table.api.scala._
//import org.apache.flink.types.Row
//import org.opencypher.link.api.io.LinkElementTable
//import org.opencypher.link.api.value.LinkRelationship
//import org.opencypher.link.testing.support.ElementTableCreationSupport
//import org.opencypher.link.testing.support.creation.graphs.{ScanGraphFactory, TestGraphFactory}
//import org.opencypher.okapi.api.graph._
//import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
//import org.opencypher.okapi.api.types._
//import org.opencypher.okapi.api.value.CypherValue.CypherMap
//import org.opencypher.okapi.ir.api.expr._
//import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
//import org.opencypher.okapi.ir.impl.util.VarConverters._
//import org.opencypher.okapi.testing.Bag
//import org.opencypher.okapi.testing.propertygraph.CreateGraphFactory
//import LinkConverters._
//
//class ScanGraphTest extends LinkGraphTest with ElementTableCreationSupport {
//
//  override def graphFactory: TestGraphFactory = ScanGraphFactory
//
//  it("executes union") {
//    val graph1 = session.graphs.create(personTable, knowsTable)
//    val graph2 = session.graphs.create(programmerTable, bookTable, readsTable)
//
//    val result = graph1 unionAll graph2
//
//    val nodeCols = Seq(
//      n,
//      nHasLabelBook,
//      nHasLabelPerson,
//      nHasLabelProgrammer,
//      nHasPropertyLanguage,
//      nHasPropertyLuckyNumber,
//      nHasPropertyName,
//      nHasPropertyTitle,
//      nHasPropertyYear
//    )
//    val relCols = Seq(
//      rStart,
//      r,
//      rHasTypeKnows,
//      rHasTypeReads,
//      rEnd,
//      rHasPropertyRecommends,
//      rHasPropertySince
//    )
//
//    val nodeData = Bag(
//      Row.of(1L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean,  null: java.lang.String, 23L: java.lang.Long, "Mats": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(2L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean,  null: java.lang.String, 42L: java.lang.Long, "Martin": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(3L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean,  null: java.lang.String, 1337L: java.lang.Long, "Max": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(4L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean,  null: java.lang.String, 9L: java.lang.Long, "Stefan": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(10L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, false: java.lang.Boolean, null: java.lang.String, null: java.lang.Long, null: java.lang.String, "1984": java.lang.String, 1949L),
//      Row.of(20L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, false: java.lang.Boolean, null: java.lang.String, null: java.lang.Long, null: java.lang.String, "Cryptonomicon": java.lang.String, 1999L: java.lang.Long),
//      Row.of(30L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, false: java.lang.Boolean, null: java.lang.String, null: java.lang.Long, null: java.lang.String, "The Eye of the World": java.lang.String, 1990L: java.lang.Long),
//      Row.of(40L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, false: java.lang.Boolean, null: java.lang.String, null: java.lang.Long, null: java.lang.String, "The Circle": java.lang.String, 2013L: java.lang.Long),
//      Row.of(100L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, "C": java.lang.String, 42L: java.lang.Long, "Alice": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(200L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, "D": java.lang.String, 23L: java.lang.Long, "Bob": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(300L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, "F": java.lang.String, 84L: java.lang.Long, "Eve": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(400L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, "R": java.lang.String, 49L: java.lang.Long, "Carl": java.lang.String, null: java.lang.String, null: java.lang.Long)
//    )
//
//    val relData = Bag(
//      Row.of(1L: java.lang.Long, 1L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, 2L: java.lang.Long, null: java.lang.Boolean, 2017L: java.lang.Long),
//      Row.of(1L: java.lang.Long, 2L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, 3L: java.lang.Long, null: java.lang.Boolean, 2016L: java.lang.Long),
//      Row.of(1L: java.lang.Long, 3L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, 4L: java.lang.Long, null: java.lang.Boolean, 2015L: java.lang.Long),
//      Row.of(2L: java.lang.Long, 4L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, 3L: java.lang.Long, null: java.lang.Boolean, 2016L: java.lang.Long),
//      Row.of(2L: java.lang.Long, 5L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, 4L: java.lang.Long, null: java.lang.Boolean, 2013L: java.lang.Long),
//      Row.of(3L: java.lang.Long, 6L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, 4L: java.lang.Long, null: java.lang.Boolean, 2016L: java.lang.Long),
//      Row.of(100L: java.lang.Long, 100L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, 10L: java.lang.Long, true: java.lang.Boolean, null: java.lang.Long),
//      Row.of(200L: java.lang.Long, 200L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, 40L: java.lang.Long, true: java.lang.Boolean, null: java.lang.Long),
//      Row.of(300L: java.lang.Long, 300L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, 30L: java.lang.Long, true: java.lang.Boolean, null: java.lang.Long),
//      Row.of(400L: java.lang.Long, 400L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, 20L: java.lang.Long, false: java.lang.Boolean, null: java.lang.Long)
//    )
//
//    verify(result.nodes("n"), nodeCols, nodeData)
//    verify(result.relationships("r"), relCols, relData)
//  }
//
//  it("dont lose schema information when mapping") {
//    val nodes = LinkElementTable.create(NodeMappingBuilder.on("id").build,
//      session.tableEnv.fromDataSet(
//        session.env.fromCollection(
//        Seq(
//          (10L: java.lang.Long),
//          (11L: java.lang.Long),
//          (12L: java.lang.Long),
//          (20L: java.lang.Long),
//          (21L: java.lang.Long),
//          (22L: java.lang.Long),
//          (25L: java.lang.Long),
//          (50L: java.lang.Long),
//          (51L: java.lang.Long)
//        )
//      ), 'id))
//
//    val rs = LinkElementTable.create(RelationshipMappingBuilder.on("ID").from("SRC").to("DST").relType("FOO").build,
//      session.tableEnv.fromDataSet(
//        session.env.fromCollection(
//        Seq(
//          (10L: java.lang.Long, 1000L: java.lang.Long, 20L: java.lang.Long),
//          (50L: java.lang.Long, 500L: java.lang.Long, 25L: java.lang.Long)
//        )
//      ), 'SRC, 'ID, 'DST))
//
//
//    val graph = session.graphs.create(nodes, rs)
//
//    val results = graph.relationships("r").asLink.toCypherMaps
//
//    results.collect().toSet should equal(
//      Set(
//        CypherMap("r" -> LinkRelationship(1000L, 10L, 20L, "FOO")),
//        CypherMap("r" -> LinkRelationship(500L, 50L, 25L, "FOO"))
//      ))
//  }
//
//  it("Construct graph from single node scan") {
//    val graph = session.graphs.create(personTable)
//    val nodes = graph.nodes("n")
//    val cols = Seq(
//      n,
//      nHasLabelPerson,
//      nHasPropertyLuckyNumber,
//      nHasPropertyName
//    )
//    val data = Bag(
//      Row.of(1L: java.lang.Long, true: java.lang.Boolean, 23L: java.lang.Long, "Mats": java.lang.String),
//      Row.of(2L: java.lang.Long, true: java.lang.Boolean, 42L: java.lang.Long, "Martin": java.lang.String),
//      Row.of(3L: java.lang.Long, true: java.lang.Boolean, 1337L: java.lang.Long, "Max": java.lang.String),
//      Row.of(4L: java.lang.Long, true: java.lang.Boolean, 9L: java.lang.Long, "Stefan": java.lang.String)
//    )
//    verify(nodes, cols, data)
//  }
//
//  it("Construct graph from multiple node scans") {
//    val graph = session.graphs.create(personTable, bookTable)
//    val nodes = graph.nodes("n")
//    val cols = Seq(
//      n,
//      nHasLabelBook,
//      nHasLabelPerson,
//      nHasPropertyLuckyNumber,
//      nHasPropertyName,
//      nHasPropertyTitle,
//      nHasPropertyYear
//    )
//    val data = Bag(
//      Row.of(1L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean,  23L: java.lang.Long, "Mats": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(2L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean,  42L: java.lang.Long, "Martin": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(3L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean,  1337L: java.lang.Long, "Max": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(4L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean,  9L: java.lang.Long, "Stefan": java.lang.String, null: java.lang.String, null: java.lang.Long),
//      Row.of(10L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, null: java.lang.Long, null: java.lang.String, "1984": java.lang.String, 1949L),
//      Row.of(20L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, null: java.lang.Long, null: java.lang.String, "Cryptonomicon": java.lang.String, 1999L: java.lang.Long),
//      Row.of(30L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, null: java.lang.Long, null: java.lang.String, "The Eye of the World": java.lang.String, 1990L: java.lang.Long),
//      Row.of(40L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, null: java.lang.Long, null: java.lang.String, "The Circle": java.lang.String, 2013L: java.lang.Long)
//    )
//    verify(nodes, cols, data)
//  }
//
//
//  it("Align node scans") {
//    val fixture =
//      """
//        |CREATE (a:A { name: 'A' })
//        |CREATE (b:B { name: 'B' })
//        |CREATE (combo:A:B { name: 'COMBO', size: 2 })
//        |CREATE (a)-[:R { since: 2004 }]->(b)
//        |CREATE (b)-[:R { since: 2005 }]->(combo)
//        |CREATE (combo)-[:S { since: 2006 }]->(combo)
//      """.stripMargin
//
//    val graph = ScanGraphFactory(CreateGraphFactory(fixture))
//
//    graph.cypher("MATCH (n) RETURN n").records.size should equal(3)
//  }
//
//  it("Align node scans when individual tables have the same node id and properties") {
//    val aDf = session.tableEnv.fromDataSet(session.env.fromCollection(Seq(
//      (0L, "A")
//    ), '_node_id, 'name)).withColumn("size", functions.lit(null))
//    val aMapping: ElementMapping = NodeMappingBuilder.on("_node_id").withPropertyKey("name").withPropertyKey("size").withImpliedLabel("A").build
//    val aTable = LinkElementTable.create(aMapping, aDf)
//
//    val bDf = session.sparkSession.createDataFrame(Seq(
//      (1L, "B")
//    )).toDF("_node_id", "name").withColumn("size", functions.lit(null))
//    val bMapping = NodeMappingBuilder.on("_node_id").withPropertyKey("name").withPropertyKey("size").withImpliedLabel("B").build
//    val bTable = LinkElementTable.create(bMapping, bDf)
//
//    val comboDf = session.sparkSession.createDataFrame(Seq(
//      (2L, "COMBO", 2)
//    )).toDF("_node_id", "name", "size")
//    val comboMapping = NodeMappingBuilder.on("_node_id").withPropertyKey("name").withPropertyKey("size").withImpliedLabels("A", "B").build
//    val comboTable = LinkElementTable.create(comboMapping, comboDf)
//
//    val graph = session.graphs.create(aTable, bTable, comboTable)
//
//    graph.cypher("MATCH (n) RETURN n").records.size should equal(3)
//  }
//
//  it("Construct graph from single node and single relationship scan") {
//    val graph = session.graphs.create(personTable, knowsTable)
//    val rels = graph.relationships("r")
//
//    val cols = Seq(
//      rStart,
//      r,
//      rHasTypeKnows,
//      rEnd,
//      rHasPropertySince
//    )
//    val data = Bag(
//      Row(1L.encodeAsLinkId.toList, 1L.encodeAsLinkId.toList, true, 2L.encodeAsLinkId.toList, 2017L),
//      Row(1L.encodeAsLinkId.toList, 2L.encodeAsLinkId.toList, true, 3L.encodeAsLinkId.toList, 2016L),
//      Row(1L.encodeAsLinkId.toList, 3L.encodeAsLinkId.toList, true, 4L.encodeAsLinkId.toList, 2015L),
//      Row(2L.encodeAsLinkId.toList, 4L.encodeAsLinkId.toList, true, 3L.encodeAsLinkId.toList, 2016L),
//      Row(2L.encodeAsLinkId.toList, 5L.encodeAsLinkId.toList, true, 4L.encodeAsLinkId.toList, 2013L),
//      Row(3L.encodeAsLinkId.toList, 6L.encodeAsLinkId.toList, true, 4L.encodeAsLinkId.toList, 2016L)
//    )
//
//    verify(rels, cols, data)
//  }
//
//  it("Extract all node scans") {
//    val graph = session.graphs.create(personTable, bookTable)
//    val nodes = graph.nodes("n", CTNode())
//    val cols = Seq(
//      n,
//      nHasLabelBook,
//      nHasLabelPerson,
//      nHasPropertyLuckyNumber,
//      nHasPropertyName,
//      nHasPropertyTitle,
//      nHasPropertyYear
//    )
//    val data = Bag(
//      Row(1L.encodeAsLinkId.toList, false, true,  23L, "Mats", null, null),
//      Row(2L.encodeAsLinkId.toList, false, true,  42L, "Martin", null, null),
//      Row(3L.encodeAsLinkId.toList, false, true,  1337L, "Max", null, null),
//      Row(4L.encodeAsLinkId.toList, false, true,  9L, "Stefan", null, null),
//      Row(10L.encodeAsLinkId.toList, true, false, null, null, "1984", 1949L),
//      Row(20L.encodeAsLinkId.toList, true, false, null, null, "Cryptonomicon", 1999L),
//      Row(30L.encodeAsLinkId.toList, true, false, null, null, "The Eye of the World", 1990L),
//      Row(40L.encodeAsLinkId.toList, true, false, null, null, "The Circle", 2013L)
//    )
//
//    verify(nodes, cols, data)
//  }
//
//  it("Extract node scan subset") {
//    val graph = session.graphs.create(personTable, bookTable)
//    val nodes = graph.nodes("n", CTNode("Person"))
//    val cols = Seq(
//      n,
//      nHasLabelPerson,
//      nHasPropertyLuckyNumber,
//      nHasPropertyName
//    )
//    val data = Bag(
//      Row(1L.encodeAsLinkId.toList, true, 23L, "Mats"),
//      Row(2L.encodeAsLinkId.toList, true, 42L, "Martin"),
//      Row(3L.encodeAsLinkId.toList, true, 1337L, "Max"),
//      Row(4L.encodeAsLinkId.toList, true, 9L, "Stefan")
//    )
//    verify(nodes, cols, data)
//  }
//
//  it("Extract all relationship scans") {
//    val graph = session.graphs.create(personTable, bookTable, knowsTable, readsTable)
//    val rels = graph.relationships("r")
//    val cols = Seq(
//      rStart,
//      r,
//      rHasTypeKnows,
//      rHasTypeReads,
//      rEnd,
//      rHasPropertyRecommends,
//      rHasPropertySince
//    )
//    val data = Bag(
//      Row(1L.encodeAsLinkId.toList, 1L.encodeAsLinkId.toList, true, false, 2L.encodeAsLinkId.toList, null, 2017L),
//      Row(1L.encodeAsLinkId.toList, 2L.encodeAsLinkId.toList, true, false, 3L.encodeAsLinkId.toList, null, 2016L),
//      Row(1L.encodeAsLinkId.toList, 3L.encodeAsLinkId.toList, true, false, 4L.encodeAsLinkId.toList, null, 2015L),
//      Row(2L.encodeAsLinkId.toList, 4L.encodeAsLinkId.toList, true, false, 3L.encodeAsLinkId.toList, null, 2016L),
//      Row(2L.encodeAsLinkId.toList, 5L.encodeAsLinkId.toList, true, false, 4L.encodeAsLinkId.toList, null, 2013L),
//      Row(3L.encodeAsLinkId.toList, 6L.encodeAsLinkId.toList, true, false, 4L.encodeAsLinkId.toList, null, 2016L),
//      Row(100L.encodeAsLinkId.toList, 100L.encodeAsLinkId.toList, false, true, 10L.encodeAsLinkId.toList, true, null),
//      Row(200L.encodeAsLinkId.toList, 200L.encodeAsLinkId.toList, false, true, 40L.encodeAsLinkId.toList, true, null),
//      Row(300L.encodeAsLinkId.toList, 300L.encodeAsLinkId.toList, false, true, 30L.encodeAsLinkId.toList, true, null),
//      Row(400L.encodeAsLinkId.toList, 400L.encodeAsLinkId.toList, false, true, 20L.encodeAsLinkId.toList, false, null)
//    )
//
//    verify(rels, cols, data)
//  }
//
//  it("Extract relationship scan subset") {
//    val graph = session.graphs.create(personTable, bookTable, knowsTable, readsTable)
//    val rels = graph.relationships("r", CTRelationship("KNOWS"))
//    val cols = Seq(
//      rStart,
//      r,
//      rHasTypeKnows,
//      rEnd,
//      rHasPropertySince
//    )
//    val data = Bag(
//      Row(1L.encodeAsLinkId.toList, 1L.encodeAsLinkId.toList, true, 2L.encodeAsLinkId.toList, 2017L),
//      Row(1L.encodeAsLinkId.toList, 2L.encodeAsLinkId.toList, true, 3L.encodeAsLinkId.toList, 2016L),
//      Row(1L.encodeAsLinkId.toList, 3L.encodeAsLinkId.toList, true, 4L.encodeAsLinkId.toList, 2015L),
//      Row(2L.encodeAsLinkId.toList, 4L.encodeAsLinkId.toList, true, 3L.encodeAsLinkId.toList, 2016L),
//      Row(2L.encodeAsLinkId.toList, 5L.encodeAsLinkId.toList, true, 4L.encodeAsLinkId.toList, 2013L),
//      Row(3L.encodeAsLinkId.toList, 6L.encodeAsLinkId.toList, true, 4L.encodeAsLinkId.toList, 2016L)
//    )
//
//    verify(rels, cols, data)
//  }
//
//  it("Extract relationship scan strict subset") {
//    val graph = session.graphs.create(personTable, bookTable, knowsTable, readsTable, influencesTable)
//    val rels = graph.relationships("r", CTRelationship("KNOWS", "INFLUENCES"))
//    val cols = Seq(
//      rStart,
//      r,
//      rHasTypeInfluences,
//      rHasTypeKnows,
//      rEnd,
//      rHasPropertySince
//    )
//    val data = Bag(
//      // :KNOWS
//      Row(1L.encodeAsLinkId.toList, 1L.encodeAsLinkId.toList, false, true, 2L.encodeAsLinkId.toList, 2017L),
//      Row(1L.encodeAsLinkId.toList, 2L.encodeAsLinkId.toList, false, true, 3L.encodeAsLinkId.toList, 2016L),
//      Row(1L.encodeAsLinkId.toList, 3L.encodeAsLinkId.toList, false, true, 4L.encodeAsLinkId.toList, 2015L),
//      Row(2L.encodeAsLinkId.toList, 4L.encodeAsLinkId.toList, false, true, 3L.encodeAsLinkId.toList, 2016L),
//      Row(2L.encodeAsLinkId.toList, 5L.encodeAsLinkId.toList, false, true, 4L.encodeAsLinkId.toList, 2013L),
//      Row(3L.encodeAsLinkId.toList, 6L.encodeAsLinkId.toList, false, true, 4L.encodeAsLinkId.toList, 2016L),
//      // :INFLUENCES
//      Row(10L.encodeAsLinkId.toList, 1000L.encodeAsLinkId.toList, true, false, 20L.encodeAsLinkId.toList, null)
//    )
//
//    verify(rels, cols, data)
//  }
//
//  it("Extract from scans with overlapping labels") {
//    val graph = session.graphs.create(personTable, programmerTable)
//    val nodes = graph.nodes("n", CTNode("Person"))
//    val cols = Seq(
//      n,
//      nHasLabelPerson,
//      nHasLabelProgrammer,
//      nHasPropertyLanguage,
//      nHasPropertyLuckyNumber,
//      nHasPropertyName
//    )
//    val data = Bag(
//      Row(1L.encodeAsLinkId.toList, true, false,  null, 23L, "Mats"),
//      Row(2L.encodeAsLinkId.toList, true, false,  null, 42L, "Martin"),
//      Row(3L.encodeAsLinkId.toList, true, false,  null, 1337L, "Max"),
//      Row(4L.encodeAsLinkId.toList, true, false,  null, 9L, "Stefan"),
//      Row(100L.encodeAsLinkId.toList, true, true, "C", 42L, "Alice"),
//      Row(200L.encodeAsLinkId.toList, true, true, "D", 23L, "Bob"),
//      Row(300L.encodeAsLinkId.toList, true, true, "F", 84L, "Eve"),
//      Row(400L.encodeAsLinkId.toList, true, true, "R", 49L, "Carl")
//    )
//
//    verify(nodes, cols, data)
//  }
//
//  it("Extract from scans with implied label but missing keys") {
//    val graph = session.graphs.create(personTable, brogrammerTable)
//    val nodes = graph.nodes("n", CTNode("Person"))
//    val cols = Seq(
//      n,
//      nHasLabelBrogrammer,
//      nHasLabelPerson,
//      nHasPropertyLanguage,
//      nHasPropertyLuckyNumber,
//      nHasPropertyName
//    )
//    val data = Bag(
//      Row(1L.encodeAsLinkId.toList, false, true,  null, 23L, "Mats"),
//      Row(2L.encodeAsLinkId.toList, false, true,  null, 42L, "Martin"),
//      Row(3L.encodeAsLinkId.toList, false, true,  null, 1337L, "Max"),
//      Row(4L.encodeAsLinkId.toList, false, true,  null, 9L, "Stefan"),
//      Row(100L.encodeAsLinkId.toList, true, true, "Node", null, null),
//      Row(200L.encodeAsLinkId.toList, true, true, "Coffeescript", null, null),
//      Row(300L.encodeAsLinkId.toList, true, true, "Javascript", null, null),
//      Row(400L.encodeAsLinkId.toList, true, true, "Typescript", null, null)
//    )
//
//    verify(nodes, cols, data)
//  }
//
//  describe("scanning complex patterns") {
//    it("can scan for NodeRelPattern") {
//      val pattern = NodeRelPattern(CTNode("Person"), CTRelationship("KNOWS"))
//
//      val graph = initGraph(
//        """
//          |CREATE (a:Person {name: "Alice"})
//          |CREATE (b:Person {name: "Bob"})
//          |CREATE (a)-[:KNOWS {since: 2017}]->(b)
//        """.stripMargin, Seq(pattern))
//
//      val scan = graph.scanOperator(pattern)
//      val renamedScan = scan.assignScanName(
//        Map(
//          pattern.nodeElement.toVar -> n,
//          pattern.relElement.toVar -> r
//        )
//      )
//      val result = session.records.from(renamedScan.header, renamedScan.table)
//
//      val cols = Seq(
//        n,
//        nHasLabelPerson,
//        nHasPropertyName,
//        rStart,
//        r,
//        rHasTypeKnows,
//        rEnd,
//        rHasPropertySince
//      )
//
//      val data = Bag(
//        Row(0L.encodeAsLinkId.toList, true, "Alice", 0L.encodeAsLinkId.toList, 2L.encodeAsLinkId.toList, true, 1L.encodeAsLinkId.toList, 2017L)
//      )
//
//      verify(result, cols, data)
//    }
//
//    it("can scan for TripletPatterns") {
//      val pattern = TripletPattern(CTNode("Person"), CTRelationship("KNOWS"), CTNode("Person"))
//
//      val graph = initGraph(
//        """
//          |CREATE (a:Person {name: "Alice"})
//          |CREATE (b:Person {name: "Bob"})
//          |CREATE (a)-[:KNOWS {since: 2017}]->(b)
//        """.stripMargin, Seq(pattern))
//      val scan = graph.scanOperator(pattern)
//
//      val result = session.records.from(scan.header, scan.table)
//
//      val sourceVar = pattern.sourceElement.toVar
//      val targetVar = pattern.targetElement.toVar
//      val relVar = pattern.relElement.toVar
//      val cols = Seq(
//        sourceVar,
//        HasLabel(sourceVar, Label("Person")),
//        ElementProperty(sourceVar, PropertyKey("name"))(CTString),
//        relVar,
//        HasType(relVar, RelType("KNOWS")),
//        StartNode(relVar)(CTAny),
//        EndNode(relVar)(CTAny),
//        ElementProperty(relVar, PropertyKey("since"))(CTInteger),
//        targetVar,
//        HasLabel(targetVar, Label("Person")),
//        ElementProperty(targetVar, PropertyKey("name"))(CTString)
//      )
//
//      val data = Bag(
//        Row(0L.encodeAsLinkId.toList, true, "Alice", 2L.encodeAsLinkId.toList, true, 0L.encodeAsLinkId.toList, 1L.encodeAsLinkId.toList, 2017L, 1L.encodeAsLinkId.toList, true, "Bob")
//      )
//
//      verify(result, cols, data)
//    }
//
//    it("can align different complex pattern scans") {
//      val scanPattern= TripletPattern(CTNode("Person"), CTRelationship("KNOWS"), CTNode)
//
//      val personPattern = TripletPattern(CTNode("Person"), CTRelationship("KNOWS"), CTNode("Person"))
//      val animalPattern = TripletPattern(CTNode("Person"), CTRelationship("KNOWS"), CTNode("Animal"))
//
//      val graph = initGraph(
//        """
//          |CREATE (a:Person {name: "Alice"})
//          |CREATE (b:Person {name: "Bob"})
//          |CREATE (c:Animal {name: "Garfield"})
//          |CREATE (a)-[:KNOWS {since: 2017}]->(b)
//          |CREATE (a)-[:KNOWS {since: 2017}]->(c)
//        """.stripMargin, Seq(personPattern, animalPattern))
//
//      val scan = graph.scanOperator(scanPattern)
//
//      val result = session.records.from(scan.header, scan.table)
//
//      val sourceVar = scanPattern.sourceElement.toVar
//      val targetVar = scanPattern.targetElement.toVar
//      val relVar = scanPattern.relElement.toVar
//      val cols = Seq(
//        sourceVar,
//        HasLabel(sourceVar, Label("Person")),
//        ElementProperty(sourceVar, PropertyKey("name"))(CTString),
//        relVar,
//        HasType(relVar, RelType("KNOWS")),
//        StartNode(relVar)(CTAny),
//        EndNode(relVar)(CTAny),
//        ElementProperty(relVar, PropertyKey("since"))(CTInteger.nullable),
//        targetVar,
//        HasLabel(targetVar, Label("Person")),
//        HasLabel(targetVar, Label("Animal")),
//        ElementProperty(targetVar, PropertyKey("name"))(CTString)
//      )
//
//      val data = Bag(
//        Row(0L.encodeAsLinkId.toList, true, "Alice", 3L.encodeAsLinkId.toList, true, 0L.encodeAsLinkId.toList, 1L.encodeAsLinkId.toList, 2017L, 1L.encodeAsLinkId.toList, true, false, "Bob"),
//        Row(0L.encodeAsLinkId.toList, true, "Alice", 4L.encodeAsLinkId.toList, true, 0L.encodeAsLinkId.toList, 2L.encodeAsLinkId.toList, 2017L, 2L.encodeAsLinkId.toList, false, true, "Garfield")
//      )
//
//      verify(result, cols, data)
//    }
//  }
//}
