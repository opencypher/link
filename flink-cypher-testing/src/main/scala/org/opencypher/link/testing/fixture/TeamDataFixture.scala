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
package org.opencypher.link.testing.fixture

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row
import org.opencypher.link.api.io.LinkElementTable
import org.opencypher.link.api.value.{LinkNode, LinkRelationship}
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTList, CTNode, CTRelationship, CTString, CTVoid}
import org.opencypher.okapi.api.value.CypherValue.{CypherList, CypherMap}
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.ir.api.expr.{ElementProperty, EndNode, Expr, HasLabel, HasType, StartNode, Var}
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._

import scala.collection.mutable

trait TeamDataFixture extends TestDataFixture {

  self: LinkSessionFixture =>


  val n: Var = Var("n")(CTNode)
  val nHasLabelGerman: Expr = HasLabel(n, Label("German"))
  val nHasLabelBook: Expr = HasLabel(n, Label("Book"))
  val nHasLabelPerson: Expr = HasLabel(n, Label("Person"))
  val nHasLabelProgrammer: Expr = HasLabel(n, Label("Programmer"))
  val nHasLabelBrogrammer: Expr = HasLabel(n, Label("Brogrammer"))
  val nHasPropertyLanguage: Expr = ElementProperty(n, PropertyKey("language"))(CTString)
  val nHasPropertyLuckyNumber: Expr = ElementProperty(n, PropertyKey("luckyNumber"))(CTInteger)
  val nHasPropertyTitle: Expr = ElementProperty(n, PropertyKey("title"))(CTString)
  val nHasPropertyYear: Expr = ElementProperty(n, PropertyKey("year"))(CTInteger)
  val nHasPropertyName: Expr = ElementProperty(n, PropertyKey("name"))(CTString)

  val r: Var = Var("r")(CTRelationship)
  val rStart: Expr = StartNode(r)(CTNode)
  val rEnd: Expr = EndNode(r)(CTNode)
  val rHasTypeKnows: Expr = HasType(r, RelType("KNOWS"))
  val rHasTypeReads: Expr = HasType(r, RelType("READS"))
  val rHasTypeInfluences: Expr = HasType(r, RelType("INFLUENCES"))
  val rHasPropertyRecommends: Expr = ElementProperty(r, PropertyKey("recommends"))(CTBoolean)
  val rHasPropertySince: Expr = ElementProperty(r, PropertyKey("since"))(CTInteger)

  override lazy val dataFixture =
    """
      |       CREATE (a:Person:German {name: "Stefan", luckyNumber: 42})
      |       CREATE (b:Person:Swede  {name: "Mats", luckyNumber: 23})
      |       CREATE (c:Person:German {name: "Martin", luckyNumber: 1337})
      |       CREATE (d:Person:German {name: "Max", luckyNumber: 8})
      |       CREATE (a)-[:KNOWS {since: 2016}]->(b)
      |       CREATE (b)-[:KNOWS {since: 2016}]->(c)
      |       CREATE (c)-[:KNOWS {since: 2016}]->(d)
    """.stripMargin

  lazy val dataFixtureSchema: PropertyGraphSchema = PropertyGraphSchema.empty
    .withNodePropertyKeys("Person", "German")("name" -> CTString, "luckyNumber" -> CTInteger,  "languages" -> CTList(CTString).nullable)
    .withNodePropertyKeys("Person", "Swede")("name" -> CTString, "luckyNumber" -> CTInteger)
    .withNodePropertyKeys("Person")("name" -> CTString, "luckyNumber" -> CTInteger, "languages" -> CTList(CTVoid))
    .withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger)

  override lazy val nbrNodes = 4

  override def nbrRels = 3

  lazy val teamDataGraphNodes: Bag[CypherMap] = Bag(
    CypherMap("n" -> LinkNode(0L, Set("Person", "German"), CypherMap("name" -> "Stefan", "luckyNumber" -> 42L, "languages" -> CypherList("German", "English", "Klingon")))),
    CypherMap("n" -> LinkNode(1L, Set("Person", "Swede"), CypherMap("name" -> "Mats", "luckyNumber" -> 23L))),
    CypherMap("n" -> LinkNode(2L, Set("Person", "German"), CypherMap("name" -> "Martin", "luckyNumber" -> 1337L))),
    CypherMap("n" -> LinkNode(3L, Set("Person", "German"), CypherMap("name" -> "Max", "luckyNumber" -> 8L))),
    CypherMap("n" -> LinkNode(4L, Set("Person"), CypherMap("name" -> "Donald", "luckyNumber" -> 8L, "languages" -> CypherList())))
  )

  lazy val teamDataGraphRels: Bag[CypherMap] = Bag(
    CypherMap("r" -> LinkRelationship(0, 0, 1, "KNOWS", CypherMap("since" -> 2016))),
    CypherMap("r" -> LinkRelationship(1, 1, 2, "KNOWS", CypherMap("since" -> 2016))),
    CypherMap("r" -> LinkRelationship(2, 2, 3, "KNOWS", CypherMap("since" -> 2016)))
  )

  lazy val csvTestGraphTags: Set[Int] = Set(0, 1)

  lazy val csvTestGraphNodes: Bag[Row] = Bag(
    Row.of(1L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, wrap(Array("german", "english")), 42L: java.lang.Long, "Stefan"),
    Row.of(2L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, wrap(Array("swedish", "english", "german")), 23L: java.lang.Long, "Mats"),
    Row.of(3L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, wrap(Array("german", "english")), 1337L: java.lang.Long, "Martin"),
    Row.of(4L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, wrap(Array("german", "swedish", "english")), 8L: java.lang.Long, "Max")
  )

  /**
    * Returns the rels for the test graph in /resources/csv/sn as expected by a
    * [[org.opencypher.okapi.relational.api.graph.RelationalCypherGraph[FlinkTable]#relationships]] call.
    *
    * @return expected rels
    */
  lazy val csvTestGraphRels: Bag[Row] = Bag(
    Row.of(1L: java.lang.Long, 10L: java.lang.Long, "KNOWS", 2L: java.lang.Long, 2016L: java.lang.Long),
    Row.of(2L: java.lang.Long, 20L: java.lang.Long, "KNOWS", 3L: java.lang.Long, 2017L: java.lang.Long),
    Row.of(3L: java.lang.Long, 30L: java.lang.Long, "KNOWS", 4L: java.lang.Long, 2015L: java.lang.Long)
  )

  /**
    * Returns the rels for the test graph in /resources/csv/sn as expected by a
    * [[org.opencypher.okapi.relational.api.graph.RelationalCypherGraph[FlinkTable]#records]] call.
    *
    * @return expected rels
    */
  lazy val csvTestGraphRelsFromRecords: Bag[Row] = Bag(
    Row.of(10L: java.lang.Long, 1L: java.lang.Long, "KNOWS", 2L: java.lang.Long, 2016L: java.lang.Long),
    Row.of(20L: java.lang.Long, 2L: java.lang.Long, "KNOWS", 3L: java.lang.Long, 2017L: java.lang.Long),
    Row.of(30L: java.lang.Long, 3L: java.lang.Long, "KNOWS", 4L: java.lang.Long, 2015L: java.lang.Long)
  )

  lazy val dataFixtureWithoutArrays =
    """
       CREATE (a:Person:German {name: "Stefan", luckyNumber: 42})
       CREATE (b:Person:Swede  {name: "Mats", luckyNumber: 23})
       CREATE (c:Person:German {name: "Martin", luckyNumber: 1337})
       CREATE (d:Person:German {name: "Max", luckyNumber: 8})
       CREATE (e:Person {name: "Donald", luckyNumber: 8})
       CREATE (a)-[:KNOWS {since: 2015}]->(b)
       CREATE (b)-[:KNOWS {since: 2016}]->(c)
       CREATE (c)-[:KNOWS {since: 2017}]->(d)
    """

  lazy val csvTestGraphNodesWithoutArrays: Bag[Row] = Bag(
    Row.of(0L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, 42L: java.lang.Long, "Stefan"),
    Row.of(1L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, 23L: java.lang.Long, "Mats"),
    Row.of(2L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, 1337L: java.lang.Long, "Martin"),
    Row.of(3L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, 8L: java.lang.Long, "Max"),
    Row.of(4L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, 8L: java.lang.Long, "Donald")
  )

  lazy val csvTestGraphRelsWithoutArrays: Bag[Row] = Bag(
    Row.of(0L: java.lang.Long, 5L: java.lang.Long, "KNOWS", 1L: java.lang.Long, 2015L: java.lang.Long),
    Row.of(1L: java.lang.Long, 6L: java.lang.Long, "KNOWS", 2L: java.lang.Long, 2016L: java.lang.Long),
    Row.of(2L: java.lang.Long, 7L: java.lang.Long, "KNOWS", 3L: java.lang.Long, 2017L: java.lang.Long)
  )

  private  def wrap[T](s: Array[T]): mutable.WrappedArray[T] = {
    mutable.WrappedArray.make(s)
  }

  private lazy val personMapping: ElementMapping = NodeMappingBuilder
    .on("ID")
    .withImpliedLabel("Person")
    .withPropertyKey("name" -> "NAME")
    .withPropertyKey("luckyNumber" -> "NUM")
    .build

  protected lazy val personDF: Table = session.tableEnv.fromDataSet(
    session.env.fromCollection(
      Seq(
        (1L: java.lang.Long, true: java.lang.Boolean, "Mats", 23L: java.lang.Long),
        (2L: java.lang.Long, false: java.lang.Boolean, "Martin", 42L: java.lang.Long),
        (3L: java.lang.Long, false: java.lang.Boolean, "Max", 1337L: java.lang.Long),
        (4L: java.lang.Long, false: java.lang.Boolean, "Stefan", 9L: java.lang.Long)
      )
    ), 'ID, 'NAME, 'NUM
  )

  lazy val personTable = LinkElementTable.create(personMapping, personDF)

  private lazy val knowsMapping: ElementMapping = RelationshipMappingBuilder
    .on("ID").from("SRC").to("DST").relType("KNOWS").withPropertyKey("since" -> "SINCE").build

  protected lazy val knowsDF: Table = session.tableEnv.fromDataSet(
    session.env.fromCollection(
      Seq(
        (1L: java.lang.Long, 1L: java.lang.Long, 2L: java.lang.Long, 2017L: java.lang.Long),
        (1L: java.lang.Long, 2L: java.lang.Long, 3L: java.lang.Long, 2016L: java.lang.Long),
        (1L: java.lang.Long, 3L: java.lang.Long, 4L: java.lang.Long, 2015L: java.lang.Long),
        (2L: java.lang.Long, 4L: java.lang.Long, 3L: java.lang.Long, 2016L: java.lang.Long),
        (2L: java.lang.Long, 5L: java.lang.Long, 4L: java.lang.Long, 2013L: java.lang.Long),
        (3L: java.lang.Long, 6L: java.lang.Long, 4L: java.lang.Long, 2016L: java.lang.Long)
      )
    ), 'SRC, 'ID, 'DST, 'SINCE
  )

  lazy val knowsTable = LinkElementTable.create(knowsMapping, knowsDF)

  private lazy val programmerMapping = NodeMappingBuilder
    .on("ID")
    .withImpliedLabel("Programmer")
    .withImpliedLabel("Person")
    .withPropertyKey("name" -> "NAME")
    .withPropertyKey("luckyNumber" -> "NUM")
    .withPropertyKey("language" -> "LANG")
    .build

  private lazy val programmerDF: Table = session.tableEnv.fromDataSet(
    session.env.fromCollection(
      Seq(
        (100L: java.lang.Long, "Alice", 42L: java.lang.Long, "C"),
        (200L: java.lang.Long, "Bob", 23L: java.lang.Long, "D"),
        (300L: java.lang.Long, "Eve", 84L: java.lang.Long, "F"),
        (400L: java.lang.Long, "Carl", 49L: java.lang.Long, "R")
      )
    ), 'ID, 'NAME, 'NUM, 'LANG
  )

  lazy val programmerTable = LinkElementTable.create(programmerMapping, programmerDF)

  private lazy val brogrammerMapping = NodeMappingBuilder
    .on("ID")
    .withImpliedLabel("Brogrammer")
    .withImpliedLabel("Person")
    .withPropertyKey("language" -> "LANG")
    .build

  private lazy val brogrammerDF = session.tableEnv.fromDataSet(
    session.env.fromCollection(
      Seq(
        (100L: java.lang.Long, "Node"),
        (200L: java.lang.Long, "Coffeescript"),
        (300L: java.lang.Long, "Javascript"),
        (400L: java.lang.Long, "Typescript")
      )
    ), 'ID, 'LANG
  )

  lazy val brogrammerTable = LinkElementTable.create(brogrammerMapping, brogrammerDF)

  private lazy val bookMapping = NodeMappingBuilder
    .on("ID")
    .withImpliedLabel("Book")
    .withPropertyKey("title" -> "NAME")
    .withPropertyKey("year" -> "YEAR")
    .build

  private lazy val bookDF: Table = session.tableEnv.fromDataSet(
    session.env.fromCollection(
      Seq(
        (10L: java.lang.Long, "1984", 1949L: java.lang.Long),
        (20L: java.lang.Long, "Cryptonomicon", 1999L: java.lang.Long),
        (30L: java.lang.Long, "The Eye of the World", 1990L: java.lang.Long),
        (40L: java.lang.Long, "The Circle", 2013L: java.lang.Long)
      )
    ), 'ID, 'NAME, 'YEAR
  )

  lazy val  bookTable = LinkElementTable.create(bookMapping, bookDF)

  private lazy val readsMapping = RelationshipMappingBuilder
    .on("ID").from("SRC").to("DST").relType("READS").withPropertyKey("recommends" -> "RECOMMENDS").build

  private lazy val readsDF = session.tableEnv.fromDataSet(
    session.env.fromCollection(
      Seq(
        (100L: java.lang.Long, 100L: java.lang.Long, 10L: java.lang.Long, true: java.lang.Boolean),
        (200L: java.lang.Long, 200L: java.lang.Long, 40L: java.lang.Long, true: java.lang.Boolean),
        (300L: java.lang.Long, 300L: java.lang.Long, 30L: java.lang.Long, true: java.lang.Boolean),
        (400L: java.lang.Long, 400L: java.lang.Long, 20L: java.lang.Long, false: java.lang.Boolean)
      )
    ), 'SRC, 'ID, 'DST, 'RECOMMENDS
  )

  lazy val readsTable = LinkElementTable.create(readsMapping, readsDF)

  private lazy val influencesMapping = RelationshipMappingBuilder
    .on("ID").from("SRC").to("DST").relType("INFLUENCES").build

  private lazy val influencesDF: Table = session.tableEnv.fromDataSet(
    session.env.fromCollection(
      Seq((10L: java.lang.Long, 1000L: java.lang.Long, 20L: java.lang.Long))
    ), 'SRC, 'ID, 'DST
  )

  lazy val influencesTable = LinkElementTable.create(influencesMapping, influencesDF)

}

