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
package org.opencypher.okapi.relational.api.schema

import org.opencypher.okapi.api.schema.{PropertyKeys, PropertyGraphSchema}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.relational.api.schema.RelationalPropertyGraphSchema._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.testing.BaseTestSuite

class RelationalPropertyGraphSchemaTest extends BaseTestSuite {

  it("creates a header for given node") {
    val schema = PropertyGraphSchema.empty
      .withNodePropertyKeys(Set("A", "B"), PropertyKeys("foo" -> CTBoolean))

    val n = Var("n")(CTNode(Set("A", "B")))

    schema.headerForNode(n) should equal(RecordHeader.empty
      .withExpr(n)
      .withExpr(HasLabel(n, Label("A")))
      .withExpr(HasLabel(n, Label("B")))
      .withExpr(ElementProperty(n, PropertyKey("foo"))(CTBoolean)))
  }

  it("creates a header for a given node and changes nullability if necessary") {
    val schema = PropertyGraphSchema.empty
      .withNodePropertyKeys(Set("A", "B"), PropertyKeys.empty)
      .withNodePropertyKeys(Set("A", "C"), PropertyKeys("foo" -> CTString))

    val n = Var("n")(CTNode(Set("A")))

    schema.headerForNode(n) should equal(RecordHeader.empty
      .withExpr(n)
      .withExpr(HasLabel(n, Label("A")))
      .withExpr(HasLabel(n, Label("B")))
      .withExpr(HasLabel(n, Label("C")))
      .withExpr(ElementProperty(n, PropertyKey("foo"))(CTString.nullable)))
  }

  it("creates a header for given node with implied labels") {
    val schema = PropertyGraphSchema.empty
      .withNodePropertyKeys(Set("A"), PropertyKeys("foo" -> CTBoolean))
      .withNodePropertyKeys(Set("A", "B"), PropertyKeys("bar" -> CTBoolean))

    val n = Var("n")(CTNode(Set("A")))

    schema.headerForNode(n) should equal(RecordHeader.empty
      .withExpr(n)
      .withExpr(HasLabel(n, Label("A")))
      .withExpr(HasLabel(n, Label("B")))
      .withExpr(ElementProperty(n, PropertyKey("foo"))(CTBoolean.nullable))
      .withExpr(ElementProperty(n, PropertyKey("bar"))(CTBoolean.nullable)))
  }

  it("creates a header for a given relationship") {
    val schema = PropertyGraphSchema.empty
      .withRelationshipPropertyKeys("A", PropertyKeys("foo" -> CTBoolean))

    val r = Var("r")(CTRelationship("A"))

    schema.headerForRelationship(r) should equal(RecordHeader.empty
      .withExpr(r)
      .withExpr(StartNode(r)(CTNode))
      .withExpr(EndNode(r)(CTNode))
      .withExpr(HasType(r, RelType("A")))
      .withExpr(ElementProperty(r, PropertyKey("foo"))(CTBoolean)))
  }
}
