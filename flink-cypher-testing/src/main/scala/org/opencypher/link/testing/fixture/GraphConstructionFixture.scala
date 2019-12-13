package org.opencypher.link.testing.fixture

import org.opencypher.link.impl.table.LinkCypherTable.FlinkTable
import org.opencypher.link.testing.support.creation.graphs.{ScanGraphFactory, TestGraphFactory}
import org.opencypher.okapi.api.graph.Pattern
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.propertygraph.CreateGraphFactory
import org.opencypher.link.impl.LinkConverters._

trait GraphConstructionFixture {
  self: LinkSessionFixture with BaseTestSuite =>

  def graphFactory: TestGraphFactory = ScanGraphFactory

  def initGraph(query: String, additionalPatterns: Seq[Pattern] = Seq.empty): RelationalCypherGraph[FlinkTable] =
    ScanGraphFactory(CreateGraphFactory(query), additionalPatterns).asLink
}
