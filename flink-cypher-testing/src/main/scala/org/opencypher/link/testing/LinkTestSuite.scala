package org.opencypher.link.testing

import org.opencypher.link.impl.table.LinkCypherTable.FlinkTable
import org.opencypher.link.testing.fixture.{FlinkSessionFixture, LinkSessionFixture}
import org.opencypher.link.testing.support.{GraphMatchingTestSupport, RecordMatchingTestSupport}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.BaseTestSuite

abstract class LinkTestSuite
  extends BaseTestSuite
  with FlinkSessionFixture
  with LinkSessionFixture
  with GraphMatchingTestSupport
  with RecordMatchingTestSupport {

 def catalog(qgn: QualifiedGraphName): Option[RelationalCypherGraph[FlinkTable]] = None
}
