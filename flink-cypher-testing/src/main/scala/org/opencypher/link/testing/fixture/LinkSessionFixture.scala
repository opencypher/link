package org.opencypher.link.testing.fixture

import org.opencypher.link.api.LinkSession
import org.opencypher.okapi.testing.{BaseTestFixture, BaseTestSuite}

trait LinkSessionFixture extends BaseTestFixture {

  self: FlinkSessionFixture with BaseTestSuite =>

  implicit lazy val session: LinkSession = LinkSession.local()

  abstract override protected def afterEach(): Unit = {
    session.catalog.source(session.catalog.sessionNamespace).graphNames.map(_.value).foreach(session.catalog.dropGraph)
    session.catalog.store(session.emptyGraphQgn, session.graphs.empty)
    super.afterEach()
  }
}
