package org.opencypher.link.impl

import org.opencypher.link.api.LinkSession
import org.opencypher.link.impl.table.LinkCypherTable.FlinkTable
import org.opencypher.okapi.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult

import scala.util.{Failure, Success, Try}

object LinkConverters {
  private def unsupported(expected: String, got: Any): Nothing =
    throw UnsupportedOperationException(s"Can only handle $expected, got $got")

  implicit class RichPropertyGraph(val graph: PropertyGraph) extends AnyVal {
    def asLink: RelationalCypherGraph[FlinkTable] = graph.asInstanceOf[RelationalCypherGraph[_]] match {
      case link: RelationalCypherGraph[_] =>
        Try {
          link.asInstanceOf[RelationalCypherGraph[FlinkTable]]
        } match {
          case Success(value) => value
          case Failure(_) => unsupported("Link graphs", link)
        }
      case other => unsupported("Link graphs", other)
    }
  }

  implicit class RichSession(session: CypherSession) {
    def asLink: LinkSession = session match {
      case link: LinkSession  => link
      case other              => unsupported("Link session", other)
    }
  }

  implicit class RichCypherRecords(val records: CypherRecords) extends AnyVal {
    def asLink: LinkRecords = records match {
      case link: LinkRecords => link
      case other => unsupported("Link records", other)
    }
  }

  implicit class RichCypherResult(val records: CypherResult) extends AnyVal {
    def asLink(implicit session: LinkSession): RelationalCypherResult[FlinkTable] = records match {
      case relational: RelationalCypherResult[_] =>
        Try {
          relational.asInstanceOf[RelationalCypherResult[FlinkTable]]
        } match {
          case Success(value) => value
          case Failure(_) => unsupported("Link results", relational)
        }
      case other => unsupported("Link results", other)
    }
  }
}
