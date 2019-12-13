package org.opencypher.link.impl

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.scala.array
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions
import org.apache.flink.table.expressions.{Expression, MapConstructor, Null, UnresolvedFieldReference}
import org.opencypher.link.impl.FlinkSQLExprMapper._
import org.opencypher.link.impl.convert.FlinkConversions._
import org.opencypher.link.impl.table.TableOperations._
import org.opencypher.okapi.api.types.CTNull
import org.opencypher.okapi.api.value.CypherValue.{CypherList, CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.relational.impl.table.RecordHeader

object LinkFunctions {

  val FALSE_LIT = expressions.Literal(false, Types.BOOLEAN)
  val TRUE_LIT = expressions.Literal(true, Types.BOOLEAN)
  val ONE_LIT = expressions.Literal(1, Types.INT)
  val E_LIT = expressions.E()
  val PI_LIT = expressions.Pi()
  def null_lit(tpe: TypeInformation[_] = Types.BOOLEAN) = Null(tpe)

  def null_safe_conversion(expr: Expr)(withConvertedChildren: Seq[Expression] => Expression)
    (implicit header: RecordHeader, table: Table, parameters: CypherMap): Expression = {
    if (expr.cypherType == CTNull) {
      null_lit(expr.cypherType.getFlinkType)
    } else {
      val evaluatedArgs = expr.children.map(_.asFlinkSQLExpr)
      withConvertedChildren(evaluatedArgs)
    }
  }

  def expression_for(expr: Expr)(implicit header: RecordHeader, table: Table): Expression = {
    val columnName = header.getColumn(expr).getOrElse(throw IllegalArgumentException(
      expected = s"Expression in ${header.expressions.mkString("[", ", ", "]")}",
      actual = expr)
    )
    if (table.columns.contains(columnName)) {
      UnresolvedFieldReference(columnName)
    } else {
      null_lit(expr.cypherType.getFlinkType)
    }
  }

  implicit class CypherValueConversion(val v: CypherValue) extends AnyVal {
    def toFlinkLiteral: Expression = {
      v.cypherType.ensureFlinkCompatible()
      v match {
        case list: CypherList => {
          val listValues = list.value.map(_.toFlinkLiteral)
          array(listValues.head, listValues.tail: _*)
        }
        case map: CypherMap => MapConstructor(map.value.foldLeft(Seq.empty[Expression]) {
          case (acc, (key, value)) => {
            acc :+ expressions.Literal(key, Types.STRING)
            acc :+ value.toFlinkLiteral
          }
        })
        case _ => expressions.Literal(v.unwrap, v.cypherType.getFlinkType)
      }
    }
  }
}
