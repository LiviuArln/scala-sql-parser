package org.rau.tablerels


case class TableRelations(tables: Map[String, String], relations: Map[(String, String), (String, String)]) {
  def join(other: TableRelations): TableRelations =
    TableRelations(tables ++ other.tables, relations ++ other.relations)

  def expandRelations = relations.map {
    case (from, to) => {
      def expand(p: (String, String)) = tables.get(p._1).map(_ -> p._2).getOrElse(p)
      expand(from) -> expand(to)
    }
  }
}

object TableRelations {
  import com.stephentu.sql._

  val emptyRels = TableRelations(Map(), Map())

  def processNode(n: Node): TableRelations = n match {
    case SelectStmt(_, relations, filter, _, _, _, _) => processNodes(relations).join(processNode(filter))
    case TableRelationAST(tableName, Some(alias), _) => TableRelations(Map(alias -> tableName), Map())
    case JoinRelation(table1, table2, _, exp, _) => processNode(table1).join(processNode(table2)).join(processNode(exp))
    case SubqueryRelationAST(subSelect, _, _) => processNode(subSelect)
    case Exists(Subselect(subSelect, _), _) => processNode(subSelect)
    case Eq(FieldIdent(Some(qualifier1), field1, _, _), FieldIdent(Some(qualifier2), field2, _, _), _) => TableRelations(Map(), Map((qualifier1, field1) -> (qualifier2, field2)))
    case Neq(FieldIdent(Some(qualifier1), field1, _, _), FieldIdent(Some(qualifier2), field2, _, _), _) => TableRelations(Map(), Map((qualifier1, field1) -> (qualifier2, field2)))
    case CaseExpr(expr, cases, default, _) => processNode(expr).join(processNodes(cases)).join(processNode(default))
    case op: Binop => processNode(op.lhs).join(processNode(op.rhs))
    case op: Unop => processNode(op.expr)
    case _: SqlProj | _: SqlAgg | _: SqlFunction | _: LiteralExpr | _: SqlGroupBy | _: SqlOrderBy | _: FieldIdent | _: TableRelationAST => emptyRels
  }

  def processNode(n: Option[Node]): TableRelations = n.map(processNode).getOrElse(emptyRels)

  def processNodes(n: Option[Seq[Node]]): TableRelations = n.map(processNodes).getOrElse(emptyRels)

  def processNodes(n: Seq[Node]): TableRelations = n.map(processNode).foldLeft(emptyRels)(_.join(_))

  def inspect(s: String) = (new ExtendedParser).parse(s).map(processNode)

}

class ExtendedParser extends com.stephentu.sql.SQLParser {
  import com.stephentu.sql._

  lexical.reserved += ("distinctrow", "group_concat", "hour", "using", "utf8")
  lexical.delimiters += ("||", "!", "&&")

  override def select: Parser[SelectStmt] =
    "select" ~ opt(distinct) ~> projections ~
      opt(relations) ~ opt(filter) ~
      opt(groupBy) ~ opt(orderBy) ~ opt(limit) <~ opt(";") ^^ {
        case p ~ r ~ f ~ g ~ o ~ l => SelectStmt(p, r, f, g, o, l)
      }

  def distinct: Parser[Any] = "distinct" | "distinctrow"

  override def or_expr: Parser[SqlExpr] =
    and_expr * (("or" | "||") ^^^ { (a: SqlExpr, b: SqlExpr) =>
      Or(a, b)
    })

  override def and_expr: Parser[SqlExpr] =
    cmp_expr * (("and" | "&&") ^^^ { (a: SqlExpr, b: SqlExpr) =>
      And(a, b)
    })

  override def cmp_expr: Parser[SqlExpr] =
    super.cmp_expr <~ "is" <~ "not" <~ "null" ^^ { x =>
      Not(Eq(x, NullLiteral()))
    } | super.cmp_expr <~ "is" <~ "null" ^^ {
      Eq(_, NullLiteral())
    } | "!" ~> super.cmp_expr ^^ {
      Not(_)
    } | super.cmp_expr

  override def known_function: Parser[SqlExpr] =
    "group_concat" ~> "(" ~> expr <~ ")" ^^ {
      Max(_) //I'm kind of ignoring this function
    } | super.known_function

  override def literal =
    "interval" ~> (opt("-") ~> numericLit) ~ ("year" ^^^ (YEAR) | "month" ^^^ (MONTH) | "day" ^^^ (DAY) | "hour" ^^^ (DAY)) ^^ { //ignore hour cose the type is sealed
      case d ~ u => IntervalLiteral(d, u)
    } | super.literal

  override def projection: Parser[SqlProj] =
    "*" ^^ { _ => StarProj()
    } | expr ~ opt(opt("as") ~> ident) ^^ {
      case expr ~ ident => ExprProj(expr, ident)
    }

  override def primary_expr: Parser[SqlExpr] =
    ident ~ opt("." ~> ident | "(" ~> repsep(expr, ",") <~ ")") <~ (("using" ~ "utf8") | ("as" ~ ident)) ^^ {
      case id ~ None            => FieldIdent(None, id)
      case a ~ Some(b: String)  => FieldIdent(Some(a), b)
      case a ~ Some(xs: Seq[_]) => FunctionCall(a, xs.asInstanceOf[Seq[SqlExpr]])
    } | super.primary_expr

}

object ProofOfConcept extends App {
  import TableRelations._
  import SQLs._
  import collection.JavaConverters._

  def sanitize(s: String): String = s.
    toLowerCase.
    replace("@", "").
    replace(":=", "=").
    replace("%s", "5")

  def addInverseRels[T](rels: Map[T, T]) =
    rels ++ rels.map(_.swap)

  val allRelations = (new SQLs).allSQLs.
    map(sanitize).
    map(_.split("union")).flatten.
    map(inspect).
    filter(_.isDefined).
    map(_.get.expandRelations).fold(Map())(_ ++ _)

  val allPlusInverse = addInverseRels(allRelations)
  
  val relsStringReperesetation =
    allPlusInverse.map {
      case (from, to) => s"${from._1}.${from._2}" -> s"${to._1}.${to._2}"
    }

  relsStringReperesetation.toList.sorted.map {
    case (from, to) => f"$from%-55s ?=? $to"
  }.zipWithIndex.foreach {
    case (rel, idx) => println(f"${idx + 1}%-3s$rel")
  }

}