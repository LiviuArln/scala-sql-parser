package org.rau.tablerels

import com.stephentu.sql._

case class TableRelations(tables: Set[String], relations: Map[String, String]) {
  def join(other: TableRelations): TableRelations =
    TableRelations(tables ++ other.tables, relations ++ other.relations)
  def show: String = {
    val tablesList = tables.fold("Tables:")(_ + "\n" + _)
    val relationsList = relations.map {
      case (from, to) => s"$from <?> $to"
    }.fold("Relations:")(_ + "\n" + _)
    tablesList + "\n\n" + relationsList
  }
}

object TableRelations {
  val emptyRels = TableRelations(Set(), Map())
  
  def processNode(n: Node): TableRelations = n match {
    case SelectStmt(_, relations, filter, _, _, _, _) => processNodes(relations).join(processNode(filter))
    case table @ TableRelationAST(_, _, _) => TableRelations(Set(table.sql), Map())
    case JoinRelation(table1, table2, _, exp, _) => processNode(table1).join(processNode(table2)).join(processNode(exp))
    case SubqueryRelationAST(subSelect, _, _) => processNode(subSelect)
    case Exists(Subselect(subSelect, _), _) => processNode(subSelect)
    case Eq(f1 @ FieldIdent(_, _, _, _), f2 @ FieldIdent(_, _, _, _), _) => TableRelations(Set(), Map(f1.sql -> f2.sql))
    case Neq(f1 @ FieldIdent(_, _, _, _), f2 @ FieldIdent(_, _, _, _), _) => TableRelations(Set(), Map(f1.sql -> f2.sql))
    case CaseExpr(expr, cases, default, _) => processNode(expr).join(processNodes(cases)).join(processNode(default))
    case op: Binop => processNode(op.lhs).join(processNode(op.rhs))
    case op: Unop => processNode(op.expr)
    case _: SqlProj | _: SqlAgg | _: SqlFunction | _: LiteralExpr | _: SqlGroupBy | _: SqlOrderBy => emptyRels
  }

  def processNode(n: Option[Node]): TableRelations = n.map(processNode).getOrElse(emptyRels)

  def processNodes(n: Option[Seq[Node]]): TableRelations = n.map(processNodes).getOrElse(emptyRels)
  
  def processNodes(n: Seq[Node]): TableRelations = n.map(processNode).foldLeft(emptyRels)(_.join(_))
}

object ProofOfConcept extends App {
  val s = "select * from table1 t1,table2 inner join table3 on t.id=t2.id where t2.x=t.y and a=b"

  val x = (new SQLParser).parse(s).map(TableRelations.processNode)

  println(x.get.show)
}