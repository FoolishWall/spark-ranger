package org.apache.spark.sql.catalyst.parser
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * @author qiang.bi
 * @date 2021/5/17
 * @description:
 * */
case class RangerSparkParser(parser: ParserInterface) extends ParserInterface {

  private val LOG = LogFactory.getLog(classOf[RangerSparkParser])
  /**
   * Parse a string to a [[LogicalPlan]].
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    LOG.info("***parser sqlText ***" + sqlText)
    val logicalPlan = parser.parsePlan(sqlText)
    LOG.info("***parser logicalPlan ***" + logicalPlan)
    logicalPlan
  }

  /**
   * Parse a string to an [[Expression]].
   */
  override def parseExpression(sqlText: String): Expression = {
    val expression = parser.parseExpression(sqlText)
    LOG.info("***parser expression ***" + expression)
    expression
  }

  /**
   * Parse a string to a [[TableIdentifier]].
   */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    val tableIdentifier = parser.parseTableIdentifier(sqlText)
    LOG.info("***parser tableIdentifier ***" + tableIdentifier)
    tableIdentifier
  }

  /**
   * Parse a string to a [[FunctionIdentifier]].
   */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    val functionIdentifier = parser.parseFunctionIdentifier(sqlText)
    LOG.info("***parser functionIdentifier ***" + functionIdentifier)
    functionIdentifier
  }

  /**
   * Parse a string to a [[StructType]]. The passed SQL string should be a comma separated
   * list of field definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = {
    val structType = parser.parseTableSchema(sqlText)
    LOG.info("***parser structType ***" + structType)
    structType
  }

  /**
   * Parse a string to a [[DataType]].
   */
  override def parseDataType(sqlText: String): DataType = {
    val dataType = parser.parseDataType(sqlText)
    LOG.info("***parser dataType ***" + dataType)
    dataType
  }
}
