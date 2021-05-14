package org.apache.spark.sql.execution

import org.apache.commons.logging.LogFactory
import org.apache.ranger.authorization.spark.authorizer.{RangerSparkAuthorizer, SparkPrivilegeObject, SparkPrivilegeObjectType}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{DescribeTableCommand, RunnableCommand}

/**
 * @author qiang.bi
 * @date 2021/5/14
 * @description:
 * */
case class RangerDescribeTableCommand(child: DescribeTableCommand) extends RunnableCommand {

  private val LOG = LogFactory.getLog(classOf[RangerDescribeTableCommand])

  override val output: Seq[Attribute] = child.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val rows = child.run(sparkSession)
    rows.filter(r => RangerSparkAuthorizer.isAllowed(toSparkPrivilegeObject(r)))
  }

  private def toSparkPrivilegeObject(row: Row): SparkPrivilegeObject = {
    val database = row.getString(0)
    LOG.info("***database***" + database)
    val table = row.getString(1)
    LOG.info("***table***" + table)
    new SparkPrivilegeObject(SparkPrivilegeObjectType.TABLE_OR_VIEW, database, table)
  }
}
