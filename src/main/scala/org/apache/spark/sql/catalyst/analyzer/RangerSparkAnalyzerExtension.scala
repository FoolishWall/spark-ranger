package org.apache.spark.sql.catalyst.analyzer

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{DescribeTableCommand}

/**
 * @author qiang.bi
 * @date 2021/5/14
 * @description:
 * */
case class RangerSparkAnalyzerExtension(spark: SparkSession) extends Rule[LogicalPlan] {

  private val LOG = LogFactory.getLog(classOf[RangerSparkAnalyzerExtension])

  override def apply(plan: LogicalPlan): LogicalPlan = {
    LOG.info("*** analyzer plan***" + plan)
    plan match {
      case s: DescribeTableCommand =>
        LOG.info("*** analyzer plan database ***" + s.table.database)
        plan
      case _ => plan
    }
  }
}
