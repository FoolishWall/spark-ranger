package org.apache.ranger.authorization.spark.authorizer

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.ranger.plugin.util.ServicePolicies

/**
 * @author qiang.bi
 * @date 2021/5/10
 * @description:
 * */
class RangerAdminRestClientExtend extends RangerAdminRESTClient {
  private val LOG: Log = LogFactory.getLog(classOf[RangerAdminRestClientExtend])

  override def getServicePoliciesIfUpdated(lastKnownVersion: Long, lastActivationTimeInMillis: Long): ServicePolicies = {
    LOG.info("***get policy***")
    null
  }

}
