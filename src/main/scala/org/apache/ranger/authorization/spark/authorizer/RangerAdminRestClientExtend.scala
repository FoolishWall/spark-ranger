package org.apache.ranger.authorization.spark.authorizer

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration
import org.apache.ranger.plugin.util.{RangerRESTUtils, ServicePolicies}

/**
 * @author qiang.bi
 * @date 2021/5/10
 * @description:
 * */
class RangerAdminRestClientExtend extends RangerAdminRESTClient {
  private val LOG: Log = LogFactory.getLog(classOf[RangerAdminRestClientExtend])

  private var serviceName = ""
  private var appId = ""
  private var pluginId = ""
  private var clusterName = ""
  private val restUtils = new RangerRESTUtils

  override def init(serviceName: String, appId: String, propertyPrefix: String): Unit = {
    super.init(serviceName, appId, propertyPrefix)
    this.serviceName = serviceName
    this.appId = appId
    this.pluginId = this.restUtils.getPluginId(serviceName, appId);
    this.clusterName = RangerConfiguration.getInstance.get(propertyPrefix + ".ambari.cluster.name", "")
  }

  override def getServicePoliciesIfUpdated(lastKnownVersion: Long, lastActivationTimeInMillis: Long): ServicePolicies = {
    LOG.info("***serviceName***" + this.serviceName)
    LOG.info("***PluginId***" + this.pluginId)
    LOG.info("***ClusterName***" + this.clusterName)
    LOG.info("***get policy***")
    null
  }

}
