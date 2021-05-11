package org.apache.ranger.authorization.spark.authorizer

import com.sun.jersey.api.client.ClientResponse
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.ranger.plugin.util.ServiceTags

/**
 * @author qiang.bi
 * @date 2021/5/10
 * @description:
 * */
class RangerAdminRestClientExtend extends RangerAdminRESTClient {
  private val LOG: Log = LogFactory.getLog(classOf[RangerAdminRestClientExtend])

  override def getServiceTagsIfUpdated(lastKnownVersion: Long, lastActivationTimeInMillis: Long): ServiceTags = {
    LOG.info("***serviceName***" + super.getServiceName)
    LOG.info("***PluginId***" + super.getPluginId)
    LOG.info("***ClusterName***" + super.getClusterName)
    LOG.info("***get policy***")

    val serviceNameUrlParam = super.getServiceNameUrlParam
    val restClient = super.getRestClient
    val pluginId = super.getPluginId
    val clusterName = super.getClusterName
    val supportsPolicyDeltas = super.getSupportsPolicyDeltas

    val webResource = restClient.getResource("/service/plugins/policies/download/" + serviceNameUrlParam)
      .queryParam("lastKnownVersion", lastKnownVersion.toString)
      .queryParam("lastActivationTime", lastActivationTimeInMillis.toString)
      .queryParam("pluginId", pluginId).queryParam("clusterName", clusterName)
      .queryParam("supportsPolicyDeltas", supportsPolicyDeltas)

    val response = webResource.accept("application/json").get(classOf[ClientResponse])

    LOG.info("***status***" + response.getStatus)

    response.getEntity(classOf[ServiceTags])
  }

}
