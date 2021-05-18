package org.apache.ranger.authorization.spark.authorizer

import com.sun.jersey.api.client.ClientResponse
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.ranger.admin.client.datatype.RESTResponse
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration
import org.apache.ranger.authorization.utils.StringUtil
import org.apache.ranger.plugin.util.{RangerRESTClient, RangerRESTUtils, RangerServiceNotFoundException, ServicePolicies}

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
  private var restClient = new RangerRESTClient
  private val restUtils = new RangerRESTUtils

  override def init(serviceName: String, appId: String, propertyPrefix: String): Unit = {
    super.init(serviceName, appId, propertyPrefix)
    //get parameter
    this.serviceName = serviceName
    this.appId = appId
    this.pluginId = this.restUtils.getPluginId(serviceName, appId);
    this.clusterName = RangerConfiguration.getInstance.get(propertyPrefix + ".ambari.cluster.name", "")

    //init RangerRESTClient
    var url = ""
    val tmpUrl = RangerConfiguration.getInstance.get(propertyPrefix + ".policy.rest.url")
    LOG.info("*** tmpUrl ***" + tmpUrl)
    val sslConfigFileName = RangerConfiguration.getInstance.get(propertyPrefix + ".policy.rest.ssl.config.file")
    LOG.info("*** sslConfigFileName ***" + sslConfigFileName)
    val restClientConnTimeOutMs = RangerConfiguration.getInstance.getInt(propertyPrefix + ".policy.rest.client.connection.timeoutMs", 120000)
    LOG.info("*** restClientConnTimeOutMs ***" + restClientConnTimeOutMs)
    val restClientReadTimeOutMs = RangerConfiguration.getInstance.getInt(propertyPrefix + ".policy.rest.client.read.timeoutMs", 30000)
    LOG.info("*** restClientReadTimeOutMs ***" + restClientReadTimeOutMs)
    if (!StringUtil.isEmpty(tmpUrl)) url = tmpUrl.trim
    if (url.endsWith("/")) url = url.substring(0, url.length - 1)
    this.restClient = new RangerRESTClient(url, sslConfigFileName)
    this.restClient.setRestClientConnTimeOutMs(restClientConnTimeOutMs)
    this.restClient.setRestClientReadTimeOutMs(restClientReadTimeOutMs)
  }

  override def getServicePoliciesIfUpdated(lastKnownVersion: Long, lastActivationTimeInMillis: Long): ServicePolicies = {
    LOG.info("=========start get policy=========")
    LOG.info("***lastKnownVersion***" + lastKnownVersion)
    LOG.info("***lastActivationTimeInMillis***" + lastActivationTimeInMillis)
    LOG.info("***serviceName***" + this.serviceName)
    LOG.info("***PluginId***" + this.pluginId)
    LOG.info("***ClusterName***" + this.clusterName)

    val webResource = this.restClient.getResource("/service/plugins/policies/download/" + this.serviceName)
        .queryParam("lastKnownVersion", lastKnownVersion.toString)
        .queryParam("lastActivationTime", lastActivationTimeInMillis.toString)
        .queryParam("pluginId", this.pluginId).queryParam("clusterName", this.clusterName)

    val response = webResource.accept("application/json").get(classOf[ClientResponse])

    LOG.info("*** response ***" + response)

    var ret = ServicePolicies
    var resp = RESTResponse
    if (response != null && response.getStatus != 304)
      if (response.getStatus == 200) {
        ret = response.getEntity(classOf[ServicePolicies])
      }
      else if (response.getStatus == 404) {
        LOG.error("Error getting policies; service not found." +
            " response=" + response.getStatus +
            ", serviceName=" + this.serviceName +
            ", lastKnownVersion=" + lastKnownVersion +
            ", lastActivationTimeInMillis=" + lastActivationTimeInMillis)
        val exceptionMsg = if (response.hasEntity) response.getEntity(classOf[String])
        else null
        RangerServiceNotFoundException.throwExceptionIfServiceNotFound(this.serviceName, exceptionMsg)
        LOG.warn("Received 404 error code with body:[" + exceptionMsg + "], Ignoring")
      }
      else {
        resp = RESTResponse.fromClientResponse(response)
        LOG.warn("Error getting policies. " + ", response=" + resp + ", serviceName=" + this.serviceName)
        ret = null
      }
    else {
      if (response == null)
        LOG.error("Error getting policies; Received NULL response!! " + ", serviceName=" + this.serviceName)
      else {
        resp = RESTResponse.fromClientResponse(response)
        LOG.debug("No change in policies. " + ", response=" + resp + ", serviceName=" + this.serviceName)
      }
      ret = null
    }
    ret
  }

}
