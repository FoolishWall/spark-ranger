package org.apache.ranger.authorization.spark.authorizer

import com.sun.jersey.api.client.ClientResponse
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.ranger.admin.client.datatype.RESTResponse
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration
import org.apache.ranger.authorization.utils.StringUtil
import org.apache.ranger.plugin.util.{RangerRESTClient, RangerRESTUtils, RangerServiceNotFoundException, ServicePolicies}
import sun.misc.BASE64Encoder

/**
 * @author qiang.bi
 * @date 2021/5/10
 * @description:
 * */
class RangerAdminRestClientExtend extends RangerAdminRESTClient {

  import RangerAdminRestClientExtend._

  private val LOG: Log = LogFactory.getLog(classOf[RangerAdminRestClientExtend])

  private var serviceName = ""
  private var appId = ""
  private var pluginId = ""
  private var clusterName = ""
  private var username = ""
  private var password = ""
  private var restClient = new RangerRESTClient
  private val restUtils = new RangerRESTUtils

  override def init(serviceName: String, appId: String, propertyPrefix: String): Unit = {
    //get parameter
    super.init(serviceName, appId, propertyPrefix)

    //set parameter
    this.serviceName = serviceName
    this.appId = appId
    this.pluginId = this.restUtils.getPluginId(serviceName, appId);
    this.clusterName = RangerConfiguration.getInstance.get(propertyPrefix + ".ambari.cluster.name", "")
    this.username = RangerConfiguration.getInstance.get(propertyPrefix + ".username")
    this.password = RangerConfiguration.getInstance.get(propertyPrefix + ".password")

    //init RangerRESTClient
    var url = ""
    val tmpUrl = RangerConfiguration.getInstance.get(propertyPrefix + ".policy.rest.url")
    val sslConfigFileName = RangerConfiguration.getInstance.get(propertyPrefix + ".policy.rest.ssl.config.file")
    val restClientConnTimeOutMs = RangerConfiguration.getInstance.getInt(propertyPrefix + ".policy.rest.client.connection.timeoutMs", 120000)
    val restClientReadTimeOutMs = RangerConfiguration.getInstance.getInt(propertyPrefix + ".policy.rest.client.read.timeoutMs", 30000)
    if (!StringUtil.isEmpty(tmpUrl)) url = tmpUrl.trim
    if (url.endsWith("/")) url = url.substring(0, url.length - 1)
    this.restClient = new RangerRESTClient(url, sslConfigFileName)
    this.restClient.setRestClientConnTimeOutMs(restClientConnTimeOutMs)
    this.restClient.setRestClientReadTimeOutMs(restClientReadTimeOutMs)
  }

  override def getServicePoliciesIfUpdated(lastKnownVersion: Long, lastActivationTimeInMillis: Long): ServicePolicies = {
    LOG.info("===============start get policy===============")
    //authorisationToken
    val authString = this.username + ":" + this.password
    val authStringEnc = new BASE64Encoder().encode(authString.getBytes)

    val webResource = this.restClient.getResource(REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + this.serviceName)
        .queryParam(REST_PARAM_LAST_KNOWN_POLICY_VERSION, lastKnownVersion.toString)
        .queryParam(REST_PARAM_LAST_ACTIVATION_TIME, lastActivationTimeInMillis.toString)
        .queryParam(REST_PARAM_PLUGIN_ID, this.pluginId)
        .queryParam(REST_PARAM_CLUSTER_NAME, this.clusterName)
    val response = webResource.accept("application/json")
        .header("Authorization", "Basic " + authStringEnc)
        .get(classOf[ClientResponse])

    LOG.info("*** response ***" + response)

    //deal with response
    var ret = new ServicePolicies
    var resp = new RESTResponse
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
        LOG.warn("Error getting policies. " + "response=" + resp + ", serviceName=" + this.serviceName)
        ret = null
      }
    else {
      if (response == null)
        LOG.error("Error getting policies; Received NULL response!! " + ", serviceName=" + this.serviceName)
      else {
        resp = RESTResponse.fromClientResponse(response)
        LOG.warn("No change in policies. " + ", response=" + resp + ", serviceName=" + this.serviceName)
      }
      ret = null
    }
    ret
  }

}

object RangerAdminRestClientExtend {
  private val REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED = "/service/plugins/secure/policies/download/"
  private val REST_PARAM_LAST_KNOWN_POLICY_VERSION = "lastKnownVersion"
  private val REST_PARAM_LAST_ACTIVATION_TIME = "lastActivationTime"
  private val REST_PARAM_PLUGIN_ID = "pluginId"
  private val REST_PARAM_CLUSTER_NAME = "clusterName"
}
