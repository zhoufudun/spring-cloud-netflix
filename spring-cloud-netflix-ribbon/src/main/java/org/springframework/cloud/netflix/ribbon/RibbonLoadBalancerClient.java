/*
 * Copyright 2013-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.netflix.ribbon;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.Server;

import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.LoadBalancerClient;
import org.springframework.cloud.client.loadbalancer.LoadBalancerRequest;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import static org.springframework.cloud.netflix.ribbon.RibbonUtils.updateToSecureConnectionIfNeeded;

/**
 * @author Spencer Gibb
 * @author Dave Syer
 * @author Ryan Baxter
 * @author Tim Ysewyn
 */
public class RibbonLoadBalancerClient implements LoadBalancerClient {

	private SpringClientFactory clientFactory; // org.springframework.cloud.netflix.ribbon.SpringClientFactory@72358ce6

	public RibbonLoadBalancerClient(SpringClientFactory clientFactory) {
		this.clientFactory = clientFactory;
	}

	/**
	 * 对于使用ribbon的情况
	 *
	 * @param instance RibbonServer{serviceId='nacos-user-service', server=10.2.40.18:8207, secure=false, metadata={preserved.register.source=SPRING_CLOUD}}
	 * @param original http://nacos-user-service/user/create
	 * @return
	 */
	@Override
	public URI reconstructURI(ServiceInstance instance, URI original) {
		Assert.notNull(instance, "instance can not be null");
		String serviceId = instance.getServiceId(); // nacos-user-service
		/**
		 * org.springframework.cloud.netflix.ribbon.RibbonLoadBalancerContext:
		 * 1、clientName=nacos-user-service
		 * 2、vipAddresses=nacos-user-service
		 * 3、maxAutoRetriesNextServer=1
		 * 4、lb=ZoneAwareLoadBalancer=DynamicServerListLoadBalancer:{NFLoadBalancer:name=nacos-user-service,current list of Servers=[10.2.40.18:8207, 10.2.40.18:8206],Load balancer stats=Zone stats: {unknown=[Zone:unknown;	Instance count:2;	Active connections count: 0;	Circuit breaker tripped count: 0;	Active connections per server: 0.0;]
		 * },Server stats: [[Server:10.2.40.18:8207;	Zone:UNKNOWN;	Total Requests:0;	Successive connection failure:0;	Total blackout seconds:0;	Last connection made:Fri Jan 26 16:54:54 CST 2024;	First connection made: Fri Jan 26 16:54:54 CST 2024;	Active Connections:0;	total failure count in last (1000) msecs:0;	average resp time:0.0;	90 percentile resp time:0.0;	95 percentile resp time:0.0;	min resp time:0.0;	max resp time:0.0;	stddev resp time:0.0]
		 * , [Server:10.2.40.18:8206;	Zone:UNKNOWN;	Total Requests:0;	Successive connection failure:0;	Total blackout seconds:0;	Last connection made:Thu Jan 01 08:00:00 CST 1970;	First connection made: Thu Jan 01 08:00:00 CST 1970;	Active Connections:0;	total failure count in last (1000) msecs:0;	average resp time:0.0;	90 percentile resp time:0.0;	95 percentile resp time:0.0;	min resp time:0.0;	max resp time:0.0;	stddev resp time:0.0]
		 * ]}ServerList:com.alibaba.cloud.nacos.ribbon.NacosServerList@21227ed1
		 * 5、maxAutoRetries=0
		 * 6、okToRetryOnAllOperations=false
		 */
		RibbonLoadBalancerContext context = this.clientFactory.getLoadBalancerContext(serviceId);

		URI uri;
		Server server;
		if (instance instanceof RibbonServer) {
			RibbonServer ribbonServer = (RibbonServer) instance;
			// NacosServer=10.2.40.18:8207
			server = ribbonServer.getServer();
			uri = updateToSecureConnectionIfNeeded(original, ribbonServer);
		} else {
			server = new Server(instance.getScheme(), instance.getHost(),
					instance.getPort());
			IClientConfig clientConfig = clientFactory.getClientConfig(serviceId);
			ServerIntrospector serverIntrospector = serverIntrospector(serviceId);
			uri = updateToSecureConnectionIfNeeded(original, clientConfig,
					serverIntrospector, server);
		}
		return context.reconstructURIWithServer(server, uri);
	}

	@Override
	public ServiceInstance choose(String serviceId) {
		return choose(serviceId, null);
	}

	/**
	 * New: Select a server using a 'key'.
	 *
	 * @param serviceId of the service to choose an instance for
	 * @param hint      to specify the service instance
	 * @return the selected {@link ServiceInstance}
	 */
	public ServiceInstance choose(String serviceId, Object hint) {
		Server server = getServer(getLoadBalancer(serviceId), hint);
		if (server == null) {
			return null;
		}
		return new RibbonServer(serviceId, server, isSecure(server, serviceId),
				serverIntrospector(serviceId).getMetadata(server));
	}

	@Override
	public <T> T execute(String serviceId, LoadBalancerRequest<T> request)
			throws IOException {
		return execute(serviceId, request, null);
	}

	/**
	 * New: Execute a request by selecting server using a 'key'. The hint will have to be
	 * the last parameter to not mess with the `execute(serviceId, ServiceInstance,
	 * request)` method. This somewhat breaks the fluent coding style when using a lambda
	 * to define the LoadBalancerRequest.
	 *
	 * @param <T>       returned request execution result type
	 * @param serviceId id of the service to execute the request to
	 * @param request   to be executed
	 * @param hint      used to choose appropriate {@link Server} instance
	 * @return request execution result
	 * @throws IOException executing the request may result in an {@link IOException}
	 */
	public <T> T execute(String serviceId, LoadBalancerRequest<T> request, Object hint)
			throws IOException {
		// ZoneAwareLoadBalancer: DynamicServerListLoadBalancer:{NFLoadBalancer:name=nacos-user-service,current list of Servers=[10.2.40.18:8206],Load balancer stats=Zone stats: {unknown=[Zone:unknown;	Instance count:1;	Active connections count: 1;	Circuit breaker tripped count: 0;	Active connections per server: 1.0;]},Server stats: [[Server:10.2.40.18:8206;	Zone:UNKNOWN;	Total Requests:0;	Successive connection failure:0;	Total blackout seconds:0;	Last connection made:Wed Jan 24 13:19:19 CST 2024;	First connection made: Wed Jan 24 13:19:19 CST 2024;	Active Connections:1;	total failure count in last (1000) msecs:0;	average resp time:0.0;	90 percentile resp time:0.0;	95 percentile resp time:0.0;	min resp time:0.0;	max resp time:0.0;	stddev resp time:0.0]]}ServerList:com.alibaba.cloud.nacos.ribbon.NacosServerList@212efc1f
		ILoadBalancer loadBalancer = getLoadBalancer(serviceId); // serviceId=nacos-user-service
		Server server = getServer(loadBalancer, hint); // NacosServer: 10.2.40.18:8206
		if (server == null) {
			throw new IllegalStateException("No instances available for " + serviceId);
		}
		RibbonServer ribbonServer = new RibbonServer(serviceId, server,
				isSecure(server, serviceId),
				serverIntrospector(serviceId).getMetadata(server));

		return execute(serviceId, ribbonServer, request);
	}

	/**
	 * @param serviceId       nacos-user-service
	 * @param serviceInstance RibbonServer{serviceId='nacos-user-service', server=10.2.40.18:8206, secure=false, metadata={preserved.register.source=SPRING_CLOUD}}
	 * @param request         org.springframework.cloud.client.loadbalancer.LoadBalancerRequestFactory$$Lambda$578/829204661@7c0dc36d
	 */
	@Override
	public <T> T execute(String serviceId, ServiceInstance serviceInstance,
						 LoadBalancerRequest<T> request) throws IOException {
		Server server = null;
		if (serviceInstance instanceof RibbonServer) {
			server = ((RibbonServer) serviceInstance).getServer(); // NacosServer: 10.2.40.18:8206
		}
		if (server == null) {
			throw new IllegalStateException("No instances available for " + serviceId);
		}
		// org.springframework.cloud.netflix.ribbon.RibbonLoadBalancerContext
		RibbonLoadBalancerContext context = this.clientFactory.getLoadBalancerContext(serviceId);
		/**
		 * ServerStats=[Server:10.2.40.18:8207;	Zone:UNKNOWN;	Total Requests:0;	Successive connection failure:0;	Total blackout seconds:0;	Last connection made:Fri Jan 26 16:54:54 CST 2024;	First connection made: Fri Jan 26 16:54:54 CST 2024;	Active Connections:1;	total failure count in last (1000) msecs:0;	average resp time:0.0;	90 percentile resp time:0.0;	95 percentile resp time:0.0;	min resp time:0.0;	max resp time:0.0;	stddev resp time:0.0]
		 * context=RibbonLoadBalancerContext
		 */
		RibbonStatsRecorder statsRecorder = new RibbonStatsRecorder(context, server);

		try {
			T returnVal = request.apply(serviceInstance);
			statsRecorder.recordStats(returnVal);
			return returnVal;
		}
		// catch IOException and rethrow so RestTemplate behaves correctly
		catch (IOException ex) {
			statsRecorder.recordStats(ex);
			throw ex;
		} catch (Exception ex) {
			statsRecorder.recordStats(ex);
			ReflectionUtils.rethrowRuntimeException(ex);
		}
		return null;
	}

	private ServerIntrospector serverIntrospector(String serviceId) {
		ServerIntrospector serverIntrospector = this.clientFactory.getInstance(serviceId,
				ServerIntrospector.class);
		if (serverIntrospector == null) {
			serverIntrospector = new DefaultServerIntrospector();
		}
		return serverIntrospector; // NacosServerIntrospector
	}

	private boolean isSecure(Server server, String serviceId) {
		// ClientConfig:ConnectionManagerTimeout:2000, ProxyHost:null, ServerListRefreshInterval:null, MaxRetriesPerServerPrimeConnection:9, IsHostnameValidationRequired:null, PoolKeepAliveTimeUnits:SECONDS, ServerListUpdaterClassName:null, RequestSpecificRetryOn:null, PoolMinThreads:1, ProxyPort:null, MaxTotalConnections:200, NFLoadBalancerMaxTotalPingTime:null, NIWSServerListClassName:com.netflix.loadbalancer.ConfigurationBasedServerList, EnableZoneExclusivity:false, IgnoreUserTokenInConnectionPoolForSecureClient:null, PrimeConnectionsClassName:com.netflix.niws.client.http.HttpPrimeConnection, TrustStore:null, ConnectTimeout:1000, VipAddressResolverClassName:com.netflix.client.SimpleVipAddressResolver, IsClientAuthRequired:false, RulePredicateClasses:null, RequestIdHeaderName:null, ConnectionPoolCleanerTaskEnabled:true, EnableGZIPContentEncodingFilter:false, CustomSSLSocketFactoryClassName:null, EnableZoneAffinity:false, NFLoadBalancerClassName:com.netflix.loadbalancer.ZoneAwareLoadBalancer, MaxAutoRetries:0, TargetRegion:null, VipAddress:null, ReadTimeout:1000, KeyStore:null, InitializeNFLoadBalancer:null, BackoffTimeout:null, ForceClientPortConfiguration:null, TrustStorePassword:null, OkToRetryOnAllOperations:false, IsSecure:null, ServerDownStatWindowInMillis:null, MaxTotalTimeToPrimeConnections:30000, ClientClassName:com.netflix.niws.client.http.RestClient, Port:7001, EnablePrimeConnections:false, GZipPayload:true, NFLoadBalancerPingInterval:null, PoolKeepAliveTime:900, EnableMarkingServerDownOnReachingFailureLimit:null, MinPrimeConnectionsRatio:1.0, SecurePort:null, KeyStorePassword:null, NFLoadBalancerRuleClassName:com.netflix.loadbalancer.AvailabilityFilteringRule, DeploymentContextBasedVipAddresses:nacos-user-service, SendBufferSize:null, ConnIdleEvictTimeMilliSeconds:30000, UseIPAddrForServer:false, EnableConnectionPool:true, StaleCheckingEnabled:null, Linger:null, FollowRedirects:false, PoolMaxThreads:200, ReceiveBufferSize:null, ServerDownFailureLimit:null, AppName:null, NFLoadBalancerPingClassName:com.netflix.loadbalancer.DummyPing, NFLoadBalancerStatsClassName:null, PrioritizeVipAddressBasedServers:true, ConnectionCleanerRepeatInterval:30000, listOfServers:, MaxConnectionsPerHost:50, MaxAutoRetriesNextServer:1, NIWSServerListFilterClassName:null, MaxHttpConnectionsPerHost:50, MaxTotalHttpConnections:200, Version:null, PrimeConnectionsURI:/
		IClientConfig config = this.clientFactory.getClientConfig(serviceId);
		ServerIntrospector serverIntrospector = serverIntrospector(serviceId); // ServerIntrospector 接口的作用是用于获取关于服务实例的元数据信息。在 Spring Cloud 中，它通常用于获取服务实例的一些重要信息，如主机名、端口号、是否为安全连接等
		return RibbonUtils.isSecure(config, serverIntrospector, server);
	}

	// Note: This method could be removed?
	protected Server getServer(String serviceId) {
		return getServer(getLoadBalancer(serviceId), null);
	}

	protected Server getServer(ILoadBalancer loadBalancer) {
		return getServer(loadBalancer, null);
	}

	protected Server getServer(ILoadBalancer loadBalancer, Object hint) {
		if (loadBalancer == null) {
			return null;
		}
		// Use 'default' on a null hint, or just pass it on?
		return loadBalancer.chooseServer(hint != null ? hint : "default");
	}

	protected ILoadBalancer getLoadBalancer(String serviceId) {
		return this.clientFactory.getLoadBalancer(serviceId);
	}

	/**
	 * Ribbon-server-specific {@link ServiceInstance} implementation.
	 */
	public static class RibbonServer implements ServiceInstance {

		private final String serviceId;

		private final Server server;

		private final boolean secure;

		private Map<String, String> metadata;

		public RibbonServer(String serviceId, Server server) {
			this(serviceId, server, false, Collections.emptyMap());
		}

		public RibbonServer(String serviceId, Server server, boolean secure,
							Map<String, String> metadata) {
			this.serviceId = serviceId; // nacos-user-service
			this.server = server; // 10.2.40.18:8206, instance={"clusterName":"DEFAULT","enabled":true,"ephemeral":true,"healthy":true,"instanceHeartBeatInterval":5000,"instanceHeartBeatTimeOut":15000,"instanceId":"10.2.40.18#8206#DEFAULT#DEFAULT_GROUP@@nacos-user-service","ip":"10.2.40.18","ipDeleteTimeout":30000,"metadata":{"preserved.register.source":"SPRING_CLOUD"},"port":8206,"serviceName":"DEFAULT_GROUP@@nacos-user-service","weight":1.0}
			this.secure = secure; // false
			this.metadata = metadata; // {preserved.register.source=SPRING_CLOUD}
		}

		@Override
		public String getInstanceId() {
			return this.server.getId();
		}

		@Override
		public String getServiceId() {
			return this.serviceId;
		}

		@Override
		public String getHost() {
			return this.server.getHost();
		}

		@Override
		public int getPort() {
			return this.server.getPort();
		}

		@Override
		public boolean isSecure() {
			return this.secure;
		}

		@Override
		public URI getUri() {
			return DefaultServiceInstance.getUri(this);
		}

		@Override
		public Map<String, String> getMetadata() {
			return this.metadata;
		}

		public Server getServer() {
			return this.server;
		}

		@Override
		public String getScheme() {
			return this.server.getScheme();
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder("RibbonServer{");
			sb.append("serviceId='").append(serviceId).append('\'');
			sb.append(", server=").append(server);
			sb.append(", secure=").append(secure);
			sb.append(", metadata=").append(metadata);
			sb.append('}');
			return sb.toString();
		}

	}

}
