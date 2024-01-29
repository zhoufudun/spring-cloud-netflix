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

import java.util.Map;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.StringUtils;

/**
 * @author Dave Syer
 */
public class RibbonClientConfigurationRegistrar implements ImportBeanDefinitionRegistrar {

	/**
	 *
	 * @param metadata org.springframework.core.type.classreading.AnnotationMetadataReadingVisitor@280d9edc
	 * @param registry org.springframework.beans.factory.support.DefaultListableBeanFactory@7249dadf: defining beans [org.springframework.context.annotation.internalConfigurationAnnotationProcessor,org.springframework.context.annotation.internalAutowiredAnnotationProcessor,org.springframework.context.annotation.internalCommonAnnotationProcessor,org.springframework.context.event.internalEventListenerProcessor,org.springframework.context.event.internalEventListenerFactory,nacosRibbonServiceApplication,org.springframework.boot.autoconfigure.internalCachingMetadataReaderFactory,ribbonConfig,userRibbonController,restTemplate,user,org.springframework.boot.autoconfigure.AutoConfigurationPackages,org.springframework.cloud.client.serviceregistry.AutoServiceRegistrationConfiguration,spring.cloud.service-registry.auto-registration-org.springframework.cloud.client.serviceregistry.AutoServiceRegistrationProperties,org.springframework.boot.context.properties.ConfigurationPropertiesBindingPostProcessor,org.springframework.boot.context.properties.ConfigurationBeanFactoryMetadata,org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration,org.springframework.boot.autoconfigure.condition.BeanTypeRegistry,propertySourcesPlaceholderConfigurer,org.springframework.boot.autoconfigure.websocket.servlet.WebSocketServletAutoConfiguration$TomcatWebSocketConfiguration,websocketServletWebServerCustomizer,org.springframework.boot.autoconfigure.websocket.servlet.WebSocketServletAutoConfiguration,org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryConfiguration$EmbeddedTomcat,tomcatServletWebServerFactory,org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration,servletWebServerFactoryCustomizer,tomcatServletWebServerFactoryCustomizer,server-org.springframework.boot.autoconfigure.web.ServerProperties,webServerFactoryCustomizerBeanPostProcessor,errorPageRegistrarBeanPostProcessor,org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration$DispatcherServletConfiguration,dispatcherServlet,spring.http-org.springframework.boot.autoconfigure.http.HttpProperties,spring.mvc-org.springframework.boot.autoconfigure.web.servlet.WebMvcProperties,org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration$DispatcherServletRegistrationConfiguration,dispatcherServletRegistration,org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration,org.springframework.cloud.netflix.archaius.ArchaiusAutoConfiguration$PropagateEventsConfiguration,org.springframework.cloud.netflix.archaius.ArchaiusAutoConfiguration$ArchaiusEndpointConfiguration,archaiusEndpoint,org.springframework.cloud.netflix.archaius.ArchaiusAutoConfiguration,configurableEnvironmentConfiguration,org.springframework.boot.autoconfigure.task.TaskExecutionAutoConfiguration,taskExecutorBuilder,applicationTaskExecutor,spring.task.execution-org.springframework.boot.autoconfigure.task.TaskExecutionProperties,org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration,defaultValidator,methodValidationPostProcessor,org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration$WhitelabelErrorViewConfiguration,error,beanNameViewResolver,org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration$DefaultErrorViewResolverConfiguration,conventionErrorViewResolver,org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration,errorAttributes,basicErrorController,errorPageCustomizer,preserveErrorControllerTargetClassPostProcessor,spring.resources-org.springframework.boot.autoconfigure.web.ResourceProperties,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration$WebMvcAutoConfigurationAdapter$FaviconConfiguration,faviconHandlerMapping,faviconRequestHandler,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration$EnableWebMvcConfiguration,requestMappingHandlerAdapter,requestMappingHandlerMapping,welcomePageHandlerMapping,mvcConversionService,mvcValidator,mvcContentNegotiationManager,mvcPathMatcher,mvcUrlPathHelper,viewControllerHandlerMapping,beanNameHandlerMapping,resourceHandlerMapping,mvcResourceUrlProvider,defaultServletHandlerMapping,mvcUriComponentsContributor,httpRequestHandlerAdapter,simpleControllerHandlerAdapter,handlerExceptionResolver,mvcViewResolver,mvcHandlerMappingIntrospector,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration$WebMvcAutoConfigurationAdapter,defaultViewResolver,viewResolver,requestContextFilter,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration,hiddenHttpMethodFilter,formContentFilter,org.springframework.cloud.client.serviceregistry.AutoServiceRegistrationAutoConfiguration,com.alibaba.cloud.nacos.NacosDiscoveryAutoConfiguration,nacosServiceRegistry,nacosRegistration,nacosAutoServiceRegistration,com.alibaba.cloud.nacos.discovery.NacosDiscoveryClientAutoConfiguration,nacosProperties,nacosDiscoveryClient,nacosWatch,com.alibaba.cloud.nacos.endpoint.NacosDiscoveryEndpointAutoConfiguration,nacosDiscoveryEndpoint,org.springframework.cloud.netflix.ribbon.RibbonAutoConfiguration,ribbonFeature,springClientFactory,loadBalancerClient,propertiesFactory]; parent: org.springframework.beans.factory.support.DefaultListableBeanFactory@58326051
	 */
    @Override
    public void registerBeanDefinitions(AnnotationMetadata metadata,
                                        BeanDefinitionRegistry registry) {
        Map<String, Object> attrs = metadata
                .getAnnotationAttributes(RibbonClients.class.getName(), true);
        if (attrs != null && attrs.containsKey("value")) {
            AnnotationAttributes[] clients = (AnnotationAttributes[]) attrs.get("value");
            for (AnnotationAttributes client : clients) {
                registerClientConfiguration(registry, getClientName(client),
                        client.get("configuration"));
            }
        }
        if (attrs != null && attrs.containsKey("defaultConfiguration")) {
            String name;
            if (metadata.hasEnclosingClass()) {
                name = "default." + metadata.getEnclosingClassName();
            } else {
				// default.org.springframework.cloud.netflix.ribbon.RibbonAutoConfiguration
				// default.com.alibaba.cloud.nacos.ribbon.RibbonNacosAutoConfiguration
                name = "default." + metadata.getClassName();
            }
            registerClientConfiguration(registry, name,
                    attrs.get("defaultConfiguration"));
        }
        Map<String, Object> client = metadata
                .getAnnotationAttributes(RibbonClient.class.getName(), true);
        String name = getClientName(client);
        if (name != null) {
            registerClientConfiguration(registry, name, client.get("configuration"));
        }
    }

    private String getClientName(Map<String, Object> client) {
        if (client == null) {
            return null;
        }
        String value = (String) client.get("value");
        if (!StringUtils.hasText(value)) {
            value = (String) client.get("name");
        }
        if (StringUtils.hasText(value)) {
            return value;
        }
        throw new IllegalStateException(
                "Either 'name' or 'value' must be provided in @RibbonClient");
    }

	/**
	 * 注册配置类
	 * @param registry  org.springframework.beans.factory.support.DefaultListableBeanFactory@7249dadf: defining beans [org.springframework.context.annotation.internalConfigurationAnnotationProcessor,org.springframework.context.annotation.internalAutowiredAnnotationProcessor,org.springframework.context.annotation.internalCommonAnnotationProcessor,org.springframework.context.event.internalEventListenerProcessor,org.springframework.context.event.internalEventListenerFactory,nacosRibbonServiceApplication,org.springframework.boot.autoconfigure.internalCachingMetadataReaderFactory,ribbonConfig,userRibbonController,restTemplate,user,org.springframework.boot.autoconfigure.AutoConfigurationPackages,org.springframework.cloud.client.serviceregistry.AutoServiceRegistrationConfiguration,spring.cloud.service-registry.auto-registration-org.springframework.cloud.client.serviceregistry.AutoServiceRegistrationProperties,org.springframework.boot.context.properties.ConfigurationPropertiesBindingPostProcessor,org.springframework.boot.context.properties.ConfigurationBeanFactoryMetadata,org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration,org.springframework.boot.autoconfigure.condition.BeanTypeRegistry,propertySourcesPlaceholderConfigurer,org.springframework.boot.autoconfigure.websocket.servlet.WebSocketServletAutoConfiguration$TomcatWebSocketConfiguration,websocketServletWebServerCustomizer,org.springframework.boot.autoconfigure.websocket.servlet.WebSocketServletAutoConfiguration,org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryConfiguration$EmbeddedTomcat,tomcatServletWebServerFactory,org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration,servletWebServerFactoryCustomizer,tomcatServletWebServerFactoryCustomizer,server-org.springframework.boot.autoconfigure.web.ServerProperties,webServerFactoryCustomizerBeanPostProcessor,errorPageRegistrarBeanPostProcessor,org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration$DispatcherServletConfiguration,dispatcherServlet,spring.http-org.springframework.boot.autoconfigure.http.HttpProperties,spring.mvc-org.springframework.boot.autoconfigure.web.servlet.WebMvcProperties,org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration$DispatcherServletRegistrationConfiguration,dispatcherServletRegistration,org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration,org.springframework.cloud.netflix.archaius.ArchaiusAutoConfiguration$PropagateEventsConfiguration,org.springframework.cloud.netflix.archaius.ArchaiusAutoConfiguration$ArchaiusEndpointConfiguration,archaiusEndpoint,org.springframework.cloud.netflix.archaius.ArchaiusAutoConfiguration,configurableEnvironmentConfiguration,org.springframework.boot.autoconfigure.task.TaskExecutionAutoConfiguration,taskExecutorBuilder,applicationTaskExecutor,spring.task.execution-org.springframework.boot.autoconfigure.task.TaskExecutionProperties,org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration,defaultValidator,methodValidationPostProcessor,org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration$WhitelabelErrorViewConfiguration,error,beanNameViewResolver,org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration$DefaultErrorViewResolverConfiguration,conventionErrorViewResolver,org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration,errorAttributes,basicErrorController,errorPageCustomizer,preserveErrorControllerTargetClassPostProcessor,spring.resources-org.springframework.boot.autoconfigure.web.ResourceProperties,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration$WebMvcAutoConfigurationAdapter$FaviconConfiguration,faviconHandlerMapping,faviconRequestHandler,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration$EnableWebMvcConfiguration,requestMappingHandlerAdapter,requestMappingHandlerMapping,welcomePageHandlerMapping,mvcConversionService,mvcValidator,mvcContentNegotiationManager,mvcPathMatcher,mvcUrlPathHelper,viewControllerHandlerMapping,beanNameHandlerMapping,resourceHandlerMapping,mvcResourceUrlProvider,defaultServletHandlerMapping,mvcUriComponentsContributor,httpRequestHandlerAdapter,simpleControllerHandlerAdapter,handlerExceptionResolver,mvcViewResolver,mvcHandlerMappingIntrospector,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration$WebMvcAutoConfigurationAdapter,defaultViewResolver,viewResolver,requestContextFilter,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration,hiddenHttpMethodFilter,formContentFilter,org.springframework.cloud.client.serviceregistry.AutoServiceRegistrationAutoConfiguration,com.alibaba.cloud.nacos.NacosDiscoveryAutoConfiguration,nacosServiceRegistry,nacosRegistration,nacosAutoServiceRegistration,com.alibaba.cloud.nacos.discovery.NacosDiscoveryClientAutoConfiguration,nacosProperties,nacosDiscoveryClient,nacosWatch,com.alibaba.cloud.nacos.endpoint.NacosDiscoveryEndpointAutoConfiguration,nacosDiscoveryEndpoint,org.springframework.cloud.netflix.ribbon.RibbonAutoConfiguration,ribbonFeature,springClientFactory,loadBalancerClient,propertiesFactory]; parent: org.springframework.beans.factory.support.DefaultListableBeanFactory@58326051
	 * @param name  default.org.springframework.cloud.netflix.ribbon.RibbonAutoConfiguration或者default.com.alibaba.cloud.nacos.ribbon.RibbonNacosAutoConfiguration
	 * @param configuration com.alibaba.cloud.nacos.ribbon.NacosRibbonClientConfiguration
	 */
    private void registerClientConfiguration(BeanDefinitionRegistry registry, Object name,
                                             Object configuration) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder
                .genericBeanDefinition(RibbonClientSpecification.class); // Bean定义的类型是RibbonClientSpecification类，后续会通过反射调用RibbonClientSpecification的构造器
        builder.addConstructorArgValue(name);
        builder.addConstructorArgValue(configuration);
        registry.registerBeanDefinition(name + ".RibbonClientSpecification", builder.getBeanDefinition());
    }

}
