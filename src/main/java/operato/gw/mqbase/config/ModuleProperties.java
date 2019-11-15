/* Copyright © HatioLab Inc. All rights reserved. */
package operato.gw.mqbase.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import xyz.elidom.sys.system.config.module.IModuleProperties;
import xyz.elidom.util.FormatUtil;

/**
 * MQ 기반의 게이트웨이 API 구현 모듈  
 * 
 * @author shortstop
 */
@Component("operatoGwMqbaseModuleProperties")
@EnableConfigurationProperties
@PropertySource("classpath:/properties/operato-gw-mqbase.properties")
public class ModuleProperties implements IModuleProperties {

	/**
	 * 모듈명
	 */
	@Value("${operato.gw.mqbase.name}")
	private String name;

	/**
	 * 버전
	 */
	@Value("${operato.gw.mqbase.version}")
	private String version;

	/**
	 * Module Built Time
	 */
	@Value("${operato.gw.mqbase.built.at}")
	private String builtAt;

	/**
	 * 모듈 설명
	 */
	@Value("${operato.gw.mqbase.description}")
	private String description;

	/**
	 * 부모 모듈
	 */
	@Value("${operato.gw.mqbase.parentModule}")
	private String parentModule;

	/**
	 * 모듈 Scada Package
	 */
	@Value("${operato.gw.mqbase.basePackage}")
	private String basePackage;

	/**
	 * Scan Service Path
	 */
	@Value("${operato.gw.mqbase.scanServicePackage}")
	private String scanServicePackage;

	/**
	 * Scan Entity Path
	 */
	@Value("${operato.gw.mqbase.scanEntityPackage}")
	private String scanEntityPackage;
	
	/**
	 * 모듈에서 사용할 rabbitmq 큐 명칭 
	 */
	@Value("${operato.gw.mqbase.rabbitQueue:not_use}")
	private String rabbitQueue;
	/**
	 * Project Name
	 * 
	 * @return
	 */
	@Value("${operato.gw.mqbase.projectName}")
	private String projectName;

	public String getName() {
		return this.name;
	}

	public String getVersion() {
		return this.version;
	}

	public String getBuiltAt() {
		return builtAt;
	}

	public String getDescription() {
		return this.description;
	}

	public String getParentModule() {
		return this.parentModule;
	}

	public String getBasePackage() {
		return this.basePackage;
	}

	public String getScanServicePackage() {
		return this.scanServicePackage;
	}

	public String getScanEntityPackage() {
		return this.scanEntityPackage;
	}

	public String getProjectName() {
		return this.projectName;
	}

	public String getRabbitQueue() {
		return this.rabbitQueue;
	}

	@Override
	public String toString() {
		return FormatUtil.toJsonString(this);
	}
}