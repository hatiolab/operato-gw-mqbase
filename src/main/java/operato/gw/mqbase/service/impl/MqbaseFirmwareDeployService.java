package operato.gw.mqbase.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import xyz.anythings.gw.entity.Deployment;
import xyz.anythings.gw.entity.Gateway;
import xyz.anythings.gw.service.api.IFirmwareDeployService;
import xyz.anythings.sys.service.AbstractQueryService;
import xyz.anythings.sys.util.AnyEntityUtil;
import xyz.elidom.sys.util.ValueUtil;

/**
 * 펌웨어 배포 관련 서비스.
 * 
 * @author shortstop
 */
@Component
public class MqbaseFirmwareDeployService extends AbstractQueryService implements IFirmwareDeployService {

	@Autowired
	private MqbaseIndicatorRequestService indSendService;
	
	/**
	 * 펌웨어 배포
	 * 
	 * @param deployment
	 */
	public void deployFirmware(Deployment deployment) {
		Long domainId = deployment.getDomainId();
		String gwCd = deployment.getTargetId();
		
		// 1. Gateway 조회 
		Gateway gw = AnyEntityUtil.findEntityByCode(domainId, true, Gateway.class, "gwCd", gwCd);
		
		// 2. Gateway 펌웨어 배포
		if(ValueUtil.isEqualIgnoreCase(deployment.getTargetType(), Deployment.TARGET_TYPE_GW)) {
			this.indSendService.deployGatewayFirmware(domainId, gw.getGwNm(), deployment.getVersion(), deployment.computeDownloadUrl(), deployment.getFileName(), deployment.getForceFlag());
		// 3. Gateway 펌웨어 배포			
		} else {
			this.indSendService.deployIndFirmware(domainId, gw.getGwNm(), deployment.getVersion(), deployment.computeDownloadUrl(), deployment.getFileName(), deployment.getForceFlag());
		}
	}
	
}
