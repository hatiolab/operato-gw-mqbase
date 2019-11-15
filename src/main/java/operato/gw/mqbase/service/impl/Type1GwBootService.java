package operato.gw.mqbase.service.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import xyz.anythings.gw.entity.Gateway;
import xyz.anythings.gw.entity.Indicator;
import xyz.anythings.gw.event.GatewayRebootEvent;
import xyz.anythings.gw.service.api.IGwBootService;
import xyz.anythings.gw.service.mq.model.GatewayInitResGwConfig;
import xyz.anythings.gw.service.mq.model.GatewayInitResIndConfig;
import xyz.anythings.gw.service.mq.model.GatewayInitResIndList;
import xyz.anythings.gw.service.mq.model.GatewayInitResponse;
import xyz.anythings.gw.service.util.BatchIndConfigUtil;
import xyz.anythings.gw.service.util.StageIndConfigUtil;
import xyz.anythings.sys.event.EventPublisher;
import xyz.anythings.sys.service.AbstractQueryService;
import xyz.anythings.sys.util.AnyEntityUtil;
import xyz.elidom.orm.OrmConstants;
import xyz.elidom.sys.util.ValueUtil;

/**
 * Gateway, Indicator Boot 프로세스
 * 
 * @author shortstop
 */
@Component
public class Type1GwBootService extends AbstractQueryService implements IGwBootService {
	
	/**
	 * Event Publisher
	 */
	@Autowired
	protected EventPublisher eventPublisher;
	/**
	 * 표시기 점등 서비스
	 */
	@Autowired
	private Type1IndicatorRequestService indSendService;

	/**
	 * Gateway, batchId, 표시기 정보 리스트로 표시기 부팅 
	 * 
	 * @param gateway
	 * @param batchId
	 * @param indList
	 */
	@Override
	public void respondGatewayBoot(Gateway gateway, String batchId, List<GatewayInitResIndList> indList) {
		// 1. domainId
		Long domainId = gateway.getDomainId();
		
		// 2. Gateway 초기화 설정 정보 가져오기.
		GatewayInitResponse gwInitRes = new GatewayInitResponse();
		gwInitRes.setGwConf(this.newGatewayInitConfig(gateway));
		
		// 3. Gateway 소속 표시기 List를 설정
		gwInitRes.setIndList(indList);
		
		// 4. Gateway가 관리하는 인디케이터 리스트 및 각각의 Indicator 별 설정 정보 가져오기.		
		GatewayInitResIndConfig gwInitResIndConfig = (batchId != null) ?
				BatchIndConfigUtil.getGatewayBootConfig(batchId, gateway) : StageIndConfigUtil.getGatewayBootConfig(gateway);
		gwInitRes.setIndConf(gwInitResIndConfig);
		
		// 5. Gateway 최신버전 정보 설정.
		String latestGatewayVer = (batchId != null) ? 
				BatchIndConfigUtil.getGwLatestReleaseVersion(batchId, gateway) : StageIndConfigUtil.getGwLatestReleaseVersion(gateway);
		gwInitRes.setGwVersion(latestGatewayVer);
		
		// 6. Indicator 최신버전 정보 설정.
		String latestIndVer = (batchId != null) ? 
				BatchIndConfigUtil.getIndLatestReleaseVersion(batchId) : StageIndConfigUtil.getIndLatestReleaseVersion(gateway);
		gwInitRes.setIndVersion(latestIndVer);

		// 7. 현재 시간 설정 - 밀리세컨드 제외
		gwInitRes.setSvrTime((long)(new Date().getTime() / 1000));
		
		// 8. 상태 보고 주기 설정.
		int healthPeriod = (batchId != null) ? 
				BatchIndConfigUtil.getIndHealthPeriod(batchId) : StageIndConfigUtil.getIndHealthPeriod(gateway.getDomainId(), gateway.getStageCd()); 
		gwInitRes.setHealthPeriod(healthPeriod);
		
		// 9. 게이트웨이 초기화 응답 전송 
		this.indSendService.respondGatewayInit(domainId, gateway.getGwNm(), gwInitRes);		
	}
	
	/**
	 * Gateway 초기화 리포트에 대한 처리.(Gateway Version 정보 Update)
	 * 
	 * @param domain
	 * @param gwNm
	 * @param version
	 */
	public void handleGatewayInitReport(Long domainId, String gwNm, String version) {
		// Gateway 정보 조회
		Gateway gateway = AnyEntityUtil.findEntityByCode(domainId, true, Gateway.class, "gwNm", gwNm);
		
		if(gateway != null) {
			if (ValueUtil.isNotEqual(gateway.getVersion(), version)) {
				// Gateway Version 정보 업데이트
				gateway.setVersion(version);
				this.queryManager.update(gateway, OrmConstants.ENTITY_FIELD_VERSION);
			}
			
			this.restoreIndicatorsOn(gateway);
		}
	}
	
	/**
	 * 게이트웨이 별로 이전 작업 (표시기) 상황을 다시 조회해서 재점등 -> 각 모듈이 받아서 처리한다.
	 * 
	 * @param gateway
	 */
	public void restoreIndicatorsOn(Gateway gateway) {		
		// 호기 코드, 게이트웨이 코드로 표시기 이전 상태 복원을 위한 게이트웨이 리부 이벤트 발생
		GatewayRebootEvent gwRebbotEvent = new GatewayRebootEvent(GatewayRebootEvent.EVENT_STEP_AFTER, gateway);
		this.eventPublisher.publishEvent(gwRebbotEvent);
	}

	/**
	 * Indicator 초기화 리포트에 대한 처리. (Indicator Version 정보 Update)
	 * 
	 * @param domain
	 * @param indCd
	 * @param version
	 */
	public void handleIndInitReport(Long domainId, String indCd, String version) {
		Indicator indicator = AnyEntityUtil.findEntityByCode(domainId, true, Indicator.class, "indCd", indCd);
		
		if (indicator != null && ValueUtil.isNotEqual(indicator.getVersion(), version)) {
			indicator.setVersion(version);
			this.queryManager.update(indicator, OrmConstants.ENTITY_FIELD_VERSION);
		}
	}

	/**
	 * Gateway 초기화 설정 정보 생성
	 * 
	 * @param gateway
	 * @return
	 */
	private GatewayInitResGwConfig newGatewayInitConfig(Gateway gateway) {
		GatewayInitResGwConfig initConfig = new GatewayInitResGwConfig();
		initConfig.setId(gateway.getGwNm());
		initConfig.setChannel(gateway.getChannelNo());
		initConfig.setPan(gateway.getPanNo());
		return initConfig;
	}

}
