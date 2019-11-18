package operato.gw.mqbase.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import xyz.anythings.gw.entity.Gateway;
import xyz.anythings.gw.entity.Indicator;
import xyz.anythings.gw.event.GatewayBootEvent;
import xyz.anythings.gw.event.GatewayInitEvent;
import xyz.anythings.gw.event.IndicatorInitEvent;
import xyz.anythings.gw.service.api.IGwBootService;
import xyz.anythings.gw.service.model.GwIndInit;
import xyz.anythings.gw.service.mq.model.GatewayInitResGwConfig;
import xyz.anythings.gw.service.mq.model.GatewayInitResIndConfig;
import xyz.anythings.gw.service.mq.model.GatewayInitResIndList;
import xyz.anythings.gw.service.mq.model.GatewayInitResponse;
import xyz.anythings.gw.service.util.BatchIndConfigUtil;
import xyz.anythings.gw.service.util.StageIndConfigUtil;
import xyz.anythings.sys.service.AbstractExecutionService;
import xyz.elidom.orm.OrmConstants;
import xyz.elidom.sys.util.ValueUtil;

/**
 * Gateway, Indicator Boot 프로세스
 * 
 * @author shortstop
 */
@Component("mqbaseGwBootService")
public class MqbaseGwBootService extends AbstractExecutionService implements IGwBootService {
	
	/**
	 * 표시기 점등 서비스
	 */
	@Autowired
	private MqbaseIndicatorRequestService indReqService;

	@Override
	public boolean gatewayBootResponse(Gateway gateway, List<GwIndInit> indList) {
		return this.gatewayBootResponse(gateway, null, indList);
	}
	
	@Override
	public boolean gatewayBootResponse(Gateway gateway, String batchId, List<GwIndInit> indList) {
		GatewayBootEvent gwBootBefore = new GatewayBootEvent(GatewayInitEvent.EVENT_STEP_BEFORE, gateway);
		this.eventPublisher.publishEvent(gwBootBefore);
		
		// 1. domainId
		Long domainId = gateway.getDomainId();
		
		// 2. Gateway 초기화 설정 정보 조회 후 설정
		GatewayInitResponse gwInitRes = new GatewayInitResponse();
		gwInitRes.setGwConf(this.newGatewayInitConfig(gateway));
		
		// 3. Gateway 소속 표시기 List를 설정
		List<GatewayInitResIndList> gwIndInitList = new ArrayList<GatewayInitResIndList>(indList.size());
		for(GwIndInit gwIndInit : indList) {
			GatewayInitResIndList gwInitResInd = ValueUtil.populate(gwIndInit, new GatewayInitResIndList());
			gwIndInitList.add(gwInitResInd);
		}
		gwInitRes.setIndList(gwIndInitList);
		
		// 4. Gateway가 관리하는 인디케이터 리스트 및 각각의 Indicator 별 설정 정보 조회 후 설정	
		GatewayInitResIndConfig gwInitResIndConfig = (batchId != null) ?
				BatchIndConfigUtil.getGatewayBootConfig(batchId, gateway) : StageIndConfigUtil.getGatewayBootConfig(gateway);
		gwInitRes.setIndConf(gwInitResIndConfig);
		
		// 5. Gateway 최신버전 정보 설정
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
		this.indReqService.respondGatewayInit(domainId, gateway.getGwNm(), gwInitRes);
		
		GatewayBootEvent gwBootAfter = new GatewayBootEvent(GatewayInitEvent.EVENT_STEP_AFTER, gateway);
		this.eventPublisher.publishEvent(gwBootAfter);		
		return true;
	}
	
	@Override
	public void handleGatewayInitReport(Gateway gateway, Object ... params) {
		GatewayInitEvent gwInitBefore = new GatewayInitEvent(GatewayInitEvent.EVENT_STEP_BEFORE, gateway);
		this.eventPublisher.publishEvent(gwInitBefore);
		
		String version = this.extractVersion(params);
		
		// 넘어온 게이트웨이 버전이 이전과 다르다면 버전 업데이트
		if (ValueUtil.isNotEqual(gateway.getVersion(), version)) {
			gateway.setVersion(version);
			this.queryManager.update(gateway, OrmConstants.ENTITY_FIELD_VERSION);
		}
		
		GatewayInitEvent gwInitAfter = new GatewayInitEvent(GatewayInitEvent.EVENT_STEP_AFTER, gateway);
		this.eventPublisher.publishEvent(gwInitAfter);
	}
	
	@Override
	public void handleIndicatorInitReport(Indicator indicator, Object ... params) {
		IndicatorInitEvent indInitBefore = new IndicatorInitEvent(GatewayInitEvent.EVENT_STEP_BEFORE, indicator);
		this.eventPublisher.publishEvent(indInitBefore);
		
		String version = this.extractVersion(params);
		
		// 넘어온 표시기 버전이 이전과 다르다면 버전 업데이트
		if (ValueUtil.isNotEqual(indicator.getVersion(), version)) {
			indicator.setVersion(version);
			this.queryManager.update(indicator, OrmConstants.ENTITY_FIELD_VERSION);
		}
		
		IndicatorInitEvent indInitAfter = new IndicatorInitEvent(GatewayInitEvent.EVENT_STEP_AFTER, indicator);
		this.eventPublisher.publishEvent(indInitAfter);
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
	
	/**
	 * 파라미터에서 버전 추출 ...
	 * 
	 * @param params
	 * @return
	 */
	private String extractVersion(Object ... params) {
		String version = null;
		
		if(params != null && params.length > 0) {
			version = ValueUtil.toString(params[0]);
		}
		
		return version;
	}

}
