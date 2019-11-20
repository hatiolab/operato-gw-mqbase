package operato.gw.mqbase.service.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import operato.gw.mqbase.MqbaseConfigConstants;
import xyz.anythings.base.LogisConstants;
import xyz.anythings.base.entity.JobBatch;
import xyz.anythings.base.query.store.IndicatorQueryStore;
import xyz.anythings.base.query.util.IndicatorQueryUtil;
import xyz.anythings.gw.GwConstants;
import xyz.anythings.gw.entity.Gateway;
import xyz.anythings.gw.entity.Indicator;
import xyz.anythings.gw.event.GatewayBootEvent;
import xyz.anythings.gw.event.GatewayInitEvent;
import xyz.anythings.gw.event.IndicatorInitEvent;
import xyz.anythings.gw.service.api.IIndHandlerService;
import xyz.anythings.gw.service.mq.MqSender;
import xyz.anythings.gw.service.mq.model.GatewayInitResGwConfig;
import xyz.anythings.gw.service.mq.model.GatewayInitResIndConfig;
import xyz.anythings.gw.service.mq.model.GatewayInitResIndList;
import xyz.anythings.gw.service.mq.model.GatewayInitResponse;
import xyz.anythings.gw.service.mq.model.MiddlewareConnInfoModRequest;
import xyz.anythings.gw.service.mq.model.TimesyncResponse;
import xyz.anythings.gw.service.util.BatchIndConfigUtil;
import xyz.anythings.gw.service.util.StageIndConfigUtil;
import xyz.anythings.sys.service.AbstractExecutionService;
import xyz.anythings.sys.util.AnyEntityUtil;
import xyz.elidom.orm.OrmConstants;
import xyz.elidom.sys.SysConstants;
import xyz.elidom.sys.entity.Domain;
import xyz.elidom.sys.util.SettingUtil;
import xyz.elidom.sys.util.ValueUtil;
import xyz.elidom.util.BeanUtil;

/**
 * 게이트웨이로 받은 메시지를 처리하는 서비스 인터페이스
 * 
 * @author shortstop
 */
@Component("mqbaseIndHandlerService")
public class MqbaseIndHandlerService extends AbstractExecutionService implements IIndHandlerService {
	/**
	 * 미들웨어로 메시지를 전송하기 위한 유틸리티
	 */
	@Autowired
	private MqSender mqSender;	

	@Override
	public boolean handleGatewayBootReq(Gateway gateway) {
		// 1. 게이트웨이에 걸쳐진 호기에 걸린 작업 배치를 찾는다.
		List<JobBatch> batchList = IndicatorQueryUtil.searchRunningBatchesByGwCd(gateway);
		JobBatch batch = ValueUtil.isEmpty(batchList) ? null : batchList.get(0);
		
		Long domainId = gateway.getDomainId();
		String jobType = (batch == null) ? LogisConstants.JOB_TYPE_DAS : batch.getJobType();
		String stageCd = gateway.getStageCd();
		String gwPath = gateway.getGwNm();
		String viewType = StageIndConfigUtil.getIndViewType(domainId, stageCd);
		
		// 2. 게이트웨이 초기화를 위한 게이트웨이 소속의 표시기 리스트를 조회한다.
		String sql = BeanUtil.get(IndicatorQueryStore.class).getSearchIndListForGwInitQuery();
		List<GatewayInitResIndList> gwInitIndList = AnyEntityUtil.searchItems(domainId, false, GatewayInitResIndList.class, sql, "domainId,gwNm,jobType,viewType,activeFlag", domainId, gwPath, jobType, viewType, true);
		
		// 3. 게이트웨이 부팅 요청 처리
		return this.handleGatewayBootReq(gateway, (batch == null) ? null : batch.getId(), gwInitIndList);
	}
	
	/**
	 * 게이트웨이 부팅 요청 처리
	 * 
	 * @param gateway
	 * @param batchId
	 * @param gwIndInitList
	 * @return
	 */
	public boolean handleGatewayBootReq(Gateway gateway, String batchId, List<GatewayInitResIndList> gwIndInitList) {
		// 1. 게이트웨이 부트 전 처리
		GatewayBootEvent gwBootBefore = new GatewayBootEvent(GatewayInitEvent.EVENT_STEP_BEFORE, gateway);
		this.eventPublisher.publishEvent(gwBootBefore);
		
		// 2. Gateway 초기화 설정 정보 조회 후 설정
		Long domainId = gateway.getDomainId();
		GatewayInitResponse gwInitRes = new GatewayInitResponse();
		gwInitRes.setGwConf(this.newGatewayInitConfig(gateway));
		
		// 3. Gateway 소속 표시기 List를 설정
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
		this.mqSender.sendRequest(domainId, gateway.getStageCd(), gateway.getGwNm(), gwInitRes);
		
		// 10. 게이트웨이 부트 후 처리
		GatewayBootEvent gwBootAfter = new GatewayBootEvent(GatewayInitEvent.EVENT_STEP_AFTER, gateway);
		this.eventPublisher.publishEvent(gwBootAfter);
		
		// 11. 결과 리턴
		return true;
	}
	
	@Override
	public void handleTimesyncReq(Long domainId, String stageCd, String msgDestId) {
		long serverTime = new Date().getTime();
		this.mqSender.sendRequest(domainId, stageCd, msgDestId, new TimesyncResponse(serverTime));
	}
	
	@Override
	public void handleGatewayInitReport(Gateway gateway, Object ... params) {
		// 1. 게이트웨이 상태 보고 전 처리, TODO 이벤트 전송 여부를 설정 정보로 처리 
		GatewayInitEvent gwInitBefore = new GatewayInitEvent(GatewayInitEvent.EVENT_STEP_BEFORE, gateway);
		this.eventPublisher.publishEvent(gwInitBefore);
		
		// 2. 게이트웨이 상태 보고 처리
		String version = this.extractVersion(params);
		
		// 넘어온 게이트웨이 버전이 이전과 다르다면 버전 업데이트
		if (ValueUtil.isNotEqual(gateway.getVersion(), version)) {
			gateway.setVersion(version);
			this.queryManager.update(gateway, OrmConstants.ENTITY_FIELD_VERSION);
		}
		
		// 3. 게이트웨이 상태 보고 후 처리 
		GatewayInitEvent gwInitAfter = new GatewayInitEvent(GatewayInitEvent.EVENT_STEP_AFTER, gateway);
		this.eventPublisher.publishEvent(gwInitAfter);
	}
	
	@Override
	public void handleIndicatorInitReport(Indicator indicator, String stageCd, Object ... params) {
		// 1. 표시기 상태 보고 전 처리, TODO 이벤트 전송 여부를 설정 정보로 처리 
		IndicatorInitEvent indInitBefore = new IndicatorInitEvent(GatewayInitEvent.EVENT_STEP_BEFORE, indicator, stageCd);
		this.eventPublisher.publishEvent(indInitBefore);
		
		// 2. 표시기 상태 보고 처리
		String version = this.extractVersion(params);
		
		// 넘어온 표시기 버전이 이전과 다르다면 버전 업데이트
		if (ValueUtil.isNotEqual(indicator.getVersion(), version)) {
			indicator.setVersion(version);
			this.queryManager.update(indicator, OrmConstants.ENTITY_FIELD_VERSION);
		}
		
		// 3. 게이트웨이 상태 보고 후 처리 
		IndicatorInitEvent indInitAfter = new IndicatorInitEvent(GatewayInitEvent.EVENT_STEP_AFTER, indicator, stageCd);
		this.eventPublisher.publishEvent(indInitAfter);
	}
	
	@Override
	public void handleGatewayStatusReport(Gateway gateway, String status, String version, Object... params) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void handleIndicatorStatusReport(Indicator indicator, String stageCd, String status, String version, Object... params) {
		// 1. 상태가 접속 해제인 경우 
		if (ValueUtil.isEqualIgnoreCase(status, GwConstants.EQUIP_STATUS_OFFLINE)) {
			if(indicator != null) {
				indicator.setVersion(version);
				indicator.setStatus(LogisConstants.EQUIP_STATUS_BREAK_DOWN);
				this.queryManager.update(indicator, "version", "status");
			}

			//String message = String.format("MPI(%s) is disconnected.", mpiCd).toString();
			//this.saveEquipmentLog(domainId, MpsConstants.EQUIP_INDICATOR, mpiCd, gatewayPath, EquipmentLog.LOG_LEVEL_ERROR, message);
			
		// 3. 상태가 OK인 경우
		} else {
			if(indicator != null && status.equalsIgnoreCase(GwConstants.IND_BIZ_FLAG_OK)) {
				indicator.setVersion(version);
				indicator.setStatus(LogisConstants.EQUIP_STATUS_OK);
				this.queryManager.update(indicator, "version", "status");
			}
			
			//this.saveEquipmentLog(domainId, MpsConstants.EQUIP_INDICATOR, mpiCd, gatewayPath, EquipmentLog.LOG_LEVEL_INFO, status);
		}
	}	

	@Override
	public void handleDeploymentResponse(Gateway gateway, boolean deploySuccess, Object... params) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void handleChangeCellOfIndReq(Long domainId, String indCd, String locCd) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void handleChangeIndOfCellReq(Long domainId, String fromIndCd, String toIndCd) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void handleChangeAccessInfo(Domain domain, List<Gateway> gwList, boolean changeSuccess) {
		if(ValueUtil.isNotEmpty(gwList)) {
			Long domainId = domain.getId();
			String mwIpStr = SettingUtil.getValue(domainId, MqbaseConfigConstants.RABBIT_MQ_BROKER_ADDR);
			
			if(ValueUtil.isNotEmpty(mwIpStr)) {
				String mwPortStr = SettingUtil.getValue(domainId, MqbaseConfigConstants.RABBIT_MQTT_PORT, "1883");
				String[] mwIpArr = mwIpStr.split(SysConstants.COMMA);
				String[] mwPortArr = mwPortStr.split(SysConstants.COMMA);
				int[] mwPortList = new int[mwPortArr.length];
				for(int i = 0 ; i < mwPortArr.length ; i++) {
					mwPortList[i] = ValueUtil.toInteger(mwPortArr[i]);
				}
				
				for(Gateway gw : gwList) {
					// 2. 접속 정보 설정
					String mwSiteCd = domain.getMwSiteCd();
					MiddlewareConnInfoModRequest connModifyReq = new MiddlewareConnInfoModRequest();					
					connModifyReq.setMwIp(mwIpArr);
					connModifyReq.setMwPort(mwPortList);
					connModifyReq.setMwClientId(mwSiteCd + SysConstants.SLASH + gw.getGwNm());
					connModifyReq.setMwSite(mwSiteCd);
					connModifyReq.setMwTopicId(mwSiteCd, connModifyReq.getMwClientId());
			
					// 3. 각 게이트웨이에 접속 정보 변경 요청
					this.mqSender.sendRequest(domainId, gw.getStageCd(), gw.getGwNm(), connModifyReq);
				}				
			}
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
