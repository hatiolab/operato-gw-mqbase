package operato.gw.mqbase.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import operato.gw.mqbase.service.impl.MqbaseIndHandlerService;
import xyz.anythings.comm.rabbitmq.event.MwErrorEvent;
import xyz.anythings.gw.entity.Gateway;
import xyz.anythings.gw.entity.Indicator;
import xyz.anythings.gw.service.mq.MqCommon;
import xyz.anythings.gw.service.mq.MqSender;
import xyz.anythings.gw.service.mq.model.Action;
import xyz.anythings.gw.service.mq.model.ErrorReportAck;
import xyz.anythings.gw.service.mq.model.GatewayInitReport;
import xyz.anythings.gw.service.mq.model.GatewayInitReportAck;
import xyz.anythings.gw.service.mq.model.GatewayInitRequest;
import xyz.anythings.gw.service.mq.model.GatewayInitRequestAck;
import xyz.anythings.gw.service.mq.model.IndicatorInitReport;
import xyz.anythings.gw.service.mq.model.IndicatorInitReportAck;
import xyz.anythings.gw.service.mq.model.IndicatorOffResponseAck;
import xyz.anythings.gw.service.mq.model.IndicatorOnResponseAck;
import xyz.anythings.gw.service.mq.model.IndicatorStatusReportAck;
import xyz.anythings.gw.service.mq.model.MessageObject;
import xyz.anythings.gw.service.mq.model.MiddlewareConnInfoModResponseAck;
import xyz.anythings.gw.service.mq.model.TimesyncRequestAck;
import xyz.anythings.gw.service.util.MwMessageUtil;
import xyz.anythings.sys.event.EventPublisher;
import xyz.anythings.sys.util.AnyEntityUtil;
import xyz.elidom.exception.server.ElidomRuntimeException;
import xyz.elidom.rabbitmq.client.event.SystemMessageReceiveEvent;
import xyz.elidom.sys.entity.Domain;
import xyz.elidom.sys.system.context.DomainContext;
import xyz.elidom.sys.util.ValueUtil;

/**
 * 메시징 미들웨어로 부터 메시지를 접수받아 처리하는 포인트
 * 
 * 1. 게이트웨이 / 표시기 메시지 처리 서비스
 * 	1) 게이트웨이 초기화 메시지 처리 
 * 	2) 게이트웨이 초기화 완료 메시지 처리
 * 	3) 표시기 초기화 메시지 처리
 * 	4) 장비 상태 보고
 * 	5) 장비 에러 메시지 처리
 * 
 * 2. 공통 표시기 트랜잭션 메시지 처리 서비스
 * 	1) 표시기 관련 요청 Reply 처리
 * 	2) 표시기 작업 처리 (버튼 터치)
 * 	3) 표시기 기능 버튼 처리 - M/F/C
 * 	C) 표시기 취소 (Cancel)
 * 	F) 표시기 Full 처리 (Full)
 * 	M) 표시기 수량 변경 처리 (Modified)
 * 
 * @author shortstop
 */
@Component
public class MqMessageReceiver extends MqCommon {
	
	/**
	 * 미들웨어 전송 서비스
	 */
	@Autowired
	private MqbaseIndHandlerService indHandlerService;
	/**
	 * 미들웨어 전송 서비스
	 */
	@Autowired
	private MqSender mwSender;
	/**
	 * Event Publisher
	 */
	@Autowired
	protected EventPublisher eventPublisher;
	/**
	 * 도메인 맵 : Site Code - Domain
	 */
	private Map<String, Domain> domainMap = new ConcurrentHashMap<String, Domain>(8);
	
	/**
	 * 도메인 맵에 siteCd - Domain 매핑 추가
	 * 
	 * @param siteCd
	 * @param domain
	 */
	private synchronized void addDomainMap(String siteCd, Domain domain) {
		if(!this.domainMap.containsKey(siteCd)) {
			this.domainMap.put(siteCd, domain);
		}
	}

	@Transactional
	@EventListener(condition = "#event.equipType == 'GW' and #event.equipVendor == 'mqbase'")
	public void messageReceiveEvent(SystemMessageReceiveEvent event) {
		// 1. 이벤트를 MessageObject로 파싱
		MessageObject msgObj = MwMessageUtil.toMessageObject(event);
		
		// 2. 사이트 도메인 조회
		String vHost = event.getVhost();
		Domain siteDomain = null;
		
		if(this.domainMap.containsKey(vHost)) {
			siteDomain = this.domainMap.get(vHost);
		} else {
			siteDomain = Domain.findByMwSiteCd(event.getVhost());
			this.addDomainMap(vHost, siteDomain);
		}

		// 3. 사이트 도메인 조회를 못했다면 에러
		if(siteDomain == null) {
			ElidomRuntimeException ee = new ElidomRuntimeException("Failed to find site by virtual host code [" + event.getVhost() + "]!");
			MwErrorEvent errorEvent = new MwErrorEvent(Domain.systemDomain().getId(), event, ee, true, true);
			this.eventPublisher.publishEvent(errorEvent);
			return;
		}
		
		// 4. 스레드 로컬 변수에서 currentDomain 설정
		DomainContext.setCurrentDomain(siteDomain);
		try {
			// 5. MPS에서 요청한 메시지에 대한 응답 (즉 ACK)에 대한 처리.
			if (ValueUtil.toBoolean(msgObj.getProperties().getIsReply())) {
				this.handleReplyMessage(siteDomain, event.getStageCd(), msgObj);
			// 6. 타 시스템 혹은 장비에서 MPS에 요청 메시지에 대한 처리.
			} else {
				this.handleReceivedMessage(siteDomain, event.getStageCd(), msgObj);
			}
		} catch (Exception e) {
			// 7. 예외 처리
			MwErrorEvent errorEvent = new MwErrorEvent(Domain.systemDomain().getId(), event, e, true, true);
			this.eventPublisher.publishEvent(errorEvent);
			
		} finally {
			// 8. 스레드 로컬 변수에서 currentDomain 리셋 
			DomainContext.unsetAll();
		}
	}

	/**
	 * 설비에서 요청한 메시지에 대한 응답에 대한 처리.
	 * 
	 * @param siteDomain
	 * @param stageCd
	 * @param msgObj
	 */
	private void handleReplyMessage(Domain siteDomain, String stageCd, MessageObject msgObj) {
		// this.logInfoMessage(siteDomain.getId(), msgObj);		
	}

	/**
	 * 표시기 측에서의 처리 이벤트를 실행한다.
	 * 
	 * @param siteDomain
	 * @param stageCd
	 * @param msgObj
	 */
	private void handleReceivedMessage(Domain siteDomain, String stageCd, MessageObject msgObj) {
		// 메시지 로깅
		this.logInfoMessage(siteDomain.getId(), msgObj);
		
		String msgId = msgObj.getProperties().getId();
		String sourceId = msgObj.getProperties().getSourceId();
		String action = msgObj.getBody().getAction();
		
		switch (action) {
			// 1. 버튼 터치 - 표시기 버튼 터치로 인한 작업 처리 메시지 처리 (OK, Full Box, 수정)
			case Action.Values.IndicatorOnResponse :
				this.mwSender.sendResponse(siteDomain.getId(), msgId, sourceId, new IndicatorOnResponseAck());
				//this.handleMpiResponse(siteDomain, msgObj);
				break;

			// 2. Gateway별 표시기 소등 처리
			case Action.Values.IndicatorOffResponse :
				this.mwSender.sendResponse(siteDomain.getId(), msgId, sourceId, new IndicatorOffResponseAck());
				//this.handleMpiOffResponse(siteDomain, msgObj);
				break;

			// 3. 게이트웨이 초기화 요청에 대한 응답 처리.
			case Action.Values.GatewayInitRequest :
				// 요청에 대한 응답 메시지 전송.
				this.mwSender.sendResponse(siteDomain.getId(), msgId, sourceId, new GatewayInitRequestAck());
				// 게이트웨이 초기화 요청에 대한 응답 처리.
				this.respondGatewayInit(siteDomain, stageCd, msgObj);
				break;

			// 4. 게이트웨이 초기화 리포트 메시지 처리
			case Action.Values.GatewayInitReport :
				this.mwSender.sendResponse(siteDomain.getId(), msgId, sourceId, new GatewayInitReportAck());
				this.handleGwInitReport(siteDomain, stageCd, msgObj);
				break;

			// 5. 표시기 초기화 리포트 메시지 처리
			case Action.Values.IndicatorInitReport :
				this.mwSender.sendResponse(siteDomain.getId(), msgId, sourceId, new IndicatorInitReportAck());
				this.handleIndInitReport(siteDomain, stageCd, msgObj);
				break;

			// 6. 표시기 상태 리포트 메시지 처리 
			case Action.Values.IndicatorStatusReport :
				this.mwSender.sendResponse(siteDomain.getId(), msgId, sourceId, new IndicatorStatusReportAck());
				//this.handleMpiStatusReport(siteDomain, msgObj);
				break;

			// 7. 시간 정보 동기화 요청에 대한 응답 처리
			case Action.Values.TimesyncRequest :
				// 요청에 대한 응답 메시지 전송.
				this.mwSender.sendResponse(siteDomain.getId(), msgId, sourceId, new TimesyncRequestAck());
				// 시간 정보 동기화 응답 처리.
				this.respondTimesync(siteDomain, stageCd, msgObj);
				break;

			// 8. Error Report 메시지 처리
			case Action.Values.ErrorReport :
				this.mwSender.sendResponse(siteDomain.getId(), msgId, sourceId, new ErrorReportAck());
				//this.handleErrorReport(siteDomain, msgObj);
				break;

			// 9. 미들웨어 접속 정보 변경
			case Action.Values.MiddlewareConnInfoModResponse :
				this.mwSender.sendResponse(siteDomain.getId(), msgId, sourceId, new MiddlewareConnInfoModResponseAck());
				//this.handleMwConnectionModification(siteDomain, msgObj);
				break;

			// 10. 장비 (Kiosk, Tablet, PDA) 상태 보고 
			case Action.Values.EquipStatusReport :
				//this.handleEquipStatusReport(siteDomain, msgObj);
				break;
			
			// 11. Unkown action 메시지 처리
			default :
				this.handleUnkownMessage(siteDomain, msgObj);
		}		
	}

	/**
	 * Unkown action type에 대한 메시지 처리
	 * 
	 * @param siteDomain
	 * @param msgObj
	 */
	private void handleUnkownMessage(Domain siteDomain, MessageObject msgObj) {
		throw new ElidomRuntimeException("Unknown type Message Received");
	}

	/**
	 * 게이트웨이 초기화 요청에 대한 응답 처리.
	 * 
	 * @param siteDomain
	 * @param stageCd
	 * @param msgObj
	 */
	private void respondGatewayInit(Domain siteDomain, String stageCd, MessageObject msgObj) {
		GatewayInitRequest gwInitReq = (GatewayInitRequest) msgObj.getBody();
		Gateway gw = AnyEntityUtil.findEntityBy(siteDomain.getId(), true, Gateway.class, "domainId,stageCd,gwNm", siteDomain.getId(), stageCd, gwInitReq.getId());
		this.indHandlerService.handleGatewayBootReq(gw);
	}
	
	/**
	 * Gateway 초기화 Report에 대한 처리.
	 * 
	 * @param siteDomain
	 * @param stageCd
	 * @param msgObj
	 */
	private void handleGwInitReport(Domain siteDomain, String stageCd, MessageObject msgObj) {
		GatewayInitReport gwInitRtp = (GatewayInitReport) msgObj.getBody();
		Gateway gw = AnyEntityUtil.findEntityBy(siteDomain.getId(), true, Gateway.class, "domainId,stageCd,gwCd", siteDomain.getId(), stageCd, gwInitRtp.getId());
		this.indHandlerService.handleGatewayInitReport(gw, gwInitRtp.getVersion());
	}

	/**
	 * Indicator 초기화 리포트에 대한 처리. (Indicator Version 정보 Update)
	 * 
	 * @param siteDomain
	 * @param stageCd
	 * @param msgObj
	 */
	private void handleIndInitReport(Domain siteDomain, String stageCd, MessageObject msgObj) {
		IndicatorInitReport indInitRpt = (IndicatorInitReport) msgObj.getBody();
		String sql = "select * from indicators where domain_id = :domainId and ind_cd = :indCd and gw_cd in (select gw_cd from gateways where domain_id = :domainId and stage_cd = :stageCd)";
		Map<String, Object> params = ValueUtil.newMap("domainId,indCd,stageCd", siteDomain.getId(), indInitRpt.getId(), stageCd);
		Indicator indicator = this.queryManager.selectBySql(sql, params, Indicator.class);
		this.indHandlerService.handleIndicatorInitReport(indicator, indInitRpt.getVersion());
	}
	
	/**
	 * 타임 서버가 별도 존재하지 않으므로, 시스템이 타임서버 역할을 해 게이트웨이와 시간 동기화
	 * 
	 * @param siteDomain
	 * @param stageCd
	 * @param msgObj
	 */
	private void respondTimesync(Domain siteDomain, String stageCd, MessageObject msgObj) {
		this.indHandlerService.handleTimesyncReq(siteDomain.getId(), stageCd, msgObj.getProperties().getSourceId());
	}

}
