package operato.gw.mqbase.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import xyz.anythings.comm.rabbitmq.event.MwErrorEvent;
import xyz.anythings.gw.service.mq.MqCommon;
import xyz.anythings.gw.service.mq.model.MessageObject;
import xyz.anythings.gw.service.util.MwMessageUtil;
import xyz.anythings.sys.event.EventPublisher;
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
public class MqMessageReceiver extends MqCommon  {
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
	//@EventListener(condition = "#event.queueName == @anythingsGwModuleProperties.getRabbitQueue()")
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
				this.handleReplyMessage(siteDomain, msgObj);
			// 6. 타 시스템 혹은 장비에서 MPS에 요청 메시지에 대한 처리.
			} else {
				this.handleReceivedMessage(siteDomain, msgObj);
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
	 * MPS에서 요청한 메시지에 대한 응답에 대한 처리.
	 * 
	 * @param siteDomain
	 * @param msgObj
	 */
	private void handleReplyMessage(Domain siteDomain, MessageObject msgObj) {
		// this.logInfoMessage(siteDomain.getId(), msgObj);		
	}

	/**
	 * 표시기 측에서의 처리 이벤트를 실행한다.
	 * 
	 * @param siteDomain
	 * @param msgObj
	 */
	private void handleReceivedMessage(Domain siteDomain, MessageObject msgObj) {
		// 메시지 로깅
		this.logInfoMessage(siteDomain.getId(), msgObj);
	}

	/**
	 * Unkown action type에 대한 메시지 처리
	 * 
	 * @param siteDomain
	 * @param msgObj
	 */
	/*private void handleUnkownMessage(Domain siteDomain, MessageObject msgObj) {
		throw new ElidomRuntimeException("Unknown type Message Received");
	}*/

}
