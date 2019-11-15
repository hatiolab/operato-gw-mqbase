package operato.gw.mqbase.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import xyz.anythings.gw.GwConstants;
import xyz.anythings.gw.service.api.IIndicatorRequestService;
import xyz.anythings.gw.service.model.IndOffReq;
import xyz.anythings.gw.service.mq.MqSender;
import xyz.anythings.gw.service.mq.model.GatewayDepRequest;
import xyz.anythings.gw.service.mq.model.GatewayInitResponse;
import xyz.anythings.gw.service.mq.model.IndicatorDepRequest;
import xyz.anythings.gw.service.mq.model.IndicatorOffRequest;
import xyz.anythings.gw.service.mq.model.IndicatorOnInformation;
import xyz.anythings.gw.service.mq.model.IndicatorOnRequest;
import xyz.anythings.gw.service.mq.model.LedOffRequest;
import xyz.anythings.gw.service.mq.model.LedOnRequest;
import xyz.anythings.gw.service.mq.model.MiddlewareConnInfoModRequest;
import xyz.anythings.gw.service.mq.model.TimesyncResponse;
import xyz.anythings.gw.service.util.BatchIndConfigUtil;
import xyz.anythings.gw.service.util.MwMessageUtil;
import xyz.anythings.sys.service.AbstractQueryService;
import xyz.elidom.rabbitmq.message.MessageProperties;
import xyz.elidom.sys.util.ValueUtil;

/**
 * 표시기 인터페이스 관련 서비스
 * 1) 표시기 점등 요청
 * 2) 표시기 소등 요청
 * 
 * @author shortstop
 */
@Component
public class Type1IndicatorRequestService extends AbstractQueryService implements IIndicatorRequestService {

	/**
	 * 미들웨어로 메시지를 전송하기 위한 유틸리티
	 */
	@Autowired
	private MqSender mwMsgSender;
		
	/**********************************************************************
	 * 							1. 표시기 On 요청
	 **********************************************************************/	
	
	/**
	 * 여러 표시기 한꺼번에 재고 실사용 점등 요청
	 * 
	 * @param domainId
	 * @param stockIndOnList
	 */
	@Override
	public void requestStockIndOn(Long domainId, Map<String, List<IndicatorOnInformation>> stockIndOnList) {
		if (ValueUtil.isNotEmpty(stockIndOnList)) {
			stockIndOnList.forEach((gwPath, stockOnList) -> {
				MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
				this.mwMsgSender.send(domainId, property, new IndicatorOnRequest("DPS", GwConstants.IND_ACTION_TYPE_STOCK, stockOnList));
			});
		}
	}
	
	/**
	 * 여러 표시기에 한꺼번에 분류 처리를 위한 점등 요청
	 * 
	 * @param domainId
	 * @param jobType
	 * @param actionType
	 * @param indOnForPickList - key : gwPath, value : indOnInfo 
	 */
	@Override
	public void requestIndsOn(Long domainId, String jobType, String actionType, Map<String, List<IndicatorOnInformation>> indOnForPickList) {
		if (ValueUtil.isNotEmpty(indOnForPickList)) {
			indOnForPickList.forEach((gwPath, indOnList) -> {
				MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
				this.mwMsgSender.send(domainId, property, new IndicatorOnRequest(jobType, actionType, indOnList));
			});
		}
	}
	
	/**
	 * 여러 표시기에 한꺼번에 분류 처리를 위한 점등 요청
	 * 
	 * @param domainId
	 * @param jobType
	 * @param indOnForPickList - key : gwPath, value : indOnInfo 
	 */
	@Override
	public void requestIndsInspectOn(Long domainId, String jobType, Map<String, List<IndicatorOnInformation>> indOnForPickList) {
		if (ValueUtil.isNotEmpty(indOnForPickList)) {
			indOnForPickList.forEach((gwPath, indOnList) -> {
				MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
				for(IndicatorOnInformation indOnInfo : indOnList) {
					indOnInfo.setBtnMode(BatchIndConfigUtil.IND_BUTTON_MODE_STOP);
				}
				this.mwMsgSender.send(domainId, property, new IndicatorOnRequest(jobType, GwConstants.IND_ACTION_TYPE_INSPECT, indOnList));
			});
		}
	}
	
	/**
	 * 하나의 표시기에 분류 처리를 위한 점등 요청
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param color
	 * @param boxQty
	 * @param eaQty
	 */
	@Override
	public void requestPickIndOn(Long domainId, String jobType, String gwPath, String indCd, String bizId, String color, Integer boxQty, Integer eaQty) {
		this.requestCommonIndOn(domainId, jobType, gwPath, indCd, bizId, GwConstants.IND_ACTION_TYPE_PICK, color, boxQty, eaQty);
	}
	
	/**
	 * 하나의 표시기에 검수를 위한 점등 요청
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param color
	 * @param boxQty
	 * @param eaQty
	 */
	@Override
	public void requestInspectIndOn(Long domainId, String jobType, String gwPath, String indCd, String bizId, String color, Integer boxQty, Integer eaQty) {
		this.requestCommonIndOn(domainId, jobType, gwPath, indCd, bizId, GwConstants.IND_ACTION_TYPE_INSPECT, color, boxQty, eaQty);
	}
	
	/**
	 * 하나의 표시기에 액션 타입별 점등 요청 
	 * 
	 * @param domainId
	 * @param jobType
	 * @param indCd
	 * @param bizId
	 * @param actionType
	 * @param color
	 * @param boxQty
	 * @param eaQty
	 */
	/*public void requestCommonIndOn(Long domainId, String jobType, String indCd, String bizId, String actionType, String color, Integer boxQty, Integer eaQty) {
		String gwPath = GwQueryUtil.findGatewayPathByIndCd(domainId, indCd);
		requestCommonIndOn(domainId, jobType, indCd, gwPath, bizId, actionType, color, boxQty, eaQty);
	}*/
	
	/**
	 * 하나의 표시기에 액션 타입별 점등 요청 
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param actionType
	 * @param color
	 * @param boxQty
	 * @param eaQty
	 */
	@Override
	public void requestCommonIndOn(Long domainId, String jobType, String gwPath, String indCd, String bizId, String actionType, String color, Integer boxQty, Integer eaQty) {
		MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
		List<IndicatorOnInformation> indOnList = new ArrayList<IndicatorOnInformation>(1);
		IndicatorOnInformation indOnInfo = new IndicatorOnInformation();
		indOnInfo.setId(indCd);
		indOnInfo.setBizId(bizId);
		indOnInfo.setColor(color);
		indOnInfo.setOrgBoxQty(boxQty);
		indOnInfo.setOrgEaQty(eaQty);
		indOnList.add(indOnInfo);
		this.mwMsgSender.send(domainId, property, new IndicatorOnRequest(jobType, actionType, indOnList));
	}	
	
	/**********************************************************************
	 * 							2. 표시기 Off 요청
	 **********************************************************************/	
	/**
	 * 표시기 하나에 대한 소등 요청 
	 * 
	 * @param domainId
	 * @param gwPath
	 * @param indCd
	 * @param forceOff
	 */
	@Override
	public void requestIndOff(Long domainId, String gwPath, String indCd, boolean forceOff) {
		//String gwPath = GwQueryUtil.findGatewayPathByIndCd(domainId, indCd);
		IndicatorOffRequest indOff = new IndicatorOffRequest();
		indOff.setIndOff(ValueUtil.newStringList(indCd));
		indOff.setForceFlag(forceOff);
		this.mwMsgSender.sendRequest(domainId, gwPath, indOff);
	}
	
	/**
	 * 표시기 하나에 대한 소등 요청 
	 * 
	 * @param domainId
	 * @param indCd
	 */
	@Override
	public void requestIndOff(Long domainId, String gwPath, String indCd) {
		this.requestIndOff(domainId, gwPath, indCd, false);
	}
	
	/**
	 * 게이트웨이 - 표시기 리스트 값으로 표시기 소등 요청
	 * 
	 * @param domainId
	 * @param indOffMap gwPath -> indicator Code List
	 * @param forceOff
	 */
	@Override
	public void requestIndListOff(Long domainId, Map<String, List<String>> indOffMap, boolean forceOff) {
		if (ValueUtil.isNotEmpty(indOffMap)) {
			indOffMap.forEach((gwPath, indCdList) -> {
				this.requestIndListOff(domainId, gwPath, indCdList, forceOff);
			});
		}
	}
		
	/**
	 * 게이트웨이에 게이트웨이 소속 모든 표시기 소등 요청
	 * 
	 * @param domainId
	 * @param gwPath
	 * @param indCdList
	 * @param forceOff 강제 소등 여부
	 */
	@Override
	public void requestIndListOff(Long domainId, String gwPath, List<String> indCdList, boolean forceOff) {
		if (ValueUtil.isNotEmpty(indCdList)) {
			IndicatorOffRequest indOff = new IndicatorOffRequest();
			indOff.setIndOff(indCdList);
			indOff.setForceFlag(forceOff);
			// 현재는 forceOff와 endOff가 동일값을 가짐
			indOff.setEndOffFlag(forceOff);
			this.mwMsgSender.sendRequest(domainId, gwPath, indOff);
		}
	}
	
	/**
	 * 호기별 표시기 Off 요청 전송 
	 * 
	 * @param domainId
	 * @param indOffList 소등할 표시기 리스트 
	 * @param forceOff 강제 소등 여부
	 */
	@Override
	public void requestIndListOff(Long domainId, List<IndOffReq> indList, boolean forceOff) {
		// 1. 게이트웨이 별로 표시기 리스트를 보내서 소등 요청을 한다.
		Map<String, List<String>> indsByGwPath = new HashMap<String, List<String>>();
		String prevGwPath = null;
		
		for(IndOffReq indOff : indList) {
			String gwPath = indOff.getGwPath();
			
			if(ValueUtil.isNotEqual(gwPath, prevGwPath)) {
				indsByGwPath.put(gwPath, ValueUtil.newStringList(indOff.getIndCd()));
				prevGwPath = gwPath;
			} else {
				indsByGwPath.get(gwPath).add(indOff.getIndCd());
			}
		}
		
		// 2. 게이트웨이 별로 표시기 코드 리스트로 소등 요청 
		Iterator<String> gwIter = indsByGwPath.keySet().iterator();
		while(gwIter.hasNext()) {
			String gwPath = gwIter.next();
			List<String> gwIndList = indsByGwPath.get(gwPath);
			this.requestIndListOff(domainId, gwPath, gwIndList, forceOff);
		}
	}
	
	/**
	 * 호기 및 장비 작업 존 별 표시기 Off 요청 전송 
	 * 
	 * @param domainId
	 * @param rackCd
	 * @param equipZoneCd
	 * @param sideCd
	 */
	/*public void requestIndOffByEquipZone(Long domainId, String rackCd, String equipZoneCd, String sideCd) {
		// 1. 로케이션 정보로 부터 호기, 장비 존, 호기 사이드 코드로 표시기, 게이트웨이 정보를 추출한다. 
		List<IndOffReq> indList = GwQueryUtil.searchIndByEquipZone(domainId, rackCd, equipZoneCd, sideCd);
		// 2. 표시기별 소등 요청
		this.requestOffByIndList(domainId, indList, false);
	}*/
	
	/**
	 * 호기 및 작업 스테이션 별 표시기 Off 요청 전송 
	 * 
	 * @param domainId
	 * @param rackCd
	 * @param zoneCd
	 */
	/*public void requestIndOffByStation(Long domainId, String rackCd, String zoneCd) {
		// 1. 로케이션 정보로 부터 호기, 장비 존, 호기 사이드 코드로 표시기, 게이트웨이 정보를 추출한다. 
		List<IndOffReq> indList = GwQueryUtil.searchIndByStation(domainId, rackCd, zoneCd);
		// 2. 표시기별 소등 요청
		this.requestOffByIndList(domainId, indList, false);
	}*/
	
	/**********************************************************************
	 * 							3. 표시기 숫자, 문자 표시 요청
	 **********************************************************************/		
	
	/**
	 * 작업 완료 표시기 표시 요청 
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param finalEnd 최종 완료 (End End 표시 후 Fullbox까지 마쳤는지) 여부
	 */
	@Override
	public void requestIndEndDisplay(Long domainId, String jobType, String gwPath, String indCd, String bizId, boolean finalEnd) {
		//String gwPath = GwQueryUtil.findGatewayPathByIndCd(domainId, indCd);
		MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
		IndicatorOnInformation indOnInfo = new IndicatorOnInformation();
		indOnInfo.setId(indCd);
		indOnInfo.setBizId(bizId);
		indOnInfo.setEndFullBox(!finalEnd);
		IndicatorOnRequest indOnReq = new IndicatorOnRequest(jobType, GwConstants.IND_BIZ_FLAG_END, ValueUtil.toList(indOnInfo));
		indOnReq.setReadOnly(finalEnd);
		this.mwMsgSender.send(domainId, property, indOnReq);		
	}
	
	/**
	 * 로케이션 별 공박스 매핑 필요 표시 요청
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 */
	@Override
	public void requestIndNoBoxDisplay(Long domainId, String jobType, String gwPath, String indCd) {
		this.requestIndDisplay(domainId, jobType, gwPath, indCd, indCd, GwConstants.IND_ACTION_TYPE_NOBOX, false, null, null, null);
	}
	
	/**
	 * Fullbox시에 로케이션-공박스 매핑이 안 된 에러를 표시기에 표시하기 위한 요청
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 */
	@Override
	public void requestIndErrBoxDisplay(Long domainId, String jobType, String gwPath, String indCd) {
		this.requestIndDisplay(domainId, jobType, gwPath, indCd, indCd, GwConstants.IND_ACTION_TYPE_ERRBOX, false, null, null, null);
	}
	
	/**
	 * 표시기에 버튼 점등은 되지 않고 eaQty 정보로 표시 - 사용자 터치 반응 안함 
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param pickEaQty
	 */
	@Override
	public void requestIndDisplayOnly(Long domainId, String jobType, String gwPath, String indCd, String bizId, Integer pickEaQty) {
		this.requestIndDisplay(domainId, jobType, gwPath, indCd, bizId, GwConstants.IND_ACTION_TYPE_DISPLAY, true, null, null, pickEaQty);
	}
	
	/**
	 * 세그먼트 정보를 커스터마이징 한 표시기 표시 - 이 때 Fullbox가 되어야 하므로 readOnly는 false로
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param firstSegQty
	 * @param secondSegQty
	 * @param thirdSegQty
	 */
	@Override
	public void requestIndSegmentDisplay(Long domainId, String jobType, String gwPath, String indCd, String bizId, Integer firstSegQty, Integer secondSegQty, Integer thirdSegQty) {
		this.requestIndDisplay(domainId, jobType, gwPath, indCd, bizId, GwConstants.IND_ACTION_TYPE_DISPLAY, false, firstSegQty, secondSegQty, thirdSegQty);
	}
	
	/**
	 * 각종 옵션으로 표시기에 표시 요청 
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param displayActionType
	 * @param readOnly
	 * @param firstSegQty
	 * @param secondSegQty
	 * @param thirdSegQty
	 */
	@Override
	public void requestIndDisplay(Long domainId, String jobType, String gwPath, String indCd, String bizId, String displayActionType, boolean readOnly, Integer firstSegQty, Integer secondSegQty, Integer thirdSegQty) {
		//String gwPath = GwQueryUtil.findGatewayPathByIndCd(domainId, indCd);
		MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
		List<IndicatorOnInformation> indOnList = new ArrayList<IndicatorOnInformation>(1);
		IndicatorOnInformation indOnInfo = new IndicatorOnInformation();
		indOnInfo.setId(indCd);
		indOnInfo.setBizId(bizId);
		indOnInfo.setOrgRelay(firstSegQty);
		indOnInfo.setOrgBoxQty(secondSegQty);
		indOnInfo.setOrgEaQty(thirdSegQty);
		indOnList.add(indOnInfo);
		IndicatorOnRequest indOnReq = new IndicatorOnRequest(jobType, displayActionType, indOnList);
		indOnReq.setReadOnly(readOnly);
		this.mwMsgSender.send(domainId, property, indOnReq);
	}
	
	/**
	 * 각종 옵션으로 표시기에 표시 요청 
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param displayActionType
	 * @param segRole
	 * @param readOnly
	 * @param firstSegQty
	 * @param secondSegQty
	 * @param thirdSegQty
	 */
	@Override
	public void requestIndDisplay(Long domainId, String jobType, String gwPath, String indCd, String bizId, String displayActionType, String[] segRole, boolean readOnly, Integer firstSegQty, Integer secondSegQty, Integer thirdSegQty) {
		//String gwPath = GwQueryUtil.findGatewayPathByIndCd(domainId, indCd);
		MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
		List<IndicatorOnInformation> indOnList = new ArrayList<IndicatorOnInformation>(1);
		IndicatorOnInformation indOnInfo = new IndicatorOnInformation();
		indOnInfo.setId(indCd);
		indOnInfo.setBizId(bizId);
		indOnInfo.setSegRole(segRole);
		indOnInfo.setOrgRelay(firstSegQty);
		indOnInfo.setOrgBoxQty(secondSegQty);
		indOnInfo.setOrgEaQty(thirdSegQty);
		indOnList.add(indOnInfo);
		IndicatorOnRequest indOnReq = new IndicatorOnRequest(jobType, displayActionType, indOnList);
		indOnReq.setReadOnly(readOnly);
		this.mwMsgSender.send(domainId, property, indOnReq);
	}
	
	/**
	 * 총 처리한 수량 / 방금 처리한 수량을 표시
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param accumQty
	 * @param pickedQty
	 */
	@Override
	public void requestIndDisplayAccumQty(Long domainId, String jobType, String gwPath, String indCd, String bizId, Integer accumQty, Integer pickedQty) {
		MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
		List<IndicatorOnInformation> indOnList = new ArrayList<IndicatorOnInformation>(1);
		IndicatorOnInformation indOnInfo = new IndicatorOnInformation();
		indOnInfo.setId(indCd);
		indOnInfo.setBizId(bizId);
		indOnInfo.setSegRole(new String[] { BatchIndConfigUtil.IND_SEGMENT_ROLE_RELAY_SEQ, BatchIndConfigUtil.IND_SEGMENT_ROLE_PCS });
		indOnInfo.setOrgAccmQty(accumQty);
		indOnInfo.setOrgEaQty(pickedQty);
		indOnList.add(indOnInfo);
		IndicatorOnRequest indOnReq = new IndicatorOnRequest(jobType, GwConstants.IND_ACTION_TYPE_DISPLAY, indOnList);
		indOnReq.setReadOnly(true);
		this.mwMsgSender.send(domainId, property, indOnReq);
	}
		
	/**
	 * FullBox 표시기 표시 요청 
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param color
	 */
	@Override
	public void requestFullbox(Long domainId, String jobType, String gwPath, String indCd, String bizId, String color) {
		this.requestCommonIndOn(domainId, jobType, gwPath, indCd, bizId, GwConstants.IND_BIZ_FLAG_FULL, color, 0, 0);
	}

	/**
	 * 표시기에 문자열 표시 요청
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param displayStr
	 */
	@Override
	public void requestShowString(Long domainId, String jobType, String gwPath, String indCd, String bizId, String displayStr) {
//		if(ValueUtil.isEmpty(gwPath)) {
//			gwPath = GwQueryUtil.findGatewayPathByIndCd(domainId, indCd);
//		}
		
		MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
		List<IndicatorOnInformation> indOnList = new ArrayList<IndicatorOnInformation>(1);
		IndicatorOnInformation indOnInfo = new IndicatorOnInformation();
		indOnInfo.setId(indCd);
		indOnInfo.setBizId(bizId);
		indOnInfo.setViewStr(displayStr);
		indOnList.add(indOnInfo);
		this.mwMsgSender.send(domainId, property, new IndicatorOnRequest(jobType, GwConstants.IND_ACTION_TYPE_STR_SHOW, indOnList));
	}

	/**
	 * 표시기 표시 방향과 숫자를 동시에 표시 - 왼쪽은 'L' or 'R' 표시 오른쪽은 숫자 표시
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param leftSideFlag 왼쪽 로케이션 표시용인지 여부
	 * @param rightQty
	 */
	@Override
	public void requestDisplayDirectionAndQty(Long domainId, String jobType, String gwPath, String indCd, String bizId, boolean leftSideFlag, Integer rightQty) {		
		requestDisplayLeftStringRightQty(domainId, jobType, gwPath, indCd, bizId, leftSideFlag ? " L " : " R ", rightQty);
	}
	
	/**
	 * 왼쪽은 문자 오른쪽은 숫자 표시
	 * 
	 * @param domainId
	 * @param jobType
	 * @param gwPath
	 * @param indCd
	 * @param bizId
	 * @param leftStr
	 * @param rightQty
	 */
	@Override
	public void requestDisplayLeftStringRightQty(Long domainId, String jobType, String gwPath, String indCd, String bizId, String leftStr, Integer rightQty) {		
		//String gwPath = GwQueryUtil.findGatewayPathByIndCd(domainId, indCd);
		MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
		List<IndicatorOnInformation> indOnList = new ArrayList<IndicatorOnInformation>(1);
		IndicatorOnInformation indOnInfo = new IndicatorOnInformation();
		indOnInfo.setId(indCd);
		indOnInfo.setBizId(bizId);
		indOnInfo.setSegRole(new String[] { BatchIndConfigUtil.IND_SEGMENT_ROLE_STR, BatchIndConfigUtil.IND_SEGMENT_ROLE_PCS });
		indOnInfo.setViewStr(leftStr);
		indOnInfo.setOrgEaQty(rightQty);
		indOnList.add(indOnInfo);
		IndicatorOnRequest indOnReq = new IndicatorOnRequest(jobType, GwConstants.IND_ACTION_TYPE_DISPLAY, indOnList);
		indOnReq.setReadOnly(true);
		this.mwMsgSender.send(domainId, property, indOnReq);
	}
	
	/**
	 * 표시기 표시 방향과 표시 수량을 좌, 우측에 동시에 표시 
	 * 
	 * @param domainId
	 * @param jobType
	 * @param indCd
	 * @param bizId
	 * @param leftQty
	 * @param rightQty
	 */
	@Override
	public void requestDisplayBothDirectionQty(Long domainId, String jobType, String gwPath, String indCd, String bizId, Integer leftQty, Integer rightQty) {
		StringBuffer showStr = new StringBuffer();
		
		if(leftQty != null) {
			showStr.append(GwConstants.IND_LEFT_SEGMENT).append(StringUtils.leftPad(ValueUtil.toString(leftQty), 2));
		} else {
			showStr.append("   ");
		}
		
		if(rightQty != null) {
			showStr.append(GwConstants.IND_RIGHT_SEGMENT).append(StringUtils.leftPad(ValueUtil.toString(rightQty), 2));
		} else {
			showStr.append("   ");
		}
		
		this.requestShowString(domainId, jobType, gwPath, indCd, bizId, showStr.toString());
	}

	/**********************************************************************
	 * 							4. 게이트웨이 초기화 요청
	 **********************************************************************/	
	
	/**
	 * 게이트웨이 초기화 응답 전송.
	 * 
	 * @param domainId
	 * @param msgDestId
	 * @param gatewayInitRes
	 */
	@Override
	public void respondGatewayInit(Long domainId, String msgDestId, GatewayInitResponse gatewayInitRes) {
		this.mwMsgSender.sendRequest(domainId, msgDestId, gatewayInitRes);
	}
	
	/**********************************************************************
	 * 							5. 미들웨어 정보 변경 요청
	 **********************************************************************/	

	/**
	 * 게이트웨이에 미들웨어 접속 정보 변경 요청
	 * 
	 * @param domainId
	 * @param msgDestId
	 * @param mwConnModifyReq
	 */
	@Override
	public void requestMwConnectionModify(Long domainId, String msgDestId, MiddlewareConnInfoModRequest mwConnModifyReq) {
		this.mwMsgSender.sendRequest(domainId, msgDestId, mwConnModifyReq);
	}
	
	/**********************************************************************
	 * 							6. 게이트웨이 시스템 시간 동기화 
	 **********************************************************************/	

	/**
	 * 게이트웨이와 시스템간의 시간 동기화 응답 요청.
	 * 
	 * @param domainId
	 * @param msgDestId
	 * @param serverTime
	 */
	@Override
	public void respondTimesync(Long domainId, String msgDestId, long serverTime) {
		this.mwMsgSender.sendRequest(domainId, msgDestId, new TimesyncResponse(serverTime));
	}
	
	/**********************************************************************
	 * 							7. 게이트웨이 / 표시기 펌웨어 배포  
	 **********************************************************************/	
	
	/**
	 * 게이트웨이에 게이트웨이 펌웨어 배포 정보 전송 
	 * 
	 * @parma domainId
	 * @param gwChannel 게이트웨이 구분 채널 
	 * @param gwVersion 게이트웨이 펌웨어 버전 
	 * @param gwFwDownloadUrl 게이트웨이 펌웨어 다운로드 URL
	 * @param filename 파일명
	 * @param forceFlag 강제 업데이트 여부
	 */
	@Override
	public void deployGatewayFirmware(Long domainId, String gwChannel, String gwVersion, String gwFwDownloadUrl, String filename, Boolean forceFlag) {
		GatewayDepRequest gwDeploy = new GatewayDepRequest();
		gwDeploy.setGwUrl(gwFwDownloadUrl);
		gwDeploy.setVersion(gwVersion);
		gwDeploy.setFilename(filename);
		gwDeploy.setForceFlag(forceFlag);
		this.mwMsgSender.sendRequest(domainId, gwChannel, gwDeploy);
	}
	
	/**
	 * 게이트웨이에 표시기 펌웨어 배포 정보 전송 
	 * 
	 * @param domainId
	 * @param gwChannel 게이트웨이 구분 채널
	 * @param indVersion 표시기 펌웨어 버전 
	 * @param indFwDownloadUrl 표시기 펌웨어 다운로드 URL
	 * @param filename 파일명
	 * @param forceFlag 강제 업데이트 여부
	 */
	@Override
	public void deployIndFirmware(Long domainId, String gwChannel, String indVersion, String indFwDownloadUrl, String filename, Boolean forceFlag) {
		IndicatorDepRequest indDeploy = new IndicatorDepRequest();
		indDeploy.setVersion(indVersion);
		indDeploy.setIndUrl(indFwDownloadUrl);
		indDeploy.setFilename(filename);
		indDeploy.setForceFlag(forceFlag);
		this.mwMsgSender.sendRequest(domainId, gwChannel, indDeploy);
	}
	
	/**********************************************************************
	 * 							8. LED 바 점등 / 소등 
	 **********************************************************************/	
	
	/**
	 * 표시기 LED 점등 
	 * 
	 * @param domainId
	 * @param gwPath
	 * @param indCd
	 * @param ledBarBrightness
	 */
	@Override
	public void requestLedOn(Long domainId, String gwPath, String indCd, Integer ledBarBrightness) {
		LedOnRequest ledOnReq = new LedOnRequest();
		ledOnReq.setId(indCd);
		ledOnReq.setLedBarBrtns(ledBarBrightness);
		//String gwPath = GwQueryUtil.findGatewayPathByIndCd(domainId, indCd);
		MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
		this.mwMsgSender.send(domainId, property, ledOnReq);
	}
	
	/**
	 * 표시기 LED 소등 
	 * 
	 * @param domainId
	 * @param gwPath
	 * @param indCd
	 */
	@Override
	public void requestLedOff(Long domainId, String gwPath, String indCd) {
		LedOffRequest ledOffReq = new LedOffRequest();
		ledOffReq.setId(indCd);
		//String gwPath = GwQueryUtil.findGatewayPathByIndCd(domainId, indCd);
		MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
		this.mwMsgSender.send(domainId, property, ledOffReq);		
	}
	
	/**
	 * 표시기 LED 리스트 점등 
	 * 
	 * @param domainId
	 * @param indList
	 * @param ledBrightness
	 */
	@Override
	public void requestLedListOn(Long domainId, List<IndOffReq> indList, Integer ledBrightness) {
		// 1. 게이트웨이 별로 표시기 리스트를 보내서 점등 요청을 한다.
		Map<String, List<String>> indsByGwPath = new HashMap<String, List<String>>();
		String prevGwPath = null;
		
		for(IndOffReq indOff : indList) {
			String gwPath = indOff.getGwPath();
			
			if(ValueUtil.isNotEqual(gwPath, prevGwPath)) {
				indsByGwPath.put(gwPath, ValueUtil.newStringList(indOff.getIndCd()));
				prevGwPath = gwPath;
			} else {
				indsByGwPath.get(gwPath).add(indOff.getIndCd());
			}
		}
		
		// 2. 게이트웨이 별로 표시기 코드 리스트로 소등 요청
		Iterator<String> gwPathIter = indsByGwPath.keySet().iterator();
		while(gwPathIter.hasNext()) {
			String gwPath = gwPathIter.next();
			List<String> indCdList = indsByGwPath.get(gwPath);
			
			// TODO 아래 부분을 한번에 보내도록 수정
			for(String indCd : indCdList) {
				LedOnRequest ledOnReq = new LedOnRequest();
				ledOnReq.setId(indCd);
				ledOnReq.setLedBarBrtns(ledBrightness);
				MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
				this.mwMsgSender.send(domainId, property, ledOnReq);			
			}			
		}
	}
	
	/**
	 * 표시기 LED 리스트 소등 
	 * 
	 * @param domainId
	 * @param indList
	 */
	@Override
	public void requestLedListOff(Long domainId, List<IndOffReq> indList) {
		// 1. 게이트웨이 별로 표시기 리스트를 보내서 점등 요청을 한다.
		Map<String, List<String>> indsByGwPath = new HashMap<String, List<String>>();
		String prevGwPath = null;
		
		for(IndOffReq indOff : indList) {
			String gwPath = indOff.getGwPath();
			
			if(ValueUtil.isNotEqual(gwPath, prevGwPath)) {
				indsByGwPath.put(gwPath, ValueUtil.newStringList(indOff.getIndCd()));
				prevGwPath = gwPath;
			} else {
				indsByGwPath.get(gwPath).add(indOff.getIndCd());
			}
		}
		
		// 2. 게이트웨이 별로 표시기 코드 리스트로 소등 요청
		Iterator<String> gwPathIter = indsByGwPath.keySet().iterator();
		while(gwPathIter.hasNext()) {
			String gwPath = gwPathIter.next();
			List<String> indCdList = indsByGwPath.get(gwPath);
			
			// TODO 아래 부분을 한번에 보내도록 수정
			for(String indCd : indCdList) {
				LedOffRequest ledOnReq = new LedOffRequest();
				ledOnReq.setId(indCd);
				MessageProperties property = MwMessageUtil.newReqMessageProp(gwPath);
				this.mwMsgSender.send(domainId, property, ledOnReq);			
			}			
		}
	}

}