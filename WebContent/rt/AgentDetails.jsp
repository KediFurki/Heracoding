<%@page import="comapp.DBUtility"%>
<%@page import="java.util.Calendar"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.util.Date"%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Properties"%>
<%@page import="org.apache.log4j.Logger"%>
<html>
<head>
<meta charset="UTF-8">
<!-- <link rel="stylesheet" href="../css/global.css"> -->
<link rel="stylesheet" type="text/css" href="../css/comapp.css">
<script src="../js/jquery.min.js"></script>
<script src="../js/jquery.stickytableheaders.js"></script>

</head>
<body style="overflow-y: auto;">
	<div class="bottom-right">
		<table>
			<tr>
				<td class=""><img style="height: 20px" src='../<%=ConfigServlet.getProperties().getProperty("Logo1")%>'></td>
				<td class="" style="float: right; "><img style="height: 20px" src='../<%=ConfigServlet.getProperties().getProperty("Logo2")%>'></td>
			</tr>
		</table>
	</div>
	<%
		Logger log = Logger.getLogger("comapp." + this.getClass().getName());
		log.info(session.getId() + " - ******* new request ***** ");
		String SystemEnvironment = ConfigServlet.getProperties().getProperty("SystemEnvironment", "CCC");
		//==	REQUEST PARAMETERS	====
		String CodIvr = request.getParameter("CodIvr");
		if (StringUtils.isBlank(CodIvr)) {
			CodIvr = (String) session.getAttribute("CodIvr");
		} else {
			session.setAttribute("CodIvr", CodIvr);
		}
		SimpleDateFormat sdf_day_of_year = new SimpleDateFormat("MM-dd");
		SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf_time = new SimpleDateFormat("HH:mm");
		SimpleDateFormat sdf_full_day_of_year_and_time = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				
		String sFilterDay = request.getParameter("change_filter_day");
		if (StringUtils.isBlank(sFilterDay))
			sFilterDay = (String) session.getAttribute("change_filterRT_day");
		if (StringUtils.isBlank(sFilterDay)){		
			Date filterDay = new Date();
			sFilterDay = sdf_full_day_of_year.format(filterDay);
		}
		
		String sFilterTime = request.getParameter("change_filter_time");
		if (StringUtils.isBlank(sFilterTime))
			sFilterTime = (String) session.getAttribute("change_filterRT_time");
		if (StringUtils.isBlank(sFilterTime)){		
			Date filterTime = new Date();
			sFilterTime = sdf_time.format(filterTime);
		}
		
		Date filterToday = new Date();
		session.setAttribute("change_filterRT_time", sFilterTime);
		session.setAttribute("change_filterRT_day",sFilterDay);
		Date dt = sdf_full_day_of_year_and_time.parse(sFilterDay+" "+ sFilterTime);
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(dt);
		int m  = cal.get(Calendar.MINUTE);
		m = m- (m%15);
		cal.set(Calendar.MINUTE,m);
		cal.set(Calendar.SECOND,0);
		cal.set(Calendar.MILLISECOND,0);

		Calendar calT = Calendar.getInstance();
		calT.setTime(filterToday);
		int mT  = calT.get(Calendar.MINUTE);
		mT = mT- (mT%15);
		calT.set(Calendar.MINUTE,mT);
		calT.set(Calendar.SECOND,0);
		calT.set(Calendar.MILLISECOND,0);

		if (cal.before(calT)) {
			response.sendRedirect("AgentDetailsHistory.jsp");
		}
	    
		try {
			String change1_Threshold_name =request.getParameter("change1_Threshold_name");
			String change2_Threshold_name =request.getParameter("change2_Threshold_name");
			String change3_Threshold_name =request.getParameter("change3_Threshold_name");
			String change4_Threshold_name =request.getParameter("change4_Threshold_name");
			
			String change1_Threshold_op =request.getParameter("change1_Threshold_op");
			String change2_Threshold_op =request.getParameter("change2_Threshold_op");
			String change3_Threshold_op =request.getParameter("change3_Threshold_op");
			String change4_Threshold_op =request.getParameter("change4_Threshold_op");
			
			String change1_Threshold_1 =request.getParameter("change1_Threshold_1");
			String change2_Threshold_1 =request.getParameter("change2_Threshold_1");
			String change3_Threshold_1 =request.getParameter("change3_Threshold_1");
			String change4_Threshold_1 =request.getParameter("change4_Threshold_1");
			
			
			String change1_Threshold_2 =request.getParameter("change1_Threshold_2");
			String change2_Threshold_2 =request.getParameter("change2_Threshold_2");
			String change3_Threshold_2 =request.getParameter("change3_Threshold_2");
			String change4_Threshold_2 =request.getParameter("change4_Threshold_2");
		
			DBUtility.putThresholds(SystemEnvironment,  CodIvr, change1_Threshold_name, change1_Threshold_op, change1_Threshold_1,change1_Threshold_2);
			DBUtility.putThresholds(SystemEnvironment,  CodIvr, change2_Threshold_name, change2_Threshold_op, change2_Threshold_1,change2_Threshold_2);
			DBUtility.putThresholds(SystemEnvironment,  CodIvr, change3_Threshold_name, change3_Threshold_op, change3_Threshold_1,change3_Threshold_2);
			DBUtility.putThresholds(SystemEnvironment,  CodIvr, change4_Threshold_name, change4_Threshold_op, change4_Threshold_1,change4_Threshold_2);
		} catch (Exception e){
			log.warn(e.getMessage(),e);
		}

		String DistributedForecastTot = DBUtility.getForecastDaily(SystemEnvironment, CodIvr, "1", cal.getTime());
		String DistributedForecastLinetech = DBUtility.getForecastDaily(SystemEnvironment, CodIvr, "2", cal.getTime());
		String DistributedForecastKoine = DBUtility.getForecastDaily(SystemEnvironment, CodIvr, "3", cal.getTime());
		String DistributedForecastVk = DBUtility.getForecastDaily(SystemEnvironment, CodIvr, "4", cal.getTime());
		
		String TrafficDailyForecastTot = DistributedForecastTot;		
		String TrafficDailyForecastLinetech = DistributedForecastLinetech;
		String TrafficDailyForecastKoine = DistributedForecastKoine;
		String TrafficDailyForecastVk = DistributedForecastVk;
		
		
// 		EXEC   @return_value = [dashboard].[Graph_GetSingleForecastValues]
//             @cod_ivr = N'IVR_4',
//             @graph = 2,
//             @day = N'2019/10/25 09:30:00'

		String DistributedQuarterForecastTot = DBUtility.getForecastQuarter(SystemEnvironment, CodIvr, "1", cal.getTime());
		String DistributedQuarterForecastLinetech = DBUtility.getForecastQuarter(SystemEnvironment, CodIvr, "2", cal.getTime());
		String DistributedQuarterForecastKoine =  DBUtility.getForecastQuarter(SystemEnvironment, CodIvr, "3", cal.getTime());
		String DistributedQuarterForecastVk =  DBUtility.getForecastQuarter(SystemEnvironment, CodIvr, "4", cal.getTime());
	
		//==============================
		Properties prop = ConfigServlet.getProperties();
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		//==	ACTION PARAMETERS	====
		//		NONE
		//==============================
		int i = 0;
		String MainMenu = null;
		int errorCode = 0;
		try {
			if (StringUtils.isBlank(CodIvr)) {
			} else {
	%>
	<script>
		var webSocket;
		var statArray = [];
		function isFunction(functionToCheck) {
			 return functionToCheck && {}.toString.call(functionToCheck) === '[object Function]';
			}
		function openSocket() {
			if (webSocket !== undefined && webSocket.readyState !== WebSocket.CLOSED) {
				console.warn("[openSocket] - already created");
				return;
			}
			// console.debug("[openSocket] -> opening");
			webSocket = new WebSocket("ws://" + location.host + "/DashBoard/WSSAgentDetails");
			console.debug("[openSocket] -  completed");
			webSocket.onopen = function(event) {
				console.log("[openSocket] <- event onopen");
				var js = {};
				js['command'] = 'register';
				js['CodIvr'] = '<%=CodIvr%>'; //da moficare con codice variabile				
				console.debug("[openSocket] -> sent " + JSON.stringify(js));
				webSocket.send(JSON.stringify(js));
			};
			
			webSocket.onmessage = function(event) {
				 console.debug("[onmessage] <- " + event.data);
				var jData = JSON.parse(event.data);
				var rn ="";
				var v=""
				try {
					statArray[jData["ReportName"]] = jData;
					rn = jData["ReportName"];
					v = jData["Value"];
					// console.debug("document.getElementById(" + rn + ").innerHTML = "+ v);
					document.getElementById(rn).innerHTML = v;
					if (isFunction(document.getElementById(rn).onchange))
							document.getElementById(rn).onchange();
					
				} catch (e) {
					console.warn("[onmessage] - error on received message " + e);
					try {
						sendError("document.getElementById(" + rn + ").innerHTML = " +v+"; -> " + e);
					} catch (e) {
						alert(e);
					}
				}

			}
			webSocket.onclose = function(event) {
				console.warn("[onclose] <- closed");
				//alert("Connection closed, server: " + location.host);
			};
			
		}
		sendError = function(error){
			var js = {};
			js['command'] = 'error';
			js['CodIvr'] = '<%=CodIvr%>'; //da moficare con codice variabile	
			js['msg'] =  error;
			webSocket.send(JSON.stringify(js));
		}
		
	</script>

	<table class="center">
		<tr>
			<td style="width: 10%"></td>
			<td colspan="2">

				<table id="stickytable" class="roundedCorners small">
					<thead>
						<tr class="listGroupItem active blue ">
							<th colspan="1">
								<table style="width: 100%">
									<tr>
										<td>
											<div class="title">Agent Details</div>
										</td>
										<td valign="top">
											<div class="container_img blue title right" onclick="confirmchange_filter('<%=sFilterDay%>')">
												<img alt="" class="" src="../images/PointWhite_.png">
											</div>
										</td>
									</tr>
								</table>
							</th>
						</tr>
					</thead>
					<tr>
						<td>

							<table style="width: 100%">
								<tr>
									<th colspan="2" width="36%">Descrizione</th>
									<th width="15%">Totale</th>
									<th width="16%">Linetech</th>
									<th width="16%">koine</th>
									<th width="16%">Vk</th>
								</tr>
							</table>
							<table style="width: 100%">
								<tr>
									<td rowspan="5" width="20%">AgentStatus</td>
									<td width="16%">ready</td>
									<td width="16%" id="ReadyTotal"></td>
									<td width="16%" id="ReadyLinetech"></td>
									<td width="16%" id="ReadyKoine"></td>
									<td width="16%" id="ReadyVk"></td>
								</tr>
								<tr>
									<td id="">conv</td>
									<td width="16%" id="ConvTotal"></td>
									<td width="16%" id="ConvLinetech"></td>
									<td width="16%" id="ConvKoine"></td>
									<td width="16%" id="ConvVk"></td>
								</tr>
								<tr>
									<td>acw</td>
									<td width="16%" id="ACWTotal"></td>
									<td width="16%" id="ACWLinetech"></td>
									<td width="16%" id="ACWKoine"></td>
									<td width="16%" id="ACWVk"></td>
								</tr>
								<tr>
									<td>pausa</td>
									<td width="16%" id="NotReadyTotal"></td>
									<td width="16%" id="NotReadyLinetech"></td>
									<td width="16%" id="NotReadyKoine"></td>
									<td width="16%" id="NotReadyVk"></td>
								</tr>
								<tr>
									<td>ready/loggati</td>
									<td width="16%" id="ReadyLoggedTotal"></td>
									<td width="16%" id="ReadyLoggedLinetech"></td>
									<td width="16%" id="ReadyLoggedKoine"></td>
									<td width="16%" id="ReadyLoggedVk"></td>
								</tr>


								<tr>
									<td rowspan="3">traffico day</td>
									<td onclick="changeThreshold('TrafficDailyTot','TrafficDailyLinetech','TrafficDailyKoine','TrafficDailyVk')">%<img src="../images/Point_.png" id="" class="right" style="width: 10px; height: 10px; display: inline; right: 0px">  </td>
									<td width="16%" class="threshold" id="TrafficDailyPercentageTot"></td>
									<td width="16%" class="threshold" id="TrafficDailyPercentageLinetech"></td>
									<td width="16%" class="threshold" id="TrafficDailyPercentageKoine"></td>
									<td width="16%" class="threshold" id="TrafficDailyPercentageVk"></td>
								</tr>
								<tr>
									<td>Current</td>
									<td width="16%" id="TrafficDailyTot" onchange="TrafficDailyTotChange(this); "></td>
									<td width="16%" id="TrafficDailyLinetech" onchange="TrafficDailyLinetechChange(this);"></td>
									<td width="16%" id="TrafficDailyKoine" onchange="TrafficDailyKoineChange(this);"></td>
									<td width="16%" id="TrafficDailyVk" onchange="TrafficDailyVkChange(this); "></td>
								</tr>
								<tr>
									<td>Forecast</td>
									<td width="16%" id="TrafficDailyForecastTot"><%=TrafficDailyForecastTot%></td>
									<td width="16%" id="TrafficDailyForecastLinetech"><%=TrafficDailyForecastLinetech%></td>
									<td width="16%" id="TrafficDailyForecastKoine"><%=TrafficDailyForecastKoine%></td>
									<td width="16%" id="TrafficDailyForecastVk"><%=TrafficDailyForecastVk%></td>
								</tr>


								<tr>
									<td rowspan="2">LDS</td>
									<td onclick="changeThreshold('LDSTot','LDSLinetech','LDSKoine','LDSVk')">Day<img src="../images/Point_.png" id="" class="right" style="width: 10px; height: 10px; display: inline; right: 0px">  </td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="LDSIconTot" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="LDSTot" onchange="LDSTotChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/yellow.png" id="LDSIconLinetech" style="width: 20px; height: 23px">
												</td>
												<td>
													<div style="border: 0px solid grey" id="LDSLinetech" onchange="LDSLinetechChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="LDSIconKoine" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="LDSKoine" onchange="LDSKoineChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="LDSIconVk" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="LDSVk" onchange="LDSVkChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
								</tr>
								<tr>
									<td  onclick="changeThreshold('LDSQuarterTot','LDSQuarterLinetech','LDSQuarterKoine','LDSQuarterVk')">15'<img src="../images/Point_.png" id="" class="right" style="width: 10px; height: 10px; display: inline; right: 0px">  </td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="LDSQuarterIconTot" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="LDSQuarterTot" onchange="LDSQuarterTotChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/red.png" id="LDSQuarterIconLinetech"  style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="LDSQuarterLinetech" onchange="LDSQuarterLinetechChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="LDSQuarterIconKoine" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="LDSQuarterKoine" onchange="LDSQuarterKoineChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="LDSQuarterIconVk" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="LDSQuarterVk" onchange="LDSQuarterVkChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
								</tr>
								<tr>
									<td rowspan="2">TMA</td>
									<td onclick="changeThreshold('TMATot','TMALinetech','TMAKoine','TMAVk')">day<img src="../images/Point_.png" id="" class="right" style="width: 10px; height: 10px; display: inline; right: 0px">  </td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMAIconTot" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="TMATot" onchange="TMATotChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMAIconLinetech" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="TMALinetech" onchange="TMALinetechChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMAIconKoine" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="TMAKoine" onchange="TMAKoineChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMAIconVk" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="TMAVk" onchange="TMAVkChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
								</tr>
								<tr>
									<td onclick="changeThreshold('TMAQuarterTot','TMAQuarterLinetech','TMAQuarterKoine','TMAQuarterVk')">15'<img src="../images/Point_.png" id="" class="right" style="width: 10px; height: 10px; display: inline; right: 0px">  </td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMAQuarterIconTot" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="TMAQuarterTot" onchange="TMAQuarterTotChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMAQuarterIconLinetech"  style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="TMAQuarterLinetech" onchange="TMAQuarterLinetechChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMAQuarterIconKoine" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="TMAQuarterKoine" onchange="TMAQuarterKoineChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMAQuarterIconVk" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="TMAQuarterVk" onchange="TMAQuarterVkChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
								</tr>


								<tr>
									<td rowspan="2">TMC</td>
									<td onclick="changeThreshold('TMCTot','TMCLinetech','TMCKoine','TMCVk')">day<img src="../images/Point_.png" id="" class="right" style="width: 10px; height: 10px; display: inline; right: 0px">  </td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMCIconTot" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													<div style="border: 0px solid grey;" id="TMCTot" onchange="TMCTotChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMCIconLinetech" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>

													<div style="border: 0px solid grey;" id="TMCLinetech" onchange="TMCLinetechChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMCIconKoine" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>

													<div style="border: 0px solid grey;" id="TMCKoine" onchange="TMCKoineChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMCIconVk" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>

													<div style="border: 0px solid grey;" id="TMCVk" onchange="TMCVkChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
								</tr>
								<tr>
									<td onclick="changeThreshold('TMCQuarterTot','TMCQuarterLinetech','TMCQuarterKoine','TMCQuarterVk')">15'<img src="../images/Point_.png" id="" class="right" style="width: 10px; height: 10px; display: inline; right: 0px">  </td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMCQuarterIconTot" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>

													<div style="border: 0px solid grey;" id="TMCQuarterTot" onchange="TMCQuarterTotChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMCQuarterIconLinetech" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>

													<div style="border: 0px solid grey;" id="TMCQuarterLinetech" onchange="TMCQuarterLinetechChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMCQuarterIconKoine" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>

													<div style="border: 0px solid grey;" id="TMCQuarterKoine" onchange="TMCQuarterKoineChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="TMCQuarterIconVk" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>

													<div style="border: 0px solid grey;" id="TMCQuarterVk" onchange="TMCQuarterVkChange(this);"></div>
												</td>
											</tr>
										</table>
									</td>
								</tr>


								<tr>
									<td rowspan="2" style="border-bottom: 0px solid gray;">% distribuzione chiamate</td>
									<td onclick="changeThreshold('DistributedTot','DistributedLinetech','DistributedKoine','DistributedVk')">day<img src="../images/Point_.png" id="" class="right" style="width: 10px; height: 10px; display: inline; right: 0px">  </td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="DistributedIconTot" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
												
													<div style="border: 0px solid grey;" id="DistributedTot" onchange="DistributedTotChange(this);    "></div>
													
												</td>

											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="DistributedIconLinetech" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													
													<div style="border: 0px solid grey;" id="DistributedLinetech" onchange="DistributedLinetechChange(this);"></div>
													
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png"  id="DistributedIconKoine" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
												
													<div style="border: 0px solid grey;" id="DistributedKoine" onchange="DistributedKoineChange(this);"></div>
													
												</td>
											</tr>
										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png"  id="DistributedIconVk" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													
													<div style="border: 0px solid grey;" id="DistributedVk" onchange="DistributedVkChange(this);"></div>
													
												</td>
											</tr>
										</table>
									</td>
								</tr>
								<tr>
									<td onclick="changeThreshold('DistributedQuarterTot','DistributedQuarterLinetech','DistributedQuarterKoine','DistributedQuarterVk')">15'<img src="../images/Point_.png" id="" class="right" style="width: 10px; height: 10px; display: inline; right: 0px">  </td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png"  id="DistributedQuarterIconTot"  style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													
													<div style="border: 0px solid grey;" id="DistributedQuarterTot" onchange="DistributedQuarterTotChange(this);"></div>
													
												</td>
											</tr>

										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png"  id="DistributedQuarterIconLinetech" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													
													<div style="border: 0px solid grey;" id="DistributedQuarterLinetech" onchange="DistributedQuarterLinetechChange(this);"></div>
													
												</td>
											</tr>

										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png" id="DistributedQuarterIconKoine" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
												
													<div style="border: 0px solid grey;" id="DistributedQuarterKoine" onchange="DistributedQuarterKoineChange(this);"></div>
													
												</td>
											</tr>

										</table>
									</td>
									<td>
										<table>
											<tr>
												<td>
													<img src="../images/green.png"  id="DistributedQuarterIconVk" style="width: 20px; height: 23px; display: inline">
												</td>
												<td>
													
													<div style="border: 0px solid grey;" id="DistributedQuarterVk" onchange="DistributedQuarterVkChange(this);"></div>
													
												</td>
											</tr>
										</table>
									</td>
								</tr>

							</table>

						</td>
					</tr>
				</table>
			</td>

			<td style="width: 10%"></td>
		</tr>
	</table>
	<%
		}
		} catch (Exception e) {
			log.error(session.getId() + " - general: " + e.getMessage(), e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {
			}
			try {
				cstmt.close();
			} catch (Exception e) {
			}
			try {
				conn.close();
			} catch (Exception e) {
			}
		}
	%>
<div id="change_filter" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td><h2>Cambia Data di Visualizzazione</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#change_filter').hide();">
							<img alt="" class="" src="../images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_change_filter" action="AgentDetails.jsp" method="post">
			<input type="hidden" id="chAction" name="action" Value="change_filter">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="change_filter_day">Giorno</label></td>
					<td><input type="date" id="change_filter_day" name="change_filter_day"></td>
					<td><input type="time" id="change_filter_time" name="change_filter_time" value="<%=sFilterTime%>"></td>
					<td><button type="button" class="buttonsmall blue" onclick="$('#change_filter_day').val('<%=sdf_full_day_of_year.format(filterToday)%>'); $('#change_filter_time').val('<%=sdf_time.format(filterToday)%>')">Oggi</button></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="button" id="CloseModal" class="button blue" onclick="checkDate()  ">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="change_Threshold" class="modal">
	<div class="modal-content"  style="width: 830px">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td><h2>Cambia Soglia</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#change_Threshold').hide();">
							<img alt="" class="" src="../images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_change_filter" action="AgentDetails.jsp" method="post">
			<input type="hidden" id="chAction" name="action" Value="change_Threshold">
			<input type="hidden" id="chCodIvr" name="CodIvr" Value="<%=CodIvr%>">
			<input type="hidden" id="chChangeThresholdId" name="ChangeThresholdId" Value="">			
			<input type="hidden" id="change1_Threshold_3" name="change1_Threshold_3">
			<input type="hidden" id="change2_Threshold_3" name="change2_Threshold_3">
			<input type="hidden" id="change3_Threshold_3" name="change3_Threshold_3">
			<input type="hidden" id="change4_Threshold_3" name="change4_Threshold_3">
			<input type="hidden" id="change1_Threshold_4" name="change1_Threshold_4">
			<input type="hidden" id="change2_Threshold_4" name="change2_Threshold_4">
			<input type="hidden" id="change3_Threshold_4" name="change3_Threshold_4">
			<input type="hidden" id="change4_Threshold_4" name="change4_Threshold_4">				
			<table>
				<tr><td><input type="text" id="change1_Threshold_name" name="change1_Threshold_name"></td> <td><input type="text" id="change2_Threshold_name" name="change2_Threshold_name"></td><td><input type="text" id="change3_Threshold_name" name="change3_Threshold_name"></td> <td><input type="text" id="change4_Threshold_name" name="change4_Threshold_name"></td> </tr>			
				<tr><td><input type="text" id="change1_Threshold_op" name="change1_Threshold_op" ></td> <td><input type="text" id="change2_Threshold_op" name="change2_Threshold_op"></td><td><input type="text" id="change3_Threshold_op" name="change3_Threshold_op"></td> <td><input type="text" id="change4_Threshold_op" name="change4_Threshold_op"></td> </tr>
				<tr><td><input type="text" id="change1_Threshold_1" name="change1_Threshold_1"></td> <td><input type="text" id="change2_Threshold_1" name="change2_Threshold_1"></td><td><input type="text" id="change3_Threshold_1" name="change3_Threshold_1"></td> <td><input type="text" id="change4_Threshold_1" name="change4_Threshold_1"></td> </tr>
				<tr><td><input type="text" id="change1_Threshold_2" name="change1_Threshold_2"></td> <td><input type="text" id="change2_Threshold_2" name="change2_Threshold_2"></td><td><input type="text" id="change3_Threshold_2" name="change3_Threshold_2"></td> <td><input type="text" id="change4_Threshold_2" name="change4_Threshold_2"></td> </tr>
				
			
				<tr>
					<td colspan="2">
						<button type="submit" id="CloseModal" class="button blue" >Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

	<script type="text/javascript">
	
	function changeThreshold(name1,name2,name3, name4){		
		var t = statArray[name1].Threshold;
		document.getElementById('change1_Threshold_name').value=name1;
		document.getElementById('change1_Threshold_op').value = statArray[name1].ThresholdOp;
	
		try {
			document.getElementById('change1_Threshold_1').value = t[0];
		} catch (e) {}
		try {
			document.getElementById('change1_Threshold_2').value = t[1];
		} catch (e) {}
		try {
			document.getElementById('change1_Threshold_3').value = t[2];
		} catch (e) {}
		try {
			document.getElementById('change1_Threshold_4').value = t[3];
		} catch (e) {}		
		var t = statArray[name2].Threshold;
		document.getElementById('change2_Threshold_name').value = name2;
		document.getElementById('change2_Threshold_op').value = statArray[name2].ThresholdOp;
		
		try {
			document.getElementById('change2_Threshold_1').value = t[0];
		} catch (e) {}
		try {
			document.getElementById('change2_Threshold_2').value = t[1];
		} catch (e) {}
		try {
			document.getElementById('change2_Threshold_3').value = t[2];
		} catch (e) {}
		try {
			document.getElementById('change2_Threshold_4').value = t[3];
		} catch (e) {}
		var t = statArray[name3].Threshold;
		document.getElementById('change3_Threshold_name').value = name3;
		document.getElementById('change3_Threshold_op').value = statArray[name3].ThresholdOp;
		
		try {
			document.getElementById('change3_Threshold_1').value = t[0];
		} catch (e) {}
		try {
			document.getElementById('change3_Threshold_2').value = t[1];
		} catch (e) {}
		try {
			document.getElementById('change3_Threshold_3').value = t[2];
		} catch (e) {}
		try {
			document.getElementById('change3_Threshold_4').value = t[3];
		} catch (e) {}
		
		var t = statArray[name4].Threshold;
		document.getElementById('change4_Threshold_op').value = statArray[name4].ThresholdOp;
		document.getElementById('change4_Threshold_name').value = name4;
		try {
			document.getElementById('change4_Threshold_1').value = t[0];
		} catch (e) {}
		try {
			document.getElementById('change4_Threshold_2').value = t[1];
		} catch (e) {}
		try {
			document.getElementById('change4_Threshold_3').value = t[2];
		} catch (e) {}
		try {
			document.getElementById('change4_Threshold_4').value = t[3];
		} catch (e) {}
		$("#change_Threshold").show();		
	}
	
		function checkDate(){
		
			 var startDate = new Date(document.getElementById('change_filter_day').value +" "+ document.getElementById('change_filter_time').value);
			 var n = startDate.getMinutes();
			 var skip  = n%15;
			 n = n + 15-skip;
			 startDate.setMinutes(n);
			 var today = new Date();
			// console.debug(startDate.getTime() +"<"+ today.getTime())
			 
			 if (startDate.getTime() < today.getTime()) {
				 
				 // console.warn(startDate.getTime() < today.getTime());
				  document.getElementById('form_change_filter').action="AgentDetailsHistory.jsp";
				  document.getElementById('form_change_filter').submit(); 
			 }else{
				 document.getElementById('form_change_filter').action="AgentDetails.jsp";
				 document.getElementById('form_change_filter').submit(); 
			 }
		}
		$(function() {
			try {
				parent.ChangeActiveMenu("#MenuDetails");
				parent.ChangeActivedFooter("#Agents", "rt/AgentDetails.jsp");
	<%if (!StringUtils.isBlank(CodIvr)) {%>
		parent.EnabledFooterService();
				parent.ChangeActivedFooterService();
	<%}%>
		} catch (e) {
			}
		})

		function evalThreshold(name, result){
			try {
				var t = statArray[name].Threshold;
				i = t.length - 1;
				var ev;
				
				for (; i >= 0; i--) {				
					try {
						if (t[i]==null){
							//console.debug(name + " - threshold: " + t[i]);
							continue;
						}
						ev = result +  statArray[name].ThresholdOp +  t[i];
						// console.info(name + " - threshold: " + ev + " eval(ev):"+eval(ev));					
						if ( eval(ev))
							break;
					} catch ( e) {
						console.warn(name + " - error on threshold eval (" + ev + "): " + e);
					}
				}
// 				if (i>=0){
// 					console.info(name + "threshold -->" +ev+" threshold: "+i+" ")
// 				}
				return i;
			} catch (e) {
				console.warn(" evalThreshold:"+name + " " +e);
			}
			return -1;
		}
		
		function renderColor(name, th){
			//document.getElementById(name).innerHTML= parseInt(_this.innerHTML)/forecast;	
			switch (th) {
				case 0:		document.getElementById(name).classList.add('yellowthr');
							document.getElementById(name).classList.remove('redthr');
							document.getElementById(name).classList.remove('greenthr');
					break;
				case 1:		document.getElementById(name).classList.remove('yellowthr');
							document.getElementById(name).classList.add('redthr');
							document.getElementById(name).classList.remove('greenthr');
					break;
				default:	document.getElementById(name).classList.remove('yellowthr');
							document.getElementById(name).classList.remove('redthr');
							document.getElementById(name).classList.add('greenthr');
					break;
			}
		}

		function renderIcon(name, th){
			//document.getElementById(name).innerHTML= parseInt(_this.innerHTML)/forecast;	
			try {
				switch (th){
					case 0:		document.getElementById(name).src = "../images/yellow.png";
						break;
					case 1:		document.getElementById(name).src="../images/red.png";
						break;
					default:	document.getElementById(name).src="../images/green.png";
						break;
				}
			} catch (e) {
				console.warn(" renderIcon:"+name + " th "+th+" "+e);
			}
		}

		function render(_this,name){
			// console.debug(" -  statArray["+name+"]= " +  JSON.stringify(statArray[name]));
			var result =  parseFloat(_this.innerHTML);  
			var th = evalThreshold(name, result)
			// console.debug(" result:"+result + " th "+th);
			return th;
		}
		
		function 	TrafficDailyTotChange(_this) {			
			// console.debug(" -  statArray[TrafficDailyTot]= " +  JSON.stringify(statArray["TrafficDailyTot"]));
			var result =  parseFloat(_this.innerHTML)/<%=TrafficDailyForecastTot%>;
			result = (Math.round(result * 1000) / 10) ;
			document.getElementById('TrafficDailyPercentageTot').innerHTML=result
			var th = evalThreshold("TrafficDailyTot", result)
			renderColor('TrafficDailyPercentageTot', th);
		}
		
		
		function 	TrafficDailyLinetechChange(_this){
			// console.debug(" -  statArray[TrafficDailyLinetech]= " +  JSON.stringify(statArray["TrafficDailyLinetech"]));
			var result =  parseFloat(_this.innerHTML)/<%=TrafficDailyForecastLinetech%>;
			result = (Math.round(result * 1000) / 10 );
			document.getElementById('TrafficDailyPercentageLinetech').innerHTML=result
			var th = evalThreshold("TrafficDailyLinetech", result)
			renderColor('TrafficDailyPercentageLinetech', th);
		}
		
		function 	 TrafficDailyKoineChange(_this){
			// console.debug(" -  statArray[TrafficDailyKoine]= " +  JSON.stringify(statArray["TrafficDailyKoine"]));
			var result =  parseFloat(_this.innerHTML)/<%=TrafficDailyForecastKoine%>;
			result = (Math.round(result * 1000) / 10 );
			document.getElementById('TrafficDailyPercentageKoine').innerHTML=result
			var th = evalThreshold("TrafficDailyKoine", result)
			renderColor('TrafficDailyPercentageKoine', th);
		}
		
		function 	TrafficDailyVkChange(_this){
			// console.debug(" -  statArray[TrafficDailyVk]= " +  JSON.stringify(statArray["TrafficDailyVk"]));
			var result =  parseFloat(_this.innerHTML)/<%=TrafficDailyForecastVk%>;
			result = (Math.round(result * 1000) / 10);
			document.getElementById('TrafficDailyPercentageVk').innerHTML=result
			var th = evalThreshold("TrafficDailyVk", result)
			renderColor('TrafficDailyPercentageVk', th);
		}
			
		function LDSTotChange(_this){					
			renderIcon('LDSIconTot', render(_this,"LDSTot"));				
		}
		function LDSLinetechChange(_this){	
			renderIcon('LDSIconLinetech', render(_this,"LDSLinetech"));	
		}
		function LDSKoineChange(_this){
			renderIcon('LDSIconKoine', render(_this,"LDSKoine"));	
		}
		function LDSVkChange(_this){
			renderIcon('LDSIconVk', render(_this,"LDSVk"));	
		}		
		function LDSQuarterTotChange(_this){
			renderIcon('LDSQuarterIconTot', render(_this,"LDSQuarterTot"));	
		}
		function LDSQuarterLinetechChange(_this){
			renderIcon('LDSQuarterIconLinetech', render(_this,"LDSQuarterLinetech"));	
		}
		function LDSQuarterKoineChange(_this){
			renderIcon('LDSQuarterIconKoine', render(_this,"LDSQuarterKoine"));	
		}
		function LDSQuarterVkChange(_this){
			renderIcon('LDSQuarterIconVk', render(_this,"LDSQuarterVk"));	
		}
		function TMATotChange(_this){
			renderIcon('TMAIconTot', render(_this,"TMATot"));	
		}
		function TMALinetechChange(_this){
			renderIcon('TMAIconLinetech', render(_this,"TMALinetech"));	
		}
		function TMAKoineChange(_this){
			renderIcon('TMAIconKoine', render(_this,"TMAKoine"));	
		}
		function TMAVkChange(_this){
			renderIcon('TMAIconVk', render(_this,"TMAVk"));	
		}
		function TMAQuarterTotChange(_this){
			renderIcon('TMAQuarterIconTot', render(_this,"TMAQuarterTot"));	
		}
		function TMAQuarterLinetechChange(_this){
			renderIcon('TMAQuarterIconLinetech', render(_this,"TMAQuarterLinetech"));	
		}
		
		function TMAQuarterKoineChange(_this){
			renderIcon('TMAQuarterIconKoine', render(_this,"TMAQuarterKoine"));	
		}
		function TMAQuarterVkChange(_this){
			renderIcon('TMAQuarterIconVk', render(_this,"TMAQuarterVk"));	
		}
		function TMCTotChange(_this){
			renderIcon('TMCIconTot', render(_this,"TMCTot"));	
		}
		function TMCLinetechChange(_this){
			renderIcon('TMCIconLinetech', render(_this,"TMCLinetech"));	
		}
		function TMCKoineChange(_this){
			renderIcon('TMCIconKoine', render(_this,"TMCKoine"));	
		}
		function TMCVkChange(_this){
			renderIcon('TMCIconVk', render(_this,"TMCVk"));	
		}
		function TMCQuarterTotChange(_this){
			renderIcon('TMCQuarterIconTot', render(_this,"TMCQuarterTot"));	
		}
		function TMCQuarterLinetechChange(_this){
			renderIcon('TMCQuarterIconLinetech', render(_this,"TMCQuarterLinetech"));	
		}
		function TMCQuarterKoineChange(_this){
			renderIcon('TMCQuarterIconKoine', render(_this,"TMCQuarterKoine"));	
		}
		function TMCQuarterVkChange(_this){
			renderIcon('TMCQuarterIconVk', render(_this,"TMCQuarterVk"));	
		}
		
		function DistributedTotChange(_this){
			try {
				// console.warn(" -  statArray[DistributedTot]= " +  JSON.stringify(statArray["DistributedTot"]));
				var result =  parseFloat(_this.innerHTML)/<%=DistributedForecastTot%>;  
				result = (Math.round(result * 1000) / 10 );
				document.getElementById('DistributedTot').innerHTML=result
				var th = evalThreshold("DistributedTot", result)
				// console.warn(" -  th= " +  th);
				renderIcon('DistributedIconTot', th);
			} catch (e) {
				console.error(" -  statArray[DistributedTot]= " +  e);
			}
		}
		
		function DistributedLinetechChange(_this){
			try{
				// console.debug(" -  statArray[DistributedLinetech]= " +  JSON.stringify(statArray["DistributedLinetech"]));
				var result =  parseFloat(_this.innerHTML)/<%=DistributedForecastLinetech%>; 
				result = (Math.round(result * 1000) / 10 );
				document.getElementById('DistributedLinetech').innerHTML=result
				var th = evalThreshold("DistributedLinetech", result)		
				renderIcon('DistributedIconLinetech', th);	
			} catch (e) {
				console.error(" -  statArray[DistributedLinetech]= " +  e);
			}
		}
		
		function DistributedKoineChange(_this){			
			try{
				// console.debug(" -  statArray[DistributedKoine]= " +  JSON.stringify(statArray["DistributedKoine"]));
				var result =  parseFloat(_this.innerHTML)/<%=DistributedForecastKoine%>;
				result = (Math.round(result * 1000) / 10 );
				document.getElementById('DistributedKoine').innerHTML=result
				var th = evalThreshold("DistributedKoine", result)		
				renderIcon('DistributedIconKoine', th);	
			} catch (e) {
				console.error(" -  statArray[DistributedKoine]= " +  e);
			}
		}
		
		function DistributedVkChange(_this){
			try{
				// console.debug(" -  statArray[DistributedVk]= " +  JSON.stringify(statArray["DistributedVk"]));
				var result =  parseFloat(_this.innerHTML)/<%=DistributedForecastVk%>;
				result = (Math.round(result * 1000) / 10 );
				document.getElementById('DistributedVk').innerHTML=result
				var th = evalThreshold("DistributedVk", result)		
				renderIcon('DistributedIconVk', th);	
			} catch (e) {
				console.error(" -  statArray[DistributedVk]= " +  e);
			}
		}
		
		function DistributedQuarterTotChange(_this){
			try{
				// console.debug(" -  statArray[DistributedQuarterTot]= " +  JSON.stringify(statArray["DistributedQuarterTot"]));
				var result =  parseFloat(_this.innerHTML)/<%=DistributedQuarterForecastTot%>;
				result = (Math.round(result * 1000) / 10 );
				document.getElementById('DistributedQuarterTot').innerHTML=result
				var th = evalThreshold("DistributedQuarterTot", result)		
				renderIcon('DistributedQuarterIconTot', th);	
			} catch (e) {
				console.error(" -  statArray[DistributedQuarterTot]= " +  e);
			}
		}
		
		function DistributedQuarterLinetechChange(_this){
			try{
				// console.debug(" -  statArray[DistributedQuarterLinetech]= " +  JSON.stringify(statArray["DistributedQuarterLinetech"]));
				var result =  parseFloat(_this.innerHTML)/<%=DistributedQuarterForecastTot%>;
				result = (Math.round(result * 1000) / 10 );
				document.getElementById('DistributedQuarterLinetech').innerHTML=result
				var th = evalThreshold("DistributedQuarterLinetech", result)		
				renderIcon('DistributedQuarterIconLinetech', th);	
			} catch (e) {
				console.error(" -  statArray[DistributedQuarterLinetech]= " +  e);
			}
		}
		
		function DistributedQuarterKoineChange(_this){
			try{
				// console.debug(" -  statArray[DistributedQuarterKoine]= " +  JSON.stringify(statArray["DistributedQuarterKoine"]));
				var result =  parseFloat(_this.innerHTML)/<%=DistributedQuarterForecastKoine%>;
				result = (Math.round(result * 1000) / 10 );
				document.getElementById('DistributedQuarterKoine').innerHTML=result
				var th = evalThreshold("DistributedQuarterKoine", result)		
				renderIcon('DistributedQuarterIconKoine', th);	
			} catch (e) {
				console.error(" -  statArray[DistributedQuarterKoine]= " +  e);
			}
		}
		
		function DistributedQuarterVkChange(_this){
			try{
				// console.debug(" -  statArray[DistributedQuarterVk]= " +  JSON.stringify(statArray["DistributedQuarterVk"]));
				var result =  parseFloat(_this.innerHTML)/<%=DistributedQuarterForecastVk%>;
				result = (Math.round(result * 1000) / 10 );
				document.getElementById('DistributedQuarterVk').innerHTML=result
				var th = evalThreshold("DistributedQuarterVk", result)		
				renderIcon('DistributedQuarterIconVk', th);	
			} catch (e) {
				console.error(" -  statArray[DistributedQuarterVk]= " +  e);
			}
		}
		
		confirmchange_filter = function(id) {
			$("#change_filter").show();
			$("#change_filter_day").val(id);
		}
		
		
		openSocket();
	</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>