<!DOCTYPE html>
<%@page import="java.util.Iterator"%>
<%@page import="org.json.JSONArray"%>
<%@page import="comapp.ConfigurationUtility"%>
<%@page import="org.json.JSONObject"%>
<%@page import="org.apache.commons.lang3.StringEscapeUtils"%>
<%@page import="comapp.Utility"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DissuasionCCT</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/getBrowser.js"></script>
</head>
<body style="overflow-y: auto;">
<div class="bottom-right">
	<table><tr>
		<td class=""><img style="height: 20px" src='<%=ConfigServlet.getProperties().getProperty("Logo1")%>'></td>
		<td class="" style="float: right; "><img style="height: 20px" src='<%=ConfigServlet.getProperties().getProperty("Logo2")%>'></td>
	</tr></table>
</div>
<%
	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");
	//==	REQUEST PARAMETERS	====
	String CodIvr = request.getParameter("CodIvr");
	if (StringUtils.isBlank(CodIvr)) {
		CodIvr = (String) session.getAttribute("CodIvr");
	} else {
		session.setAttribute("CodIvr", CodIvr);
	}
	//==============================
	DataSource ds = null;
	Connection conn = null;
	Connection connCnf = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String p_MsgCodNodo = request.getParameter("MsgCodNodo");
	String p_MsgStatus = request.getParameter("MsgStatus");
	p_MsgStatus = p_MsgStatus==null?"OFF":p_MsgStatus.toUpperCase();
	String p_HngCodNodo = request.getParameter("HngCodNodo");
	String p_HngStatus = request.getParameter("HngStatus");
	p_HngStatus = p_HngStatus==null?"OFF":p_HngStatus.toUpperCase();
	String p_IdLimiteON = request.getParameter("IdLimiteON");
	String p_TransactionON = request.getParameter("TransactionON");
	String p_LimiteON = request.getParameter("LimiteON");
	String p_IdLimiteOFF = request.getParameter("IdLimiteOFF");
	String p_TransactionOFF = request.getParameter("TransactionOFF");
	String p_LimiteOFF = request.getParameter("LimiteOFF");
	String p_CodNodo = request.getParameter("CodNodo");
	String p_CodMessaggio = request.getParameter("CodMessaggio");
// 	String p_TestoTts = request.getParameter("TestoTts");
// 	if (StringUtils.isBlank(p_TestoTts)) p_TestoTts = "";
	String environment = (String)session.getAttribute("Environment");
	String dbsystemproperties = (String)session.getAttribute("DBSystemProperties");
	//==============================
	//==	ACTION START	====
 	log.info(session.getId() + " - action:"+ action );			
	try {
		Context ctx = new InitialContext();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTF");
		log.info(session.getId() + " - connection CCTF wait...");
		conn = ds.getConnection();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CNF");
		log.info(session.getId() + " - connection CNF wait...");
		connCnf = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "SAVE" :
				if (StringUtils.isNoneBlank(p_MsgCodNodo)) {
	 				log.info(session.getId() + " - dashboard.Dissuasion_Update('" + CodIvr + "','"+ "DISSUASIONE_MSG" + "','" + p_MsgStatus + "')");
					cstmt = conn.prepareCall("{ call dashboard.Dissuasion_Update(?,?,?)} ");
					cstmt.setInt(1, Integer.parseInt(CodIvr));
					cstmt.setString(2, "DISSUASIONE_MSG");
					cstmt.setString(3, p_MsgStatus);
					cstmt.execute();
					log.debug(session.getId() + " - executeCall complete");
				}
				try { cstmt.close(); } catch (Exception e) {}
				if (StringUtils.isNoneBlank(p_HngCodNodo)) {
					log.info(session.getId() + " - dashboard.Dissuasion_Update('" + CodIvr + "','"+ "DISSUASIONE_HNG" + "','" + p_HngStatus + "')");
					cstmt = conn.prepareCall("{ call dashboard.Dissuasion_Update(?,?,?)} ");
					cstmt.setInt(1, Integer.parseInt(CodIvr));
					cstmt.setString(2, "DISSUASIONE_HNG");
					cstmt.setString(3, p_HngStatus);
					cstmt.execute();					
					log.debug(session.getId() + " - executeCall complete");
				}
				try { cstmt.close(); } catch (Exception e) {}
				if (StringUtils.isNoneBlank(p_IdLimiteON) || StringUtils.isNoneBlank(p_IdLimiteOFF)) {
					String transactionName = "";
					transactionName = (StringUtils.isNoneBlank(p_TransactionON))?p_TransactionON:p_TransactionOFF;
					JSONObject jo = ConfigurationUtility.getTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), transactionName);
					log.info(session.getId() + " - "+transactionName+" -> "+jo.toString());
					Iterator<String> jokeys = jo.keys();
					while(jokeys.hasNext()) {
					    String key = jokeys.next();
					    JSONArray ja = jo.getJSONArray(key);
					    for (int ija=0; ija<ja.length();ija++) {
							JSONObject jajo = ja.getJSONObject(ija);
							Iterator<String> jajokeys = jajo.keys();
							while(jajokeys.hasNext()) {
							    String jkey = jajokeys.next();
							    String jvalue = jajo.getString(jkey);
								if (StringUtils.isNoneBlank(p_IdLimiteON)) {
									if (jkey.equals("DISSUASIONE_LIMITE_ON")) {
			 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_LimiteON);
										jajo.put(jkey, p_LimiteON);
									}
								}
								if (StringUtils.isNoneBlank(p_IdLimiteOFF)) {
									if (jkey.equals("DISSUASIONE_LIMITE_OFF")) {
			 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_LimiteOFF);
										jajo.put(jkey, p_LimiteOFF);
									}
								}
							}
					    }
					}
					log.info(session.getId() + " - "+transactionName+" -> "+jo.toString());
					ConfigurationUtility.saveTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), transactionName, jo);
// 					if (StringUtils.isNoneBlank(p_IdLimiteON)) {
// 						log.info(session.getId() + " - dashboard.Dissuasion_UpdateThreshold('" + p_IdLimiteON + "','"+ p_LimiteON + "')");
// 						cstmt = connCnf.prepareCall("{ call dashboard.Dissuasion_UpdateThreshold(?,?)} ");
// 						cstmt.setInt(1, Integer.parseInt(p_IdLimiteON));
// 						cstmt.setString(2, p_LimiteON);
// 						cstmt.execute();					
// 						log.debug(session.getId() + " - executeCall complete");
// 					}
// 					try { cstmt.close(); } catch (Exception e) {}
// 					if (StringUtils.isNoneBlank(p_IdLimiteOFF)) {
// 						log.info(session.getId() + " - dashboard.Dissuasion_UpdateThreshold('" + p_IdLimiteOFF + "','"+ p_LimiteOFF + "')");
// 						cstmt = connCnf.prepareCall("{ call dashboard.Dissuasion_UpdateThreshold(?,?)} ");
// 						cstmt.setInt(1, Integer.parseInt(p_IdLimiteOFF));
// 						cstmt.setString(2, p_LimiteOFF);
// 						cstmt.execute();					
// 						log.debug(session.getId() + " - executeCall complete");
// 					}
				}
				break;
			case "MODIFYMSG" :
//				log.info(session.getId() + " - dashboard.Dissuasion_UpdateMsg('" + CodIvr + "','"+ p_CodNodo + "','" + p_CodMessaggio + "','" + p_TestoTts + "')");
// 				cstmt = conn.prepareCall("{ call dashboard.Dissuasion_UpdateMsg(?,?,?,?)} ");
				log.info(session.getId() + " - dashboard.Dissuasion_UpdateMsg('" + CodIvr + "','"+ p_CodNodo + "','" + p_CodMessaggio + "')");
				cstmt = conn.prepareCall("{ call dashboard.Dissuasion_UpdateMsg(?,?,?)} ");
				cstmt.setInt(1, Integer.parseInt(CodIvr));
				cstmt.setString(2, p_CodNodo);
				cstmt.setString(3, p_CodMessaggio);
// 				cstmt.setString(4, p_TestoTts);
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	}
	finally{
		try { cstmt.close(); } catch (Exception e) {}
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
		
	try {			
		if (StringUtils.isBlank(CodIvr)) {
		} else {
%>
<table class="center"  >
	<tr>
		<td style="width: 5%"></td>
		<td style="width: 35%">
			<form id="form_recall_1" action="DissuasionCCT.jsp" method="post">
				<input type="hidden" id="mAction" name="action" Value="SAVE"> 
				<table class="roundedCorners small" >
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan="7">
								<table>
									<tr>
										<td>
											<div class="title">Dissuasione</div>
										</td>
										<td>
											<div class="container title right">
												<label></label>										
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
<%
			boolean fPresenzaFlag = false;
			boolean fPresenzaSoglie = false;
			boolean MSG_Present = false;
			String MSG_stato = "";
			String MSG_cod_messaggio = "";
// 			String MSG_testo_tts = "";
			boolean HNG_Present = false;
			String HNG_stato = "";
			String HNG_cod_messaggio = "";
// 			String HNG_testo_tts = "";
			log.info(session.getId() + " - dashboard.Dissuasion_Get('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.Dissuasion_Get(?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			while (rs.next()) {
				String cod_nodo = rs.getString("cod_nodo");
				if (cod_nodo.equals("DISSUASIONE_MSG")) {
					fPresenzaFlag = true;
					MSG_Present = true;
					MSG_stato = Utility.getNotNull(rs.getString("stato"));
					MSG_cod_messaggio = Utility.getNotNull(rs.getString("cod_messaggio"));
// 					MSG_testo_tts = Utility.getNotNull(rs.getString("testo_tts"));
// 					MSG_testo_tts = StringEscapeUtils.escapeEcmaScript(MSG_testo_tts);
				} else if (cod_nodo.equals("DISSUASIONE_HNG")) {
					fPresenzaFlag = true;
					HNG_Present = true;
					HNG_stato = Utility.getNotNull(rs.getString("stato"));
					HNG_cod_messaggio = Utility.getNotNull(rs.getString("cod_messaggio"));
// 					HNG_testo_tts = Utility.getNotNull(rs.getString("testo_tts"));
// 					HNG_testo_tts = StringEscapeUtils.escapeEcmaScript(HNG_testo_tts);
				} 
// 				int MaxRecall = rs.getInt("MAXRECALL");
// 				int Interval = rs.getInt("INTERVAL");
			}
%>
					<tr style="width: 100%" class="listGroupItem" >
<%
			if (MSG_Present) {
%>
						<td>MESSAGGIO</td>
						<td>
							<label class="switch">
								<input type="checkbox" id="msgstatus" name="MsgStatus" <%=((MSG_stato.equalsIgnoreCase("ON")) ? "checked=true" : "")%>> 
								<span class="slider round blue"></span>
							</label>
							<input type="hidden" id="msgcodnodo" name="MsgCodNodo" Value="DISSUASIONE_MSG"> 
						</td>
						<td onclick="modifyMsg('DISSUASIONE_MSG','MESSAGGIO','<%=MSG_cod_messaggio%>')"><%=MSG_cod_messaggio%></td>
<%-- 						<td onclick="modifyMsg('DISSUASIONE_MSG','MESSAGGIO','<%=MSG_cod_messaggio%>','<%=MSG_testo_tts%>')"><%=MSG_cod_messaggio%></td> --%>
<%
			} else {
%>
						<td colspan="3">
							<div class="head_parameter_alert" style="text-align: center;">NODO MESSAGGIO DISSUASIONE NON PRESENTE</div>
						</td>
<%
			}
%>
					</tr>
					<tr style="width: 100%" class="listGroupItem" >
<%

			if (HNG_Present) {
%>
						<td>HANGUP</td>
						<td>
							<label class="switch">
								<input type="checkbox" id="hngstatus" name="HngStatus" <%=((HNG_stato.equalsIgnoreCase("ON")) ? "checked=true" : "")%>>
								<span class="slider round blue"></span>
							</label>
							<input type="hidden" id="hngcodnodo" name="HngCodNodo" Value="DISSUASIONE_HNG"> 
						</td>
						<td onclick="modifyMsg('DISSUASIONE_HNG','HANGUP','<%=HNG_cod_messaggio%>')"><%=HNG_cod_messaggio%></td>
<%-- 						<td onclick="modifyMsg('DISSUASIONE_HNG','HANGUP','<%=HNG_cod_messaggio%>','<%=HNG_testo_tts%>')"><%=HNG_cod_messaggio%></td> --%>
<%
			} else {
%>
						<td colspan="3">
							<div class="head_parameter_alert" style="text-align: center;">NODO HANGUP DISSUASIONE NON PRESENTE</div>
						</td>
<%
			}
%>
					</tr>
<%
			try { rs.close(); } catch (Exception e) {}
		 	try { cstmt.close(); } catch (Exception e) {}
		 	
			log.info(session.getId() + " - dashboard.Dissuasion_GetServiceThreshold('" + CodIvr + "')");
			cstmt = connCnf.prepareCall("{ call dashboard.Dissuasion_GetServiceThreshold(?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			while (rs.next()) {
				int dbid =  rs.getInt("dbid"); 
				String key = rs.getString("key");
				String value = rs.getString("value");
				String transactionname = rs.getString("transactionname");
				if (key.equals("DISSUASIONE_LIMITE_ON")) {
					fPresenzaSoglie = true;
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td>DISSUASIONE LIMITE ON</td>
						<td colspan="2">
							<input type="number" id="LimiteON" name="LimiteON" min="0" value="<%=value%>">
							<input type="hidden" id="IdLimiteON" name="IdLimiteON" Value="<%=dbid%>"> 
							<input type="hidden" id="TransactionON" name="TransactionON" Value="<%=transactionname%>"> 
						</td>
					</tr>
<%
				} else if (key.equals("DISSUASIONE_LIMITE_OFF")) {
					fPresenzaSoglie = true;
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td>DISSUASIONE LIMITE OFF</td>
						<td colspan="2">
							<input type="number" id="LimiteOFF" name="LimiteOFF" min="0" value="<%=value%>">
							<input type="hidden" id="IdLimiteOFF" name="IdLimiteOFF" Value="<%=dbid%>"> 
							<input type="hidden" id="TransactionOFF" name="TransactionOFF" Value="<%=transactionname%>"> 
						</td>
					</tr>
<%					
				}
			}	
			try { rs.close(); } catch (Exception e) {}
		 	try { cstmt.close(); } catch (Exception e) {}
		 	if (!fPresenzaSoglie) {
%>
					<tr>
						<td colspan="3">
							<div class="head_parameter_alert" style="text-align: center;">NESSUNA SOGLIA CONFIGURATA</div>
						</td>
					</tr>
<%
		 	}
		 	if (fPresenzaFlag || fPresenzaSoglie) {
%>
					<tr>
						<td colspan="3">
							<button type="submit" class="button blue">SALVA</button>
						</td>
					</tr>
<%
		 	}
%>
				</table>
			</form>
		</td>
		<td style="width: 55%">
			<iframe src="DissuasionCCTDetail.jsp" id="iFrame_DissuasionCCTDetail" name="iFrame_DissuasionCCTDetail" style="width: 100%; border: 0 solid grey"></iframe>
		</td>
		<td style="width: 5%"></td>
	</tr>
</table>
<%
		}
%>
<div id="ModifyMsg" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Modifica</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#ModifyMsg').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_modifymsg" action="DissuasionCCT.jsp" method="post">
			<input type="hidden" id="mAction" name="action" Value="MODIFYMSG">
			<input type="hidden" id="mCodNodo" name="CodNodo" readonly="readonly">
			<table style="width: 100%">
				<tr><td colspan="2"><br></td></tr>
				<tr>
					<td>
						<label>Descrizione</label>
					</td>
					<td>
						<label class="bold" id="mEtichetta"></label>
					</td>
				</tr>
				<tr><td colspan="2"><br></td></tr>
				<tr>
					<td>
						<label for="mCodMessaggio">Messaggio</label>
					</td>
					<td>
						<select id="mCodMessaggio" name="CodMessaggio" readonly=true>
							<option value="" selected></option>
<%
		log.info(session.getId() + " - dashboard.Dissuasion_ListMsg('" + CodIvr + "')");
		cstmt = conn.prepareCall("{ call dashboard.Dissuasion_ListMsg(?)} ");
		cstmt.setInt(1, Integer.parseInt(CodIvr));
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rs.next()) {
			String Cod_Messaggio = rs.getString("cod_messaggio");
// 			String Descrizione = rs.getString("DESCRIZIONE"); 
%>
							<option value="<%=Cod_Messaggio%>"><%=Cod_Messaggio%></option>
<%
		}
%>
						</select>
					</td>
				</tr>
<!-- 				<tr> -->
<!-- 					<td> -->
<!-- 						<label for="mTestoTts">Testo</label> -->
<!-- 					</td> -->
<!-- 					<td> -->
<!-- 						<textarea id="mTestoTts" name="TestoTts" maxlength="400" rows=10 class="messageBox"></textarea> -->
<!-- 					</td> -->
<!-- 				</tr> -->
				<tr><td colspan="2"><br></td></tr>
				<tr>
					<td>
						<button type="submit" id="" class="button blue">SALVA</button>
					</td>
					<td>
					 	<div  class="container_img button blue right" onclick="playMsg()">
							<img  src="images/PlayWhite_.png" >
						</div>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>
<%
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
		try { connCnf.close(); } catch (Exception e) {}
	}
%>

<div id="Error" class="modal">
	<div class="modal-content">
		<div class="modal-header pink">				
			<table style="width: 100%">
				<tr>
					<td><h2>Error</h2></td>
					<td>
						<div class="container_img title right pink "  onclick="$('#Error').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>		
		<input type="hidden" id="asAction" name="action" Value="add_message">
		<table>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="1"><label>Error Code:</label></td>
				<td colspan="1"><label id='ErrorCode'></label></td>
			</tr>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="1"><label>Error Message:</label></td>
				<td colspan="1"><label id='ErrorMessage'></label></td>
			</tr>
			<tr>
				<td colspan="2">
					<button type="button" id="AddCloseModal" class="button pink" onclick="$('#Error').hide();">Ok</button>
				</td>
			</tr>
		</table>			
	</div>
</div>


<div id="Play" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td><h2>Messaggio</h2></td>
					<td><h2><label id="message_played" name="message_played"></label></h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#audioPlay')[0].pause();  $('#Play').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<audio controls autoplay id="audioPlay" style="width:100%;">
			<source id="srcPlayWav" src="" type="audio/wav">	
			<source id="srcPlayMp3" src="" type="audio/mp3">	
		</audio>
	</div>
</div>

<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#DissuasionCCT","DissuasionCCT.jsp");
			$("#iFrame_DissuasionCCTDetail").height($(window).height()-10 );
			$(window).resize(function() {
				$("#iFrame_DissuasionCCTDetail").height($(window).height()-10 );		
			});	
		} catch (e) {}
//  		$("#mCodMessaggio").change(function() {
// 			changeCodMessaggio();
// 		});
	})

<%
	String ErrorCode= request.getParameter("ErrorCode");
	String ErrorMessage= request.getParameter("ErrorMessage");
	if (StringUtils.isNotBlank(ErrorCode)){
		
		out.println("$('#ErrorCode').html('"+ErrorCode+"');");
		out.println("$('#ErrorMessage').html('"+ErrorMessage+"');");
		out.println("$('#Error').show();");
	}
%>
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#ModifyMsg').hide();
			$('#Error').hide();
		}
	}

	resizeTts = function() {
// 		var scroll_height = $("#mTestoTts").get(0).scrollHeight;
// 		$("#mTestoTts").css('height', scroll_height + 'px');
	}

// 	changeCodMessaggio = function() {
// 		if ($("#mCodMessaggio").val().indexOf('_TTS') >= 0) {
// 			$("#mTestoTts").prop('disabled', false);
// 		} else {
// 			$("#mTestoTts").prop('disabled', true);
// 		}
// 	}

	modifyMsg = function(codnodo, etichetta, codmessaggio){
// 	modifyMsg = function(codnodo, etichetta, codmessaggio, testotts){
		$('#ModifyMsg').show();
		$("#mCodNodo").val(codnodo);
		$("#mEtichetta").html(etichetta);
		$("#mCodMessaggio").val(codmessaggio);
// 		$("#mTestoTts").val(testotts);
// 		resizeTts();
// 		changeCodMessaggio();
		return false;			
	}
	
	playMsg = function(){
		if ($("#mCodMessaggio").val().indexOf('_TTS') >= 0) {
		} else {
			var name = $("#mCodMessaggio").val();
			$("#message_played").text(name);
			if (jQuery.browser.msie) {
				$("#srcPlayMp3").attr("src", "UploadDownloadDissuasion?action=play&name="+name+"&browserIE=1");
			} else {
				$("#srcPlayWav").attr("src", "UploadDownloadDissuasion?action=play&name="+name+"&browserIE=0");
			}
			var audio = $("#audioPlay");
			try {
				audio[0].pause();
				audio[0].load();
				audio[0].oncanplaythrough = audio[0].play();
			} catch (e) {}
			$('#Play').show();

		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>