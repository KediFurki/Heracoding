<!DOCTYPE html>
<%@page import="org.apache.commons.text.StringEscapeUtils"%>
<%@page import="org.json.JSONArray"%>
<%@page import="org.json.JSONObject"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="comapp.Utility"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="java.util.Properties"%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Services</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/getBrowser.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
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
	//		NONE
	//==============================
 	Connection conn = null;
	CallableStatement cstmt = null;
	CallableStatement cstmtMsgList = null;
	ResultSet rs = null;
	ResultSet rsMsgList = null;
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String codice = request.getParameter("codice");
	String nodo = request.getParameter("nodo");
	String priority = request.getParameter("priority");
	String stato = request.getParameter("stato");
	stato = stato==null?"OFF":stato.toUpperCase();
	String codmessaggio = request.getParameter("codmessaggio");
	String testotts = request.getParameter("testotts");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:" + action +
			" codice: " + codice + 
			" nodo: " + nodo +
			" priority: " + priority +
			" stato: " + stato +
			" codmessaggio: " + codmessaggio +
			" testotts: " + testotts);
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTA");
		log.info(session.getId() + " - connection CCTA wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
// 			case "deleteSurvey" :
// 				log.info(session.getId() + " - dashboard.Survey_Update('" + codice + "','N')");
// 				cstmt = conn.prepareCall("{ call dashboard.Survey_Update(?,?)} ");
// 				cstmt.setInt(1, Integer.parseInt(codice));
// 				cstmt.setString(2, "N");
// 				cstmt.execute();
// 				log.debug(session.getId() + " - executeCall complete");
// 				break;
// 			case "addSurvey" :
// 				log.info(session.getId() + " - dashboard.Survey_Update('" + codice + "','Y')");
// 				cstmt = conn.prepareCall("{ call dashboard.Survey_Update(?,?)} ");
// 				cstmt.setInt(1, Integer.parseInt(codice));
// 				cstmt.setString(2, "Y");
// 				cstmt.execute();
// 				log.debug(session.getId() + " - executeCall complete");
// 				break;
			case "updPriority":
				log.info(session.getId() + " - dashboard.Priority_Update('" + codice + "','"+ nodo + "','" + priority + "')");
				cstmt = conn.prepareCall("{ call dashboard.Priority_Update(?,?,?)} ");
				cstmt.setInt(1, Integer.parseInt(codice));
				cstmt.setString(2, nodo);
				cstmt.setInt(3, Integer.parseInt(priority));
				cstmt.execute();					
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "updEmergengy" :
				log.info(session.getId() + " - dashboard.SetEmergency_Update('" + codice + "','"+ nodo + "','" + stato + "','" + codmessaggio + "','" + testotts + "')");
				cstmt = conn.prepareCall("{ call dashboard.SetEmergency_Update(?,?,?,?,?)} ");
				cstmt.setInt(1, Integer.parseInt(codice));
				cstmt.setString(2, nodo);
				cstmt.setString(3, stato);
				cstmt.setString(4, codmessaggio);
				cstmt.setString(5, testotts);
				cstmt.execute();					
				log.debug(session.getId() + " - executeCall complete");
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	} finally {
		try { cstmt.close(); } catch (Exception e) {}
	}
%>
<script type="text/javascript">
	var listMsg = new Array();
</script>
<table class="center"  >
	<tr>
		<td style="width: 10%"></td>
		<td colspan="2" >
			<form id="form_emergency_1" action="ServiceGlobalCCTA.jsp" method="post">
				<table id="stickytable" class="roundedCorners ">
				<thead>
					<tr class="listGroupItem active green">
						<th colspan="4">
							<table style="width: 100%" >
								<tr>
									<td>
										<div class="title">Service</div>
									</td>
									<td>
										<div class="container title right" style="visibility: hidden">
											<label></label>
										</div>
									</td>
								</tr>
							</table>
						</th>
					</tr>
					<tr class='listGroupSubItem active green'>
					 	<th style='width:190px'>Numero Verde</th>
					 	<th>Descrizione</th>
						<th>Priorità</th>
					 	<th>Messaggio Emergenza</th>
					</tr>
				</thead>
<%
	try {
		String firstPage = "";
		log.info(session.getId() + " - dashboard.Service_ListEx('CCT-A')");
		cstmt = conn.prepareCall("{ call dashboard.Service_ListEx(?)}");
		cstmt.setString(1,"CCT-A");
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rs.next()) {
			String COD_IVR = rs.getString("COD_IVR");
			String LABEL = rs.getString("LABEL");
			String DESCRIZIONE = rs.getString("DESCRIZIONE");
			
// 			String SURVEY = rs.getString("SURVEY");
			
			String PRI_COD_NODO = rs.getString("PRI_COD_NODO");
			int PRI_priorita = rs.getInt("PRI_priorita");
			
			String EME_COD_NODO = rs.getString("EME_COD_NODO");
			String EME_ETICHETTA = rs.getString("EME_ETICHETTA");
			String EME_stato = rs.getString("EME_stato");
			String EME_cod_messaggio = rs.getString("EME_cod_messaggio");
			String EME_testo_tts = Utility.getNotNull(rs.getString("EME_testo_tts"));
			EME_testo_tts = StringEscapeUtils.escapeEcmaScript(EME_testo_tts);
			
			firstPage = "'Priority.jsp?CodIvr=" + COD_IVR + "'";
			String descService = "'"+StringEscapeUtils.escapeEcmaScript(LABEL + " - "+ DESCRIZIONE)+"'";
%>
					<tr class='listGroupItem'>
					 	<td style='width:190px' onclick="selectServ(<%=descService%>,<%=firstPage%>)"><%=LABEL%></td>
					 	<td                     onclick="selectServ(<%=descService%>,<%=firstPage%>)"><%=DESCRIZIONE%></td>
<!-- 					 	<td> -->
<!-- 					 		<label class='switch'> -->
<%-- 								<input type='checkbox' id='StatuSur' name='Status' <%=(SURVEY.equalsIgnoreCase("Y")?"checked=true":"")%> onclick='changeSurvey(this,"<%=COD_IVR%>")'> --%>
<!-- 								<span class='slider round blue'></span> -->
<!-- 							</label> -->
<!-- 					 	</td> -->
<%
			if (PRI_priorita>0) {
%>
						<td>
							<select id="mPriorita" name="Priorita" onchange='changePriority(this,"<%=COD_IVR%>","<%=PRI_COD_NODO%>")'>
								<option value="1" <%=((PRI_priorita==1)?"selected":"") %>>1</option>
								<option value="2" <%=((PRI_priorita==2)?"selected":"") %>>2</option>
								<option value="3" <%=((PRI_priorita==3)?"selected":"") %>>3</option>
								<option value="4" <%=((PRI_priorita==4)?"selected":"") %>>4</option>
								<option value="5" <%=((PRI_priorita==5)?"selected":"") %>>5</option>
								<option value="6" <%=((PRI_priorita==6)?"selected":"") %>>6</option>
								<option value="7" <%=((PRI_priorita==7)?"selected":"") %>>7</option>
								<option value="8" <%=((PRI_priorita==8)?"selected":"") %>>8</option>
								<option value="9" <%=((PRI_priorita==9)?"selected":"") %>>9</option>
								<option value="10" <%=((PRI_priorita==10)?"selected":"") %>>10</option>
							</select>
						</td>
<%
			} else {
%>
					 	<td                     onclick="selectServ(<%=descService%>,<%=firstPage%>)"></td>
<%
			}
			if (EME_stato!=null) {
%>
						<td onclick="return changeEmergency('<%=COD_IVR%>', <%=descService%>, '<%=EME_COD_NODO%>', '<%=EME_ETICHETTA%>', <%=EME_stato.equalsIgnoreCase("ON")%>, '<%=EME_cod_messaggio%>', '<%=EME_testo_tts%>')">
					 		<label class='switch'>
								<input type='checkbox' id='StatuEme' name='Status' <%=(EME_stato.equalsIgnoreCase("ON")?"checked=true":"")%> onclick='return false;'>
								<span class='slider round blue'></span>
							</label>
						</td>
<%
				log.info(session.getId() + " - dashboard.SetEmergency_List('" + COD_IVR + "')");
				cstmtMsgList = conn.prepareCall("{ call dashboard.SetEmergency_List(?)} ");
				cstmtMsgList.setInt(1, Integer.parseInt(COD_IVR));
				rsMsgList = cstmtMsgList.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
				JSONArray eleMsgs = new JSONArray();
				while (rsMsgList.next()) {
					String Cod_Messaggio = rsMsgList.getString("COD_MESSAGGIO");
					String Descrizione = rsMsgList.getString("DESCRIZIONE");
					JSONObject msgs = new JSONObject();
					msgs.append("cod", Cod_Messaggio);
					msgs.append("des", Descrizione);
					eleMsgs.put(msgs);
				}
%>
<script type='text/javascript'>
	listMsg[<%=COD_IVR%>] = '<%=eleMsgs.toString()%>';
</script>
<%
				cstmtMsgList.close();
				rsMsgList.close();
			} else {
%>
					 	<td                     onclick="selectServ(<%=descService%>,<%=firstPage%>)"></td>
<%
			}
%>
		 			</tr>
<%
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { rsMsgList.close(); } catch (Exception e) {}
		try { cstmtMsgList.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%> 
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>

<!-- <div id="Change_Survey" class="modal"> -->
<!-- 	<form id="form_change_survey" action="ServiceGlobalCCTA.jsp" method="post" > -->
<!-- 		<input type="text" id="csAction" name="action" Value="">  -->
<!-- 		<input type="text" id="csCodIvr" name="codice" > -->
<!-- 	</form> -->
<!-- </div> -->

<div id="Change_Priority" class="modal">
	<form id="form_change_priority" action="ServiceGlobalCCTA.jsp" method="post" >
		<input type="text" id="cpAction" name="action" Value="updPriority"> 
		<input type="text" id="cpCodIvr" name="codice" >
		<input type="text" id="cpCodNodo" name="nodo" >
		<input type="text" id="cpPriority" name="priority" >
	</form>
</div>

<div id="Modify_Emergency" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td><h2>Modifica</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#Modify_Emergency').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_modify_emergency" action="ServiceGlobalCCTA.jsp" method="post">
			<input type="hidden" id="meAction" name="action" Value="updEmergengy">
			<input type="hidden" id="meCodIvr" name="codice">
			<input type="hidden" id="meCodNodo" name="nodo" readonly="readonly">
			<table style="width: 100%">
				<tr><td><br></td></tr>
				<tr>
					<td><label>Servizio</label></td>
					<td><label class="bold" id="meServizio"></label></td>
				</tr>
				<tr>
					<td><label>Nodo</label></td>
					<td><label class="bold" id="meEtichetta"></label></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td><label>Stato</label></td>
					<td class="divTableCell" >
						<label class="switch"> <input type="checkbox" id="meCurrentState" name="stato" checked="true"> <span class="slider round blue"></span></label>
					</td>
				</tr>
				<tr><td colspan="2"></td></tr>
				<tr>
					<td><label for="meCodMessaggio">Messaggio</label></td>
					<td><select id="meCodMessaggio" name="codmessaggio" readonly=true>
							<option value="" selected></option>
					</select></td>
				</tr>
				<tr><td colspan="2"></td></tr>
				<tr>
					<td><label for="meTestoTts">Testo</label></td>
					<td><textarea id="meTestoTts" name="testotts" maxlength="400" rows=10 class="messageBox"></textarea></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td><button type="submit" id="" class="button blue">SALVA</button></td>
					<td  style="width: 60px">
					 	<div  class="container_img button blue right" onclick="playMsg()">
							<img  src="images/PlayWhite_.png" >
						</div>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="Play_Emergency" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td><h2>Messaggio</h2></td>
					<td><h2><label id="message_played" name="message_played"></label></h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#audioPlay')[0].pause();  $('#Play_Emergency').hide();">
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

<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceGlobal");
		} catch (e) {}
 		$("#stickytable").stickyTableHeaders();
// 		$("#meTestoTts").on('input', function() {
// 			var scroll_height = $("#meTestoTts").get(0).scrollHeight;
// 			$("#meTestoTts").css('height', scroll_height + 'px');
// 		});
 		$("#meCodMessaggio").change(function() {
			changeCodMessaggio();
		});
	})
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#Modify_Emergency').hide();
			$('#audioPlay')[0].pause();  $('#Play_Emergency').hide();
		}
	}


	resizeTts = function() {
// 		var scroll_height = $("#meTestoTts").get(0).scrollHeight;
// 		$("#meTestoTts").css('height', scroll_height + 'px');
	}

	changeCodMessaggio = function() {
		if ($("#meCodMessaggio").val().indexOf('_TTS') >= 0) {
			$("#meTestoTts").prop('disabled', false);
		} else {
			$("#meTestoTts").prop('disabled', true);
		}
	}

	selectServ = function(descService,firstPage) {
		 parent.SetServiceSelected(descService);
		 window.location.assign(firstPage);
	}
	
// 	changeSurvey = function(_this,CodIvr){
// 		if ($(_this).is(':checked'))
// 			$("#csAction").val('addSurvey'); 
// 		else
// 			$("#csAction").val('deleteSurvey');
// 		$("#csCodIvr").val(CodIvr);
// 		$("#form_change_survey")[0].submit();
// 	}
	
	changePriority = function(_this,CodIvr,CodNodo){
		$("#cpCodIvr").val(CodIvr);
		$("#cpCodNodo").val(CodNodo);
		$("#cpPriority").val($(_this).val());
		$("#form_change_priority")[0].submit();
	}
	
	changeEmergency = function(CodIvr, servizio, codnodo, etichetta, status, codmessaggio, testotts){
		$('#Modify_Emergency').show();
		$("#meCodIvr").val(CodIvr);
		$("#meCodNodo").val(codnodo);
		$("#meServizio").html(servizio);
		$("#meEtichetta").html(etichetta);
		$("#meCurrentState").prop('checked', status);
		loadCodMessaggio(CodIvr);
		$("#meCodMessaggio").val(codmessaggio);
		$("#meTestoTts").val(testotts);
		resizeTts();
		changeCodMessaggio();
		return false;			
	}
	
	loadCodMessaggio = function(CodIvr) {
		var selMsg = listMsg[CodIvr];
		var cnfMsg = JSON.parse(selMsg); 
		$('#meCodMessaggio').empty();
		$('#meCodMessaggio').append('<option value=""></option>');
		
		for (var i=0; i<cnfMsg.length ; i++) {
			key = cnfMsg[i];
			$('#meCodMessaggio').append('<option value="' + key.cod + '">' + key.des + '</option>');
		}
	}
	
	playMsg = function(){
		if ($("#meCodMessaggio").val().indexOf('_TTS') >= 0) {
		} else {
			var name = $("#meCodMessaggio").val();
			$("#message_played").text(name);
			if (jQuery.browser.msie) {
				$("#srcPlayMp3").attr("src", "UploadDownloadSetEmergency?action=play&name="+name+"&browserIE=1");
			} else {
				$("#srcPlayWav").attr("src", "UploadDownloadSetEmergency?action=play&name="+name+"&browserIE=0");
			}
			var audio = $("#audioPlay");
			try {
				audio[0].pause();
				audio[0].load();
				audio[0].oncanplaythrough = audio[0].play();
			} catch (e) {}
			$('#Play_Emergency').show();

		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>