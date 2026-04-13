<!DOCTYPE html>
<%@page import="org.apache.commons.text.StringEscapeUtils"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="comapp.Utility"%>
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
<title>SetEmergency</title>
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
	String CodIvr = request.getParameter("CodIvr");
	if (StringUtils.isBlank(CodIvr)) {
		CodIvr = (String) session.getAttribute("CodIvr");
	} else {
		session.setAttribute("CodIvr", CodIvr);
	}
	//==============================
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String p_Status = request.getParameter("Status");
	p_Status = p_Status==null?"OFF":p_Status.toUpperCase();
	String p_CodNodo = request.getParameter("CodNodo");
	String p_CodMessaggio = request.getParameter("CodMessaggio");
	String p_TestoTts = request.getParameter("TestoTts");
	//==============================
	//==	ACTION START	====
 	log.info(session.getId() + " - action:"+ action+" CodNodo: "+p_CodNodo+" Status: "+p_Status+" CodMessaggio: "+p_CodMessaggio+" TestoTts:"+p_TestoTts );			
	try {
		Context ctx = new InitialContext();
		DataSource ds = null;
		String environment = (String)session.getAttribute("Environment");
		switch (environment) {
		case "CCT-F":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTF");
			log.info(session.getId() + " - connection CCTF wait...");
			break;
		case "CCT-E":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
			log.info(session.getId() + " - connection CCTE wait...");
			break;
		case "CCT-A":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTA");
			log.info(session.getId() + " - connection CCTA wait...");
			break;
		case "STC":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"STC");
			log.info(session.getId() + " - connection STC wait...");
			break;
		}
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
		case "SAVE" :
			log.info(session.getId() + " - dashboard.SetEmergency_Update('" + CodIvr + "','"+ p_CodNodo + "','" + p_Status + "','" + p_CodMessaggio + "','" + p_TestoTts + "')");
			cstmt = conn.prepareCall("{ call dashboard.SetEmergency_Update(?,?,?,?,?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			cstmt.setString(2, p_CodNodo);
			cstmt.setString(3, p_Status);
			cstmt.setString(4, p_CodMessaggio);
			cstmt.setString(5, p_TestoTts);
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
		<td style="width: 10%"></td>
		<td colspan="1" >	
			<form id="form_emergency_1" action="SetEmergency.jsp" method="post">
				<table id="stickytable" class="roundedCorners struct" >
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan="7">
								<table>
									<tr>
										<td>
											<div class="title">Impostazione Messaggi</div>
										</td>
										<td>
											<div  class="container_img blue title right">
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
<%
			log.info(session.getId() + " - dashboard.SetEmergency_GetStructure('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.SetEmergency_GetStructure(?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			int nLevel = 0;
			while (rs.next()) {
				String Si_COD_NODO = rs.getString("COD_NODO");
				String Si_ETICHETTA = rs.getString("ETICHETTA");
				String Si_COD_NODO_PADRE = rs.getString("COD_NODO_PADRE");
				String Si_SCELTA_MENU = rs.getString("SCELTA_MENU");
				String Si_TIPO = rs.getString("TIPO");
				String Si_ORDINE = rs.getString("ORDINE");
				int Si_LIVELLO =  rs.getInt("LIVELLO"); 
				String Sn_stato = Utility.getNotNull(rs.getString("stato"));
				String Sn_cod_messaggio = Utility.getNotNull(rs.getString("cod_messaggio"));
				String Sn_testo_tts = Utility.getNotNull(rs.getString("testo_tts"));
				Sn_testo_tts = StringEscapeUtils.escapeEcmaScript(Sn_testo_tts);
				String refNODO = (StringUtils.isBlank(Si_COD_NODO_PADRE)?Si_COD_NODO:Si_COD_NODO_PADRE+"-"+Si_COD_NODO);
				String image = "";
				switch (Si_TIPO) {
				case "INGRESSO":
					image = "<img  src='images/ingresso.gif'>";
					break;
				case "MENU":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/menu_ON.png'>";	break;
					case "OFF":	image = "<img  src='images/menu_OFF.png'>";	break;
					default:	image = "<img  src='images/menu.png'>";		break;
					}
					break;
				case "MESSAGGIO":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/messaggio_ON.jpg'>";		break;
					case "OFF":	image = "<img  src='images/messaggio_OFF.jpg'>";	break;
					default:	image = "<img  src='images/messaggio.jpg'>";		break;
					}
					break;
				case "TRASFERIMENTO":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/trasferimento_ON.jpg'>";		break;
					case "OFF":	image = "<img  src='images/trasferimento_OFF.jpg'>";	break;
					default:	image = "<img  src='images/trasferimento.jpg'>";		break;
					}
					break;
				case "RITORNO":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/freccia_ON.jpg'>";	break;
					case "OFF":	image = "<img  src='images/freccia_OFF.jpg'>";	break;
					default:	image = "<img  src='images/freccia.jpg'>";		break;
					}
					break;
				case "INSERIMENTO":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/inserimento_ON.jpg'>";	break;
					case "OFF":	image = "<img  src='images/inserimento_OFF.jpg'>";	break;
					default:	image = "<img  src='images/inserimento.jpg'>";		break;
					}
					break;
				case "CONTROLLO":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/controllo_ON.jpg'>";		break;
					case "OFF":	image = "<img  src='images/controllo_OFF.jpg'>";	break;
					default:	image = "<img  src='images/controllo.jpg'>";		break;
					}
					break;
				default:
					image = "";
					break;
				}
%>
					<tr style="width: 100%" class="listGroupItem" id="ref_<%=refNODO%>" <%if(!StringUtils.isBlank(Sn_stato)){%> onclick="return change('<%=Si_COD_NODO%>', '<%=Si_ETICHETTA%>', <%=Sn_stato.equalsIgnoreCase("ON")%>, '<%=Sn_cod_messaggio%>', '<%=Sn_testo_tts%>')"<%}%>>
<%-- 					<tr style="width: 100%" class="listGroupItem" id="ref_<%=refNODO%>" <%if(!StringUtils.isBlank(Sn_stato)){%> onclick="return change(codnodo, etichetta, status, codmessaggio, testotts)"<%}%>> --%>
<%
				if (Si_LIVELLO>0) {
%>
						<td colspan="<%=Si_LIVELLO%>" style="width: 26px"></td>
<%
				}
%>
						<td style="width: 26px">
						 	<div class="container_img struct grey right">
								<%=image%>
							</div>
						</td>
						<td colspan="<%=(8-Si_LIVELLO)%>"><%=(StringUtils.isBlank(Si_SCELTA_MENU)?Si_ETICHETTA:Si_SCELTA_MENU+" - "+Si_ETICHETTA)%></td>
					</tr>
<%
			}
			try { rs.close(); } catch (Exception e) {}
		 	try { cstmt.close(); } catch (Exception e) {}
%>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>		
<%
		}
%>

<div id="Modify" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Modifica</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#Modify').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_emergency_2" action="SetEmergency.jsp" method="post">
			<input type="hidden" id="mAction" name="action" Value="SAVE"> <input type="hidden" id="mCodNodo" name="CodNodo" readonly="readonly">
			<table style="width: 100%">
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label>Nodo</label>
					</td>
					<td>
						<label class="bold" id="mEtichetta"></label>
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label>Stato</label>
					</td>
					<td class="divTableCell" >
						<label class="switch"> <input type="checkbox" id="mCurrentState" name="Status" checked="true"> <span class="slider round blue"></span></label>
					</td>
				</tr>
				<tr><td colspan="2"></td></tr>
				<tr>
					<td>
						<label for="mCodMessaggio">Messaggio</label>
					</td>
					<td>
						<select id="mCodMessaggio" name="CodMessaggio" readonly=true>
							<option value="" selected></option>
<%
		log.info(session.getId() + " - dashboard.SetEmergency_List('" + CodIvr + "')");
		cstmt = conn.prepareCall("{ call dashboard.SetEmergency_List(?)} ");
		cstmt.setInt(1, Integer.parseInt(CodIvr));
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rs.next()) {
			String Cod_Messaggio = rs.getString("COD_MESSAGGIO");
			String Descrizione = rs.getString("DESCRIZIONE"); 
%>
							<option value="<%=Cod_Messaggio%>"><%=Descrizione%></option>
<%
		}
%>
						</select>
					</td>
				</tr>
				<tr><td colspan="2"></td></tr>
				<tr>
					<td>
						<label for="mTestoTts">Testo</label>
					</td>
					<td>
						<textarea id="mTestoTts" name="TestoTts" maxlength="400" rows=10 class="messageBox"></textarea>
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<button type="submit" id="" class="button blue">SALVA</button>
					</td>
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
<%
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
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

<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#SetEmergency","SetEmergency.jsp");
		} catch (e) {}
// 		$("#mTestoTts").on('input', function() {
// 			var scroll_height = $("#mTestoTts").get(0).scrollHeight;
// 			$("#mTestoTts").css('height', scroll_height + 'px');
// 		});
 		$("#stickytable").stickyTableHeaders();
 		$("#mCodMessaggio").change(function() {
			changeCodMessaggio();
		});
	})
	
	resizeTts = function() {
// 		var scroll_height = $("#mTestoTts").get(0).scrollHeight;
// 		$("#mTestoTts").css('height', scroll_height + 'px');
	}

	changeCodMessaggio = function() {
		if ($("#mCodMessaggio").val().indexOf('_TTS') >= 0) {
			$("#mTestoTts").prop('disabled', false);
		} else {
			$("#mTestoTts").prop('disabled', true);
		}
	}

	change = function(codnodo, etichetta, status, codmessaggio, testotts){
		$('#Modify').show();
		$("#mCodNodo").val(codnodo);
		$("#mEtichetta").html(etichetta);
		$("#mCurrentState").prop('checked', status);
		$("#mCodMessaggio").val(codmessaggio);
		$("#mTestoTts").val(testotts);
		resizeTts();
		changeCodMessaggio();
		return false;			
	}
	
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
			$('#Modify').hide();
			$('#Error').hide();
			$('#audioPlay')[0].pause();  $('#Play').hide();
		}
	}

	playMsg = function(){
		if ($("#mCodMessaggio").val().indexOf('_TTS') >= 0) {
		} else {
			var name = $("#mCodMessaggio").val();
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
			$('#Play').show();

		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>