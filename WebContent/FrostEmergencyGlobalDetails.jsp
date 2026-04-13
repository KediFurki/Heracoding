<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.sql.Types"%>
<%@page import="org.json.JSONArray"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Timestamp"%>
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
<title>Flag</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/getBrowser.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
</head>
<body style="overflow-y: auto;">
<%
	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");
	//==	REQUEST PARAMETERS	====
	String ConnId = request.getParameter("ConnId");
	if (ConnId==null) {
		ConnId = (String)session.getAttribute("fe_ConnId");
	} else {
		session.setAttribute("fe_ConnId", Utility.getNotNull(ConnId)); 
	}
	String Dal = request.getParameter("Dal");
	if (Dal==null) {
		Dal = (String)session.getAttribute("fe_Dal");
	} else {
		session.setAttribute("fe_Dal", Utility.getNotNull(Dal));
	}
	String Al = request.getParameter("Al");
	if (Al==null) {
		Al = (String)session.getAttribute("fe_Al");
	} else {
		session.setAttribute("fe_Al", Utility.getNotNull(Al));
	}
	String Stato = request.getParameter("Stato");
	if (Stato==null) {
		Stato = (String)session.getAttribute("fe_Stato");
	} else {
		session.setAttribute("fe_Stato", Utility.getNotNull(Stato));
	}
	String UserName = (String) session.getAttribute("UserName");
	//==============================
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String CallId = request.getParameter("CallId");
	String Status = request.getParameter("Status");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+ action+" CallId: "+CallId+" Status:"+Status );			
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTF");
		log.info(session.getId() + " - connection CCTF wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "update" :
				log.info(session.getId() + " - dashboard.FrostEmergency_Update('" + CallId + "','" + Status + "','" + UserName + "')");
				cstmt = conn.prepareCall("{ call dashboard.FrostEmergency_Update(?,?,?)} ");
				cstmt.setString(1, CallId);
				cstmt.setString(2, Status);
				cstmt.setString(3, UserName);
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
		if (action.equals("search")||action.equals("update")) {
%>
<div id="_left" style="overflow: auto;">
	<table id="stickytable" class="roundedCorners small no_top">
		<thead>
			<tr class='listGroupItem active green'>
				<th>ConnId</th>
				<th>Data</th>
				<th>Numero Chiamante</th>
				<th>Stato</th>
				<th>Cambio Stato</th>
				<th>Utente</th>
			</tr>
		</thead>
		<tbody>
<%
			log.info(session.getId() + " - dashboard.FrostEmergency_Get('"+Stato+"','"+ConnId+"','"+Dal+"','"+Al+"')");
			cstmt = conn.prepareCall("{ call dashboard.FrostEmergency_Get(?,?,?,?)} ");
			if (StringUtils.isNotBlank(Stato)) {
				cstmt.setString(1, Stato);			
			} else {
				cstmt.setNull(1, Types.VARCHAR);			
			}
			if (StringUtils.isNotBlank(ConnId)) {
				cstmt.setString(2, ConnId);			
			} else {
				cstmt.setNull(2, Types.VARCHAR);			
			}
			if (StringUtils.isNotBlank(Dal)) {
				cstmt.setTimestamp(3, new Timestamp(sdf_full_day_of_year.parse(Dal).getTime()));
			} else {
				cstmt.setNull(3, Types.TIMESTAMP);			
			}
			if (StringUtils.isNotBlank(Al)) {
				cstmt.setTimestamp(4, new Timestamp(sdf_full_day_of_year.parse(Al).getTime()));
			} else {
				cstmt.setNull(4, Types.TIMESTAMP);			
			}
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			int i = 0;
			String CodIvr="";
			while (rs.next()) {
				i++;
				String CALL_ID = rs.getString("CALL_ID");
				String TIME_STAMP = rs.getString("TIME_STAMP");
				String CLI = rs.getString("CLI");
				String STATO = rs.getString("STATO");
				String TIME_STAMP_CAMBIO_STATO = rs.getString("TIME_STAMP_CAMBIO_STATO");
				String UTENTE = rs.getString("UTENTE");
				String PATH_FILE = rs.getString("PATH_FILE");
				PATH_FILE = PATH_FILE.replaceAll("\\\\", "/");
%>
			<tr class='listGroupItem' onclick="return change('<%=CALL_ID%>', '<%=STATO%>', '<%=PATH_FILE%>')">
				<td><label ><%=CALL_ID%></label></td>
				<td><label ><%=TIME_STAMP%></label></td>
				<td><label ><%=CLI%></label></td>
				<td><label ><%=STATO%></label></td>
				<td><label ><%=TIME_STAMP_CAMBIO_STATO%></label></td>
				<td><label ><%=UTENTE%></label></td>
			</tr>
<%
			}
%>
		</tbody>
	</table>
</div>
<%
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>

<div id="Modify" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Modifica</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#Modify').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_modify_gelo" action="FrostEmergencyGlobalDetails.jsp" method="post">
			<input type="hidden" id="mAction" name="action" Value="update">
			<input type="hidden" id="mPath" name="Path" readonly="readonly">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label for="mCallId">ConnId</label>
					</td>
					<td>
						<input class="noBorder bold" type="text" id="mCallId" name="CallId" readonly="readonly">
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label for="mStatus">Stato</label>
					</td>
					<td>
						<select id="mStatus" name="Status" readonly=true>
							<option value=""></option>
							<option value="Da Lavorare">Da Lavorare</option>
							<option value="In Lavorazione">In Lavorazione</option>
							<option value="Lavorato">Lavorato</option>
						</select>
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<button type="submit" id="" class="button green">SALVA</button>
					</td>
					<td  style="width: 60px">
					 	<div  class="container_img button green right" onclick="downloadMsg()">
							<img  src="images/SaveWhite_.png" >
						</div>
					</td>
<!-- 					<td  style="width: 60px"> -->
<!-- 					 	<div  class="container_img button green right" onclick="playMsg()"> -->
<!-- 							<img  src="images/PlayWhite_.png" > -->
<!-- 						</div> -->
<!-- 					</td> -->
				</tr>
			</table>
		</form>
	</div>
</div>

<!-- <div id="Play" class="modal"> -->
<!-- 	<div class="modal-content"> -->
<!-- 		<div class="modal-header green"> -->
<!-- 			<table style="width: 100%"> -->
<!-- 				<tr> -->
<!-- 					<td><h2>Messaggio</h2></td> -->
<!-- 					<td><h2><label id="message_played" name="message_played"></label></h2></td> -->
<!-- 					<td> -->
<!-- 						<div class="container_img title right green "  onclick="$('#audioPlay')[0].pause();  $('#Play').hide();"> -->
<!-- 							<img alt="" class="" src="images/CloseWhite_.png"> -->
<!-- 						</div> -->
<!-- 					</td> -->
<!-- 				</tr> -->
<!-- 			</table> -->
<!-- 		</div> -->
<!-- 		<audio controls autoplay id="audioPlay" style="width:100%;"> -->
<!-- 			<source id="srcPlayWav" src="" type="audio/wav">	 -->
<!-- 			<source id="srcPlayMp3" src="" type="audio/mp3">	 -->
<!-- 		</audio> -->
<!-- 	</div> -->
<!-- </div> -->

<div id="Info" class="modal">
	<div class="modal-content">
		<div class="modal-header yellow">
			<table style="width: 100%">
				<tr>
					<td><h2>Info</h2></td>
					<td>
						<div class="container_img title right yellow " onclick="$('#Info').hide();">
							<img alt="" class="" src="images/Close_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<input type="hidden" id="asAction" name="action" Value="add_message">
		<table>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="1"><label>Info Message:</label></td>
				<td colspan="1"><label id='InfoMessage'></label></td>
			</tr>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="2">
					<button type="button" id="AddCloseModal" class="button yellow" onclick="$('#Info').hide();">Ok</button>
				</td>
			</tr>
		</table>
	</div>
</div>

<div id="Error" class="modal">
	<div class="modal-content">
		<div class="modal-header pink">
			<table style="width: 100%">
				<tr>
					<td><h2>Error</h2></td>
					<td>
						<div class="container_img title right pink " onclick="$('#Error').hide();">
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

<div id="Download" class="modal">
	<form id="form_down_message" action="UploadDownloadFrostEmergency" method="post" enctype="multipart/form-data" >
		<input type="text" id="downAction" name="action" Value="download">
		<input type="text" id="downMessage" name="name">
	</form>
</div>

<script type="text/javascript">
	downloadMsg = function(name){
		$("#downMessage").val($("#mPath").val()+$("#mCallId").val()+'.zip');
		$("#form_down_message")[0].submit();
	}

	$(function() {
		try {
			parent.ChangeActiveMenu("#FrostEmergencyGlobal");
		} catch (e) {}
 		$("#stickytable").stickyTableHeaders();
	})

	change = function(callid, stato, path){
		if (stato=='') {
			$('#InfoMessage').html('Non ci sono registrazioni per la chiamata selezionata');
			$('#Info').show();
		} else {
			$('#Modify').show();
			$("#mStatus").val(stato);
			$("#mCallId").val(callid);
			$("#mPath").val(path);
		}
		return false;			
	}
	
	
<%
	String ErrorCode = request.getParameter("ErrorCode");
	String ErrorMessage = request.getParameter("ErrorMessage");
	if (StringUtils.isNotBlank(ErrorCode)) {
		out.println("$('#ErrorCode').html('" + ErrorCode + "');");
		out.println("$('#ErrorMessage').html('" + ErrorMessage + "');");
		out.println("$('#Error').show();");
	}
%>

	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#Modify').hide();
			$('#Error').hide();
			$('#Info').hide();
// 			$('#audioPlay')[0].pause();  $('#Play').hide();
		}
	}

// 	playMsg = function(){
// 		var name = $("#mPath").val()+$("#mCallId").val();
// 		$("#message_played").text(name);
// 		if (jQuery.browser.msie) {
// 			$("#srcPlayMp3").attr("src", "UploadDownloadFrostEmergency?action=play&name="+name+"&browserIE=1");
// 		} else {
// 			$("#srcPlayWav").attr("src", "UploadDownloadFrostEmergency?action=play&name="+name+"&browserIE=0");
// 		}
// 		var audio = $("#audioPlay");
// 		try {
// 			audio[0].pause();
// 			audio[0].load();
// 			audio[0].oncanplaythrough = audio[0].play();
// 		} catch (e) {}
// 		$('#Play').show();
// 	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>