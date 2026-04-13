<!DOCTYPE html>
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
<title>Emergency</title>
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
	String p_Territorio = request.getParameter("Territorio");
	String p_CodMessaggio = request.getParameter("CodMessaggio");
	String p_Id_Msg = request.getParameter("Id_Msg");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+ action+" Status: "+p_Status+" Territorio:"+p_Territorio+" CodMessaggio:"+p_CodMessaggio+" Id_Msg:"+p_Id_Msg );			
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "NEW" :
				log.info(session.getId() + " - dashboard.Emergency_AddMsg('" + CodIvr + "','','','')");
				cstmt = conn.prepareCall("{ call dashboard.Emergency_AddMsg(?,?,?,?)} ");
				cstmt.setString(1, CodIvr);
				cstmt.setString(2, "");
				cstmt.setString(3, "");
				cstmt.setString(4, "");
				cstmt.execute();					
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "SAVE" :
				log.info(session.getId() + " - dashboard.Emergency_Update('" + p_Id_Msg + "','"+ p_Status + "','" + p_Territorio + "','" + p_CodMessaggio + "')");
				cstmt = conn.prepareCall("{ call dashboard.Emergency_Update(?,?,?,?)} ");
				cstmt.setInt(1, Integer.parseInt(p_Id_Msg));
				cstmt.setString(2, p_Status);
				cstmt.setString(3, p_Territorio);
				cstmt.setString(4, p_CodMessaggio);
				cstmt.execute();					
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "DELETE" :
				log.info(session.getId() + " - dashboard.Emergency_DeleteMsg('" + p_Id_Msg + "')");
				cstmt = conn.prepareCall("{ call dashboard.Emergency_DeleteMsg(?)} ");
				cstmt.setInt(1, Integer.parseInt(p_Id_Msg));
				cstmt.execute();					
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "INSERT" :
				log.info(session.getId() + " - dashboard.Emergency_AddMsg('" + CodIvr + "','"	+ p_Status + "','" + p_Territorio + "','" + p_CodMessaggio + "')");
				cstmt = conn.prepareCall("{ call dashboard.Emergency_AddMsg(?,?,?,?)} ");
				cstmt.setString(1, CodIvr);
				cstmt.setString(2, p_Status);
				cstmt.setString(3, p_Territorio);
				cstmt.setString(4, p_CodMessaggio);
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
			<form id="form_emergency_1" action="Emergency.jsp" method="post">
				<table id="stickytable"  class="roundedCorners small" >
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan="7">
								<table>
									<tr>
										<td>
											<div class="title">Emergenza</div>
										</td>
										<td>
											<div  class="container_img blue title right" onclick="loadfile();">
												<img alt=""  src="images/NotePlusWhite_.png">
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
<%
			log.info(session.getId() + " - dashboard.Emergency_GetMsgs('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.Emergency_GetMsgs(?)} ");
			cstmt.setString(1, CodIvr);
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			while (rs.next()) {
				int Id_Msg = rs.getInt("ID");
				String Territorio = rs.getString("TERRITORIO");
				String Status = rs.getString("STATO");
				String CodMessaggio = rs.getString("COD_MESSAGGIO");
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td onclick="modifyTerritorio('<%=Id_Msg%>',<%=Status.equalsIgnoreCase("ON")%>,'<%=Territorio%>','<%=CodMessaggio%>')">
							<label class="switch" id="status"> <input type="checkbox" id="iCurrentState" name="Id_Msg" value="<%=Id_Msg%>" <%=((Status.equalsIgnoreCase("ON")) ? "checked=true" : "")%> onclick="return change('<%=Id_Msg%>',this,'<%=Territorio%>','<%=CodMessaggio%>')"> <span class="slider round blue"></span>
							</label>
						</td>
						<td onclick="modifyTerritorio('<%=Id_Msg%>',<%=Status.equalsIgnoreCase("ON")%>,'<%=Territorio%>','<%=CodMessaggio%>')"> <%=Territorio%></td>
						<td  onclick="modifyTerritorio('<%=Id_Msg%>',<%=Status.equalsIgnoreCase("ON")%>,'<%=Territorio%>','<%=CodMessaggio%>')"><%=CodMessaggio%></td>
						<td  style="width: 60px" onclick="playMsg('<%=CodMessaggio%>')">
						 	<div  class="container_img grey right">
								<img  src="images/PlayGrey_.png" >
							</div>
						</td>
						<td  style="width: 60px" onclick="downloadMsg('<%=CodMessaggio%>')">
						 	<div  class="container_img grey right">
								<img  src="images/SaveGrey_.png" >
							</div>
						</td>
						<td  style="width: 60px">
						    <div  class="container_img grey right">
							   <img src="images/TrashGrey_.png"   onclick="confirmDelete('<%=Id_Msg%>',<%=Status.equalsIgnoreCase("ON")%>,'<%=Territorio%>','<%=CodMessaggio%>')" >
						    </div>
						</td>
					</tr>
<%
			}
			try { rs.close(); } catch (Exception e) {}
		 	try { cstmt.close(); } catch (Exception e) {}
%>
					<tr>
						<td colspan="7">
							 <div  class="container_img grey"> 
								 <img alt=""   src="images/PlusGrey_.png" onclick="NewMsg(); " > 
							</div> 
						</td>
					</tr>
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
		<form id="form_emergency_2" action="Emergency.jsp" method="post">
			<input type="hidden" id="mAction" name="action" Value="SAVE"> <input type="hidden" id="mIdMsg" name="Id_Msg" readonly="readonly">
			<table>
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
						<label for="Territorio">Territorio</label>
					</td>
					<td>
						<select id="mTerritorio" name="Territorio" readonly=true>
							<option value=""></option>
							<option value="BO">Bologna</option>
							<option value="IM">Imola</option>
							<option value="FE">Ferrara</option>
							<option value="MO">Modena</option>
							<option value="FC">Forlì-Cesena</option>
							<option value="RA">Ravenna</option>
							<option value="RN">Rimini</option>
							<option value="FI">Firenze</option>
							<option value="ALL">Tutti</option>
						</select>
					</td>
				</tr>
				<tr><td colspan="2"></td></tr>
				<tr>
					<td>
						<label for="mWav">Messaggio già caricati</label>
					</td>
					<td>
						<select id="mWav" name="CodMessaggio">
							<option value=""></option>
<%
		log.info(session.getId() + " - dashboard.Emergency_GetList()");
		cstmt = conn.prepareCall("{ call dashboard.Emergency_GetList()} ");
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rs.next()) {
			String Cod_Messaggio = rs.getString("COD_MESSAGGIO");
%>
							<option value="<%=Cod_Messaggio%>"><%=Cod_Messaggio%></option>
<%
		}
%>
						</select>
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="" class="button blue">SALVA</button>
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

<div id="Delete" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			
			<table style="width: 100%">
				<tr>
					<td>	<h2>Elimina</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#Delete').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_emergency_delete" action="Emergency.jsp" method="post">
			<input type="hidden" id="dAction" name="action" Value="DELETE">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label for="dIdMsg">Id Messaggio</label>
					</td>
					<td>
						<input class="bold" type="text" id="dIdMsg" name="Id_Msg" readonly="readonly" style="border: 0px">
					</td>
				</tr>
				<tr><td colspan="2"></td></tr>
				<tr>
					<td>
						<label for="Territorio">Territorio</label>
					</td>
					<td>
						<input class="bold" id="dTerritorio" name="Territorio" readonly="readonly" style="border: 0px">
					</td>
				</tr>
				<tr><td colspan="2"></td></tr>
				<tr>
					<td>
						<label for="dWav">Codice Messaggio</label>
					</td>
					<td>
						<input class="bold" id="dWav" name="CodMessaggio" readonly="readonly" style="border: 0px">
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="CloseModal" class="button blue">Confermi Eleminazione</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="Add" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
				<table style="width: 100%">
				<tr>
					<td>	<h2>Aggiungi un messaggio</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#Add').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add_message" action="UploadDownloadEmergency" method="post" enctype="multipart/form-data">
			<input type="hidden" id="asAction" name="action" Value="upload">
			<table style="width: 100%">
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<label class="bold" for="Day">Attenzione il messaggio aggiunto sarà disponibile anche per altri flow</label>
					</td>
				</tr>
				<tr><td><br></td></tr>			
				<tr>
					<td>
						<label for="add_message_day_stime">Nome: </label>
					</td>
					<td>
						<input id="add_message_name" name="name" type="text"   >
					</td>
				</tr>
				<tr>
					<td colspan="2">
						<input type="file" name="file" id="add_message_file" accept=".wav" class="compress"/>
						<button type="button" id="choosefile" class="button blue" onclick="chooseFile();">Sfoglia&hellip;</button>
						<label for="add_message_file"><span id="add_message_filename" class="bold" style="display: inline-block;"></span></label>
					</td>				
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<label class="bold">Attenzione confermando sarà inserito un messaggio di emergenza disabilitato. </label>	
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="AddCloseModal" class="button blue">Confermi</button>
					</td>						
				</tr>
			</table>
		</form>
	</div>
</div>

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

<div id="Download" class="modal">
	<form id="form_play_message" action="UploadDownloadEmergency" method="post" enctype="multipart/form-data" >
		<input type="text" id="playAction" name="action" Value="download">
		<input type="text" id="playMessage" name="name">
	</form>
</div>

<script>
	playMsg = function(name){
		$("#message_played").text(name);
		if (jQuery.browser.msie) {
			$("#srcPlayMp3").attr("src", "UploadDownloadEmergency?action=play&name="+name+"&browserIE=1");
		} else {
			$("#srcPlayWav").attr("src", "UploadDownloadEmergency?action=play&name="+name+"&browserIE=0");
		}
		var audio = $("#audioPlay");
		try {
			audio[0].pause();
			audio[0].load();
			audio[0].oncanplaythrough = audio[0].play();
		} catch (e) {}
		$('#Play').show();
	}
	
	downloadMsg = function(name){
		$("#playMessage").val(name);
		$("#form_play_message")[0].submit();
	}


	modifyTerritorio = function(id, status, territorio, wav) {
		//Modify.style.display = "block";
		$('#Modify').show();
		$("#mIdMsg").val(id);
		$("#mCurrentState").prop('checked', status);
		$("#mTerritorio").val(territorio);
		$("#mWav").val(wav);
	}
	
	confirmDelete =  function(id, status, territorio, wav) {
		//Delete.style.display = "block"; 			
		$('#Delete').show();
		$("#dIdMsg").val(id);
		$("#dTerritorio").val(territorio);
		$("#dWav").val(wav);
	}
		
	change = function(id, status, territorio, wav){
		modifyTerritorio(id, $(status).prop('checked'), territorio, wav);
		return false;			
	}
	
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#Emergency","Emergency.jsp");
		} catch (e) {
		}
 		$("#stickytable").stickyTableHeaders();
	})

	NewMsg = function() {
		$('#form_emergency_1').attr('action', "Emergency.jsp?action=NEW");
		$("#form_emergency_1").submit();
	}

	loadfile = function() {
		$('#add_message_name').val('');
		$("#add_message_file").val('')
		$("#add_message_filename").html('');
		$("#Add").show();
	}

	chooseFile = function() {
		$("#add_message_file").click();
	}

	$("#add_message_file").on('change', function(e) {
		var fileName = '';
		if( e.target.value )
			fileName = e.target.value.split( '\\' ).pop();
		if( fileName ) {
			$("#add_message_filename").html( fileName );
			var tmp = fileName.substring(0,fileName.length-4);
			var i = tmp.lastIndexOf("\\");
			$('#add_message_name').val(tmp.substring(i+1));
		} else
			$("#add_message_filename").html( '' );
	});
	
// 	$('#add_message_file').on("click", function () {		    this.value = null;		});
// 	$('#add_message_file').on("change", function () {		 
// 		if ($('#add_message_name').val()!=null) 	{
// 			var tmp = this.value.substring(0,this.value.length-4);
// 			var i = tmp.lastIndexOf("\\");
// 			$('#add_message_name').val(tmp.substring(i+1));
// 		}
// 	});
	
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
			$('#Delete').hide();
			$('#Modify').hide();
			$('#Error').hide();
			$('#Add').hide();
			$('#audioPlay')[0].pause();  $('#Play').hide();
			$('#Download').hide();
		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>