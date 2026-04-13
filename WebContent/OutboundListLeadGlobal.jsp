<!DOCTYPE html>
<%@page import="org.apache.commons.lang3.RandomStringUtils"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.apache.log4j.Logger"%>
<%@page import="java.sql.Connection"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OutboundListLeadGlobal</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
<script src="js/jquery.paging.js"></script>
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
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+action  );
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "delete":
// 				String d_ntel = request.getParameter("delete_ntel");
// 				log.info(session.getId() + " - dashboard.AmbienteSpecial_Delete('"+environment+"','"+d_ntel+"')");
// 				cstmt = conn.prepareCall("{ call dashboard.AmbienteSpecial_Delete(?,?)} ");
// 				cstmt.setString(1,environment);
// 				cstmt.setString(2, d_ntel);
// 				cstmt.execute();
// 				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add":
// 				String a_ntel = request.getParameter("add_ntel");
// 				String a_descrizione = request.getParameter("add_descr");
// 				log.info(session.getId() + " - dashboard.AmbienteSpecial_Add('"+environment+"','" + a_ntel + "','"+a_descrizione+"')");
// 				cstmt = conn.prepareCall("{ call dashboard.AmbienteSpecial_Add(?,?,?)} ");
// 				cstmt.setString(1,environment);
// 				cstmt.setString(2, a_ntel);
// 				cstmt.setString(3, a_descrizione);
// 				cstmt.execute();
// 				log.debug(session.getId() + " - executeCall complete");
				break;
			default:
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	} finally {
		try { cstmt.close(); } catch (Exception e) {}
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
%>
<div class="pagination inline_cmd"></div>
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td style="width: 80%">
			<form id="form_without_control" action="AmbienteSpecialGlobal.jsp" method="post">
				<input type="hidden" name="action" value="day_of_year">
				<table id="stickytable" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active green">
						<td colspan='7'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Lista OutBound</div>
									</td>
									<td>
										<div class="container_img title right green " onclick="loadfile();">
											<img alt="" class="" src="images/MenuPlusWhite_.png">
										</div>
									</td>
								</tr>
							</table>
						</td>
					</tr>
			 	 </thead>
<%
	try {
	} catch (Exception e) {
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>
			 	 <tbody id="pagbody">
			 	 </tbody>
			 	 <tfoot>
				</tfoot>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>
<div class="pagination inline_cmd"></div>

<div id="LoadFile" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Carica Lista OutBound</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#LoadFile').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_load_file" enctype="multipart/form-data">
			<input type="hidden" id="asAction" name="action" Value="upload">
			<table style="width: 100%">
				<tr><td><br></td></tr>
				<tr>
				 	<td colspan="2">
						<input type="file" name="uploadfile" id="uploadfile" accept=".csv" class="compress"/>
						<button type="button" id="choosefile" class="button green" onclick="chooseFile();">Sfoglia&hellip;</button>
						<label for="uploadfile"><span id="uploadfilename" class="bold" style="display: inline-block;"></span></label>
				 	</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="button" id="Carica" class="button green" onclick="caricaAmbienteSpecial();">Carica</button>
					</td>						
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="Wait" class="modal">
	<img id="wait_image" class="imagehidden" style='position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);' src='images/Wait.gif'/>
</div>

<div id="Error" class="modal">
	<div class="modal-content">
		<div class="modal-header pink">
			<table style="width: 100%">
				<tbody>
					<tr>
						<td><h2>Error</h2></td>
						<td>
							<div class="container_img title right pink " onclick="$('#form_reload')[0].submit();">
								<img alt="" class="" src="images/CloseWhite_.png">
							</div>
						</td>
					</tr>
				</tbody>
			</table>
		</div>
		<table>
			<tbody><tr><td><br></td></tr>
				<tr>
					<td colspan="1"><label>Error Code:</label></td>
					<td colspan="1"><label id="ErrorCode"></label></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="1"><label>Error Message:</label></td>
					<td colspan="1"><label id="ErrorMessage"></label></td>
				</tr>
				<tr>
					<td colspan="2">
						<button type="button" id="errorButton" class="button pink" onclick="$('#form_reload')[0].submit();">Ok</button>
					</td>
				</tr>
			</tbody>
		</table>
	</div>
</div>

<div id="Info" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tbody>
					<tr>
						<td><h2>Risultato</h2></td>
						<td>
							<div class="container_img title right green " onclick="$('#form_reload')[0].submit();">
								<img alt="" class="" src="images/CloseWhite_.png">
							</div>
						</td>
					</tr>
				</tbody>
			</table>
		</div>
		<table style="width: 100%">
			<tbody><tr><td><br></td></tr>
				<tr>
					<td><label class="bold">Record Letti</label></td>
					<td><label id="Inforead"></label></td>
				</tr>
				<tr>
					<td colspan="2">
						<button type="button" id="infoButton" class="button green" onclick="$('#form_reload')[0].submit();">Ok</button>
					</td>
				</tr>
			</tbody>
		</table>
	</div>
</div>

<div id="Reload" class="modal">
	<form id="form_reload" action="OutboundListLeadGlobal.jsp" method="post">
	</form>
</div>

<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#OutboundListLeadGlobal");			
		} catch (e) {
		}
 		$("#stickytable").stickyTableHeaders();
	})

	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#LoadFile').hide();
		}
	}

	loadfile = function() {
		$("#uploadfile").val('')
		$("#uploadfilename").html('');
		$("#Carica").prop("disabled", true);
		$("#LoadFile").show();
	}

	chooseFile = function() {
		$("#uploadfile").click();
	}

	$("#uploadfile").on('change', function(e) {
		var fileName = '';
		if( e.target.value )
			fileName = e.target.value.split( '\\' ).pop();
		if( fileName ) {
			$("#uploadfilename").html( fileName );
			$("#Carica").prop("disabled", false);
		} else {
			$("#uploadfilename").html( '' );
			$("#Carica").prop("disabled", true);
		}
	});
	
	caricaAmbienteSpecial = function() {
		$("#Wait").fadeIn(500);
		$("#wait_image").removeClass("imagehidden");
		$("#wait_image").addClass("imagevisible");
		var formData = new FormData(document.getElementById("form_load_file"));
		$.ajax({
			url : 'UploadOutboundListLead',
			type : 'POST',
			data : formData,
			contents: "json",
			processData: false,  // tell jQuery not to process the data
			contentType: false,  // tell jQuery not to set contentType
			success : function(result) {
				var data = JSON.parse(result);
				if (data.res == "OK") {
					$('#LoadFile').hide();
					$('#Inforead').html(data.read);
					$('#Info').show();
				} else {
					$('#LoadFile').hide();
					$('#ErrorCode').html(data.errcode);
					$('#ErrorMessage').html(data.err);
					$('#Error').show();
				}
				$("#Wait").fadeOut(500);
				$("#wait_image").removeClass("imagevisible");
				$("#wait_image").addClass("imagehidden");
				return;
			},
			error: function(error) {
				$("#Wait").fadeOut(500);
				$("#wait_image").removeClass("imagevisible");
				$("#wait_image").addClass("imagehidden");
				$('#ErrorCode').html("ServLet Exception");
				$('#ErrorMessage').html(error);
				$('#Error').show();
				return;
			}
		});
	}	
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>