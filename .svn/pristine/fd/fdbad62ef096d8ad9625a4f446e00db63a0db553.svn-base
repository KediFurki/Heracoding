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
<title>Switch</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
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
	String status = request.getParameter("status");
	String codnodo = request.getParameter("codnodo");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - Action:" + action + "Status: " + status + "  CodNodo:" + codnodo);
	try {
		Context ctx = new InitialContext();
		DataSource ds = null;
		String environment = (String)session.getAttribute("Environment");
		switch (environment) {
// 		case "CCT-F":
// 			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTF");
// 			log.info(session.getId() + " - connection CCTF wait...");
// 			break;
		case "CCT-E":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
			log.info(session.getId() + " - connection CCTE wait...");
			break;
		case "CCT-A":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTA");
			log.info(session.getId() + " - connection CCTA wait...");
			break;
		}
		conn = ds.getConnection();
		status = status == null ? "OFF" : status.toUpperCase();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "update" :
				log.info(session.getId() + " - dashboard.ServiceSwitch_Update('" + CodIvr + "','" + codnodo+ "','" + status + "')");
				cstmt = conn.prepareCall("{ call dashboard.ServiceSwitch_Update(?,?,?)} ");
				cstmt.setInt(1, Integer.parseInt(CodIvr));
				cstmt.setString(3, status);
				cstmt.setString(2, codnodo);
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	} finally {
		try { cstmt.close(); } catch (Exception e) {}
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
	try {
		if (StringUtils.isBlank(CodIvr)) {
		} else {
%>
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td colspan="1">
			<form id="form_switch_1" action="SwitchCCT.jsp" method="post">
				<table id="stickytable" class="roundedCorners small">
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan="7">
								<table>
									<tr>
										<td>
											<div class="title">Switch Node</div>
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
			log.info(session.getId() + " - dashboard.ServiceSwitch_Get('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.ServiceSwitch_Get(?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			rs = cstmt.executeQuery();
			log.debug(session.getId() + " - executeCall complete");
			while (rs.next()) {
				String TimeStamp = rs.getString("TimeStamp");
				String Status = rs.getString("Stato");
				String CodNodo = rs.getString("Cod_Nodo");
				String Etichetta = rs.getString("Etichetta");
%>
					<tr style="width: 100%" class="listGroupItem">
						<td >
							<label class="switch" id="status">
								<input type="checkbox" id="iCurrentState" name="status" value="<%=Status%>" <%=((Status.equalsIgnoreCase("ON")) ? "checked=true" : "")%> onclick="return save('<%=CodNodo%>', this)"> 
								<span class="slider round blue"></span>
							</label>
						</td>
						<td>
							<%=Etichetta%>
						</td>
						<td><%=TimeStamp%>
						</td>
					</tr>
<%
			}
%>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>
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

<div id="Save" class="modal">
	<form id="form_save" action="SwitchCCT.jsp" method="post" >
		<input type="text" id="saction" name="action" Value="update"> 
		<input type="text" id="cCodNodo" name="codnodo" >
		<input type="text" id="cStato" name="status" >
	</form>
</div>

<script>
	save = function(CodNodo, Status) {
		$("#cCodNodo").val(CodNodo);
		$("#cStato").val($(Status).prop('checked')?"ON":"OFF");
		$("#form_save")[0].submit();
	}

	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#SwitchCCT","SwitchCCT.jsp");
		} catch (e) {
		}
 		$("#stickytable").stickyTableHeaders();
	})

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
			$('#Error').hide();
		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>