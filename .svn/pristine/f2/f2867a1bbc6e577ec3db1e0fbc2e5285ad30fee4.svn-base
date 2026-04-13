<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.json.JSONArray"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Flag</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<meta http-equiv="refresh" content="60">
</head>
<body style="overflow-y: auto;">
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
	//		NONE
	//==============================
	JSONArray jaCodIvr = (JSONArray)session.getAttribute("jaCodIvr");
	JSONArray jaTipo = (JSONArray)session.getAttribute("jaTipo");
	
	try {
		Context ctx = new InitialContext();
		String environment = (String)session.getAttribute("Environment");
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
%>
<form id="form_flag" name="form_flag" action="FlagGlobalDetails.jsp" method="post" style="">
<%--	<input type="hidden" id="mServices" name="Services" value="<%=Services%>"> --%>
	<div id="_left" style="overflow: auto;">
		<table id="_tab" class="roundedCorners small no_top">
<%
		log.info(session.getId() + " - dashboard.Flag_Status('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.Flag_Status(?)} ");
		cstmt.setString(1,environment);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		int i = 0;
		String CodIvr="";
		while (rs.next()) {
			i++;
			String ts = rs.getString("timestamp");
			String _CodIvr = rs.getString("cod_ivr");
			String stato = rs.getString("stato");
			String descrisione = rs.getString("descrizione");
			String tipo = rs.getString("tipo");
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
			
			boolean foundCodIvr = false;
			boolean foundTipo = false;
			
			if (jaCodIvr!=null) {
				for (int k=0; k<jaCodIvr.length(); k++){
					String value = jaCodIvr.getString(k);
					foundCodIvr = StringUtils.equalsIgnoreCase(value, _CodIvr);
					if (foundCodIvr)
						break;
				}
			} else foundCodIvr = true;
			
			if (jaTipo!=null) {
				for (int k=0; k<jaTipo.length(); k++){
					String value = jaTipo.getString(k);
					foundTipo = StringUtils.equalsIgnoreCase(value, tipo);
					if (foundTipo)
						break;
				}
			} else foundTipo = true;
			
			if (!foundCodIvr && !foundTipo)
				continue;
			
			if (foundCodIvr) {
				if (!StringUtils.equalsIgnoreCase(CodIvr,_CodIvr)){
					CodIvr=_CodIvr;
					out.println("<tr class='listGroupItem active blue'>");
					out.println("<td colspan='7'>");
					out.println("<table>");
					out.println("<tr>");
					out.println("<td>");
					out.println("<div class='title'>"+_CodIvr+" "+ descrisione +" </div>");
					out.println("</td>");
					out.println("<td valign='top'>");
	// 				out.println("<label class='switch' style='background-color:white;border-radius: 4px;'> ");
	// 				out.println("<input type='checkbox' id='mCurrentState' name='Status' "+(!found?"checked='true'":"")+" onclick='change(this,\""+CodIvr+"\")'>");
	// 				out.println("<span class='slider round blue'></span>");
	//				out.println("</label>");
					out.println("</td>");
					out.println("</tr>");
					out.println("</table>");
					out.println("</td>");
					out.println("</tr>");
				}
											
				if (foundTipo) {
					out.println("<tr class='listGroupItem' onclick=''>	");
					out.println("<td  >");
					if (StringUtils.equalsIgnoreCase(stato,"on"))
						out.println("<img  class='container_img' src='images/CircleRed_.png'>");
					if (StringUtils.equalsIgnoreCase(stato,"off"))
						out.println("<img  class='container_img' src='images/CircleGreen_.png'>");
					if (StringUtils.equalsIgnoreCase(stato,"aeeg_on"))
						out.println("<img  class='container_img' src='images/CircleYellowX_.png'>");
					if (StringUtils.equalsIgnoreCase(stato,"aeeg_off"))
						out.println("<img  class='container_img' src='images/CircleYellow_.png'>");
					out.println("</td> ");
					out.println("<td  >");
					out.println("<label >"+ tipo+ " </label>");
					out.println("</td> ");
					out.println("<td  >");
					out.println("<label >"+ ts+ " </label>");
					out.println("</td> ");
					out.println("</tr>");
				}
			}
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>
		</table>
	</div>
</form>

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

<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#FlagGlobal");
			
		} catch (e) {
		}
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