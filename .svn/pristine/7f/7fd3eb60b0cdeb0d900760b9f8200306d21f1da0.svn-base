<!DOCTYPE html>
<%@page import="comapp.LogonData"%>
<%@page import="org.apache.commons.text.StringEscapeUtils"%>
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
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================
	LogonData ld = (LogonData)session.getAttribute("LogonData");
	String PersonProfile = ld.getPersonProfile();
%>
<table class="center"  >
	<tr>
		<td style="width: 10%"></td>
		<td colspan="2" >
			<form id="form_emergency_1" action="ServiceGlobal.jsp" method="post">
				<table id="stickytable" class="roundedCorners ">
				<thead>
					<tr class="listGroupItem active green">
						<th colspan="3">
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
				</thead>
<%
	try {
		Context ctx = new InitialContext();
		DataSource ds = null;
		String firstPage = "";
		String environment = (String)session.getAttribute("Environment");
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		log.info(session.getId() + " - dashboard.Service_List('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.Service_List(?)}");
		cstmt.setString(1,environment);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rs.next()) {
			String COD_IVR = rs.getString("COD_IVR");
			String LABEL = rs.getString("LABEL");
			String DESCRIZIONE = rs.getString("DESCRIZIONE");
		
			if (!PersonProfile.equals("ADMIN-CCC")) {
				firstPage = "'Privacy.jsp?CodIvr=" + rs.getString("COD_IVR") + "'";
			} else {
				firstPage = "'MapEsigenze.jsp?CodIvr=" + rs.getString("COD_IVR") + "'";
			}
//			String descService = "'"+StringEscapeUtils.escapeEcmaScript(rs.getString("COD_IVR") + " - " + rs.getString("LABEL") + " - "+ rs.getString("DESCRIZIONE"))+"'";
			String descService = "'"+StringEscapeUtils.escapeEcmaScript(COD_IVR + " - "+ LABEL + " - "+ DESCRIZIONE)+"'";
%>
					<tr class='listGroupItem' onclick="parent.SetServiceSelected(<%=descService%>); window.location.assign(<%=firstPage%>)">
						<td style='width:80px'><%=COD_IVR%></td>
						<td style='width:190px'><%=LABEL%></td>
						<td><%=DESCRIZIONE%></td>
					</tr>
<%		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>
<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceGlobal");
		} catch (e) {}
 		$("#stickytable").stickyTableHeaders();
	})
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>