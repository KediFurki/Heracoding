<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%-- <%@page import="comapp.Node"%> --%>
<%-- <%@page import="java.util.ArrayList"%> --%>
<%-- <%@page import="comapp.Tree"%> --%>
<%-- <%@page import="java.util.Hashtable"%> --%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%-- <%@page import="org.apache.commons.io.IOUtils"%> --%>
<%-- <%@page import="java.nio.charset.StandardCharsets"%> --%>
<%-- <%@page import="comapp.ConfigServlet"%> --%>
<%-- <%@page import="org.json.JSONObject"%> --%>
<%-- <%@page import="java.util.Properties"%> --%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>HighTraffic</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
</head>
<body>
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
	String p_Level = request.getParameter("Level");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+ action+" Level: "+p_Level );			
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
		log.info(session.getId() + " - connection CCTE wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "SAVE" :
				log.info(session.getId() + " - dashboard.LivelloPesse_Update('" + CodIvr + "'," + p_Level + ")");
				cstmt = conn.prepareCall("{ call dashboard.LivelloPesse_Update(?,?)} ");
				cstmt.setInt(1, Integer.parseInt(CodIvr));
				cstmt.setInt(2, Integer.parseInt(p_Level));
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
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td colspan="1">
			<form action="LivelloPesse.jsp" method="post">
				<input type="hidden" id="mAction" name="action" Value="SAVE">
<%
			log.info(session.getId() + " - dashboard.LivelloPesse_Get('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.LivelloPesse_Get(?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			if (rs.next()) {
				int Level = rs.getInt("Level");
				String DataModifica = rs.getString("timestamp");
%>
				<table class="roundedCorners small">
					<tr class="listGroupItem active blue">
						<td>
							<table>
								<tr>
									<td style="width: 30%">
										<div class="title left" >Livello Pesse</div>
									</td>
									<td>
										<div class="container title right">
											<label>Data Modifica: <%=DataModifica %></label>										
										</div>
									</td>
								</tr>
							</table>
						</td>
					</tr>
					<tr style="width: 100%" >
						<td>
							<select id="mLevel" name="Level">
								<option value="0" <%=((Level==0)?"selected":"") %>>Pesse Disabilitato</option>
								<option value="1" <%=((Level==1)?"selected":"") %>>Livello 1</option>
								<option value="2" <%=((Level==2)?"selected":"") %>>Livello 2</option>
								<option value="3" <%=((Level==3)?"selected":"") %>>Livello 3</option>
								<option value="4" <%=((Level==4)?"selected":"") %>>Livello 4</option>
								<option value="5" <%=((Level==5)?"selected":"") %>>Livello 5</option>
							</select>
						</td>
					</tr>
					<tr>
						<td>
							<button type="submit" class="button blue">SALVA</button>
						</td>
					</tr>
				</table>
<%
			} else {
%>
				<div class="head_parameter_alert" style="text-align: center;">LIVELLO PESSE NON CONFIGURATO</div>
<%
			}
%>
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

<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#LivelloPesse","LivelloPesse.jsp");
		} catch (e) {}
	})
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>