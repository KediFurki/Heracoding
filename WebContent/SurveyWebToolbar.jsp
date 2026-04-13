<!DOCTYPE html>
<%-- <%@page import="comapp.Node"%> --%>
<%@page import="java.util.Locale"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Date"%>
<%@page import="java.time.LocalTime"%>
<%@page import="java.time.format.DateTimeFormatter"%>
<%@page import="java.text.DateFormat"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Statement"%>
<%@page import="org.apache.jasper.tagplugins.jstl.core.ForEach"%>
<%@page import="java.util.Calendar"%>
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
<%@page import="java.util.Properties"%>
<%@page import="org.apache.log4j.Logger"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Survey Web</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
</head>
<body>
	<%
	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");
		String user = (String)session.getAttribute("UserName");
		if (StringUtils.isBlank(user))
			user = "-";
		user = Character.toUpperCase(user.charAt(0)) + user.substring(1);
		//==	REQUEST PARAMETERS	====
		//		NONE
		//==============================
// 		Properties prop = ConfigServlet.getProperties();
		Connection conn = null;
		CallableStatement cs = null;
		ResultSet rs = null;
		//==	ACTION PARAMETERS	====
		//		NONE
		//==============================
	%>
	<table class="roundedCorners">

		<tr class="listGroupItem">
			<td>
				<label>Giorno</label>
					<div id="Giorno" style="position: relative; float: right; height: 1em; width: 1em; border-radius: 0.5em; background-color: red; margin: 0; padding: 0;"></div>
				
				

			</td>
		</tr>
		<tr class="listGroupItem">
			<td>

				<label>Orario</label>

				<div id="Orario" style="position: relative; float: right; height: 1em; width: 1em; border-radius: 0.5em; background-color: red; margin: 0; padding: 0;"></div>
			</td>
		</tr>
		<tr class="listGroupItem">
			<td>
				
				<div id="msg" style=" height: 1em; width: 1em; border-radius: 0.5em; background-color: green; margin: 0; padding: 0; float: right;"></div>
				<label>Msg. regime perturbato</label>
				
				

			</td>
		</tr>
		<tr class="listGroupItem">
			<td>

				<label>Campagne PESSE</label>

				<div id="PESSE" style="position: relative; float: right; height: 1em; width: 1em; border-radius: 0.5em; background-color: green; margin: 0; padding: 0;"></div>
			</td>
		</tr>
		<tr class="listGroupItem">
			<td>

				<label> <%=(new SimpleDateFormat("d-MMM-YYYY hh.mm.ss", Locale.ITALY)).format(new Date())%></label>

			</td>
		</tr>
	</table>







	<script type="text/javascript">
		$(function() {
			try {
				parent.ChangeActiveMenu("#SurveyWeb");
			} catch (e) {
			}
		})
	</script>

	<%
		try {
			Context ctx = new InitialContext();
			DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
			log.info(session.getId() + " - connection CCTE wait...");
			conn = ds.getConnection();
			
			log.info(session.getId() + " - dashboard.SurveyWeb_Toolbar_GetInTime(" + (Calendar.getInstance()).get(Calendar.DAY_OF_WEEK)+ ")");
			cs = conn.prepareCall("{call dashboard.SurveyWeb_Toolbar_GetInTime(?)}");
			cs.setInt(1,(Calendar.getInstance()).get(Calendar.DAY_OF_WEEK) );
			rs = cs.executeQuery();
			log.debug(session.getId() + " - executeCall complete");
	 
			if (rs.next() == false) {
	%>
	<script type="text/javascript">
		jQuery('#Orario').css("background-color", "red");
	</script>
	<%
		} else {
	%>
	<script type="text/javascript">
		jQuery('#Giorno').css('background-color', 'green');
	</script>
	<%
			String dalle = "";
			String alle = "";
			DateFormat sdf = new SimpleDateFormat("HH:mm");
			do {
				dalle = rs.getString("dalle");
				alle = rs.getString("alle");
			} while (rs.next());
			
			Date dNow = sdf.parse(sdf.format(new Date()));
			Date dDalle = sdf.parse(dalle);
			Date dAlle = sdf.parse(alle);

			log.info(session.getId() + " -  dNow: " +dNow);
			log.info(session.getId() + " -  dDalle: " +dDalle);
			log.info(session.getId() + " -  dAlle: " +dAlle);
			if (dNow.after(dDalle) && dNow.before(dAlle)) {
				%>
				<script type="text/javascript">
					jQuery('#Orario').css('background-color', 'green');
				</script>
				<%
			}
		}
		rs.close();
		cs.close();
		
		log.info(session.getId() + " - dashboard.SurveyWeb_Toolbar_GetMessage()");
		cs = conn.prepareCall("{call dashboard.SurveyWeb_Toolbar_GetMessage()}");
		rs = cs.executeQuery();
		log.debug(session.getId() + " - executeCall complete");
		if (rs.next() == false) {
	%>
	<script type="text/javascript">
		jQuery('#msg').css("background-color", "green");
	</script>
	<%
		} else {
				do {
					int tot = rs.getInt("TOTAL");
					if (tot > 0) {
	%>
	<script type="text/javascript">
		jQuery('#msg').css('background-color', 'red');
	</script>
	<%
		}
				} while (rs.next());
			}
			rs.close();
			cs.close();
			log.info(session.getId() + " - dashboard.SurveyWeb_Toolbar_GetPESSE()");
			cs = conn.prepareCall("{call dashboard.SurveyWeb_Toolbar_GetPESSE()}");
			rs = cs.executeQuery();
			log.debug(session.getId() + " - executeCall complete");
			
			if (rs.next() == false) {
	%>
	<script type="text/javascript">
		jQuery('#PESSE').css("background-color", "green");
	</script>
	<%
		} else {
				do {
					int tot = rs.getInt("TOTAL");
					if (tot > 0) {
	%>
	<script type="text/javascript">
		jQuery('#PESSE').css('background-color', 'red');
	</script>
	<%
		}
				} while (rs.next());
			}
			log.info(session.getId() + " Toolbar complete");
		} catch (Exception e) {
			log.error(session.getId() + " - general: " + e.getMessage(), e);
		} finally {
			try { rs.close(); } catch (Exception e) {}
			try { cs.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}
	%>

</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>