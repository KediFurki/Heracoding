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
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Survey</title>
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
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td colspan="1">
			<form action="Survey.jsp" method="post">
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
	//		NONE
	//==============================
	boolean survey = false;
	boolean clsStato = false;
	String Stato = "N";
	try {
		if (StringUtils.isBlank(CodIvr)) {
		} else {
			String PreviousState = request.getParameter("PreviousState");
			String CurrentState = request.getParameter("CurrentState");
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
			case "STC":
				ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"STC");
				log.info(session.getId() + " - connection STC wait...");
				break;
			}
			conn = ds.getConnection();
			if (PreviousState != null && CurrentState != null && !PreviousState.equalsIgnoreCase(CurrentState)) {
				log.info(session.getId() + " - dashboard.Survey_Update('" + CodIvr + "','" + CurrentState + "')");
				cstmt = conn.prepareCall("{ call dashboard.Survey_Update(?,?)} ");
				cstmt.setInt(1, Integer.parseInt(CodIvr));
				cstmt.setString(2, CurrentState);
			} else {
				log.info(session.getId() + " - dashboard.Survey_Get('" + CodIvr + "')");
				cstmt = conn.prepareCall("{ call dashboard.Survey_Get(?)} ");
				cstmt.setInt(1, Integer.parseInt(CodIvr));
			}
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			if (rs.next()) {
				survey = true;
				Stato = rs.getString("SURVEY");
				if (Stato.equalsIgnoreCase("Y"))
					clsStato = true;
%>
				<table class="roundedCorners small">
					<tr class="listGroupItem active blue">
						<td colspan="7">
							<table>
								<tr>
									<td style="width: 30%">
										<div class="title left">Survey</div>
									</td>
									<td >
										<div class="container title right">
										</div>
									</td>
								</tr>
							</table>
						</td>
					</tr>
					<tr style="width: 100%" >
						<td  colspan="7" style="width: 60px">
							<input type="hidden" id="PreviousState" name="PreviousState" value="<%=Stato%>"> <input type="hidden" id="_CurrentState" name="CurrentState" value="<%=Stato%>"> <br> <label class="switch" id="status"> <input type="checkbox" id="iCurrentState"> <span class="slider round blue"></span>
							</label><br> <label id="lCurrentState"></label>
						</td>
					</tr>
					<tr>
						<td colspan="7">
							<button type="submit" class="button blue">SALVA</button>
						</td>
					</tr>
				</table>
<%
			} else {
%>
				<div class="head_parameter_alert" style="text-align: center;">SURVEY NON CONFIGURATA</div>
<%
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
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>

<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#Survey","Survey.jsp");
		} catch (e) {}
		try {
<%	if (survey) { %>
			$('#iCurrentState').prop("checked", <%=(StringUtils.equalsAnyIgnoreCase(Stato, "Y"))%>);
			$('#lCurrentState').html("<%=(StringUtils.equalsAnyIgnoreCase(Stato, "Y") ? "Attiva" : "Disattiva")%>	");
			$('#iCurrentState').change(function() {
				if (this.checked) {
					$('#_CurrentState').val("Y");
					$('#lCurrentState').html("Attiva");
				} else {
					$('#_CurrentState').val("N");
					$('#lCurrentState').html("Disattiva");
				}
			});
<%	} %>
		} catch (e) {
			alert(e);
		}
	})
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>