<!DOCTYPE html>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="org.apache.log4j.Logger"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="javax.naming.Context"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<!-- <link rel="icon" rel="images/red.ico"> -->
<link rel="shortcut icon" href="images/favicon.ico" />
<title>Hera DashBoard</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
</head>
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
	String descService = "Servizio non selezionato";
	if (StringUtils.isNotBlank(CodIvr)) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			String environment = (String)session.getAttribute("Environment");
			ds = (DataSource) ctx.lookup("java:/comp/	env/jdbc/"+ConfigServlet.web_app+"CCC");
			log.info(session.getId() + " - connection CCC wait...");
			conn = ds.getConnection();
			log.info(session.getId() + " - dashboard.Service_List('"+environment+"','"+CodIvr+"')");
			cstmt = conn.prepareCall("{ call dashboard.Service_List(?,?)}");
			cstmt.setString(1,environment);
			cstmt.setString(2,CodIvr);
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			if (rs.next()) {
				descService = rs.getString("COD_IVR") + " - " + rs.getString("LABEL") + " - "+ rs.getString("DESCRIZIONE");
			}
		} catch (Exception e) {
			log.error(session.getId() + " - general: " + e.getMessage(), e);
		} finally {
			try { rs.close(); } catch (Exception e) {}
			try { cstmt.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}
	}
	boolean ap_mail = false;
	try {
		ap_mail = (boolean) session.getAttribute("AP-MAIL");
	} catch (Exception ex) {
	}
	boolean ap_mail_rt = false;
	try {
		ap_mail_rt = (boolean) session.getAttribute("AP-MAIL-RT");
	} catch (Exception ex) {
	}
	boolean ap_infomart = false;
	try {
		ap_infomart = (boolean) session.getAttribute("AP-INFOMART");
	} catch (Exception ex) {
	}	
%>
<body>
	<table style="width: 100%; top: -3px">
		<tr>
			<td>
				<table id="Header" class="navbar green">
					<tr>
						<td style="width: 70%">
							<table class="left">
								<tr>
									<td><a class="green" id="ServiceGlobal" href="ServiceGlobal.jsp" target="_MainIframe">Lista Servizi</a></td>
									<td><a class="green" id="AutorityGlobal" href="AutorityGlobal.jsp" target="_MainIframe">Autorità</a></td>
									<td><a class="green" id="CalendarGlobal" href="CalendarGlobal.jsp" target="_MainIframe">Calendario</a></td>
									<td><a class="green" id="SwitchGlobal" href="SwitchGlobal.jsp" target="_MainIframe">Switch Generale</a></td>
									<td><a class="green" id="FlagGlobal" href="FlagGlobal.jsp" target="_MainIframe">Flag</a></td>
									<td><a class="green" id="ListGlobal" href="ListGlobal.jsp" target="_MainIframe">Liste</a></td>
									<td><a class="green" id="AgentStateGlobal" href="AgentStateGlobal.jsp" target="_MainIframe">Stato Agente</a></td>
									<td><a class="green" id="PerturbedTrafficGlobal" href="PerturbedTrafficGlobal.jsp" target="_MainIframe">Traffico Perturbato</a></td>
<% 	if (ap_mail) { %>
									<td><a class="green" id="MailWeb" href="MailWeb.jsp" target="_MainIframe">Web Mail</a></td>
<% 	} %>
<% 	if (ap_infomart) { %>
									<td><a class="green" id="InfomartGlobal" href="InfomartGlobal.jsp" target="_MainIframe">Esclusione Giornate Infomart</a></td>
<% 	} %>
								</tr>
							</table>
						</td>
						<td style="width: 4%">
							<table class="center">
								<tr>
									<td>
										<div class="container_img green title" onclick="location.href='index.jsp'">
											<img alt="" src="images/ShutDownWhite_.png">
										</div>
									</td>
								</tr>
							</table>
						</td>
						<td>
							<div class="left"><a class="green bold "><%=ConfigServlet.applVersion%></a></div>
							<div class="right"><a class="pink bold" id="ServiceDetails" href="Privacy.jsp" target="_MainIframe" class='title1'><%=descService %></a></div>
						</td>
					</tr>
				</table>
			</td>
		</tr>
		<tr>
			<td>
				<iframe src="ServiceGlobal.jsp" id="MainIframe" name="_MainIframe" style="width: 100%; border: 0 solid grey"></iframe>
			</td>
		</tr>
		<tr>
			<td>
				<table id="Footer" class="navbar bottom blue">
					<tr>
						<td style="width: 60%">
							<table class="left">
								<tr>
									<td><a class="blue" id="Privacy" href="Privacy.jsp" target="_MainIframe">Privacy</a></td>
									<td><a class="blue" id="Emergency" href="Emergency.jsp" target="_MainIframe">Emergenza</a></td>
									<td><a class="blue" id="Calendar" href="Calendar.jsp" target="_MainIframe">Calendario</a></td>
									<td><a class="blue" id="HighTraffic" href="HighTraffic.jsp" target="_MainIframe">Alto traffico</a></td>
									<td><a class="blue" id="Switch" href="Switch.jsp" target="_MainIframe">Switch</a></td>
									<td><a class="blue" id="Autority" href="Autority.jsp" target="_MainIframe">Autorità</a></td>
									<td><a class="blue" id="PerturbedTraffic" href="PerturbedTraffic.jsp" target="_MainIframe">Traffico Perturbato</a></td>
								</tr>
							</table>
						</td>
						<td>
						<% if (ap_mail_rt) { %>
							<table class="right">
								<tr>
									<td><a class="blue" id="Agents" href="rt/AgentDetails.jsp" target="_MainIframe">Dettaglio Agenti</a></td>
									<td><a class="blue" id="Graphs" href="rt/Graphs.jsp" target="_MainIframe">Grafici Previsionali</a></td>
									<td><a class="blue" id="Details" href="rt/Details.jsp" target="_MainIframe">Dettaglio Ivr</a></td>
								</tr>
							</table>
							<%} %>
						</td>
					</tr>
				</table>
			</td>
		</tr>
	</table>

	<script type="text/javascript">
		$(function() {
			$("#MainIframe").height($(window).height() - $("#Footer").height() - $("#Header").height() - 10);
			$("#Footer").width($(window).width() - 5);
			$(window).resize(function() {
				$("#MainIframe").height($(window).height() - $("#Footer").height() - $("#Header").height() - 10);
				$("#Footer").width($(window).width() - 5);
			});
		})

		ChangeActiveMenu = function(menu) {
			$("#ServiceGlobal").removeClass("active");
			$("#AutorityGlobal").removeClass("active");
			$("#CalendarGlobal").removeClass("active");
			$("#SwitchGlobal").removeClass("active");
			$("#FlagGlobal").removeClass("active");
			$("#ListGlobal").removeClass("active");
			$("#AgentStateGlobal").removeClass("active");
			$("#PerturbedTrafficGlobal").removeClass("active");
// 			$("#SelfReadingGlobal").removeClass("active");
			$("#MailWeb").removeClass("active");
			$("#InfomartGlobal").removeClass("active");
			//	$("#SurveyWeb").removeClass("active");
// 			$("#WizardGlobal").removeClass("active");
			//$("#ServiceDetails").removeClass("active");		
			$(menu).addClass("active");
			$("#Footer").css('display', 'none');
		}

		ChangeActivedFooter = function(menu, href) {
			$("#Footer").css('display', 'inline-table');
			$("#Agents").removeClass("active");
			$("#Graphs").removeClass("active");
			$("#Details").removeClass("active");

			$("#Privacy").removeClass("active");
			$("#Emergency").removeClass("active");
			$("#Calendar").removeClass("active");
			$("#HighTraffic").removeClass("active");
			$("#Switch").removeClass("active");
			$("#Autority").removeClass("active");
			$("#PerturbedTraffic").removeClass("active");
// 			$("#SelfReading").removeClass("active");
// 			$("#Parameters").removeClass("active");
			if (menu != undefined) {
				$(menu).addClass("active");
				if (href != undefined) {
					$("#ServiceDetails").attr("href", href);
				}
			}
		}

		SetServiceSelected = function(serv) {
			$("#ServiceDetails").html(serv);
			$("#ServiceDetails").addClass("active");
		}
	</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>
