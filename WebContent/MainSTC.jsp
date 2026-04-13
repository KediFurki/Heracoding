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
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"STC");
			log.info(session.getId() + " - connection STC wait...");
			conn = ds.getConnection();
			log.info(session.getId() + " - dashboard.Service_List('"+environment+"','"+CodIvr+"')");
			cstmt = conn.prepareCall("{ call dashboard.Service_List(?,?)}");
			cstmt.setString(1,environment);
			cstmt.setInt(2,Integer.parseInt(CodIvr));
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
	boolean cct_f_rt = false;
	try {
		cct_f_rt = (boolean) session.getAttribute("CCT-F-RT");
	} catch (Exception ex) {
	}
%>
<body>
	<table style="width:100%; top:-3px" >
		<tr>
			<td>
				<table id="Header" class="navbar green">
					<tr>
						<td style="width: 48%" >
							<table class="left">
								<tr>
									<td><a class="green" id="ServiceGlobal" href="ServiceGlobalSTC.jsp" target="_MainIframe">Lista Servizi</a></td>
									<td><a class="green" id="ServiceGlobal" href="SurveyWeb.jsp" target="_MainIframe">Survey</a></td>
									<td><a class="green" id="CalendarGlobal" href="CalendarCCTGlobal.jsp" target="_MainIframe">Calendario</a></td>
								</tr>
							</table>
						</td>
						<td style="width: 4%">
							<table class="center">
								<tr>
									<td>
										<div class="container_img green title" onclick="location.href='index.jsp'"> 
											<img alt=""  src="images/ShutDownWhite_.png">
										</div>
									</td>
								</tr>
							</table>
						</td>
						<td>
							<div class="left"><a class="green bold "><%=ConfigServlet.applVersion%></a></div>
							<div class="right"><a class="pink bold" id="ServiceDetails" href="Priority.jsp" target="_MainIframe" class='title1'><%=descService %></a></div>
						</td>
					</tr>
				</table>
			</td>
		</tr>
		<tr>
			<td >	
				<iframe src="ServiceGlobalSTC.jsp" id="MainIframe" name="_MainIframe" style="width: 100%;  border: 0 solid grey"></iframe>
			</td>
		</tr>
		<tr >
			<td>
				<table id="Footer" class="navbar bottom blue">
					<tr>
						<td style="width: 60%">
							<table class="left">
								<tr>
									<td><a class="blue" id="Priority" href="Priority.jsp" target="_MainIframe">Priorità</a></td>
									<td><a class="blue" id="SetEmergency" href="SetEmergency.jsp" target="_MainIframe">Impostazione Messaggi</a></td>
									<td><a class="blue" id="Survey" href="Survey.jsp" target="_MainIframe">Survey</a></td>
									<td><a class="blue" id="Calendar" href="CalendarCCT.jsp" target="_MainIframe">Calendario</a></td>
									<td><a class="blue" id="DissuasionSTC" href="DissuasionSTC.jsp" target="_MainIframe">Dissuasione</a></td>
									<td><a class="blue" id="DistributionSTC" href="DistributionSTC.jsp" target="_MainIframe">Distribuzione</a></td>
								</tr>
							</table>
						</td>
						<td >
							<table class="right" >
								<tr>
									<td>
										
									</td>
								</tr>
							</table>
						</td>
					</tr>
				</table>
			</td>
		</tr>
	</table>
		
	<script type="text/javascript">
		$(function() {
			$("#MainIframe").height($(window).height() - $("#Footer").height()-$("#Header").height()-10);
			$("#Footer").width($(window).width() - 5);
			$(window).resize(function() {
				$("#MainIframe").height($(window).height() - $("#Footer").height()-$("#Header").height()-10);
				$("#Footer").width($(window).width() - 5);
				
			});
		})
	
		ChangeActiveMenu = function(menu) {
			$("#ServiceGlobal").removeClass("active");
			$("#SurveyWeb").removeClass("active");
			$("#CalendarGlobal").removeClass("active");
			//$("#ServiceDetails").removeClass("active");	
			$(menu).addClass("active");
			$("#Footer").css('display', 'none');
		}
		
		ChangeActivedFooter = function(menu,href) {
			$("#Footer").css('display', 'inline-table');				
	
			$("#Priority").removeClass("active");
			$("#SetEmergency").removeClass("active");
			$("#Survey").removeClass("active");
			$("#Calendar").removeClass("active");
			$("#DissuasionSTC").removeClass("active");
			$("#DistributionSTC").removeClass("active");
			if (menu != undefined) {
				$(menu).addClass("active");
				if (href != undefined) {
					$("#ServiceDetails").attr("href",href);
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
