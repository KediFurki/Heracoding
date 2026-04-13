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
	//==============================
	boolean oblead_bck = false;
	try {
		oblead_bck = (boolean) session.getAttribute("OBLEAD-BCK");
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
									<td><a class="green" id="OutboundListLeadGlobal" href="OutboundListLeadGlobal.jsp" target="_MainIframe">Lista Lead OutBound</a></td>
<% if (oblead_bck) { %>
									<td><a class="green" id="OutboundListBckGlobal" href="OutboundListBckGlobal.jsp" target="_MainIframe">Lista Lead OutBound Backend</a></td>
<% } %>
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
								</tr>
							</table>
						</td>
						<td>
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
			$("#OutboundListLeadGlobal").removeClass("active");
			$("#OutboundListBckGlobal").removeClass("active");
			$(menu).addClass("active");
			$("#Footer").css('display', 'none');
		}
	</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>
