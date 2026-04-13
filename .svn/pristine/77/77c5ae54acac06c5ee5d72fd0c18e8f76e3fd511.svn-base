<!DOCTYPE html>
<%-- <%@page import="comapp.Node"%> --%>
<%-- <%@page import="java.util.ArrayList"%> --%>
<%-- <%@page import="comapp.Tree"%> --%>
<%-- <%@page import="java.util.Hashtable"%> --%>
<%@page import="comapp.ConfigServlet"%>
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
	//Properties prop = ConfigServlet.getProperties();
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================
%>
<table class="center" style="height: 100%; ">
	<tr style="height: 33%">
		<td style="width: 10%; "></td>
		<td rowspan="3">
			<iframe src="SurveyWebMSurvey.jsp" id="_iframe" name="_iframe" style="width: 100%; height: 100%; margin-top:10px; border: 0 solid grey;  mergin: 0;"></iframe>
		</td>
		<td style="width: 10%" rowspan="3"></td>
	</tr>
	<tr>
		<td> 
			<table>
				<tr>
					<td>
						<table class="roundedCorners">
							<tr class="listGroupItem active green">
								<td>
									<a id="MSurvey" class="button green active" href="SurveyWebMSurvey.jsp" target="_iframe" onclick="$('a').removeClass('active'); $(this).addClass('active');">
										<nobr>Gestione survey</nobr>
									</a>
								</td>
							</tr>
							<tr class="listGroupItem active green">
								<td>
									<a id="MCallList" class="button green" href="SurveyWebMCallList.jsp" target="_iframe"  onclick="$('a').removeClass('active'); $(this).addClass('active');">
										<nobr> Gestione liste di chiamata</nobr>
									</a>
								</td>
							</tr>
							<tr class="listGroupItem active green">
								<td>
									<a id="MDayTime" class="button green" href="SurveyWebMDayTime.jsp" target="_iframe"  onclick="$('a').removeClass('active'); $(this).addClass('active');">
										<nobr> Gestione giorni e orari</nobr>
									</a>
								</td>
							</tr>
						</table>
						<iframe src="SurveyWebToolbar.jsp" id="TollbarIframe" name="_ToolbarIframe" style="width: 100%; height: 300px; border: 0 solid grey;  mergin: 0;"></iframe>
					</td>
				</tr>
			</table>
		</td>
	</tr>
</table>
<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#SurveyWeb");
		} catch (e) {
		}
		$("#_iframe").height($(window).height() - 30);
		$(window).resize(function() {
			$("#_iframe").height($(window).height() - 30);
		});
	})
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>