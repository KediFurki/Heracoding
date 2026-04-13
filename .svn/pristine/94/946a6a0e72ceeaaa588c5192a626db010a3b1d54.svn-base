<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.json.JSONArray"%>
<%@page import="java.util.Arrays"%>
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
<title>FrostEmergencyGlobal</title>
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
	//==============================
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================
%>
	
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td>
			<table class="roundedCorners smallnb no_bottom">
				<tr class="listGroupItem active green">
					<td colspan='5'>
						<table>
							<tr>
								<td>
									<div class="title">Emergenza Gelo</div>
								</td>
								<td valign="top">
								</td>
							</tr>
						</table>
					</td>
				</tr>
				<tr>
					<td><label for="ConnId">ConnId</label></td>
					<td><input type="text" id="ConnId" name="ConnId"></td>
					<td><label for="Stato">Stato</label></td>
					<td>
						<select id="Stato" name="Stato" readonly=true>
							<option value=""></option>
							<option value="Da Lavorare">Da Lavorare</option>
							<option value="In Lavorazione">In Lavorazione</option>
							<option value="Lavorato">Lavorato</option>
						</select>
					</td>
					<td rowspan="2">
						<div class="container_img title right "  onclick="return search()">
							<img alt="" class="" src="images/SearchGrey_.png">
						</div>
					</td>
				</tr>
				<tr>
					<td><label for="Dal">Dal</label></td>
					<td><input type="date" id="Dal" name="Dal"></td>
					<td><label for="Al">Al</label></td>
					<td><input type="date" id="Al" name="Al"></td>
				</tr>
			</table>
			<iframe id="iFrostEmergencyGlobalDetails" name ="iFrostEmergencyGlobalDetails"  src="FrostEmergencyGlobalDetails.jsp?action=start"  id="" style="width: 100%; height: 100%; border: 0px"></iframe>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>

<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#FrostEmergencyGlobal");
			$("#iFrostEmergencyGlobalDetails").height($(window).height()-130 );
			
			$(window).resize(function() {
				$("#iFrostEmergencyGlobalDetails").height($(window).height()-130 );		
				
			});	
		} catch (e) {}
	})

	search = function () {
		$('#iFrostEmergencyGlobalDetails').attr('src', "FrostEmergencyGlobalDetails.jsp?action=search&ConnId="+$("#ConnId").val()+"&Dal="+$("#Dal").val()+"&Al="+$("#Al").val()+"&Stato="+$("#Stato").val())
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>