<%@page import="org.apache.catalina.Session"%>
<%@page import="javax.security.auth.message.callback.PrivateKeyCallback.Request"%>
<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="java.util.Properties"%>
<%@page import="org.apache.log4j.Logger"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mail Web</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
</head>
<body>
<%
	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");
	
	String id= request.getParameter("id");
%>
<table class="center">
	<tbody>
		<tr>
			<td colspan="2" style="width: 30%">
				<div id ="email_view"></div>
			</td>
		</tr>
	</tbody>
</table>
	
<div id="Error" class="modal">
	<div class="modal-content">
		<div class="modal-header pink">
			<table style="width: 100%">
				<tbody><tr>
					<td><h2>Error</h2></td>
					<td>
						<div class="container_img title right pink " onclick="$('#Error').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</tbody></table>
		</div>
		<input type="hidden" id="asAction" name="action" value="add_message">
		<table>
			<tbody><tr><td><br></td></tr>
			<tr>
				<td colspan="1"><label>Error Code:</label></td>
				<td colspan="1"><label id="ErrorCode"></label></td>
			</tr>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="1"><label>Error Message:</label></td>
				<td colspan="1"><label id="ErrorMessage"></label></td>
			</tr>
			<tr>
				<td colspan="2">
					<button type="button" id="AddCloseModal" class="button pink" onclick="$('#Error').hide();">Ok</button>
				</td>
			</tr>
		</tbody></table>
	</div>
</div> 
	
<script>
	$(function() {
		var action = "MailWebQuery.jsp?command=OpenMail&id=<%=id%>&Download=NO";
		$.ajax({
	        url: action,
	        type: 'post',
	        success: function(data) {
	        	 jQuery("#email_view").append(data.replace(/(\r\n|\n|\r)/gm, ""));
	        },
	        error:function(data) {
	        	console.log(data);
	        } 
		});
	})

	$(document).on("click", "#Esporta", function(){
		var link = "MailWebQuery.jsp?command=OpenMail&id=<%=id%>&Download=SI";
		$("#download").prop('href', link);
	});
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>