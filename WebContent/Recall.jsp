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
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Recall</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/getBrowser.js"></script>
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
	String p_Status = request.getParameter("Status");
	p_Status = p_Status==null?"OFF":p_Status.toUpperCase();
	String p_MaxRecall = request.getParameter("MaxRecall");
	String p_Interval = request.getParameter("Interval");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+ action+" Status: "+p_Status+" MaxRecall:"+p_MaxRecall+" Interval:"+p_Interval );			
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "SAVE" :
				log.info(session.getId() + " - dashboard.Recall_Update('" + CodIvr + "','"+ p_Status + "','" + p_MaxRecall + "','" + p_Interval + "')");
				cstmt = conn.prepareCall("{ call dashboard.Recall_Update(?,?,?,?)} ");
				cstmt.setString(1, CodIvr);
				cstmt.setString(2, p_Status);
				cstmt.setInt(3, Integer.parseInt(p_MaxRecall));
				cstmt.setInt(4, Integer.parseInt(p_Interval));
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
<table class="center"  >
	<tr>
		<td style="width: 10%"></td>
		<td style="width: 50%">
			<form id="form_recall_1" action="Recall.jsp" method="post">
				<input type="hidden" id="mAction" name="action" Value="SAVE"> 
				<table class="roundedCorners small" >
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan="7">
								<table>
									<tr>
										<td>
											<div class="title">Recall</div>
										</td>
										<td>
											<div class="container title right">
												<label></label>										
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
					<tr style="width: 100%" class="listGroupItem" >
<%
			boolean configPresent = false;
			log.info(session.getId() + " - dashboard.Recall_Get('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.Recall_Get(?)} ");
			cstmt.setString(1, CodIvr);
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			if (rs.next()) {
				configPresent = true;
				String Status = rs.getString("STATO");
				String DataModifica = rs.getString("TIMESTAMP");
				int MaxRecall = rs.getInt("MAXRECALL");
				int Interval = rs.getInt("INTERVAL");
%>
						<td>
							<label class="switch"> <input type="checkbox" id="status" name="Status" <%=((Status.equalsIgnoreCase("ON")) ? "checked=true" : "")%>> <span class="slider round blue"></span></label>
						</td>
						<td>
							<label>Data Modifica: <%=DataModifica %></label>
						</td>
						<td>
							<label>Max Recall:</label>
							<input type="number" id="MaxRecall" name="MaxRecall" min="0" value="<%=MaxRecall%>">
						</td>
						<td>
							<label>Intervallo:</label>
							<input type="number" id="Interval" name="Interval" min="0" max="24" value="<%=Interval%>">
						</td>
					</tr>
					<tr>
						<td colspan="4">
							<button type="submit" class="button blue">SALVA</button>
						</td>
<%
			} else {
%>
						<td>
							<div class="head_parameter_alert" style="text-align: center;">RECALL NON CONFIGURATA</div>
						</td>
<%
			}
			try { rs.close(); } catch (Exception e) {}
		 	try { cstmt.close(); } catch (Exception e) {}
%>
					</tr>
				</table>
			</form>
		</td>
<% if (configPresent) { %>
		<td style="width: 30%">
			<iframe src="RecallBlackList.jsp" id="iFrame_RecallBlackList" name="iFrame_RecallBlackList" style="width: 100%; border: 0 solid grey"></iframe>
		</td>
<% } %>
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

<div id="Error" class="modal">
	<div class="modal-content">
		<div class="modal-header pink">				
			<table style="width: 100%">
				<tr>
					<td><h2>Error</h2></td>
					<td>
						<div class="container_img title right pink "  onclick="$('#Error').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>		
		<input type="hidden" id="asAction" name="action" Value="add_message">
		<table>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="1"><label>Error Code:</label></td>
				<td colspan="1"><label id='ErrorCode'></label></td>
			</tr>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="1"><label>Error Message:</label></td>
				<td colspan="1"><label id='ErrorMessage'></label></td>
			</tr>
			<tr>
				<td colspan="2">
					<button type="button" id="AddCloseModal" class="button pink" onclick="$('#Error').hide();">Ok</button>
				</td>
			</tr>
		</table>			
	</div>
</div>

<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#Recall","Recall.jsp");
			$("#iFrame_RecallBlackList").height($(window).height()-10 );
			$(window).resize(function() {
				$("#iFrame_RecallBlackList").height($(window).height()-10 );		
			});	
		} catch (e) {}
	})

<%
	String ErrorCode= request.getParameter("ErrorCode");
	String ErrorMessage= request.getParameter("ErrorMessage");
	if (StringUtils.isNotBlank(ErrorCode)){
		
		out.println("$('#ErrorCode').html('"+ErrorCode+"');");
		out.println("$('#ErrorMessage').html('"+ErrorMessage+"');");
		out.println("$('#Error').show();");
	}
%>
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#Error').hide();
		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>