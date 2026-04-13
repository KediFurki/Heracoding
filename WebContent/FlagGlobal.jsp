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
<title>Flag</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
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
	//==============================
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	String aCodIvr[] = request.getParameterValues("CodIvr") ;
	String aTipo[] = request.getParameterValues("Tipo") ;
	//==============================
	JSONArray jaCodIvr = (JSONArray)session.getAttribute("jaCodIvr");
	JSONArray jaTipo = (JSONArray)session.getAttribute("jaTipo");
	//==	ACTION START	====
	log.info(session.getId() + " - action:" + action + " - aCodIvr:" + Arrays.toString(aCodIvr)  + " aTipo:" + Arrays.toString(aTipo));
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
	switch (action){
		case "change_filter":
			jaCodIvr = new JSONArray();
				if (aCodIvr!=null) {
					for (String sCodIvr : aCodIvr) {
						jaCodIvr.put(sCodIvr);
					}
				}
				session.setAttribute("jaCodIvr",jaCodIvr);
			jaTipo = new JSONArray();
				if (aTipo!=null) {
					for (String sTipo : aTipo) {
						jaTipo.put(sTipo);
					}
				}
				session.setAttribute("jaTipo",jaTipo);
			break;
	}	
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
%>
	
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td>

			<table class="roundedCorners small no_bottom">
				<tr class="listGroupItem active green">
					<td colspan='7'>
						<table>
							<tr>
								<td>
									<div class="title">Flag</div>
								</td>
								<td valign="top">
									<div class="container_img green title right" onclick="$('#change_filter').show()">
										<img alt="" class="" src="images/PointWhite_.png">
									</div>
								</td>
							</tr>
						</table>
					</td>
				</tr>
			</table>
			<iframe id="iFlagGlobalDetails" name ="iFlagGlobalDetails"  src="FlagGlobalDetails.jsp"  id="" style="width: 100%; height: 100%; border: 0px"></iframe>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>

<div id="Error" class="modal">
	<div class="modal-content">
		<div class="modal-header pink">
			<table style="width: 100%">
				<tr>
					<td><h2>Error</h2></td>
					<td>
						<div class="container_img title right pink " onclick="$('#Error').hide();">
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

<div id="change_filter" class="modal">
	<div class="modal-content big">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>
						<h2>Cambia filtro di visualizzazione</h2>
					</td>
					<td>
						<div class="container_img title right green " onclick="$('#change_filter').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_change_filter" action="FlagGlobal.jsp" method="post">
			<input type="hidden" id="chAction" name="action" Value="change_filter">
			<table class="roundedCorners small">
				<tr class="listGroupItem active green">
					<td colspan='3'>
						<table>
							<tr>
								<td>
									<div class="title">Servizi</div>
								</td>
								<td valign="top" align="right">
									<button type="button" class="buttonset green white" onclick="setAll('mCodIvr')">Tutti</button>
									<button type="button" class="buttonset green white" onclick="setNone('mCodIvr')">Nessuno</button>
								</td>
							</tr>
						</table>
					</td>
				</tr>
<%
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		log.info(session.getId() + " - dashboard.Service_List('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.Service_List(?)}");
		cstmt.setString(1,environment);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		int i=0;
		while (rs.next()) {
			if (i%3==0)
				out.println("<tr>");
			
			boolean foundCodIvr = false;
			if (jaCodIvr!=null) {
				for (int k=0; k<jaCodIvr.length(); k++){
					String value = jaCodIvr.getString(k);
					foundCodIvr = StringUtils.equalsIgnoreCase(value, rs.getString("COD_IVR"));
					if (foundCodIvr)
						break;
				}
			} else foundCodIvr = true;
			
			out.println("<td >");
			out.println("<label class='switch'>"); 
			out.println("<input type='checkbox' id='mCodIvr' name='CodIvr' value='"+rs.getString("COD_IVR")+"' "+(foundCodIvr?"checked=true":"")+ " > ");
			out.println("<span class='slider round green'></span>");
			out.println("</label>");
			out.println(  rs.getString("LABEL") + " - "+ rs.getString("DESCRIZIONE"));						
			out.println("</td >");
			if (i%3==2)
				out.println("</tr>");
			i++;
		}
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
%>
			</table>
			<table class="roundedCorners small">
				<tr class="listGroupItem active green">
					<td colspan='3'>
						<table>
							<tr>
								<td>
									<div class="title">Tipo</div>
								</td>
								<td valign="top" align="right">
									<button type="button" class="buttonset green white" onclick="setAll('mTipo')">Tutti</button>
									<button type="button" class="buttonset green white" onclick="setNone('mTipo')">Nessuno</button>
								</td>
							</tr>
						</table>
					</td>
				</tr>
<%
		log.info(session.getId() + " - dashboard.flag_type('"+environment+"',");
		cstmt = conn.prepareCall("{ call dashboard.flag_type(?) }");
		cstmt.setString(1,environment);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		i=0;
		while (rs.next()) {
			if (i%3==0)
				out.println("<tr>");
					
			boolean foundTipo = false;
			if (jaTipo!=null) {
				for (int k=0; k<jaTipo.length(); k++){
					String value = jaTipo.getString(k);
					foundTipo = StringUtils.equalsIgnoreCase(value, rs.getString("Tipo"));
					if (foundTipo)
						break;
				}
			} else foundTipo = true;

			out.println("<td >");
			out.println("<label class='switch'>"); 
			out.println("<input type='checkbox' id='mTipo' name='Tipo' value='"+rs.getString("Tipo")+"' "+(foundTipo?"checked=true":"")+ " > ");
			out.println("<span class='slider round green'></span>");
			out.println("</label>");
			out.println(  rs.getString("Tipo") );						
			out.println("</td >");
			if (i%3==2)
				out.println("</tr>");
			i++;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>
				
			</table>
			<button type="submit" id="CloseModal" class="button green">Conferma</button>
		</form>
	</div>
</div>

<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#FlagGlobal");
			$("#iFlagGlobalDetails").height($(window).height()-90 );
			
			$(window).resize(function() {
				$("#iFlagGlobalDetails").height($(window).height()-90 );		
				
			});	
		} catch (e) {
		}
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

	setAll = function(cb) {
		$(':checkbox[id='+cb+']').prop("checked", true);
	}
	
	setNone = function(cb) {
		$(':checkbox[id='+cb+']').prop("checked", false);
	}

	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#change_filter').hide();
			$('#Error').hide();
		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>