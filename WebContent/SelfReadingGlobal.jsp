<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Date"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Timestamp"%>
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
<title>SelfReadingGlobal</title>
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
	//		NONE
	//==============================
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	SimpleDateFormat sdf_day_of_year = new SimpleDateFormat("MM-dd");
	SimpleDateFormat sdf_day_of_year_ext = new SimpleDateFormat("dd MMMM");
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy/MM/dd");
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	String CodIvr = request.getParameter("CodIvr");
	//==============================
	//==	OTHER PARAMETERS	====
	//==============================
// 	Date filterToday = new Date();
// 	filterToday = sdf_full_day_of_year.parse(sdf_full_day_of_year.format(filterToday));
// 	boolean enableMod = true;
	//==	ACTION START	====
	log.info(session.getId() + " - action:" + action + " CodIvr: " + CodIvr);
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "delete" :
				log.info(session.getId() + " - dashboard.SelfReading_Update('"+environment+"','" + CodIvr + "','OFF'");
				cstmt = conn.prepareCall("{ call dashboard.SelfReading_Update(?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, CodIvr);
				cstmt.setString(3, "OFF");
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add" :
			    log.info(session.getId() + " - dashboard.SelfReading_Update('"+environment+"','" + CodIvr + "','ON')");
				cstmt = conn.prepareCall("{ call dashboard.SelfReading_Update(?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, CodIvr);
				cstmt.setString(3, "ON");
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "deleteall" :
				log.info(session.getId() + " - dashboard.SelfReading_UpdateAll('"+environment+"','OFF')");
				cstmt = conn.prepareCall("{ call dashboard.SelfReading_UpdateAll(?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, "OFF");
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "addall" :
			    log.info(session.getId() + " - dashboard.SelfReading_UpdateAll('"+environment+"','ON')");
				cstmt = conn.prepareCall("{ call dashboard.SelfReading_UpdateAll(?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, "ON");
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	} finally {
		try { cstmt.close(); } catch (Exception e) {}
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
	try {
		log.info(session.getId() + " - dashboard.SelfReading_Get('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.SelfReading_Get(?)} ");
		cstmt.setString(1,environment);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		int i = 0;
%>
<table class="center"  >
	<tr>
		<td style="width: 10%"></td>
		<td>
			<table class="roundedCorners small no_bottom">
				<tr class="listGroupItem active green">
					<td colspan='7'>
						<table>
							<tr>
								<td>
									<div class="title">AutoLettura (Alto Traffico)</div>
								</td>
								<td valign="top">
									<div class="container title right">
										<button type="button" class="buttonset green white" onclick="setAll()">Tutti</button>
										<button type="button" class="buttonset green white" onclick="setNone()">Nessuno</button>
									</div>
								</td>
							</tr>
						</table>
					</td>
				</tr>
			</table>

			<form id="form_save_perturbertrafficglobal" name="form_save_perturbertrafficglobal" action="SelfReadingGlobal.jsp" method="post">
				<input type="hidden" id="mAction" name="action" value="">
				<input type="hidden" id="mCodIvr" name="CodIvr" value="">
				<div id="_left" style="overflow: auto;">
					<table id="_tab" class="roundedCorners small no_top">
<%
					
		while (rs.next()) {
			i++;
			String COD_IVR = rs.getString("COD_IVR");
			String Descrizione = rs.getString("DESCRIZIONE");									
			String Status = rs.getString("stato");
			
			if (i%2==1)
				out.println("<tr>");
			out.println("<td>");										
			out.println("<label class='switch'>"); 
			out.println("<input type='checkbox' id='mCurrentState' name='Status' "+(Status.equalsIgnoreCase("OFF")?"":"checked=true")+" onclick='change(this,\""+COD_IVR+"\")'"+"> ");
			out.println("<span class='slider round green'></span>");
			out.println("</label>");
			out.println("</td>");										
			out.println("<td><label>"+ COD_IVR + " </label> </td> ");
			out.println("<td><label>"+ Descrizione + " </label> </td> ");
			if (i%2==0)
			out.println("</tr>");
		}
%>
					</table>
				</div>
			</form>

		</td>
		<td style="width: 10%"></td>
	</tr>
</table>
<%
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>

<script type="text/javascript">
	var margintop=0;
	$(function() {
		try {
			parent.ChangeActiveMenu("#SelfReadingGlobal");
		} catch (e) {
		}
	})

	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#Error').hide();
		}
	}

	change=function(_this,CodIvr){
		if ($(_this).is(':checked'))
			$("#mAction").val('add'); // name="action"  value="" type="hidden"/>
		else
			$("#mAction").val('delete');
		$("#mCodIvr").val(CodIvr); // name="date";
		$("#form_save_perturbertrafficglobal")[0].submit();
	}

	setAll = function() {
		$(':checkbox[id=mCurrentState]').prop("checked", true);
		$("#mAction").val('addall');
		$("#form_save_perturbertrafficglobal")[0].submit();
	}
	
	setNone = function() {
		$(':checkbox[id=mCurrentState]').prop("checked", false);
		$("#mAction").val('deleteall');
		$("#form_save_perturbertrafficglobal")[0].submit();
	}
	
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>