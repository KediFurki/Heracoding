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
<title>CalendarGlobalDate</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
</head>
<body style="overflow-y: auto;">
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
	String type = request.getParameter("type");
	if (StringUtils.isBlank(type)) {
		type = "";
	}
	String date = request.getParameter("date");
	String start = request.getParameter("start");
	String stop = request.getParameter("stop");
	//==============================
	//==	OTHER PARAMETERS	====
	String pos = request.getParameter("pos");
	String row_height = request.getParameter("row_height");
	String row_number=request.getParameter("row_number");
	String container_height=	request.getParameter("container_height");
	//==============================
	Date filterToday = new Date();
	filterToday = sdf_full_day_of_year.parse(sdf_full_day_of_year.format(filterToday));
	boolean enableMod = true;
	//==	ACTION START	====
	log.info(session.getId() + " - action:" + action + " type: " + type + " date: " + date + " start: " + start + " stop: " + stop);
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		switch (type) {
			case "R":
				if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
				switch (action) {
					case "delete" :
						log.info(session.getId() + " - dashboard.SpecialDay_DeleteRecursive('"+environment+"','" + CodIvr + "','"+date+"'");
						cstmt = conn.prepareCall("{ call dashboard.SpecialDay_DeleteRecursive(?,?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setString(2, CodIvr);
						cstmt.setTimestamp(3, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
						break;
					case "add" :
					    log.info(session.getId() + " - dashboard.SpecialDay_AddRecursive('"+environment+"','" + CodIvr + "','" + date+ "','" + start+ "','" + stop+ "')");
						cstmt = conn.prepareCall("{ call dashboard.SpecialDay_AddRecursive(?,?,?,?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setString(2, CodIvr);
						cstmt.setTimestamp(3, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
						cstmt.setString(4, start);
						cstmt.setString(5, stop);
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
						break;
					case "deleteall" :
						log.info(session.getId() + " - dashboard.SpecialDay_DelAllRecursive('"+environment+"','"+date+"')");
						cstmt = conn.prepareCall("{ call dashboard.SpecialDay_DelAllRecursive(?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
						break;
					case "addall" :
					    log.info(session.getId() + " - dashboard.SpecialDay_AddAllRecursive('"+environment+"','" + date+ "','" + start+ "','" + stop+ "')");
						cstmt = conn.prepareCall("{ call dashboard.SpecialDay_AddAllRecursive(?,?,?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
						cstmt.setString(3, start);
						cstmt.setString(4, stop);
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
						break;
				}
				break;
			case "S":
				if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
				switch (action) {
					case "delete" :
						log.info(session.getId() + " - dashboard.SpecialDay_DeleteSpecial('"+environment+"','" + CodIvr + "','"+date+"'");
						cstmt = conn.prepareCall("{ call dashboard.SpecialDay_DeleteSpecial(?,?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setString(2, CodIvr);
						cstmt.setTimestamp(3, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
						break;
					case "add" :
					    log.info(session.getId() + " - dashboard.SpecialDay_AddSpecial('"+environment+"','" + CodIvr + "','" + date+ "','" + start+ "','" + stop+ "')");
						cstmt = conn.prepareCall("{ call dashboard.SpecialDay_AddSpecial(?,?,?,?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setString(2, CodIvr);
						cstmt.setTimestamp(3, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
						cstmt.setString(4, start);
						cstmt.setString(5, stop);
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
						break;
					case "deleteall" :
						log.info(session.getId() + " - dashboard.SpecialDay_DelAllSpecial('"+environment+"','"+date+"'");
						cstmt = conn.prepareCall("{ call dashboard.SpecialDay_DelAllSpecial(?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
						break;
					case "addall" :
					    log.info(session.getId() + " - dashboard.SpecialDay_AddAllSpecial('"+environment+"','" + date+ "','" + start+ "','" + stop+ "')");
						cstmt = conn.prepareCall("{ call dashboard.SpecialDay_AddAllSpecial(?,?,?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
						cstmt.setString(3, start);
						cstmt.setString(4, stop);
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
						break;
				}
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
		if (StringUtils.isNoneBlank(date) && StringUtils.isNoneBlank(start) && StringUtils.isNoneBlank(stop)){
			switch (type) {
				case "R":
					log.info(session.getId() + " - dashboard.SpecialDayGlobal_GetInfoRecursive('"+environment+"','" + date+ "','" + start+ "','" + stop+ "')");
					cstmt = conn.prepareCall("{ call dashboard.SpecialDayGlobal_GetInfoRecursive(?,?,?,?)} ");
					cstmt.setString(1,environment);
					cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
					cstmt.setString(3, start);
					cstmt.setString(4, stop);
					rs = cstmt.executeQuery();					
					log.debug(session.getId() + " - executeCall complete");
					break;
				case "S":
					log.info(session.getId() + " - dashboard.SpecialDayGlobal_GetInfoSpecial('"+environment+"','" + date+ "','" + start+ "','" + stop+ "')");
					cstmt = conn.prepareCall("{ call dashboard.SpecialDayGlobal_GetInfoSpecial(?,?,?,?)} ");
					cstmt.setString(1,environment);
					cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
					cstmt.setString(3, start);
					cstmt.setString(4, stop);
					rs = cstmt.executeQuery();					
					log.debug(session.getId() + " - executeCall complete");
					break;
			}
		}else{
			log.info(session.getId() + " - date is null or empty");
		}
		int i = 0;
%>

<%
		if (StringUtils.isNotBlank(type)) {
			String viewedDate = "";
			switch (type) {
				case "R":
					viewedDate = sdf_day_of_year_ext.format(sdf_full_day_of_year.parse(date));
					break;
				case "S":
					if (filterToday.after(sdf_full_day_of_year.parse(date))) enableMod = false;
					viewedDate = date;
					break;
			}
%>
<div id="_top">
	<table class="roundedCorners small no_bottom" style="width: 99%">
		<tr class="listGroupItem active blue">
			<td colspan='7'>
				<table>
					<tr>
						<td>
							<div class="title">
								Dettaglio
								<%=(type.equalsIgnoreCase("R")?"Festività Nazionale":"Chiusura Speciale")%>
								- data:<%=viewedDate%>
							</div>
						</td>
						<td valign="top">
							<div class="container title right">
									<button type="button" class="buttonset blue white" onclick="setAll()">Tutti</button>
									<button type="button" class="buttonset blue white" onclick="setNone()">Nessuno</button>
							</div>
						</td>
					</tr>
				</table>
			</td>
		</tr>
	</table>

	<form id="form_save_calendarglobal" name="form_save_calendarglobal" action="CalendarGlobalDate.jsp" method="post">
		<input type="hidden" id="mAction" name="action" value="" type="hidden" />
		<input type="hidden" id="mType" name="type" value="<%=type%>" type="hidden" /> 
		<input type="hidden" id="mDate" name="date" value="<%=date%>" type="hidden" /> 
		<input type="hidden" id="mStart" name="start" value="<%=start%>" type="hidden" />
		<input type="hidden" id="mStop" name="stop" value="<%=stop%>" type="hidden" />
		<input type="hidden" id="mCodIvr" name="CodIvr" value="">
		<input type="hidden" id="mPos" name="pos" value='<%=pos %>'>
		<div id="_left" style="overflow: auto;">
			<table id="_tab" class="roundedCorners small no_top" style="width: 99%">
<%
					
			while (rs.next()) {
				i++;
				String COD_IVR = rs.getString("COD_IVR");
				String Descrizione = rs.getString("DESCRIZIONE");									
				String Status = rs.getString("STATUS");
				
				if (i%2==1)
					out.println("<tr>");
				out.println("<td>");										
				out.println("<label class='switch'>"); 
				out.println("<input type='checkbox' id='mCurrentState' name='Status' "+(Status.equalsIgnoreCase("OFF")?"":"checked=true")+(enableMod?" onclick='change(this,\""+COD_IVR+"\")'":" disabled")+"> ");
				out.println("<span class='slider round blue'></span>");
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
</div>
<%
		} else {
%>
<div id="_top">
	<table class="roundedCorners small no_bottom" style="width: 99%">
		<tr class="listGroupItem active blue">
			<td colspan='7'>
				<table>
					<tr>
						<td>
							<div class="title">
								Nessun Giorno selezionato
							</div>
						</td>
						<td valign="top">
							<div class="container title right"></div>
						</td>
					</tr>
				</table>
			</td>
		</tr>
	</table>
</div>
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

<script type="text/javascript">
	var pos=<%=pos%>;
	
	var  row_height ='<%=row_height%>';
	var  row_number='<%=row_number%>';
	var  container_height='<%=container_height%>';
	var margintop=0;
	$(function() {
		
		try {

			parent.parent.ChangeActiveMenu("#CalendarGlobal");	
		} catch (e) {
		
		}
		try{
			pos[1]=Math.floor(pos[1]);
			
			$(window).resize(function() {
			
				$("#_left").height($(window).height()-90- margintop);
			});	
							
			var element = document.getElementById("_tab");
			var rows_n = element.rows.lenght;
			var _tab_height = $("#_tab").height();
			console.log(" pos="+pos+" row_height ="+row_height+" row_number="+row_number+" container_height="+container_height+" _tab_height:"+_tab_height+" pos:"+pos);

			var alf_tab_height =  Math.floor(_tab_height/3);
			margintop = pos[1]-alf_tab_height;
			
			var div_h = $(window).height() - margintop - 75;
			
			console.log(" $(window).height()="+$(window).height()+" margintop ="+margintop);
			if (div_h <_tab_height){
				console.log("set max");
				
				margintop = $(window).height() - _tab_height - 75;
			}
			
	 		if (margintop > 0  ){
				$("#_top").css("margin-top", margintop +30);
				$("#_left").height($(window).height() - margintop - 72);
				
			}
// 			console.log('h = h/2:'+h)
// 			if (pos[1]>h){ 
// 				console.log('pos[1]-h:'+pos[1]+" -" +h+" >> "+ (pos[1]-h))
// 				$("#_left").css("margin-top",(pos[1]-h));
// 				$("#_left").height($(window).height()-90 -(pos[1]-h));
// 			}
				
		} catch (e) {
			//alert(e);
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
		$("#form_save_calendarglobal")[0].submit();
	}

	setAll = function() {
		$(':checkbox[id=mCurrentState]').prop("checked", true);
		$("#mAction").val('addall');
		$("#form_save_calendarglobal")[0].submit();
	}
	
	setNone = function() {
		$(':checkbox[id=mCurrentState]').prop("checked", false);
		$("#mAction").val('deleteall');
		$("#form_save_calendarglobal")[0].submit();
	}
	
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>