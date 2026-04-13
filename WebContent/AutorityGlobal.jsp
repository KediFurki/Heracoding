<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Date"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Timestamp"%>
<%@page import="java.util.Calendar"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="java.util.ArrayList"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en">
<head >
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Autority</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
</head>
<body >
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
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	String Type = request.getParameter("type");
	String single = request.getParameter("single");
	String sdata = request.getParameter("sdata");
	String edata = request.getParameter("edata");
	String delete_date = request.getParameter("date");
	//==============================
	//==	FILTER PARAMETERS	====
	String sFilterDay = request.getParameter("change_filter_day");
	if (StringUtils.isBlank(sFilterDay))
		sFilterDay = (String) session.getAttribute("change_filter_day");
	if (StringUtils.isBlank(sFilterDay)) {
		Date filterDay = new Date();
		sFilterDay = sdf_full_day_of_year.format(filterDay);
	}
	session.setAttribute("change_filter_day", sFilterDay);
	Date filterToday = new Date();
	filterToday = sdf_full_day_of_year.parse(sdf_full_day_of_year.format(filterToday));
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:" + action + " Type: " + Type + " single:" + single + " sdata:"+ sdata + " edata:" + edata + " date:" + delete_date+" filter_day:"+sFilterDay);
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "delete" :
				log.info(session.getId() + " - dashboard.Autority_DeleteAll('"+environment+"','" + delete_date	+ "')");
				cstmt = conn.prepareCall("{ call dashboard.Autority_DeleteAll(?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(delete_date).getTime()));
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add" :
				conn.setAutoCommit(false);
				try {						
					ArrayList<Date> aDate = new ArrayList<>();
					if (StringUtils.equalsIgnoreCase(Type, "single")) {
						aDate.add(sdf_full_day_of_year.parse(single));
					} else {
						Date sdate = sdf_full_day_of_year.parse(sdata);
						Date edate = sdf_full_day_of_year.parse(edata);
						while (sdate.before(edate)) {
							aDate.add(sdate);
							Calendar cal = Calendar.getInstance();
							cal.setTime(sdate);
							cal.add(Calendar.DAY_OF_MONTH, 1);
							sdate = cal.getTime();
						}
						aDate.add(sdate);
					}
					for (Date date : aDate) {
						log.info(session.getId() + " - dashboard.Autority_AddOnlyDate('"+environment+"','" + sdf_full_day_of_year.format(date) + "')");
						cstmt = conn.prepareCall("{ call dashboard.Autority_AddOnlyDate(?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setTimestamp(2, new Timestamp(date.getTime()));
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
					}
					conn.commit();
				} catch (Exception e) {
					log.error(session.getId() + " - action error: " + e.getMessage(), e);
					conn.rollback();
				}
				conn.setAutoCommit(true);
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	} finally {
		try { cstmt.close(); } catch (Exception e) {}
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
%>
<table class="center" >
	<tr>
		<td style="width: 10%"></td>
		<td>
			<table class="roundedCorners small no_bottom">
				<tr class="listGroupItem active green">
					<td colspan='7'>
						<table>
							<tr>
								<td>
									<div class="title">Messaggi Autorità</div>
								</td>
								<td valign="top">
									<div class="container_img green title right" onclick="confirmchange_filter('<%=sFilterDay%>')">
										<img alt="" class="" src="images/PointWhite_.png">
									</div>
								</td>
							</tr>
						</table>
					</td>
				</tr>
			</table>
				 
			<form id="form_service_hour" action="Service.jsp" method="post">
				<div id="_left" style="overflow: auto;">
					<table id="_tab" class="roundedCorners small no_top">
<%
	try {
		log.info(session.getId() + " - dashboard.Autority_GetDates('"+environment+"','" + sFilterDay	+ "')");
		cstmt = conn.prepareCall("{ call dashboard.Autority_GetDates(?,?)} ");
		cstmt.setString(1,environment);
		cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime() )  );
		rs = cstmt.executeQuery(); 					
		log.debug(session.getId() + " - executeCall complete");
		int i = 0;
		while (rs.next()) {
			i++;
			Timestamp ts = rs.getTimestamp("data");
			Date dts = new Date(ts.getTime());
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
			boolean enableDel = true;
			if (!filterToday.equals(dts) && filterToday.after(dts)) enableDel = false;
			
			out.println("<tr class='listGroupItem"+(enableDel?"":" oldDate")+"' onclick='clickMe(event,this,\""+sdf.format(dts)+ "\") '>	");
			out.println("<td  >");
			out.println("<div >"	+ sdf.format(dts) + " </div> </td> ");
			out.println("<td> </td>");
			out.println("<td  >");
			out.println("   <div  class='container_img grey right'>");
			out.println("	   <img src='images/TrashGrey_.png' "+(enableDel?"onclick='$(\"#dDate\").val(\"" + sdf_full_day_of_year.format(dts) + "\"); $(\"#Delete\").show(); '":"")+ ">");
			out.println("    </div>");
			out.println("</td>");
			out.println("</tr>");
		}								
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>
						<tr>
							<td colspan="7">
								<div class="container_img grey">
									<img alt="" src="images/PlusGrey_.png" onclick="$('#Add').show() ">
								</div>
							</td>
						</tr>
					</table>
	
				</div>
			</form>
				
		</td>
		<td>
			<iframe src="AutorityGlobalDate.jsp" id="iFrame_AutorityGlobalDate" name="iFrame_AutorityGlobalDate" style="width: 100%; border: 0 solid grey"></iframe>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>

<div id="Delete" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>
						<h2>Elimina</h2> 
					</td>
					<td>
						<div class="container_img title right green " onclick="$('#Delete').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_emergency_delete" action="AutorityGlobal.jsp" method="post">
			<input type="hidden" id="dAction" name="action" Value="delete">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label for="dDate">Data</label>
					</td>
					<td>
						<input class="bold" id="dDate" name="date" readonly="readonly" style="border: 0px">
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="CloseModal" class="button green">Confermi Eleminazione</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="Add" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>
						<h2>Aggiungi date</h2>
					</td>
					<td>
						<div class="container_img title right green " onclick="$('#Add').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add" action="AutorityGlobal.jsp" method="post">
			<input type="hidden" id="asAction" name="action" Value="add">
			<input type="hidden" id="asType" name="type" Value="single">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label for="asigle_date">Data </label>
					</td>
					<td>
						<input id="asigle_date" name="single" type="date">
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3">
						<button type="button" class="button green" onclick="$('#asType').val('single'); $('#form_add')[0].submit();">Singola</button>
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label for="asigle_date">Data </label>
					</td>
					<td>
						<input id="asigle_date" name="sdata" type="date">
					</td>
					<td>
						<input id="asigle_date" name="edata" type="date">
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3">
						<button type="button" class="button green" onclick="$('#asType').val('multiple'); $('#form_add')[0].submit();">Range</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

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
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td><h2>Cambia filtro di visualizzazione</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#change_filter').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_change_filter" action="AutorityGlobal.jsp" method="post">
			<input type="hidden" id="chAction" name="action" Value="change_filter">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="change_filter_day">Giorno</label></td>
					<td><input type="date" id="change_filter_day" name="change_filter_day"></td>
					<td><button type="button" class="buttonsmall green" onclick="$('#change_filter_day').val('<%=sdf_full_day_of_year.format(filterToday)%>')">Oggi</button></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="CloseModal" class="button green">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="dates" class="modal">
	<form id="form_dates" name="form_dates" action="AutorityGlobalDate.jsp" method="post" target="iFrame_AutorityGlobalDate" >
		<input type="text" id="dates_day" name="date" > 
		<input type="text" id="dates_pos" name="pos" > 
		<input type="text" id="row_height" name="row_height" >
		<input type="text" id="row_number" name="row_number" >
		<input type="text" id="container_height" name="container_height" >
	</form>
</div>
	
<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#AutorityGlobal");		
			$("#iFrame_AutorityGlobalDate").height($(window).height()-10 );
// 			$("#_right").height($(window).height()-10 );
			$("#_left").height($(window).height()-90 );
			$(window).resize(function() {
				$("#iFrame_AutorityGlobalDate").height($(window).height()-10 );		
// 				$("#_right").height($(window).height()-10 );
				$("#_left").height($(window).height()-90 );
			});	
		} catch (e) {}
	})

	confirmchange_filter = function(id) {
		$("#change_filter").show();
		$("#change_filter_day").val(id);

	}
	
<%
	String ErrorCode = request.getParameter("ErrorCode");
	String ErrorMessage = request.getParameter("ErrorMessage");
	if (StringUtils.isNotBlank(ErrorCode)) {
		out.println("$('#ErrorCode').html('" + ErrorCode + "');");
		out.println("$('#ErrorMessage').html('" + ErrorMessage + "');");
		out.println("$('#Error').show();");
	}
%>
		
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#Delete').hide();
			$('#Error').hide();
			$('#Add').hide();
			$('#change_filter').hide();
		}
	}
	
	var tdWidth = 0;
	var tableWidth =0;
	clickMe = function(event,_this,date){
		$(_this).parent().find("td").removeClass("greyBackGround"); 
		$(_this).children("td").addClass("greyBackGround"); 
		$('#dates_day').val(date);
		$('#dates_pos').val("["+event.x+","+event.y+"]");
		$('#container_height').val($(_left).height());
		$('#row_height').val(_this.clientHeight);
		$('#row_number').val(_this.rowIndex);
		$('#form_dates')[0].submit();		
		
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>