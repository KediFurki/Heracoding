<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Date"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Timestamp"%>
<%-- <%@page import="java.util.Arrays"%> --%>
<%-- <%@page import="comapp.Utility.ComAppDay"%> --%>
<%@page import="java.util.Calendar"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%-- <%@page import="comapp.Node"%> --%>
<%@page import="java.util.ArrayList"%>
<%-- <%@page import="comapp.Tree"%> --%>
<%-- <%@page import="java.util.Hashtable"%> --%>
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
<%-- <%@page import="java.util.Properties"%> --%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Autority</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
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
	SimpleDateFormat sdf_day_of_year = new SimpleDateFormat("MM-dd");
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action="";
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
	if (StringUtils.isBlank(sFilterDay)){		
		Date filterDay = new Date();
		sFilterDay = sdf_full_day_of_year.format(filterDay);
	}
	session.setAttribute("change_filter_day",sFilterDay);
	Date filterToday = new Date();
	filterToday = sdf_full_day_of_year.parse(sdf_full_day_of_year.format(filterToday));
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:" + action + " Type: " + Type + " single:" + single + " sdata:"+ sdata + " edata:" + edata + " Id_Msg:" + delete_date+" filter_day:"+sFilterDay);
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "delete" :
				delete_date = delete_date.replace("/", "-");
				log.info(session.getId() + " - dashboard.Autority_Delete('"+environment+"','"+CodIvr+"','" + delete_date + "')");
				cstmt = conn.prepareCall("{ call dashboard.Autority_Delete(?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, CodIvr);
				cstmt.setTimestamp (3,  new Timestamp(sdf_full_day_of_year.parse(delete_date).getTime()) );
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
						log.info(session.getId() + " - dashboard.Autority_Add('"+environment+"','" + CodIvr + "','" + date	+ "')");
						cstmt = conn.prepareCall("{ call dashboard.Autority_Add(?,?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setString(2, CodIvr);
						cstmt.setTimestamp(3, new Timestamp(date.getTime()));
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
	try {
		if (StringUtils.isBlank(CodIvr)) {
		} else {
%>
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td colspan="2">
			<form id="form_service_hour" action="Autority.jsp" method="post">
				<table id="stickytable" class="roundedCorners small">
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan='7'>
								<table>
									<tr>
										<td>
											<div class="title">Messaggi Autorità</div>
										</td>
										<td valign="top">
											<div class="container_img blue title right" onclick="confirmchange_filter('<%=sFilterDay%>')">
												<img alt="" class="" src="images/PointWhite_.png">
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
<%
			log.info(session.getId() + " - dashboard.Autority_GetTreeDates('"+environment+"','" + CodIvr + "','"+sFilterDay+"')");
			cstmt = conn.prepareCall("{ call dashboard.Autority_GetTreeDates(?,?,?)} ");
			cstmt.setString(1,environment);
			cstmt.setString(2, CodIvr);
			cstmt.setTimestamp(3, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
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
				
				out.println("<tr class='listGroupItem"+(enableDel?"":" oldDate")+"'><td><label>" + sdf.format(dts) + " </label> </td> ");
				out.println("<td> </td>");
// 				out.println("<td  >");
// 				out.println("   <div  class='container_img grey right'>");
// 				out.println("	   <img src='images/TrashGrey_.png' "+(enableDel?"onclick='$(\"#dDate\").val(\"" + sdf.format(ts) + "\"); $(\"#Delete\").show(); '":"")+ ">");
// 				out.println("    </div>");
// 				out.println("</td>");
				out.println("</tr>");
			}
%>
<!-- 					<tr> -->
<!-- 						<td colspan="7"> -->
<!-- 							<div class="container_img grey"> -->
<!-- 								<img alt="" src="images/PlusGrey_.png" onclick="$('#Add').show() "> -->
<!-- 							</div> -->
<!-- 						</td> -->
<!-- 					</tr> -->
				</table>
			</form>
		</td>
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

<div id="Delete" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>
						<h2>Elimina</h2>
					</td>
					<td>
						<div class="container_img title right blue " onclick="$('#Delete').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_emergency_delete" action="Autority.jsp" method="post">
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
						<button type="submit" id="CloseModal" class="button blue">Confermi Eleminazione</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="Add" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>
						<h2>Aggiungi date</h2>
					</td>
					<td>
						<div class="container_img title right blue " onclick="$('#Add').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add" action="Autority.jsp" method="post">
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
					<td colspan="2">
						<button type="button" class="button blue" onclick="$('#asType').val('single'); $('#form_add')[0].submit();">Singola</button>
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
					<td colspan="2">
						<button type="button" class="button blue" onclick="$('#asType').val('multiple'); $('#form_add')[0].submit();">Range</button>
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
			<tr><td></td></tr>
			<tr>
				<td colspan="1"><label>Error Message:</label></td>
				<td colspan="1"><label id='ErrorMessage'></label></td>
			</tr>
			<tr><td><br></td></tr>
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
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td><h2>Cambia filtro di visualizzazione</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#change_filter').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_change_filter" action="Autority.jsp" method="post">
			<input type="hidden" id="chAction" name="action" Value="change_filter">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="change_filter_day">Giorno</label></td>
					<td><input type="date" id="change_filter_day" name="change_filter_day"></td>
					<td><button type="button" class="buttonsmall blue" onclick="$('#change_filter_day').val('<%=sdf_full_day_of_year.format(filterToday)%>')">Oggi</button></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="CloseModal" class="button blue">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<script type="text/javascript"> 
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#Autority","Autority.jsp");
		} catch (e) {}
 		$("#stickytable").stickyTableHeaders();
	})
	confirmDelete = function(date){
		
	}
	
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
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>