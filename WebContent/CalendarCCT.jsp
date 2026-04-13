<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Date"%>
<%@page import="java.util.Iterator"%>
<%@page import="java.util.Set"%>
<%@page import="java.util.Enumeration"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Timestamp"%>
<%@page import="java.util.Arrays"%>
<%@page import="comapp.DashBoardUtility.ComAppDay"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="java.util.Hashtable"%>
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
<title>Calendar</title>
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
	SimpleDateFormat sdf_day_of_year_ext = new SimpleDateFormat("dd MMMM");
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
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
	boolean privacy = false;
	boolean clsStato = false;
	String Stato = "OFF";
	String DataModifica = "";
	//==	ACTION START	====
	log.info(session.getId() + " - action:" + action + " CodIvr:" + CodIvr +" filter_day:"+sFilterDay);
	try {
		Context ctx = new InitialContext();
		DataSource ds = null;
		switch (environment) {
		case "STC":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"STC");
			log.info(session.getId() + " - connection STC wait...");
			break;
		}
		conn = ds.getConnection();
		Date d = null;
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "week" :
				for (ComAppDay value : ComAppDay.values()) {
					String[] s = request.getParameterValues(value + "_1");
					String[] s1 = request.getParameterValues(value + "_2");
					String time = "";
					String pipe = "";
					for (int i = 0; i < 3; i++) {
						try {
							log.debug(session.getId() + " - week: " + value + " slot=" + s[i] + "-" + s1[i]);
							if (StringUtils.isBlank(s[i]) || StringUtils.isBlank(s1[i]))
								continue;
							time += pipe + s[i] + "-" + s1[i];
							pipe = "|";
						} catch (Exception e) {
							log.warn(session.getId() + " - timeslot warning " + e.getMessage() + ":", e);
						}
					}
					log.info(session.getId() + " - dashboard.Calendar_Update('" + CodIvr + "','" + value.name() + "','" + (value.ordinal() + 1) + "','" + time + "')");
					cstmt = conn.prepareCall("{ call dashboard.Calendar_Update(?,?,?,?)} ");
					cstmt.setInt(1, Integer.parseInt(CodIvr));
					cstmt.setString(2, value.name());
					cstmt.setString(3, "" + (value.ordinal() + 1));
					if (StringUtils.isNoneBlank(time)) {
						cstmt.setString(4, time);
					} else {
						cstmt.setString(4, null);
					}
					cstmt.execute();
					log.debug(session.getId() + " - executeCall complete");
				}
				break;
			case "day_of_year" : 
				{
					Enumeration<String> par = request.getParameterNames();
					Hashtable<String, String> ht = new Hashtable<>();
					int i = 0;
					while (par.hasMoreElements()) {
						String k = par.nextElement();
						if (StringUtils.startsWith(k, "day_of_year")) {
							if (k.endsWith("_1")) {
								String hk = k.substring("day_of_year_".length()).replace("_1", "");
								log.debug(session.getId() + " - k: " + k + " hk:" + hk);
								String hv = ht.get(hk);
								if (StringUtils.isBlank(hv)) {
									hv = request.getParameter(k);
								} else {
									hv = request.getParameter(k) + "|" + hv;
								}
								ht.put(hk, hv);
								log.debug(session.getId() + " - key: " + hk + " value: " + hv);
							}
							if (k.endsWith("_2")) {
								String hk = k.substring("day_of_year_".length()).replace("_2", "");
								log.debug(session.getId() + " - k: " + k + " hk:" + hk);
								String hv = ht.get(hk);
								if (StringUtils.isBlank(hv)) {
									hv = request.getParameter(k);
								} else {
									hv = hv + "-" + request.getParameter(k);
								}
								ht.put(hk, hv);
								log.debug(session.getId() + " - key: " + hk + " value: " + hv);
							}
						}
					}
					Set<String> keys = ht.keySet();
					Iterator<String> itr = keys.iterator();
					while (itr.hasNext()) {
						String k = itr.next();
						String vs[] = ht.get(k).split("-");
	
						d = sdf_day_of_year.parse(k);
						log.info(session.getId() + " - dashboard.SpecialDay_UpdateRecursive('"+environment+"','" + CodIvr + "','" + d + "','" + vs[0] + "','" + vs[1] + "')");
						try {
							cstmt = conn.prepareCall("{ call dashboard.SpecialDay_UpdateRecursive(?,?,?,?,?)} ");
							cstmt.setString(1,environment);
							cstmt.setInt(2, Integer.parseInt(CodIvr));
							cstmt.setTimestamp(3, new Timestamp(d.getTime()));
							cstmt.setString(4, vs[0]);
							cstmt.setString(5, vs[1]);
							cstmt.execute();
							log.debug(session.getId() + " - executeCall complete");
							try { cstmt.close(); } catch (Exception e) {}
						} catch (Exception e) {
						}
					}
				}
				break;
			case "delete_day_of_year" :
				String delete_day_of_year_day = request.getParameter("delete_day_of_year_day");
				d = sdf_day_of_year.parse(delete_day_of_year_day);
				log.info(session.getId() + " - dashboard.SpecialDay_DeleteRecursive('"+environment+"','" + CodIvr + "','" + d + "'");
				cstmt = conn.prepareCall("{ call dashboard.SpecialDay_DeleteRecursive(?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setInt(2, Integer.parseInt(CodIvr));
				cstmt.setTimestamp(3, new Timestamp(d.getTime()));
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add_day_of_year" :
				String add_day_of_year_day = request.getParameter("add_day_of_year_day");
				String add_day_of_year_day_stime = request.getParameter("add_day_of_year_day_stime");
				String add_day_of_year_day_etime = request.getParameter("add_day_of_year_day_etime");
				d = sdf_full_day_of_year.parse(add_day_of_year_day);
				log.info(session.getId() + " - dashboard.SpecialDay_AddRecursive('"+environment+"','" + CodIvr + "','" + d + "','" + add_day_of_year_day_stime + "','" + add_day_of_year_day_etime + "')");
				cstmt = conn.prepareCall("{ call dashboard.SpecialDay_AddRecursive(?,?,?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setInt(2, Integer.parseInt(CodIvr));
				cstmt.setTimestamp(3, new Timestamp(d.getTime()));
				cstmt.setString(4, add_day_of_year_day_stime);
				cstmt.setString(5, add_day_of_year_day_etime);
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "special_day" : 
				{
					Enumeration<String> par = request.getParameterNames();
					Hashtable<String, String> ht = new Hashtable<>();
					int i = 0;
					while (par.hasMoreElements()) {
						String k = par.nextElement();
						if (StringUtils.startsWith(k, "special_day_")) {
							if (k.endsWith("_1")) {
								String hk = k.substring("special_day_".length()).replace("_1", "");
								log.debug(session.getId() + " - k: " + k + " hk:" + hk);
								String hv = ht.get(hk);
								if (StringUtils.isBlank(hv)) {
									hv = request.getParameter(k);
								} else {
									hv = request.getParameter(k) + "|" + hv;
								}
								ht.put(hk, hv);
								log.debug(session.getId() + " - key: " + hk + " value: " + hv);
							}
							if (k.endsWith("_2")) {
								String hk = k.substring("special_day_".length()).replace("_2", "");
								log.debug(session.getId() + " - k: " + k + " hk:" + hk);
								String hv = ht.get(hk);
								if (StringUtils.isBlank(hv)) {
									hv = request.getParameter(k);
								} else {
									hv = hv + "-" + request.getParameter(k);
								}
								ht.put(hk, hv);
								log.debug(session.getId() + " - key: " + hk + " value: " + hv);
							}
						}
					}
					Set<String> keys = ht.keySet();
					Iterator<String> itr = keys.iterator();
					while (itr.hasNext()) {
						String k = itr.next();
						String vs[] = ht.get(k).split("-");
						d = sdf_full_day_of_year.parse(k);
						log.info(session.getId() + " - dashboard.SpecialDay_UpdateSpecial('"+environment+"','" + CodIvr + "','" + d + "','" + vs[0] + "','" + vs[1] + "')");
						try {
							cstmt = conn.prepareCall("{ call dashboard.SpecialDay_UpdateSpecial(?,?,?,?,?)} ");
							cstmt.setString(1,environment);
							cstmt.setInt(2, Integer.parseInt(CodIvr));
							cstmt.setTimestamp(3, new Timestamp(d.getTime()));
							cstmt.setString(4, vs[0]);
							cstmt.setString(5, vs[1]);
							cstmt.execute();
							log.debug(session.getId() + " - executeCall complete");
							try { cstmt.close(); } catch (Exception e) {}
						} catch (Exception e) {
						}
					}
				}
				break;
			case "delete_special_day" :
				String delete_special_day_day = request.getParameter("delete_special_day_day");
				d = sdf_full_day_of_year.parse(delete_special_day_day);
				log.info(session.getId() + " - dashboard.SpecialDay_DeleteSpecial('"+environment+"','" + CodIvr + "','" + d + "'");
				cstmt = conn.prepareCall("{ call dashboard.SpecialDay_DeleteSpecial(?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setInt(2, Integer.parseInt(CodIvr));
				cstmt.setTimestamp(3, new Timestamp(d.getTime()));
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add_special_day" :
				String add_special_day_day = request.getParameter("add_special_day_day");
				String add_special_day_day_stime = request.getParameter("add_special_day_day_stime");
				String add_special_day_day_etime = request.getParameter("add_special_day_day_etime");
				d = sdf_full_day_of_year.parse(add_special_day_day);
				log.info(session.getId() + " - dashboard.SpecialDay_AddSpecial('"+environment+"','" + CodIvr + "','" + d + "','" + add_special_day_day_stime + "','" + add_special_day_day_etime + "')");
				cstmt = conn.prepareCall("{ call dashboard.SpecialDay_AddSpecial(?,?,?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setInt(2, Integer.parseInt(CodIvr));
				cstmt.setTimestamp(3, new Timestamp(d.getTime()));
				cstmt.setString(4, add_special_day_day_stime);
				cstmt.setString(5, add_special_day_day_etime);
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
%>
<table class="center">
<%
		if (StringUtils.isBlank(CodIvr)) {
		} else {
%>
	<tr>
		<td style="width: 10%"></td>
		<td colspan="2">
			<form id="form_week" action="CalendarCCT.jsp" method="post">
				<input type="hidden" name="action" value="week">
				<table id="stickytable_1" class="roundedCorners small">
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan='7'>
								<table>
									<tr>
										<td style="width: 45%">
											<div class="title">Orari Settimana</div>
										</td>
										<td style="width: 10%">
											<div class="container_img title center blue disabled"
												id="week">
												<img alt="" class="" src="images/SaveWhite_.png">
											</div>
										</td>
										<td>
											<div class="container_img title right blue "
												onclick="$('#form_week')[0].reset(); disabled('#week')">
												<img alt="" class="" src="images/RefreshWhite_.png">
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
<%
			String PreviousState = request.getParameter("PreviousState");
			String CurrentState = request.getParameter("CurrentState");
			log.info(session.getId() + " - dashboard.Calendar_get('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.Calendar_get(?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			int i = 0;
			while (rs.next()) {
				i++;
				int dayOfWeek = rs.getInt("Giorni");
				String timeSlot = rs.getString("ORARI");
				if (i != dayOfWeek || StringUtils.isBlank(timeSlot)) {
					for (; i < dayOfWeek; i++) {
						out.println("<tr><td><label>" + ComAppDay.values()[i - 1] + "</label> </td> ");
						out.println("<td> <input class='time_left cursor_pointer'  type='time' name='"+ComAppDay.values()[i-1]+"_1' oninput='enabled(\"#week\",\"#form_week\")'></td>"+
									"<td> <input class='time_right cursor_pointer' type='time' name='"+ComAppDay.values()[i-1]+"_2' oninput='enabled(\"#week\",\"#form_week\")'></td>");
						out.println("<td> <input class='time_left cursor_pointer'  type='time' name='"+ComAppDay.values()[i-1]+"_1' oninput='enabled(\"#week\",\"#form_week\")'></td>"+
									"<td> <input class='time_right cursor_pointer' type='time' name='"+ComAppDay.values()[i-1]+"_2' oninput='enabled(\"#week\",\"#form_week\")'></td>");
						out.println("<td> <input class='time_left cursor_pointer'  type='time' name='"+ComAppDay.values()[i-1]+"_1' oninput='enabled(\"#week\",\"#form_week\")'></td>"+
									"<td> <input class='time_right cursor_pointer' type='time' name='"+ComAppDay.values()[i-1]+"_2' oninput='enabled(\"#week\",\"#form_week\")'></td>");
						out.println("</tr>");
					}
				}
				String[] aTimeSlots = timeSlot.split("\\|");
				out.println("<tr><td><label>" + ComAppDay.values()[dayOfWeek - 1] + "</label> </td> ");
				int k = 0;
				for (; k < aTimeSlots.length; k++) {
					String ts = aTimeSlots[k];
					String[] ah = ts.split("-");
					out.println("<td> <input class='time_left cursor_pointer'  type='time' name='"+ComAppDay.values()[dayOfWeek-1]+"_1' value='" + ah[0]+"' oninput='enabled(\"#week\",\"#form_week\")' > </td>"+
								"<td> <input class='time_right cursor_pointer' type='time' name='"+ComAppDay.values()[dayOfWeek-1]+"_2' value='" + ah[1]+"' oninput='enabled(\"#week\",\"#form_week\")'></td>");
				}
				for (; k < 3; k++) {
					out.println("<td> <input class='time_left cursor_pointer'  type='time' name='"+ComAppDay.values()[dayOfWeek-1]+"_1' oninput='enabled(\"#week\",\"#form_week\")'></td>"+
								"<td> <input class='time_right cursor_pointer' type='time' name='"+ComAppDay.values()[dayOfWeek-1]+"_2' oninput='enabled(\"#week\",\"#form_week\")'></td>");
				}
				out.println("</tr>");
			}
			i++;
			for (; i <= 7; i++) {
				out.println("<tr><td><label>" + ComAppDay.values()[i - 1] + "</label> </td> ");
				out.println("<td> <input class='time_left cursor_pointer'  type='time' name='"+ComAppDay.values()[i-1]+"_1' oninput='enabled(\"#week\",\"#form_week\")'></td>"+
							"<td> <input class='time_right cursor_pointer' type='time' name='"+ComAppDay.values()[i-1]+"_2' oninput='enabled(\"#week\",\"#form_week\")'></td>");
				out.println("<td> <input class='time_left cursor_pointer'  type='time' name='"+ComAppDay.values()[i-1]+"_1' oninput='enabled(\"#week\",\"#form_week\")'></td>"+
							"<td> <input class='time_right cursor_pointer' type='time' name='"+ComAppDay.values()[i-1]+"_2' oninput='enabled(\"#week\",\"#form_week\")'></td>");
				out.println("<td> <input class='time_left cursor_pointer'  type='time' name='"+ComAppDay.values()[i-1]+"_1' oninput='enabled(\"#week\",\"#form_week\")'></td>"+
							"<td> <input class='time_right cursor_pointer' type='time' name='"+ComAppDay.values()[i-1]+"_2' oninput='enabled(\"#week\",\"#form_week\")'></td>");
				out.println("</tr>");
			}
%>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
<%
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
	}
	try {
		if (StringUtils.isBlank(CodIvr)) {
		} else {
%>
	<tr>
		<td style="width: 10%"></td>
		<td style="width: 40%">
			<form id="form_day_of_year" action="CalendarCCT.jsp" method="post">
				<input type="hidden" name="action" value="day_of_year">
				<table id="stickytable_2" class="roundedCorners small">
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan='7'>
								<table>
									<tr>
										<td style="width: 45%">
											<div class="title">Festività Nazionali</div>
										</td>
										<td style="width: 10%">
											<div class="container_img title center blue disabled"
												id="day_of_year">
												<img alt="" class="" src="images/SaveWhite_.png">
											</div>
										</td>
										<td>
										<td>
											<div class="container_img title right blue ">
												<img alt="" class="" src="images/RefreshWhite_.png"
													onclick="$('#form_day_of_year')[0].reset(); disabled('#day_of_year')">
											</div>
		
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
<%
			log.info(session.getId() + " - dashboard.SpecialDay_GetRecursive('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.SpecialDay_GetRecursive(?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			int i = 0;
			while (rs.next()) {
				i++;
				Timestamp ts = rs.getTimestamp("data");
				String start = rs.getString("dalle");
				String stop = rs.getString("alle");
	
				out.println("<tr><td><label>" + sdf_day_of_year_ext.format(ts) + " </label> </td> ");
				out.println("<td> <input class='time_left cursor_pointer' type='time' name='day_of_year_"
						+ sdf_day_of_year.format(ts) + "_1' value='" + start
						+ "' oninput='enabled(\"#day_of_year\",\"#form_day_of_year\")'></td> <td> <input class='time_right cursor_pointer' type='time' name='day_of_year_"
						+ sdf_day_of_year.format(ts) + "_2' value='" + stop
						+ "' oninput='enabled(\"#day_of_year\",\"#form_day_of_year\")'></td>");
				out.println("<td  >");
				out.println("   <div  class='container_img grey right'>");
				out.println("	   <img src='images/TrashGrey_.png' onclick='confirmdelete_day_of_year(\""
						+ sdf_day_of_year.format(ts) + "\",\"" + start + "\",\"" + stop + "\")'  >");
				out.println("    </div>");
				out.println("</td>");
				out.println("</tr>");
			}
%>
					<tr>
						<td colspan="7">
							<div class="container_img title left grey">
								<img alt="" src="images/PlusGrey_.png"
									onclick="$('#add_day_of_year').show(); ">
							</div>
						</td>
					</tr>
				</table>
			</form>
		</td>
<%
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close();} catch (Exception e) {}
	}
	try {
		if (StringUtils.isBlank(CodIvr)) {
		} else {
%>
		<td style="width: 40%">
			<form id="form_special_day" action="CalendarCCT.jsp" method="post">
				<input type="hidden" name="action" value="special_day">
				<table id="stickytable_3" class="roundedCorners small">
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan='7'>
								<table>
									<tr>
										<td style="width: 45%">
											<div class="title">Chiusure Speciali</div>
										</td>
										<td style="width: 10%">
											<div class="container_img title center blue disabled" id="special_day">
												<img alt="" class="" src="images/SaveWhite_.png">
											</div>
										</td>
										<td valign="top">
											<div class="container_img title blue right" onclick="filterFunction()">
												<img alt="" class="" src="images/PointWhite_.png">
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
<%
			log.info(session.getId() + " - dashboard.SpecialDay_GetSpecial('" + CodIvr + "','" + sFilterDay + "')");
			cstmt = conn.prepareCall("{ call dashboard.SpecialDay_GetSpecial(?,?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			int i = 0;
			while (rs.next()) {
				Timestamp ts = rs.getTimestamp("data");
				Date dts = new Date(ts.getTime());
				
				boolean enableDel = true;
				if (!filterToday.equals(dts) && filterToday.after(dts)) enableDel = false;
				
				String start = rs.getString("dalle");
				String stop = rs.getString("alle");
	
				out.println("<tr"+(enableDel?"":" class='oldDate'")+"><td><label>" + sdf_full_day_of_year.format(dts) + " </label> </td> ");
				out.println("<td> <input class='time_left cursor_pointer'  type='time' name='special_day_"+sdf_full_day_of_year.format(dts)+"_1' oninput='enabled(\"#special_day\",\"#form_special_day\")' value='"+start+"'></td>"+
							"<td> <input class='time_right cursor_pointer' type='time' name='special_day_"+sdf_full_day_of_year.format(dts)+"_2' oninput='enabled(\"#special_day\",\"#form_special_day\")' value='"+stop+"'></td>");
				out.println("<td  >");
				out.println("   <div  class='container_img grey right'>");
				out.println("	   <img   src='images/TrashGrey_.png' "+(enableDel?"onclick='confirmdelete_special_day(\""+sdf_full_day_of_year.format(dts) + "\",\"" + start + "\",\"" + stop + "\")'":"")+ ">");
				out.println("    </div>");
				out.println("</td>");
				out.println("</tr>");
			}
%>
					<tr>
						<td colspan="7">
							<div class="container_img grey">
								<img alt="" src="images/PlusGrey_.png"
									onclick="$('#add_special_day').show(); ">
							</div>
						</td>
					</tr>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
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
</table>

<div id="delete_day_of_year" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>
						<h2>Elimina</h2>
					</td>
					<td>
						<div class="container_img title right blue " onclick="$('#delete_day_of_year').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_delete_day_of_year" action="CalendarCCT.jsp" method="post">
			<input type="hidden" id="dAction" name="action" Value="delete_day_of_year">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="delete_day_of_year_day">Giorno</label></td>
					<td><input class="bold" type="text" id="delete_day_of_year_day" name="delete_day_of_year_day" readonly="readonly" style="border: 0px"></td>

					<td><label for="delete_day_of_year_day_stime">dalle</label></td>
					<td><input class="bold" id="delete_day_of_year_day_stime" type="time" readonly="readonly" style="border: 0px"></td>

					<td><label for="delete_day_of_year_day_etime">alle</label></td>
					<td><input class="bold" id="delete_day_of_year_day_etime" type="time" readonly="readonly" style="border: 0px"></td>
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

<div id="add_day_of_year" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>
						<h2>Aggiungi una Festività Nazionale</h2>
					</td>
					<td>
						<div class="container_img title right blue " onclick="$('#add_day_of_year').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add_day_of_year" action="CalendarCCT.jsp" method="post">
			<input type="hidden" id="aAction" name="action" Value="add_day_of_year">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="Day">Giorno</label></td>
					<td><input type="date" value="2019-01-01" id="add_day_of_year_day" name="add_day_of_year_day"></td>
					
					<td><label for="add_day_of_year_day_stime">dalle</label></td>
					<td><input id="add_day_of_year_day_stime" name="add_day_of_year_day_stime" type="time" value="00:00"></td>

					<td><label for="add_day_of_year_day_etime">alle</label></td>
					<td><input id="add_day_of_year_day_etime" name="add_day_of_year_day_etime" type="time" value="00:00">
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="AddCloseModal" class="button blue">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="delete_special_day" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">

			<table style="width: 100%">
				<tr>
					<td>
						<h2>Elimina</h2>
					</td>
					<td>
						<div class="container_img title right blue " onclick="$('#delete_special_day').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_delete_special_day" action="CalendarCCT.jsp"
			method="post">
			<input type="hidden" id="dsAction" name="action"
				Value="delete_special_day">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="delete_special_day_day">Giorno</label></td>
					<td><input class="bold" type="text" id="delete_special_day_day" name="delete_special_day_day" readonly="readonly" style="border: 0px"></td>

					<td><label for="delete_special_day_day_stime">dalle</label></td>
					<td><input class="bold" id="delete_special_day_day_stime" type="time" readonly="readonly" style="border: 0px"></td>

					<td><label for="delete_special_day_day_etime">alle</label></td>
					<td><input class="bold" id="delete_special_day_day_etime" type="time" readonly="readonly" style="border: 0px"></td>
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
		<form id="form_change_filter" action="CalendarCCT.jsp" method="post">
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

<div id="add_special_day" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>
						<h2>Aggiungi una Chiusura Speciale</h2>
					</td>
					<td>
						<div class="container_img title right blue " onclick="$('#add_special_day').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add_special_day" action="CalendarCCT.jsp" method="post">
			<input type="hidden" id="asAction" name="action" Value="add_special_day">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="Day">Giorno</label></td>
					<td><input type="date" value="2019-01-01" id="add_special_day_day" name="add_special_day_day"></td>
					
					<td><label for="add_special_day_day_stime">dalle</label></td>
					<td><input id="add_special_day_day_stime" name="add_special_day_day_stime" type="time" value="00:00"></td>

					<td><label for="add_special_day_day_etime">alle</label></td>
					<td><input id="add_special_day_day_etime" name="add_special_day_day_etime" type="time" value="00:00"></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="AddCloseModal" class="button blue">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<script type="text/javascript">
	filterFunction =  function(){
		console.log("filterFunction ");
		try {
			$('#form_special_day')[0].reset();
		} catch (e) {
			console.log("1 "+ e);
		}
		try {
		disabled('#special_day'); 
		} catch (e) {
			console.log("2 "+ e); 
		}
		try {
		confirmchange_filter('<%=sFilterDay%>')
		} catch (e) {
			console.log("3 "+ e);
		}
		
	}
	enabled = function(id, form) {
		$(id).removeClass("disabled").on("click", function() {
			$(form).submit()
		});
		$(id).removeClass("reset");
	}
	disabled = function(id) {
		$(id).addClass("disabled").off("click");
		$(id).addClass("disabled");
	}
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#Calendar","CalendarCCT.jsp");
		} catch (e) {}
 		$("#stickytable_1").stickyTableHeaders();
 		$("#stickytable_2").stickyTableHeaders();
 		$("#stickytable_3").stickyTableHeaders();
		var now = new Date();
		var day = ("0" + now.getDate()).slice(-2);
		var month = ("0" + (now.getMonth() + 1)).slice(-2);
		var today = now.getFullYear() + "-" + (month) + "-" + (day);
		$('#add_day_of_year_day').val(today);
		$('#add_special_day_day').val(today);

	})

	confirmdelete_day_of_year = function(id, from, to) {
		$("#delete_day_of_year").show();
		$("#delete_day_of_year_day").val(id);
		$("#delete_day_of_year_day_stime").val(from);
		$("#delete_day_of_year_day_etime").val(to);
	}
	confirmdelete_special_day = function(id, from, to) {
		$("#delete_special_day").show();
		$("#delete_special_day_day").val(id);
		$("#delete_special_day_day_stime").val(from);
		$("#delete_special_day_day_etime").val(to);
	}

	confirmchange_filter = function(id) {
		$("#change_filter").show();
		$("#change_filter_day").val(id);

	}

	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#delete_day_of_year').hide();
			$('#add_day_of_year').hide();
			$('#delete_special_day').hide();
			$('#add_special_day').hide();
			$('#change_filter').hide();
		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>