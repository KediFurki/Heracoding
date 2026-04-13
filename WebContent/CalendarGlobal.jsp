<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Date"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Timestamp"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="java.util.Hashtable"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="org.apache.log4j.Logger"%>
<%@page import="java.util.Enumeration"%>
<%@page import="java.util.Iterator"%>
<%@page import="java.util.Set"%>
<html lang="en">
<head >
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CalendarGlobal</title>
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
	SimpleDateFormat sdf_day_of_year_ext = new SimpleDateFormat("dd MMMM");
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	String date = request.getParameter("date");
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
	log.info(session.getId() + " - action:" + action + " data:" + date +" filter_day:"+sFilterDay);
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		Date d = null;
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
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
						log.info(session.getId() + " - dashboard.SpecialDayGlobal_UpdateRecursive('"+environment+"','" + d + "','" + vs[0] + "','" + vs[1] + "')");
						cstmt = conn.prepareCall("{ call dashboard.SpecialDayGlobal_UpdateRecursive(?,?,?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setTimestamp(2, new Timestamp(d.getTime()));
						cstmt.setString(3, vs[0]);
						cstmt.setString(4, vs[1]);
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
						try { cstmt.close(); } catch (Exception e) {}
					}
				}
				break;
			case "delete_day_of_year":
				String delete_day_of_year_day = request.getParameter("delete_day_of_year_day");
				d = sdf_day_of_year.parse(delete_day_of_year_day);
				log.info(session.getId() + " - dashboard.SpecialDayGlobal_DeleteRecursive('"+environment+"','"+d+"'");
				cstmt = conn.prepareCall("{ call dashboard.SpecialDayGlobal_DeleteRecursive(?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setTimestamp(2, new Timestamp(d.getTime()));				
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add_day_of_year"://SpecialDayGlobal_AddRecursiveOnlyDate
				String add_day_of_year_day = request.getParameter("add_day_of_year_day");
				String add_day_of_year_day_stime = request.getParameter("add_day_of_year_day_stime");
				String add_day_of_year_day_etime = request.getParameter("add_day_of_year_day_etime");
			    d = sdf_full_day_of_year.parse(add_day_of_year_day);
			    log.info(session.getId() + " - dashboard.SpecialDayGlobal_AddRecursiveOnlyDate('"+environment+"','"+d+"','"+add_day_of_year_day_stime+"','"+add_day_of_year_day_etime+"')");
				cstmt = conn.prepareCall("{ call dashboard.SpecialDayGlobal_AddRecursiveOnlyDate(?,?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setTimestamp(2, new Timestamp(d.getTime()));
				cstmt.setString(3, add_day_of_year_day_stime);
				cstmt.setString(4, add_day_of_year_day_etime);
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
						log.info(session.getId() + " - dashboard.SpecialDayGlobal_UpdateSpecial('"+environment+"','" + d + "','" + vs[0] + "','" + vs[1] + "')");
						cstmt = conn.prepareCall("{ call dashboard.SpecialDayGlobal_UpdateSpecial(?,?,?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setTimestamp(2, new Timestamp(d.getTime()));
						cstmt.setString(3, vs[0]);
						cstmt.setString(4, vs[1]);
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
						try { cstmt.close(); } catch (Exception e) {}
					}
				}
				break;
			case "delete_special_day" :
				String delete_special_day_day = request.getParameter("delete_special_day_day");
				d = sdf_full_day_of_year.parse(delete_special_day_day);
				log.info(session.getId() + " - dashboard.SpecialDayGlobal_DeleteSpecial('"+environment+"','" + d + "'");
				cstmt = conn.prepareCall("{ call dashboard.SpecialDayGlobal_DeleteSpecial(?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setTimestamp(2, new Timestamp(d.getTime()));
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add_special_day" :
				String add_special_day_day = request.getParameter("add_special_day_day");
				String add_special_day_day_stime = request.getParameter("add_special_day_day_stime");
				String add_special_day_day_etime = request.getParameter("add_special_day_day_etime");
				d = sdf_full_day_of_year.parse(add_special_day_day);
				log.info(session.getId() + " - dashboard.SpecialDayGlobal_AddSpecialOnlyDate('"+environment+"','" + d + "','" + add_special_day_day_stime + "','" + add_special_day_day_etime + "')");
				cstmt = conn.prepareCall("{ call dashboard.SpecialDayGlobal_AddSpecialOnlyDate(?,?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setTimestamp(2, new Timestamp(d.getTime()));
				cstmt.setString(3, add_special_day_day_stime);
				cstmt.setString(4, add_special_day_day_etime);
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
%>
<table class="center" >
	<tr>

		<td style="width: 10%"></td>

		<td style="width: 30%">
			
			<form id="form_day_of_year" action="CalendarGlobal.jsp" method="post">
				<input type="hidden" name="action" value="day_of_year">
				<table class="roundedCorners small no_bottom">
					<tr class="listGroupItem active green">
						<td colspan='7'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Festività Nazionali</div>
									</td>
									<td style="width: 10%">
										<div class="container_img title center green disabled"
											id="day_of_year">
											<img alt="" class="" src="images/SaveWhite_.png">
										</div>
									</td>
									<td valign="top">
										<div class="container title right green">
											<label></label>										
										</div>
									</td>
								</tr>
							</table>
						</td>
					</tr>
				</table>

				<div id="_leftR" style="overflow: auto;">
					<table id="_tabR" class="roundedCorners small no_top">
<%
	try {
		log.info(session.getId() + " - dashboard.SpecialDayGlobal_GetRecursive('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.SpecialDayGlobal_GetRecursive(?)} ");
		cstmt.setString(1,environment);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		int i = 0;
		while (rs.next()) {
			i++;
			Timestamp ts = rs.getTimestamp("data");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
			String start = rs.getString("dalle");
			String stop = rs.getString("alle");
			
			out.println("<tr class='listGroupItem' onclick='clickMeR(event,this,\""+sdf.format(ts)+ "\",\""+start+ "\",\""+stop+ "\") '>	");
			out.println("<td><label>" + sdf_day_of_year_ext.format(ts) + " </label> </td> ");
			out.println("<td> <input class='time_left cursor_pointer' type='time' name='day_of_year_" + sdf_day_of_year.format(ts) +  "_1' value='" + start + "' oninput='enabled(\"#day_of_year\",\"#form_day_of_year\")'></td>");
			out.println("<td> <input class='time_right cursor_pointer' type='time' name='day_of_year_" + sdf_day_of_year.format(ts) + "_2' value='" + stop + "' oninput='enabled(\"#day_of_year\",\"#form_day_of_year\")'></td>");
			out.println("<td  >");
			out.println("   <div  class='container_img grey right'>");
			out.println("	   <img src='images/TrashGrey_.png' onclick='confirmdelete_day_of_year(\""+sdf_day_of_year.format(ts)+"\",\""+start+"\",\""+stop+"\")'  >");
			out.println("    </div>");
			out.println("</td>");
			out.println("</tr>");
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
	}
%>
						<tr>
							<td colspan="7">
								<div class="container_img grey">
									<img alt="" src="images/PlusGrey_.png" onclick="$('#add_day_of_year').show() ">
								</div>
							</td>
						</tr>
					</table>
				</div>
			</form>
				
			<form id="form_special_day" action="CalendarGlobal.jsp" method="post">
				<input type="hidden" name="action" value="special_day">
				<table class="roundedCorners small no_bottom">
					<tr class="listGroupItem active green">
						<td colspan='7'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Chiusure Speciali</div>
									</td>
									<td style="width: 10%">
										<div class="container_img title center green disabled" id="special_day">
											<img alt="" class="" src="images/SaveWhite_.png">
										</div>
									</td>
									<td valign="top">
										<div class="container_img title green right" onclick="$('#form_special_day')[0].reset(); disabled('#special_day'); confirmchange_filter('<%=sFilterDay%>')">
											<img alt="" class="" src="images/PointWhite_.png">
										</div>
									</td>
								</tr>
							</table>
						</td>
					</tr>
				</table>

				<div id="_leftS" style="overflow: auto;">
					<table id="_tabS" class="roundedCorners small no_top">
<%
	try {
		log.info(session.getId() + " - dashboard.SpecialDayGlobal_GetSpecial('"+environment+"','" + sFilterDay + "')");
		cstmt = conn.prepareCall("{ call dashboard.SpecialDayGlobal_GetSpecial(?,?)} ");
		cstmt.setString(1,environment);
		cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
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

			String start = rs.getString("dalle");
			String stop = rs.getString("alle");
			
			out.println("<tr class='listGroupItem"+(enableDel?"":" oldDate")+"' onclick='clickMeS(event,this,\""+sdf.format(dts)+ "\",\""+start+ "\",\""+stop+ "\") '>	");
			out.println("<td><label>" + sdf_full_day_of_year.format(dts) + " </label> </td> ");
			out.println("<td> <input class='time_left cursor_pointer' type='time' name='special_day_" + sdf_full_day_of_year.format(dts) +  "_1' value='" + start + "' oninput='enabled(\"#special_day\",\"#form_special_day\")'></td>");
			out.println("<td> <input class='time_right cursor_pointer' type='time' name='special_day_" + sdf_full_day_of_year.format(dts) + "_2' value='" + stop + "' oninput='enabled(\"#special_day\",\"#form_special_day\")'></td>");
			out.println("<td  >");
			out.println("   <div  class='container_img grey right'>");
			out.println("	   <img src='images/TrashGrey_.png' "+(enableDel?"onclick='confirmdelete_special_day(\""+sdf_full_day_of_year.format(dts)+"\",\""+start+"\",\""+stop+"\")'":"")+ ">");
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
									<img alt="" src="images/PlusGrey_.png" onclick="$('#add_special_day').show() ">
								</div>
							</td>
						</tr>
					</table>
				</div>
			</form>

		</td>

		<td >
			<iframe src="CalendarGlobalDate.jsp" id="iFrame_CalendarGlobalDate" name="iFrame_CalendarGlobalDate" style="width: 100%; border: 0 solid grey"></iframe>
		</td>

		<td style="width: 10%"></td>

	</tr>
</table>

<div id="delete_day_of_year" class="modal">
	<div class="modal-content">
		<div class="modal-header green">						
			<table style="width: 100%">
				<tr>
					<td><h2>Elimina</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#delete_day_of_year').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_delete_day_of_year" action="CalendarGlobal.jsp" method="post">
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
						<button type="submit" id="CloseModal" class="button green">Confermi Eleminazione</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="add_day_of_year" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			
				<table style="width: 100%">
				<tr>
					<td><h2>Aggiungi una Festività Nazionale</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#add_day_of_year').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add_day_of_year" action="CalendarGlobal.jsp" method="post">
			<input type="hidden" id="aAction" name="action" Value="add_day_of_year">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="Day">Giorno</label></td>
					<td><input type="date" value="2019-01-01" id="add_day_of_year_day" name="add_day_of_year_day"   ></td>
						
					<td><label for="add_day_of_year_day_stime">dalle</label></td>
					<td><input id="add_day_of_year_day_stime" name="add_day_of_year_day_stime" type="time"   value="00:00"></td>
					
					<td><label for="add_day_of_year_day_etime">alle</label></td>
					<td><input id="add_day_of_year_day_etime" name="add_day_of_year_day_etime" type="time"   value="00:00"></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="AddCloseModal" class="button green">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="delete_special_day" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>
						<h2>Elimina</h2>
					</td>
					<td>
						<div class="container_img title right green " onclick="$('#delete_special_day').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_delete_special_day" action="CalendarGlobal.jsp" method="post">
			<input type="hidden" id="dsAction" name="action" Value="delete_special_day">
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
						<button type="submit" id="CloseModal" class="button green">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="add_special_day" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>
						<h2>Aggiungi una Chiusura Speciale</h2>
					</td>
					<td>
						<div class="container_img title right green " onclick="$('#add_special_day').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add_special_day" action="CalendarGlobal.jsp" method="post">
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
						<button type="submit" id="AddCloseModal" class="button green">Conferma</button>
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
		<form id="form_change_filter" action="CalendarGlobal.jsp" method="post">
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
					<td colspan="3">
						<button type="submit" id="CloseModal" class="button green">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="dates" class="modal">
	<form id="form_dates" name="form_dates" action="CalendarGlobalDate.jsp" method="post" target="iFrame_CalendarGlobalDate" >
		<input type="text" id="dates_type" name="type" > 
		<input type="text" id="dates_day" name="date" > 
		<input type="text" id="dates_start" name="start" > 
		<input type="text" id="dates_stop" name="stop" > 
		<input type="text" id="dates_pos" name="pos" > 
		<input type="text" id="row_height" name="row_height" >
		<input type="text" id="row_number" name="row_number" >
		<input type="text" id="container_height" name="container_height" >
	</form>
</div>
	
<script type="text/javascript">
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
			parent.ChangeActiveMenu("#CalendarGlobal");		
			$("#iFrame_CalendarGlobalDate").height($(window).height()-10 );
// 			$("#_right").height($(window).height()-10 );
			$("#_leftR").height(($(window).height()/2)-90 );
			$("#_leftS").height(($(window).height()/2)-90 );
			$(window).resize(function() {
				$("#iFrame_CalendarGlobalDate").height($(window).height()-10 );		
// 				$("#_right").height($(window).height()-10 );
				$("#_leftR").height(($(window).height()/2)-90 );
				$("#_leftS").height(($(window).height()/2)-90 );
			});	
		} catch (e) {}
		var now = new Date();
		var day = ("0" + now.getDate()).slice(-2);
		var month = ("0" + (now.getMonth() + 1)).slice(-2);
		var today = now.getFullYear() + "-" + (month) + "-" + (day);
		$('#add_day_of_year_day').val(today);
		$('#add_special_day_day').val(today);
	})

	confirmdelete_day_of_year =  function(id,from,to) {
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
			$('#delete_day_of_year').hide();
			$('#add_day_of_year').hide();
			$('#delete_special_day').hide();
			$('#add_special_day').hide();
			$('#change_filter').hide();
			$('#Error').hide();
		}
	}
	var tdWidth = 0;
	var tableWidth = 0;
	clickMeR = function(event, _this, date, start, stop) {
		$('#_tabR').find("td").removeClass("greyBackGround");
		$('#_tabS').find("td").removeClass("greyBackGround");
		$(_this).children("td").addClass("greyBackGround");
		$('#dates_type').val('R');
		$('#dates_day').val(date);
		$('#dates_start').val(start);
		$('#dates_stop').val(stop);
		$('#dates_pos').val("[" + event.x + "," + event.y + "]");
		$('#container_height').val($(_leftR).height());
		$('#row_height').val(_this.clientHeight);
		$('#row_number').val(_this.rowIndex);
		$('#form_dates')[0].submit();
	}
	clickMeS = function(event, _this, date, start, stop) {
		$('#_tabR').find("td").removeClass("greyBackGround");
		$('#_tabS').find("td").removeClass("greyBackGround");
		$(_this).children("td").addClass("greyBackGround");
		$('#dates_type').val('S');
		$('#dates_day').val(date);
		$('#dates_start').val(start);
		$('#dates_stop').val(stop);
		$('#dates_pos').val("[" + event.x + "," + event.y + "]");
		$('#container_height').val($(_leftR).height());
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