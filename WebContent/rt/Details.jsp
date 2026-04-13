<!DOCTYPE html>
<%@page import="java.sql.Timestamp"%>
<%@page import="java.util.Date"%>
<%@page import="java.util.concurrent.TimeUnit"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="java.util.ArrayList"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Properties"%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Details</title>
<link rel="stylesheet" href="../css/three.css">
<link rel="stylesheet" type="text/css" href="../css/comapp.css">
<script src="../js/jquery.min.js"></script>


<script>
var radius = 8;
var size = 2;
var color ="#008CEA";
var style="solid";

// var connections = [];

$.fn.connect = function(to){
	var el1, el2; 
	el1 = $(this);
	el2 = $(to)
	if (el1.offset().top>el2.offset().top){
		el2 = el1;
		el1 =  $(to); 
	}
	var pa = $(el1).offset();
	var pb = $(el2).offset();
	var wa = $(el1).width();
	var wb = $(el2).width();		
	var ha = $(el1).height();
	var hb = $(el2).height();
	var pTop = $(el1).css("padding-top");
	var pBootom = $(el1).css("padding-bottom");
	var paddingBootom =  parseInt(pBootom.replace("px",""));
	var paddingTop =  parseInt(pTop.replace("px",""));
	ha += paddingTop + paddingBootom;		 
	if (pb.left <= pa.left){
		if ( (pa.left - pb.left) < radius)
			rad = 0;
		else
			rad = radius;			
		
// 		console.log("paddingBootom:"+paddingBootom+" paddingTop: "+paddingTop+" pa.left: " + pa.left + " pa.top:" +pa.top+" pb.left:"+pb.left+" pb.top:"+pb.top+" (wa/2):" + (wa/2)+" (wb/2):"+(wb/2) );
		var offset = (pa.left - pb.left) - (wa/2) + (wb/2);
// 		console.log("offset: " + offset );			
		var leftconnection = pa.left + (wa/2) - offset
		var topconnection = pa.top + ha;						
		var hconnection = (pb.top - pa.top-ha)/2;
		var wconnection = offset;							
// 		console.log("topconnection:  "+topconnection + " pa.top: "+pa.top + " ha: "+ ha+"  hconnection: " + hconnection+" wconnection:" + offset);			
		var element = document.createElement("connection");	
		
// 		connections.push(element);
		
		
		var width = wconnection;// - rad - rad;
		if (width<0)
				width = rad;
		
		$(element).css({ 
		    width: wconnection,
		    height: hconnection,						    
		    position: "absolute",				    
		    top: topconnection +1, 
		    left: leftconnection ,	
		    borderBottomWidth : size,
		    borderBottomColor: color,
			borderBottomStyle: style ,
		    borderRightWidth : size, 
		    borderRightColor: color,
			borderRightStyle: style,
		    borderBottomRightRadius : rad, 			 
		    width: width, 
		    height: hconnection
		}).appendTo('body');
		
		topconnection += hconnection;
// 		console.log("topconnection: "+topconnection);
		width = wconnection - rad -rad;
		if (width<0)
				width = rad;
		
		var element = document.createElement("connection");
// 		connections.push(element);
		$(element).css({ 			    
		    position: "absolute",				    
		    top: topconnection , 
		    left: leftconnection - rad,
		    borderLeftWidth : size,
		    borderLeftColor: color,
			borderLeftStyle: style,
			borderTopWidth : size,
			borderTopColor: color,
			borderTopStyle: style,
		    borderTopLeftRadius : rad, 
		    width: width,
		    height:hconnection		    
		}).appendTo('body'); 
	}else{
		if ( (pb.left - pa.left) < radius)
			rad = 0;
		else
			rad = radius;	
		
// 		console.log("pa.left: " + pa.left +" pb.left:"+pb.left+" (wa/2)  " + (wa/2)+" (wb/2):"+(wb/2) );
		var offset = (pb.left - pa.left) - (wa/2) + (wb/2);  ///------
// 		console.log("offset: " + offset );			
		var leftconnection = pa.left + (wa/2) ///+ offset  ///---------
		var topconnection = pa.top + ha;			
		var hconnection = (pb.top - pa.top-ha)/2;
		var wconnection = offset;				
		var   width = wconnection - rad- rad;
		if (width<0)
				width = radius;
// 		console.log("topconnection:  "+topconnection + " pa.top: "+pa.top + " ha: "+ ha+"  hconnection: " + hconnection+" wconnection:" + offset);	
		var element = document.createElement("connection");
// 		connections.push(element);
		$(element).css({ 
		    position: "absolute",				    
		    top: topconnection +1 , 
		    left: leftconnection ,	
		    borderBottomWidth : size,
		    borderBottomColor: color,
			borderBottomStyle: style ,
			borderLeftWidth : size,
		    borderLeftColor: color,
			borderLeftStyle: style,
		    borderBottomLeftRadius : radius, 			 
		    width: width, 
		    height: hconnection
		}).appendTo('body');
		 width = wconnection - radius;
			if (width<0)
					width = radius;
		topconnection += hconnection;
// 		console.log("topconnection: "+topconnection);
		var element = document.createElement("connection");
// 		connections.push(element);
		$(element).css({ 
		    position: "absolute",				    
		    top: topconnection, 
		    left: leftconnection + rad,
		    borderRightWidth : size,
		    borderRightColor: color,
			borderRightStyle: style,
			borderTopWidth : size,
			borderTopColor: color,
			borderTopStyle: style,
		    borderTopRightRadius : radius, 
		    width:width,
		    height:hconnection
		}).appendTo('body'); 
	}
}
</script>
<style type="text/css">
</style>
</head>
<body style="overflow: auto;">
<div class="bottom-right">
	<table><tr>
		<td class=""><img style="height: 20px" src='../<%=ConfigServlet.getProperties().getProperty("Logo1")%>'></td>
		<td class="" style="float: right; "><img style="height: 20px" src='../<%=ConfigServlet.getProperties().getProperty("Logo2")%>'></td>
	</tr></table>
</div>
<div class="main">
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
	CallableStatement cstmt_R = null;
	CallableStatement cstmt_B = null;
	CallableStatement cstmt_L = null;
	CallableStatement cstmt_FD = null;
	CallableStatement cstmt_HT = null;
	ResultSet rs_R = null;
	ResultSet rs_B = null;
	ResultSet rs_L = null;
	ResultSet rs_FD = null;
	ResultSet rs_HT = null;
	SimpleDateFormat sdf_day_of_year = new SimpleDateFormat("MM-dd");
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String status = request.getParameter("status");
	//==============================
	//==	FILTER PARAMETERS	====
	String sFilterDay = request.getParameter("change_filter_day");
	if (StringUtils.isBlank(sFilterDay))
		sFilterDay = (String) session.getAttribute("change_filterRT_day");
	if (StringUtils.isBlank(sFilterDay)){		
		Date filterDay = new Date();
		sFilterDay = sdf_full_day_of_year.format(filterDay);
	}
	session.setAttribute("change_filterRT_day",sFilterDay);
	Date filterToday = new Date();

	if (!sdf_full_day_of_year.format(filterToday).equalsIgnoreCase(sFilterDay)) {
		response.sendRedirect("DetailsHistory.jsp");
	}
	//==============================
			
	int _nBranch = 0;
	boolean _fBranch[] = null;
	int _nLeave[] = null;
	boolean _fLeave[][] = null;
	String Root = null;
	int numBranches = 0;
	int numLeaves = 0;
	if (StringUtils.isBlank(CodIvr)) {
	} else {
		try {
			Context ctx = new InitialContext();
			DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
			log.info(session.getId() + " - connection CCC wait...");
			conn = ds.getConnection();
			//==============================
			//==	ACTION START	====
			log.info(session.getId() + " - Action:" + action + "Status: " + status);
			status = status == null ? "OFF" : status.toUpperCase();
			if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
			switch (action) {
				case "updateFD" :
					log.info(session.getId() + " - dashboard.ServiceSwitch_Update('" + CodIvr + "','FORZA_DISSUASIONE','" + status + "')");
					cstmt_FD = conn.prepareCall("{ call dashboard.ServiceSwitch_Update(?,?,?)} ");
					cstmt_FD.setString(1, CodIvr);
					cstmt_FD.setString(3, status);
					cstmt_FD.setString(2, "FORZA_DISSUASIONE");
					cstmt_FD.execute();
					log.debug(session.getId() + " - executeCall complete");
					try { cstmt_FD.close(); } catch (Exception e) {}
					break;
				case "updateHT" :
					log.info(session.getId() + " - dashboard.HighTraffic_Update('" + CodIvr + "','" + status + "')");
					cstmt_FD = conn.prepareCall("{ call dashboard.HighTraffic_Update(?,?)} ");
					cstmt_FD.setString(1, CodIvr);
					cstmt_FD.setString(2, status);
					cstmt_FD.execute();
					log.debug(session.getId() + " - executeCall complete");
					try { cstmt_FD.close(); } catch (Exception e) {}
					break;
			}
			if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
			//==	ACTION STOP		====
%>

<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td colspan="2">
				<table id="stickytable" class="roundedCorners smallnb" style="border-spacing: 0; padding: 0px;">
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan='20'>
								<table>
									<tr>
										<td>
											<div class="title">Dettaglio IVR - Data <%=sFilterDay%></div>
										</td>
										<td valign="top">
											<div class="container_img blue title right" onclick="confirmchange_filter('<%=sFilterDay%>')">
												<img alt="" class="" src="../images/PointWhite_.png">
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
					<tr>
<%
			log.info(session.getId() + " - dashboard.Detail_GetInfoRoot('" + CodIvr + "','"+sFilterDay+"')");
			cstmt_R = conn.prepareCall("{ call dashboard.Detail_GetInfoRoot(?,?)} ");
			cstmt_R.setString(1, CodIvr);
			cstmt_R.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
			rs_R = cstmt_R.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			if (rs_R.next()) {
				Root = rs_R.getString("COD_NODO");
				numBranches = rs_R.getInt("BRANCHES");
				int numSpan = (numBranches==0)?1:numBranches;
%>
						<td colspan='<%=numSpan%>'>						
										<table style="border-spacing: 0; padding: 0px;">
											<tr>
												<td>						
													<div id="Root">
														<table class="roundedCorners smallnb">
															<tr class='listGroupItem active blue nowrap'><td colspan='2' class='textcenter'><%=rs_R.getString("LABEL")%></td></tr>
															<tr class='listGroupItem active lightblue'><td colspan='2' class='textcenter'><%=rs_R.getString("DESCRIZIONE")%></td></tr>
															<tr><td colspan='1' class='nowrap'>Chiamate in Corso</td>
																<td colspan='1' class='' id="details_calls"></td></tr>
															<tr><td colspan='1' class='nowrap'>Entrate</td>
																<td colspan='1' class='' id="details_entered"></td></tr>
															<tr><td colspan='1' class='nowrap'>Riagganciate da IVR</td>
																<td colspan='1' class='' id="details_hungupivr"></td></tr>
															<tr><td colspan='1' class='nowrap'>Abbandonate su IVR</td>
													 			<td colspan='1' class='' id="details_abbandonedivr"></td></tr>
															<tr><td colspan='1' class='nowrap'>Uscite IVR</td>
																<td colspan='1' class='' id="details_exitivr"></td></tr>
															<tr><td colspan='1' class='nowrap'>Dissuase</td>
													  			<td colspan='1' class='' id="details_dissuase"></td></tr>
														</table>
													</div>
												</td>
<%
				log.info(session.getId() + " - dashboard.ServiceSwitch_Get('" + CodIvr + "','FORZA_DISSUASIONE')");
				cstmt_FD = conn.prepareCall("{ call dashboard.ServiceSwitch_Get(?,?)} ");
				cstmt_FD.setString(1, CodIvr);
				cstmt_FD.setString(2, "FORZA_DISSUASIONE");
				rs_FD = cstmt_FD.executeQuery();
				log.debug(session.getId() + " - executeCall complete");
				if (rs_FD.next()) {
					String TimeStamp = rs_FD.getString("TimeStamp");
					String Status = rs_FD.getString("Stato");
					String CodNodo = rs_FD.getString("Cod_Nodo");
					String Etichetta = rs_FD.getString("Etichetta");
%>
												<td>						
													<div id="ForaDissuasione">
														<table class="roundedCorners smallnb">
															<tr class='listGroupItem active lightblue'><td class='textcenter'>Forza Dissuasione</td></tr>
															<tr>
																<td class="textcenter">
																	<label class="switch" id="statusFD">
																		<input type="checkbox" id="iCurrentStateFD" name="statusFD" value="<%=Status%>" <%=((Status.equalsIgnoreCase("ON")) ? "checked=true" : "")%> onclick="return saveFD(this)"> 
																		<span class="slider round blue"></span>
																	</label>
																</td>
															</tr>
														</table>
													</div>
												</td>
<%
				}
				log.info(session.getId() + " - dashboard.HighTraffic_Get('" + CodIvr + "')");
				cstmt_HT = conn.prepareCall("{ call dashboard.HighTraffic_Get(?)} ");
				cstmt_HT.setString(1, CodIvr);
				rs_HT = cstmt_HT.executeQuery();
				log.debug(session.getId() + " - executeCall complete");
				if (rs_HT.next()) {
					String TimeStamp = rs_HT.getString("TimeStamp");
					String Status = rs_HT.getString("Stato");
%>
												<td>						
													<div id="AltoTraffico">
														<table class="roundedCorners smallnb">
															<tr class='listGroupItem active lightblue'><td class='textcenter'>Alto Traffico</td></tr>
															<tr>
																<td class="textcenter">
																	<label class="switch" id="statusHT">
																		<input type="checkbox" id="iCurrentStateHT" name="statusHT" value="<%=Status%>" <%=((Status.equalsIgnoreCase("ON")) ? "checked=true" : "")%> onclick="return saveHT(this)"> 
																		<span class="slider round blue"></span>
																	</label>
																</td>
															</tr>
														</table>
													</div>
												</td>
<%
				}
%>
											</tr>
										</table>
						</td>
<%
			}
%>
					</tr>
					<tr>
<%
			if (numBranches>0) {
				_fBranch = new boolean[numBranches];
				_nLeave = new int[numBranches];
				_fLeave = new boolean[numBranches][];
				String BranchesCodNodo;

				log.info(session.getId() + " - dashboard.Detail_GetInfoBranches('" + CodIvr + "','"+sFilterDay+"')");
				cstmt_B = conn.prepareCall("{ call dashboard.Detail_GetInfoBranches(?,?)} ");
				cstmt_B.setString(1, CodIvr);
				cstmt_B.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
				rs_B = cstmt_B.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
				while (rs_B.next()) {
					BranchesCodNodo = rs_B.getString("COD_NODO");
					int nLeaves = rs_B.getInt("BRANCHES");
					int nAllLeaves = rs_B.getInt("ALLBRANCHES");
					_fBranch[_nBranch] = (rs_B.getInt("ENTERED") > 0);
					_nLeave[_nBranch] = rs_B.getInt("BRANCHES");
					_fLeave[_nBranch] = new boolean[nAllLeaves];
					numLeaves = 0;
					String idBranches = "branches_"+BranchesCodNodo;
%>
						<td id="<%=idBranches%>_CARD" <%=((_fBranch[_nBranch])?"":" class='compress' ") %>>
							<table style="border-spacing: 0; padding: 0px;">
								<tr>
									<td>
										<div id="Branches_<%=_nBranch%>">
											<table class="roundedCorners smallnb">
												<tr class='listGroupItem active blue nowrap' onclick="switchMe('Leaves_<%=_nBranch%>')"><td colspan='2' class='textcenter'><%=rs_B.getString("ETICHETTA")%></td></tr>
												<tr><td colspan='1' class='nowrap'>Totali</td>
										 			<td colspan='1' class='' id="<%=idBranches%>_ENTERED"><%=rs_B.getString("ENTERED")%></td></tr>
												<tr><td colspan='1' class='nowrap'>Attive</td>
										  			<td colspan='1' class='' id="<%=idBranches%>_ACTIVECALLS"><%=rs_B.getString("ACTIVECALLS")%></td></tr>
											</table>
										</div>
									</td>
								</tr>
								<tr>
<%
					String arrScelte[] = new String[nLeaves];
					String arrSceltaMenu[] = new String[nAllLeaves];
					String arrCodNodo[] = new String[nAllLeaves];
					String arrEtichetta[] = new String[nAllLeaves];
					int arrEntered[] = new int[nAllLeaves];
					int arrActiveCalls[] = new int[nAllLeaves];
					log.info(session.getId() + " - dashboard.Detail_GetInfoLeaves('" + CodIvr + "','" + BranchesCodNodo + "','"+sFilterDay+"')");
					cstmt_L = conn.prepareCall("{ call dashboard.Detail_GetInfoLeaves(?,?,?)} ");
					cstmt_L.setString(1, CodIvr);
					cstmt_L.setString(2, BranchesCodNodo);
					cstmt_L.setTimestamp(3, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
					rs_L = cstmt_L.executeQuery();					
					log.debug(session.getId() + " - executeCall complete");
					while (rs_L.next()) {
						_fLeave[_nBranch][numLeaves] = true;
						arrSceltaMenu[numLeaves] = rs_L.getString("SCELTA_MENU");
						arrCodNodo[numLeaves] = rs_L.getString("COD_NODO");
						arrEtichetta[numLeaves] = rs_L.getString("ETICHETTA");
						arrEntered[numLeaves] = rs_L.getInt("ENTERED");
						arrActiveCalls[numLeaves] = rs_L.getInt("ACTIVECALLS");
						numLeaves++;
					}
					String CurrScelta = "";
					int iScelte = 0;
					for (int iLoopLeaves=0; iLoopLeaves<numLeaves; iLoopLeaves++) {
						if (!arrSceltaMenu[iLoopLeaves].equals(CurrScelta)) {
							arrScelte[iScelte] = arrSceltaMenu[iLoopLeaves];
							CurrScelta = arrSceltaMenu[iLoopLeaves];
							iScelte++;
						}
					}

					for (int iLoopLeaves=0; iLoopLeaves<nLeaves; iLoopLeaves++) {
						String currScelta = arrScelte[iLoopLeaves];
						String idLeaves = "lives_"+BranchesCodNodo+"_";
%>
									<td>
										<table style="border-spacing: 0; padding: 0px;">
											<tr>
												<td class="compress">
													<div id="Leaves_<%=_nBranch%>_<%=iLoopLeaves%>">
														<table class="roundedCorners smallcomp">
<%
						int numSpan = 0; 
						for (int iLoopAllLeaves=0; iLoopAllLeaves<numLeaves; iLoopAllLeaves++) {
							if (arrSceltaMenu[iLoopAllLeaves].equals(currScelta)) numSpan++;
						}
%>
															<tr class='listGroupItem active blue'><td colspan='<%=(numSpan+1)%>' class='textcenter bold'><%=currScelta%></td></tr>
															<tr class='listGroupItem active blue'><td></td>
<%
						for (int iLoopAllLeaves=0; iLoopAllLeaves<numLeaves; iLoopAllLeaves++) {
							if (arrSceltaMenu[iLoopAllLeaves].equals(currScelta)) {
%>
																<td colspan='1' class='lightblue'><%=arrEtichetta[iLoopAllLeaves]%></td>
<%
							}
						}
%>
													 		</tr>
															<tr><td colspan='1' class=''>Totali</td>
<%
						for (int iLoopAllLeaves=0; iLoopAllLeaves<numLeaves; iLoopAllLeaves++) {
							if (arrSceltaMenu[iLoopAllLeaves].equals(currScelta)) {
%>
																<td colspan='1' class='' id="<%=idLeaves+arrCodNodo[iLoopAllLeaves]%>_ENTERED"><%=arrEntered[iLoopAllLeaves]%></td>
<%
							}
						}
%>
													 		</tr>
															<tr><td colspan='1' class=''>Attive</td>
<%
						for (int iLoopAllLeaves=0; iLoopAllLeaves<numLeaves; iLoopAllLeaves++) {
							if (arrSceltaMenu[iLoopAllLeaves].equals(currScelta)) {
%>
																<td colspan='1' class='' id="<%=idLeaves+arrCodNodo[iLoopAllLeaves]%>_ACTIVECALLS"><%=arrActiveCalls[iLoopAllLeaves]%></td>
<%
							}
						}
%>
													  		</tr>
														</table>
													</div>
												</td>
											</tr>
										</table>
									</td>
<%
					}			
					rs_L.close();
					cstmt_L.close();
%>
								</tr>
							</table>
						</td>
<%
					_nBranch++;
				}
				rs_B.close();
				cstmt_B.close();
			}
		} catch (Exception e) {
			log.error(session.getId() + " - general: " + e.getMessage(), e);
		} finally {
			try { rs_R.close(); } catch (Exception e) {}
			try { rs_B.close(); } catch (Exception e) {}
			try { rs_L.close(); } catch (Exception e) {}
			try { rs_FD.close(); } catch (Exception e) {}
			try { rs_HT.close(); } catch (Exception e) {}
			try { cstmt_R.close(); } catch (Exception e) {}
			try { cstmt_B.close(); } catch (Exception e) {}
			try { cstmt_L.close(); } catch (Exception e) {}
			try { cstmt_FD.close(); } catch (Exception e) {}
			try { cstmt_HT.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}
%>
					</tr>
				</table>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>
<%
	}
%>
	
<div id="change_filter" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td><h2>Cambia Data di Visualizzazione</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#change_filter').hide();">
							<img alt="" class="" src="../images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_change_filter" action="Details.jsp" method="post">
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
						<button type="button" id="CloseModal" class="button blue" onclick="checkDate()">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="SaveFD" class="modal">
	<form id="form_saveFD" action="Details.jsp" method="post" >
		<input type="text" id="saction" name="action" Value="updateFD"> 
		<input type="text" id="cStatoFD" name="status" >
	</form>
</div>

<div id="SaveHT" class="modal">
	<form id="form_saveHT" action="Details.jsp" method="post" >
		<input type="text" id="saction" name="action" Value="updateHT"> 
		<input type="text" id="cStatoHT" name="status" >
	</form>
</div>

<script>
	var webSocket;
	var statArray = [];
	function isFunction(functionToCheck) {
		 return functionToCheck && {}.toString.call(functionToCheck) === '[object Function]';
		}
	function openSocket() {
		if (webSocket !== undefined && webSocket.readyState !== WebSocket.CLOSED) {
			console.warn("[openSocket] - already created");
			return;
		}
		// console.debug("[openSocket] -> opening");
		webSocket = new WebSocket("ws://" + location.host + "/DashBoard/WSSDetails");
		console.debug("[openSocket] -  completed");
		webSocket.onopen = function(event) {
			console.log("[openSocket] <- event onopen");
			var js = {};
			js['command'] = 'register';
			js['CodIvr'] = '<%=CodIvr%>'; //da moficare con codice variabile				
			console.debug("[openSocket] -> sent " + JSON.stringify(js));
			webSocket.send(JSON.stringify(js));
		};
		
		webSocket.onmessage = function(event) {
			// console.debug("[onmessage] <- " + event.data);
			var jData = JSON.parse(event.data);
			var rn ="";
			var v=""
			try {
				statArray[jData["ReportName"]] = jData;
				rn = jData["ReportName"];
				v = jData["Value"];
				var result =  parseFloat(v);
				result = Math.round(result) ;
				// console.debug("document.getElementById(" + rn + ").innerHTML = "+ v);
				document.getElementById(rn).innerHTML = result;
				if (isFunction(document.getElementById(rn).onchange))
						document.getElementById(rn).onchange();
				
			} catch (e) {
				console.warn("[onmessage] - error on received message " + e);
				try {
					sendError("document.getElementById(" + rn + ").innerHTML = " +v+"; -> " + e);
				} catch (e) {
					alert(e);
				}
			}

		}
		webSocket.onclose = function(event) {
			console.warn("[onclose] <- closed");
			//alert("Connection closed, server: " + location.host);
		};
		
	}
	sendError = function(error){
		var js = {};
		js['command'] = 'error';
		js['CodIvr'] = '<%=CodIvr%>'; //da moficare con codice variabile	
		js['msg'] =  error;
		webSocket.send(JSON.stringify(js));
	}
	
</script>

<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#MenuDetails");
			parent.ChangeActivedFooter("#Details","rt/Details.jsp");
			<%if (!StringUtils.isBlank(CodIvr)) {%>
				parent.EnabledFooterService();
				parent.ChangeActivedFooterService();
			<%}%>
		} catch (e) {}
		try {
			redrawConnection();
			$(window).resize(function() {
				$("connection").remove();
				redrawConnection();
			});
			worker();
		} catch (e) {}
	})

	redrawConnection = function() {
		<% for (int k = 0; k < _nBranch; k++) { %>
		if (!$('#Branches_<%=k%>').parent().parent().parent().parent().parent().hasClass('compress')) {
			$('#Root').connect('#Branches_<%=k%>');
			<% for (int j = 0; j < _nLeave[k]; j++) { %>
				if ($('#Leaves_<%=k%>_<%=j%>').parent().hasClass('uncompress')) {
					$('#Branches_<%=k%>').connect('#Leaves_<%=k%>_<%=j%>');
				}
			<% } %>
		}
		<% } %>
		}
	
	confirmchange_filter = function(id) {
		$("#change_filter").show();
		$("#change_filter_day").val(id);
	}
	
	checkDate = function() {
		var startDate = new Date(document.getElementById('change_filter_day').value);
		var today = new Date();
		startDate.setHours(0,0,0,0);
		today.setHours(0,0,0,0);
		if (startDate.getTime() < today.getTime()) {
			document.getElementById('form_change_filter').action="DetailsHistory.jsp";
			document.getElementById('form_change_filter').submit(); 
		} else {
			document.getElementById('form_change_filter').action="Details.jsp";
			document.getElementById('form_change_filter').submit(); 
		}
	}
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#change_filter').hide();
		}
	}

	switchMe = function(lea) {
		$("[id^='"+lea+"']").each(function(index,value){
			$(this).parent().toggleClass('compress');
			$(this).parent().toggleClass('uncompress');
		});
		$("connection").remove();
		redrawConnection();
	}

	saveFD = function(Status) {
		$("#cStatoFD").val($(Status).prop('checked')?"ON":"OFF");
		$("#form_saveFD")[0].submit();
	}

	saveHT = function(Status) {
		$("#cStatoHT").val($(Status).prop('checked')?"ON":"OFF");
		$("#form_saveHT")[0].submit();
	}

	function worker(){
		<%if (!StringUtils.isBlank(CodIvr)) {%>
		$.ajax({ 
	        type: 'GET', 
	        url: 'UpdateDetails.jsp', 
	        data: { CodIvr: '<%=CodIvr%>' }, 
	        success: function (data) { 
	        	var json = jQuery.parseJSON(data);
 	        	var i = 0;
 	        	for (i=0; i<json.length; i++) {
 	        		var action = json[i].action;
 	        		var id = json[i].id;
 	        		var value = json[i].value;
 	        		switch (action) {
 	        		case 'value':
 	        			document.getElementById(id).innerHTML = value;
 	        			break;
 	        		case 'show':
		       			$('#'+id).removeClass('compress');
 	        			break;
 	        		case 'hide':
		       			$('#'+id).addClass('compress');
 	        			break;
 	        		}
 	        	}
 	       		$("connection").remove();
 	       		redrawConnection();
	 	       	setTimeout(worker, 5000);
	        },
	        error: function (data) {
	        	setTimeout(worker, 5000);
	        }
	    });
		<%}%>
	}
	openSocket();
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>