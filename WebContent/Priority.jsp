<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="comapp.Utility"%>
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
<title>Priority</title>
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
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String p_CodNodo = request.getParameter("CodNodo");
	String p_Priorita = request.getParameter("Priorita");
	//==============================
	//==	ACTION START	====
 	log.info(session.getId() + " - action:"+ action+" CodNodo: "+p_CodNodo+" Priorita:"+p_Priorita );			
	try {
		Context ctx = new InitialContext();
		DataSource ds = null;
		String environment = (String)session.getAttribute("Environment");
		switch (environment) {
		case "CCT-F":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTF");
			log.info(session.getId() + " - connection CCTF wait...");
			break;
		case "CCT-A":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTA");
			log.info(session.getId() + " - connection CCTA wait...");
			break;
		case "STC":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"STC");
			log.info(session.getId() + " - connection STC wait...");
			break;
		}
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
		case "SAVE" :
			log.info(session.getId() + " - dashboard.Priority_Update('" + CodIvr + "','"+ p_CodNodo + "','" + p_Priorita + "')");
			cstmt = conn.prepareCall("{ call dashboard.Priority_Update(?,?,?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			cstmt.setString(2, p_CodNodo);
			cstmt.setInt(3, Integer.parseInt(p_Priorita));
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
		<td colspan="1" >	
			<form id="form_emergency_1" action="Priority.jsp" method="post">
				<table id="stickytable" class="roundedCorners struct" >
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan="7">
								<table>
									<tr>
										<td>
											<div class="title">Priorità</div>
										</td>
										<td>
											<div  class="container_img blue title right">
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
<%
			log.info(session.getId() + " - dashboard.Priority_GetStructure('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.Priority_GetStructure(?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			int nLevel = 0;
			while (rs.next()) {
				String Si_COD_NODO = rs.getString("COD_NODO");
				String Si_ETICHETTA = rs.getString("ETICHETTA");
				String Si_COD_NODO_PADRE = rs.getString("COD_NODO_PADRE");
				String Si_SCELTA_MENU = rs.getString("SCELTA_MENU");
				String Si_TIPO = rs.getString("TIPO");
				String Si_ORDINE = rs.getString("ORDINE");
				int Si_LIVELLO =  rs.getInt("LIVELLO"); 
				String Sn_stato = Utility.getNotNull(rs.getString("stato")); 
				int Ane_priorita = rs.getInt("priorita");
				String Ane_id_esigenza = Utility.getNotNull(rs.getString("id_esigenza"));
				String refNODO = (StringUtils.isBlank(Si_COD_NODO_PADRE)?Si_COD_NODO:Si_COD_NODO_PADRE+"-"+Si_COD_NODO);
				String image = "";
				switch (Si_TIPO) {
				case "INGRESSO":
					image = "<img  src='images/ingresso.gif'>";
					break;
				case "MENU":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/menu_ON.png'>";	break;
					case "OFF":	image = "<img  src='images/menu_OFF.png'>";	break;
					default:	image = "<img  src='images/menu.png'>";		break;
					}
					break;
				case "MESSAGGIO":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/messaggio_ON.jpg'>";		break;
					case "OFF":	image = "<img  src='images/messaggio_OFF.jpg'>";	break;
					default:	image = "<img  src='images/messaggio.jpg'>";		break;
					}
					break;
				case "TRASFERIMENTO":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/trasferimento_ON.jpg'>";		break;
					case "OFF":	image = "<img  src='images/trasferimento_OFF.jpg'>";	break;
					default:	image = "<img  src='images/trasferimento.jpg'>";		break;
					}
					break;
				case "RITORNO":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/freccia_ON.jpg'>";	break;
					case "OFF":	image = "<img  src='images/freccia_OFF.jpg'>";	break;
					default:	image = "<img  src='images/freccia.jpg'>";		break;
					}
					break;
				case "INSERIMENTO":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/inserimento_ON.jpg'>";	break;
					case "OFF":	image = "<img  src='images/inserimento_OFF.jpg'>";	break;
					default:	image = "<img  src='images/inserimento.jpg'>";		break;
					}
					break;
				case "CONTROLLO":
					switch (Sn_stato) {
					case "ON":	image = "<img  src='images/controllo_ON.jpg'>";		break;
					case "OFF":	image = "<img  src='images/controllo_OFF.jpg'>";	break;
					default:	image = "<img  src='images/controllo.jpg'>";		break;
					}
					break;
				default:
					image = "";
					break;
				}
%>
					<tr style="width: 100%" class="listGroupItem" id="ref_<%=refNODO%>" <%if(Ane_priorita>0){%> onclick="return change('<%=Si_COD_NODO%>', '<%=Si_ETICHETTA%>', '<%=Ane_priorita%>', '<%=Ane_id_esigenza%>')"<%}%>>
<%-- 					<tr style="width: 100%" class="listGroupItem" id="ref_<%=refNODO%>" <%if(!StringUtils.isBlank(Sn_stato)){%> onclick="return change(codnodo, etichetta, priorita, esigenza)"<%}%>> --%>
<%
				if (Si_LIVELLO>0) {
%>
						<td colspan="<%=Si_LIVELLO%>" style="width: 26px"></td>
<%
				}
%>
						<td style="width: 26px">
						 	<div class="container_img struct grey right">
								<%=image%>
							</div>
						</td>
						<td colspan="<%=(8-Si_LIVELLO)%>"><%=(StringUtils.isBlank(Si_SCELTA_MENU)?Si_ETICHETTA:Si_SCELTA_MENU+" - "+Si_ETICHETTA)%></td>
					</tr>
<%
			}
			try { rs.close(); } catch (Exception e) {}
		 	try { cstmt.close(); } catch (Exception e) {}
%>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>		
<%
		}
%>

<div id="Modify" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Modifica</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#Modify').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_emergency_2" action="Priority.jsp" method="post">
			<input type="hidden" id="mAction" name="action" Value="SAVE"> <input type="hidden" id="mCodNodo" name="CodNodo" readonly="readonly">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label>Nodo</label>
					</td>
					<td>
						<label class="bold" id="mEtichetta"></label>
					</td>
				</tr>
				<tr><td colspan="2"></td></tr>
				<tr>
					<td>
						<label>Esigenza</label>
					</td>
					<td>
						<label class="bold" id="mEsigenza"></label>
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label for="mPriorita">Priorità</label>
					</td>
					<td>
						<select id="mPriorita" name="Priorita" readonly=true>
							<option value="1">1</option>
							<option value="2">2</option>
							<option value="3">3</option>
							<option value="4">4</option>
							<option value="5">5</option>
							<option value="6">6</option>
							<option value="7">7</option>
							<option value="8">8</option>
							<option value="9">9</option>
							<option value="10">10</option>
						</select>
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="" class="button blue">SALVA</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>
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
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#Priority","Priority.jsp");
		} catch (e) {}
 		$("#stickytable").stickyTableHeaders();
	})

	change = function(codnodo, etichetta, priorita, esigenza){
		$('#Modify').show();
		$("#mCodNodo").val(codnodo);
		$("#mEtichetta").html(etichetta);
		$("#mPriorita").val(priorita);
		$("#mEsigenza").html(esigenza);
		return false;			
	}
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#Modify').hide();
		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>