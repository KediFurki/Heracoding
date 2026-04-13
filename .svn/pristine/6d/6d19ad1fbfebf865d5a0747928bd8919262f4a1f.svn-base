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
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RealTimeTraffic</title>
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
	//==============================
	//==	ACTION START	====
	//==	ACTION STOP		====
		
	try {			
		if (StringUtils.isBlank(CodIvr)) {
		} else {
%>
<table class="center"  >
	<tr>
		<td style="width: 10%"></td>
		<td colspan="1" >	
			<form id="form_emergency_1" action="SetEmergency.jsp" method="post">
				<table  class="roundedCorners struct" >
					<tr class="listGroupItem active blue">
						<td colspan="7">
							<table>
								<tr>
									<td>
										<div class="title">Traffico RealTime</div>
									</td>
									<td>
										<div  class="container_img blue title right">
										</div>
									</td>
								</tr>
							</table>
						</td>
					</tr>
<%
			Context ctx = new InitialContext();
			DataSource ds = null;
			String environment = (String)session.getAttribute("Environment");
			switch (environment) {
			case "CCT-F":
				ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTF");
				log.info(session.getId() + " - connection CCTF wait...");
				break;
			case "CCT-E":
				ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
				log.info(session.getId() + " - connection CCTE wait...");
				break;
			case "CCT-A":
				ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTA");
				log.info(session.getId() + " - connection CCTA wait...");
				break;
			}
			conn = ds.getConnection();
			log.info(session.getId() + " - dashboard.RealTimeTraffic_GetStructure('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.RealTimeTraffic_GetStructure(?)} ");
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
				String Sn_cod_messaggio = Utility.getNotNull(rs.getString("cod_messaggio"));
				String Sn_testo_tts = Utility.getNotNull(rs.getString("testo_tts"));
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
					<tr style="width: 100%" class="listGroupItem" id="ref_<%=refNODO%>" >
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
			parent.ChangeActivedFooter("#RealTimeTraffic","RealTimeTraffic.jsp");

		} catch (e) {}
	})
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>