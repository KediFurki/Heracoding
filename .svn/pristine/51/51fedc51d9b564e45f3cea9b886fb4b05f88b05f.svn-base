<!DOCTYPE html>
<%@page import="org.json.JSONArray"%>
<%@page import="java.util.Iterator"%>
<%@page import="comapp.Utility"%>
<%@page import="comapp.ConfigurationUtility"%>
<%@page import="org.json.JSONObject"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.apache.log4j.Logger"%>
<%@page import="java.sql.Connection"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DissuasionDetail</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
</head>
<body style="overflow-y: auto;">
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
	DataSource ds = null;
	Connection conn = null;
	Connection connCnf = null;
	CallableStatement cstmt = null;
	CallableStatement cstmtCnf = null;
	ResultSet rs = null;
	ResultSet rsCnf = null;
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String p_Esigenze = request.getParameter("nEsigenze");
	String environment = (String)session.getAttribute("Environment");
	String dbsystemproperties = (String)session.getAttribute("DBSystemProperties");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+action  );
	try {
		Context ctx = new InitialContext();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CNF");
		log.info(session.getId() + " - connection CNF wait...");
		connCnf = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "SAVE" :
				int iEsigenze = 0;
				try { iEsigenze = Integer.parseInt(p_Esigenze); } catch (Exception e) {}
				for (int iLoop=1;iLoop<=iEsigenze;iLoop++) {
					String p_IdLimiteON = request.getParameter("IdLimiteON_"+iLoop);
					String p_LimiteON = request.getParameter("LimiteON_"+iLoop);
					String p_TransactionON = request.getParameter("TransactionON_"+iLoop);
					String p_IdLimiteOFF = request.getParameter("IdLimiteOFF_"+iLoop);
					String p_LimiteOFF = request.getParameter("LimiteOFF_"+iLoop);
					String p_TransactionOFF = request.getParameter("TransactionOFF_"+iLoop);
					if (StringUtils.isNoneBlank(p_IdLimiteON) || StringUtils.isNoneBlank(p_IdLimiteOFF)) {
						String transactionName = "";
						transactionName = (StringUtils.isNoneBlank(p_TransactionON))?p_TransactionON:p_TransactionOFF;
						JSONObject jo = ConfigurationUtility.getTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), transactionName);
						log.info(session.getId() + " - "+transactionName+" -> "+jo.toString());
						Iterator<String> jokeys = jo.keys();
						while(jokeys.hasNext()) {
						    String key = jokeys.next();
						    JSONArray ja = jo.getJSONArray(key);
						    for (int ija=0; ija<ja.length();ija++) {
								JSONObject jajo = ja.getJSONObject(ija);
								Iterator<String> jajokeys = jajo.keys();
								while(jajokeys.hasNext()) {
								    String jkey = jajokeys.next();
								    String jvalue = jajo.getString(jkey);
									if (StringUtils.isNoneBlank(p_IdLimiteON)) {
										if (jkey.equals("DISSUASIONE_LIMITE_ON")) {
				 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_LimiteON);
											jajo.put(jkey, p_LimiteON);
										}
									}
									if (StringUtils.isNoneBlank(p_IdLimiteOFF)) {
										if (jkey.equals("DISSUASIONE_LIMITE_OFF")) {
				 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_LimiteOFF);
											jajo.put(jkey, p_LimiteOFF);
										}
									}
								}
						    }
						}
						log.info(session.getId() + " - "+transactionName+" -> "+jo.toString());
						ConfigurationUtility.saveTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), transactionName, jo);
// 						if (StringUtils.isNoneBlank(p_IdLimiteON)) {
// 							log.info(session.getId() + " - dashboard.Dissuasion_UpdateThreshold('" + p_IdLimiteON + "','"+ p_LimiteON + "')");
// 							cstmt = connCnf.prepareCall("{ call dashboard.Dissuasion_UpdateThreshold(?,?)} ");
// 							cstmt.setInt(1, Integer.parseInt(p_IdLimiteON));
// 							cstmt.setString(2, p_LimiteON);
// 							cstmt.execute();					
// 							log.debug(session.getId() + " - executeCall complete");
// 						}
// 						try { cstmt.close(); } catch (Exception e) {}
// 						if (StringUtils.isNoneBlank(p_IdLimiteOFF)) {
// 							log.info(session.getId() + " - dashboard.Dissuasion_UpdateThreshold('" + p_IdLimiteOFF + "','"+ p_LimiteOFF + "')");
// 							cstmt = connCnf.prepareCall("{ call dashboard.Dissuasion_UpdateThreshold(?,?)} ");
// 							cstmt.setInt(1, Integer.parseInt(p_IdLimiteOFF));
// 							cstmt.setString(2, p_LimiteOFF);
// 							cstmt.execute();					
// 							log.debug(session.getId() + " - executeCall complete");
// 						}
// 						try { cstmt.close(); } catch (Exception e) {}
					}
				}
				break;
			default:
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
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td style="width: 40%">
			<form id="form_without_control" action="DissuasionDetail.jsp" method="post">
				<input type="hidden" name="action" value="SAVE">
				<table id="stickytable_1" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active blue">
						<td colspan='7'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Esigenze</div>
									</td>
									<td>
										<div class="container title right blue">
											<label></label>										
										</div>

									</td>
								</tr>
							</table>
						</td>
					</tr>
			 	</thead>
			 	<tbody>
<%
	try {
		int nEsigenze = 0;
		boolean fPresenzaEsigenze = false;
		boolean fPresenzaSoglie = false;
		log.info(session.getId() + " - dashboard.Dissuasion_GetEsigenze('" + CodIvr + "')");
		cstmt = conn.prepareCall("{ call dashboard.Dissuasion_GetEsigenze(?)} ");
		cstmt.setString(1, CodIvr);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rs.next()) {
			nEsigenze++;
			fPresenzaEsigenze = true;
			String descrizione_nodo = rs.getString("descrizione_nodo");
			String id_esigenza = rs.getString("id_esigenza");
%>
				<tr>
					<td><label><%=descrizione_nodo%></label></td>
					<td><label><%=id_esigenza%></label></td>
<%
			int i=0;
			log.info(session.getId() + " - dashboard.Dissuasion_GetEsigenzaThreshold('" + id_esigenza + "')");
			cstmtCnf = connCnf.prepareCall("{ call dashboard.Dissuasion_GetEsigenzaThreshold(?)} ");
			cstmtCnf.setString(1, id_esigenza);
			rsCnf = cstmtCnf.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			while (rsCnf.next()) {
				int dbid =  rsCnf.getInt("dbid"); 
				String key = rsCnf.getString("key");
				String value = rsCnf.getString("value");
				String transactionname = rsCnf.getString("transactionname");
				if (key.equals("DISSUASIONE_LIMITE_ON")) {
					i++;
					fPresenzaSoglie = true;
%>
					<td>DISSUASIONE LIMITE ON<input type="number" id="LimiteON_<%=nEsigenze%>" name="LimiteON_<%=nEsigenze%>" min="0" value="<%=value%>">
						<input type="hidden" id="IdLimiteON_<%=nEsigenze%>" name="IdLimiteON_<%=nEsigenze%>" Value="<%=dbid%>"> 
						<input type="hidden" id="TransactionON_<%=nEsigenze%>" name="TransactionON_<%=nEsigenze%>" Value="<%=transactionname%>"> 
					</td>
<%
				} else if (key.equals("DISSUASIONE_LIMITE_OFF")) {
					i++;
					fPresenzaSoglie = true;
%>
					<td>DISSUASIONE LIMITE OFF<input type="number" id="LimiteOFF_<%=nEsigenze%>" name="LimiteOFF_<%=nEsigenze%>" min="0" value="<%=value%>">
						<input type="hidden" id="IdLimiteOFF_<%=nEsigenze%>" name="IdLimiteOFF_<%=nEsigenze%>" Value="<%=dbid%>"> 
						<input type="hidden" id="TransactionOFF_<%=nEsigenze%>" name="TransactionOFF_<%=nEsigenze%>" Value="<%=transactionname%>"> 
					</td>
<%					
				}
			}
			for (int x=i; x<2; x++) {
%>
				<td></td>
<%					
			}
			try { rsCnf.close(); } catch (Exception e) {}
			try { cstmtCnf.close(); } catch (Exception e) {}
%>		
				</tr>
<%
		}
		if (fPresenzaEsigenze) {
			if (fPresenzaSoglie) {
%>
					<tr>
						<td colspan="3">
						<input type="hidden" id="nEsigenze" name="nEsigenze" Value="<%=nEsigenze%>"> 
							<button type="submit" class="button blue">SALVA</button>
						</td>
					</tr>
<%
			} else {
%>
				<tr>
					<td colspan="3">
						<div class="head_parameter_alert" style="text-align: center;">NESSUNA SOGLIA CONFIGURATA</div>
					</td>
				</tr>
<%			
			}
		} else {
%>
					<tr>
						<td colspan="3">
							<div class="head_parameter_alert" style="text-align: center;">NESSUNA ESIGENZA CONFIGURATA</div>
						</td>
					</tr>
<%			
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { rsCnf.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { cstmtCnf.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
		try { connCnf.close(); } catch (Exception e) {}
	}
%>
				</tbody>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>

<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#Dissuasion","DissuasionDetail.jsp");
		} catch (e) {
		}
 		$("#stickytable_1").stickyTableHeaders();
	})

	window.onclick = function(event) {
		if (event.target.className == 'modal') {
		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>