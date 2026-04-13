<!DOCTYPE html>
<%@page import="java.sql.Types"%>
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
<title>PriorityService</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
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
	Connection connCnf = null;
	CallableStatement cstmtCnf = null;
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
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "SAVE" :
				String s_value = request.getParameter("pri_v");
				String s_trans = request.getParameter("pri_t");
				String s_key = request.getParameter("pri_k");
				if (StringUtils.isNoneBlank(s_value)) {
	 				JSONObject jo = ConfigurationUtility.getTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), s_trans);
	 				log.info(session.getId() + " - "+s_trans+" -> "+jo.toString());
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
								if (jkey.equals(s_key)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+s_value);
	 								jajo.put(jkey, s_value);
								}
	 						}
	 				    }
	 				}
	 				log.info(session.getId() + " - "+s_trans+" -> "+jo.toString());
	 				ConfigurationUtility.saveTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), s_trans, jo);
				}
				break;
			default:
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	} finally {
// 		try { cstmt.close(); } catch (Exception e) {}
// 		try { conn.close(); } catch (Exception e) {}
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
%>
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td style="width: 40%">
			<form id="form_without_control" action="PriorityService.jsp" method="post">
				<input type="hidden" name="action" value="SAVE">
				<input type="hidden" id="CodIvr" name="CodIvr" value="<%=CodIvr%>">
				<table id="stickytable_1" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active blue">
						<td colspan='2'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Priorità su Servizio</div>
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
		Context ctx = new InitialContext();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CNF");
		log.info(session.getId() + " - connection CNF wait...");
		connCnf = ds.getConnection();
		
		String pri_t = "";
		String pri_k = "";
		String pri_v = "--";
		boolean fPRIORITA_SERVIZIO = false;
		int PRIORITA_SERVIZIO = -1;
		log.info(session.getId() + " - dashboard.PriorityService_GetAnnex('" + CodIvr + "')");
		cstmtCnf = connCnf.prepareCall("{ call dashboard.PriorityService_GetAnnex(?)} ");
		cstmtCnf.setString(1, CodIvr);
		rsCnf = cstmtCnf.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		if (rsCnf.next()) {
			int dbid =  rsCnf.getInt("dbid"); 
			pri_k = rsCnf.getString("key");
			pri_v = rsCnf.getString("value");
			pri_t = rsCnf.getString("transactionname");
			fPRIORITA_SERVIZIO = true;
			PRIORITA_SERVIZIO = Integer.parseInt(pri_v);
		}
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td>PRIORITA'</td>
						<td>
							<input type="hidden" name="pri_t" Value="<%=pri_t%>"> 
							<input type="hidden" name="pri_k" Value="<%=pri_k%>"> 
							<input type="number" name="pri_v" min="0" max="10000" value="<%=PRIORITA_SERVIZIO%>">
						</td>
					</tr>
<%
		if (fPRIORITA_SERVIZIO) {
%>
					<tr>
						<td colspan="2">
							<button type="submit" class="button blue">SALVA</button>
						</td>
					</tr>
<%
		} else {
%>
					<tr>
						<td colspan="2">
							<div class="head_parameter_alert" style="text-align: center;">NESSUNA PRIORITA' SU SERVIZIO CONFIGURATA</div>
						</td>
					</tr>
<%			
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rsCnf.close(); } catch (Exception e) {}
		try { cstmtCnf.close(); } catch (Exception e) {}
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
			parent.ChangeActivedFooter("#PriorityService","PriorityService.jsp");
		} catch (e) {
		}
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