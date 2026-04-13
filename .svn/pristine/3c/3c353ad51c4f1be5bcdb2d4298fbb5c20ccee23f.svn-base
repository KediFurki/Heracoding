<!DOCTYPE html>
<%@page import="org.json.JSONArray"%>
<%@page import="java.util.Iterator"%>
<%@page import="comapp.Utility"%>
<%@page import="org.json.JSONObject"%>
<%@page import="comapp.ConfigurationUtility"%>
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
<title>DissuasionCCTDetail</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
</head>
<body style="overflow-y: auto;">
<%
	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");
	//==	REQUEST PARAMETERS	====
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
	String p_idesigenza = request.getParameter("idesigenza");
	String p_descrizionenodo = request.getParameter("descrizionenodo");
	String environment = (String)session.getAttribute("Environment");
	String dbsystemproperties = (String)session.getAttribute("DBSystemProperties");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+action  );
	try {
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "SAVE" :
				//XXXXXX
				String p_CALLBACK_LIMITE_OFF = request.getParameter("CALLBACK_LIMITE_OFF");
				String p_CALLBACK_LIMITE_ON = request.getParameter("CALLBACK_LIMITE_ON");
				String p_DISSUASIONE_CALLBACK = request.getParameter("DISSUASIONE_CALLBACK");
				p_DISSUASIONE_CALLBACK = p_DISSUASIONE_CALLBACK==null?"OFF":p_DISSUASIONE_CALLBACK.toUpperCase();
				String p_DISSUASIONE_LIMITE_OFF = request.getParameter("DISSUASIONE_LIMITE_OFF");
				String p_DISSUASIONE_LIMITE_ON = request.getParameter("DISSUASIONE_LIMITE_ON");
				String p_DISSUASIONE_MANUALE = request.getParameter("DISSUASIONE_MANUALE");
				p_DISSUASIONE_MANUALE = p_DISSUASIONE_MANUALE==null?"OFF":p_DISSUASIONE_MANUALE.toUpperCase();
				String p_FLAG_ATTIVA_CALLBACK = request.getParameter("FLAG_ATTIVA_CALLBACK");
				p_FLAG_ATTIVA_CALLBACK = p_FLAG_ATTIVA_CALLBACK==null?"false":(p_FLAG_ATTIVA_CALLBACK.equalsIgnoreCase("on")?"true":"false");
				String p_MSGATTESA_LIMITE_OFF = request.getParameter("MSGATTESA_LIMITE_OFF");
				String p_MSGATTESA_LIMITE_ON = request.getParameter("MSGATTESA_LIMITE_ON");
				String p_SOGLIA_CALLBACK = request.getParameter("SOGLIA_CALLBACK");
				String p_T_MAX_ATTESA = request.getParameter("T_MAX_ATTESA");
				String p_TIMEOUT_ATTESA	= request.getParameter("TIMEOUT_ATTESA");
				JSONObject jo = ConfigurationUtility.getTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), p_idesigenza);
				log.info(session.getId() + " - "+p_idesigenza+" -> "+jo.toString());
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
							if (jkey.equals("CALLBACK_LIMITE_OFF")) {
								if (StringUtils.isNoneBlank(p_CALLBACK_LIMITE_OFF)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_CALLBACK_LIMITE_OFF);
									jajo.put(jkey, p_CALLBACK_LIMITE_OFF);
								}
							}
							if (jkey.equals("CALLBACK_LIMITE_ON")) {
								if (StringUtils.isNoneBlank(p_CALLBACK_LIMITE_ON)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_CALLBACK_LIMITE_ON);
									jajo.put(jkey, p_CALLBACK_LIMITE_ON);
								}
							}
							if (jkey.equals("DISSUASIONE_CALLBACK")) {
								if (StringUtils.isNoneBlank(p_DISSUASIONE_CALLBACK)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_DISSUASIONE_CALLBACK);
									jajo.put(jkey, p_DISSUASIONE_CALLBACK);
								}
							}
							if (jkey.equals("DISSUASIONE_LIMITE_OFF")) {
								if (StringUtils.isNoneBlank(p_DISSUASIONE_LIMITE_OFF)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_DISSUASIONE_LIMITE_OFF);
									jajo.put(jkey, p_DISSUASIONE_LIMITE_OFF);
								}
							}
							if (jkey.equals("DISSUASIONE_LIMITE_ON")) {
								if (StringUtils.isNoneBlank(p_DISSUASIONE_LIMITE_ON)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_DISSUASIONE_LIMITE_ON);
									jajo.put(jkey, p_DISSUASIONE_LIMITE_ON);
								}
							}
							if (jkey.equals("DISSUASIONE_MANUALE")) {
								if (StringUtils.isNoneBlank(p_DISSUASIONE_MANUALE)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_DISSUASIONE_MANUALE);
									jajo.put(jkey, p_DISSUASIONE_MANUALE);
								}
							}
							if (jkey.equals("FLAG_ATTIVA_CALLBACK")) {
								if (StringUtils.isNoneBlank(p_FLAG_ATTIVA_CALLBACK)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_FLAG_ATTIVA_CALLBACK);
									jajo.put(jkey, p_FLAG_ATTIVA_CALLBACK);
								}
							}
							if (jkey.equals("MSGATTESA_LIMITE_OFF")) {
								if (StringUtils.isNoneBlank(p_MSGATTESA_LIMITE_OFF)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_MSGATTESA_LIMITE_OFF);
									jajo.put(jkey, p_MSGATTESA_LIMITE_OFF);
								}
							}
							if (jkey.equals("MSGATTESA_LIMITE_ON")) {
								if (StringUtils.isNoneBlank(p_MSGATTESA_LIMITE_ON)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_MSGATTESA_LIMITE_ON);
									jajo.put(jkey, p_MSGATTESA_LIMITE_ON);
								}
							}
							if (jkey.equals("SOGLIA_CALLBACK")) {
								if (StringUtils.isNoneBlank(p_SOGLIA_CALLBACK)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_SOGLIA_CALLBACK);
									jajo.put(jkey, p_SOGLIA_CALLBACK);
								}
							}
							if (jkey.equals("T_MAX_ATTESA")) {
								if (StringUtils.isNoneBlank(p_T_MAX_ATTESA)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_T_MAX_ATTESA);
									jajo.put(jkey, p_T_MAX_ATTESA);
								}
							}
							if (jkey.equals("TIMEOUT_ATTESA")) {
								if (StringUtils.isNoneBlank(p_TIMEOUT_ATTESA)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_TIMEOUT_ATTESA);
									jajo.put(jkey, p_TIMEOUT_ATTESA);
								}
							}
						}
				    }
				}
				log.info(session.getId() + " - "+p_idesigenza+" -> "+jo.toString());
				ConfigurationUtility.saveTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), p_idesigenza, jo);
				break;
			default:
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
	if (StringUtils.isBlank(p_idesigenza)) {
	} else {
%>
<table class="center">
	<tr>
		<td style="width: 5%"></td>
		<td style="width: 90%">
			<form id="form_without_control" action="DissuasionSTCDetail.jsp" method="post">
				<input type="hidden" name="action" value="SAVE">
				<input type="hidden" name="idesigenza" Value="<%=p_idesigenza%>"> 
				<table id="stickytable_1" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active blue">
						<td colspan='7'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Parametri Dissuasione</div>
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
		boolean fCALLBACK_LIMITE_OFF = false;
		int CALLBACK_LIMITE_OFF = -1;
		boolean fCALLBACK_LIMITE_ON = false;
		int CALLBACK_LIMITE_ON = -1;
		boolean fDISSUASIONE_CALLBACK = false;
		String DISSUASIONE_CALLBACK = null;
		boolean fDISSUASIONE_LIMITE_OFF = false;
		int DISSUASIONE_LIMITE_OFF = -1;
		boolean fDISSUASIONE_LIMITE_ON = false;
		int DISSUASIONE_LIMITE_ON = -1;
		boolean fDISSUASIONE_MANUALE = false;
		String DISSUASIONE_MANUALE = null;
		boolean fFLAG_ATTIVA_CALLBACK = false;
		String FLAG_ATTIVA_CALLBACK = null;
		boolean fMSGATTESA_LIMITE_OFF = false;
		int MSGATTESA_LIMITE_OFF = -1;
		boolean fMSGATTESA_LIMITE_ON = false;
		int MSGATTESA_LIMITE_ON = -1;
		boolean fSOGLIA_CALLBACK = false;
		int SOGLIA_CALLBACK = -1;
		boolean fT_MAX_ATTESA = false;
		int T_MAX_ATTESA = -1;
		boolean fTIMEOUT_ATTESA = false;				
		int TIMEOUT_ATTESA = -1;				
		Context ctx = new InitialContext();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CNF");
		log.info(session.getId() + " - connection CNF wait...");
		connCnf = ds.getConnection();
		log.info(session.getId() + " - dashboard.Dissuasion_GetAnnexEsigenza('" + p_idesigenza + "')");
		cstmtCnf = connCnf.prepareCall("{ call dashboard.Dissuasion_GetAnnexEsigenza(?)} ");
		cstmtCnf.setString(1, p_idesigenza);
		rsCnf = cstmtCnf.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rsCnf.next()) {
			int dbid =  rsCnf.getInt("dbid"); 
			String key = rsCnf.getString("key");
			String value = rsCnf.getString("value");
			switch (key) {
			case "CALLBACK_LIMITE_OFF":
				fCALLBACK_LIMITE_OFF = true;
				CALLBACK_LIMITE_OFF = Integer.parseInt(value);
				break;
			case "CALLBACK_LIMITE_ON":
				fCALLBACK_LIMITE_ON = true;
				CALLBACK_LIMITE_ON = Integer.parseInt(value);
				break;
			case "DISSUASIONE_CALLBACK":
				fDISSUASIONE_CALLBACK = true;
				DISSUASIONE_CALLBACK = value;
				break;
			case "DISSUASIONE_LIMITE_OFF":
				fDISSUASIONE_LIMITE_OFF = true;
				DISSUASIONE_LIMITE_OFF = Integer.parseInt(value);
				break;
			case "DISSUASIONE_LIMITE_ON":
				fDISSUASIONE_LIMITE_ON = true;
				DISSUASIONE_LIMITE_ON = Integer.parseInt(value);
				break;
			case "DISSUASIONE_MANUALE":
				fDISSUASIONE_MANUALE = true;
				DISSUASIONE_MANUALE = value;
				break;
			case "FLAG_ATTIVA_CALLBACK":
				fFLAG_ATTIVA_CALLBACK = true;
				FLAG_ATTIVA_CALLBACK = value;
				break;
			case "MSGATTESA_LIMITE_OFF":
				fMSGATTESA_LIMITE_OFF = true;
				MSGATTESA_LIMITE_OFF = Integer.parseInt(value);
				break;
			case "MSGATTESA_LIMITE_ON":
				fMSGATTESA_LIMITE_ON = true;
				MSGATTESA_LIMITE_ON = Integer.parseInt(value);
				break;
			case "SOGLIA_CALLBACK":
				fSOGLIA_CALLBACK = true;
				SOGLIA_CALLBACK = Integer.parseInt(value);
				break;
			case "T_MAX_ATTESA":
				fT_MAX_ATTESA = true;
				T_MAX_ATTESA = Integer.parseInt(value);
				break;
			case "TIMEOUT_ATTESA":
				fTIMEOUT_ATTESA = true;
				TIMEOUT_ATTESA = Integer.parseInt(value);
				break;
			}
		}
		if (fDISSUASIONE_MANUALE) {
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td>DISSUASIONE_MANUALE<br/>(Attivazione Dissuasione)</td>
						<td colspan="3">
							<label class="switch">
								<input type="checkbox" name="DISSUASIONE_MANUALE" <%=((DISSUASIONE_MANUALE.equalsIgnoreCase("ON")) ? "checked=true" : "")%>>
								<span class="slider round blue"></span>
							</label>
						</td>
					</tr>
<%
		}
%>
					<tr style="width: 100%">
						<td colspan="4">
							<table class="roundedCorners small">
								<tr class="listGroupItem active lightblue">
									<td colspan='4'>
										<table>
											<tr>
												<td style="width: 45%">
													<div class="title">CALLBACK</div>
												</td>
											</tr>
										</table>
									</td>
								</tr>
<%
		if (fFLAG_ATTIVA_CALLBACK) {
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td>FLAG_ATTIVA_CALLBACK<br/>(Attivazione CallBack)</td>
						<td colspan="3">
							<label class="switch">
								<input type="checkbox" name="FLAG_ATTIVA_CALLBACK" <%=((FLAG_ATTIVA_CALLBACK.equalsIgnoreCase("true")) ? "checked=true" : "")%>>
								<span class="slider round blue"></span>
							</label>
						</td>
					</tr>
<%
		}
		if (fSOGLIA_CALLBACK) {
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td>SOGLIA_CALLBACK<br/>(Numero Max CallBack Accettabile)</td>
						<td colspan="3"><input type="number" name="SOGLIA_CALLBACK" min="0" value="<%=SOGLIA_CALLBACK%>"></td>
					</tr>
<%
		}
%>
					<tr style="width: 100%" class="listGroupItem" >
<%
		if (fCALLBACK_LIMITE_ON) {
%>
						<td>CALLBACK_LIMITE_ON</td>
						<td><input type="number" name="CALLBACK_LIMITE_ON" min="0" value="<%=CALLBACK_LIMITE_ON%>"></td>
<%
		}
		if (fCALLBACK_LIMITE_OFF) {
%>
						<td>CALLBACK_LIMITE_OFF</td>
						<td><input type="number" name="CALLBACK_LIMITE_OFF" min="0" value="<%=CALLBACK_LIMITE_OFF%>"></td>
<%
		}
%>
					</tr>
<%
		if (fDISSUASIONE_CALLBACK) {
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td>DISSUASIONE_CALLBACK<br/>(Attivazione Dissuasione su CallBack)</td>
						<td colspan="3">
							<label class="switch">
								<input type="checkbox" name="DISSUASIONE_CALLBACK" <%=((DISSUASIONE_CALLBACK.equalsIgnoreCase("ON")) ? "checked=true" : "")%>>
								<span class="slider round blue"></span>
							</label>
						</td>
					</tr>
<%
		}
		if (fT_MAX_ATTESA) {
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td>T_MAX_ATTESA<br/>(Soglia Max Tempo Attesa prima di Dissuasione)</td>
						<td colspan="3"><input type="number" name="T_MAX_ATTESA" min="0" value="<%=T_MAX_ATTESA%>"></td>
					</tr>
<%
		}
%>
							</table>
						</td>
					</tr>
					<tr style="width: 100%">
						<td colspan="4">
							<table class="roundedCorners small">
								<tr class="listGroupItem active lightblue">
									<td colspan='4'>
										<table>
											<tr>
												<td style="width: 45%">
													<div class="title">ALTRO</div>
												</td>
											</tr>
										</table>
									</td>
								</tr>
<%
%>
					<tr style="width: 100%" class="listGroupItem" >
<%
		if (fDISSUASIONE_LIMITE_ON) {
%>
						<td>DISSUASIONE_LIMITE_ON<br/>(Attivazione Soglia Dissuasione Chiamate in Coda)</td>
						<td><input type="number" name="DISSUASIONE_LIMITE_ON" min="0" value="<%=DISSUASIONE_LIMITE_ON%>"></td>
<%
		}
		if (fDISSUASIONE_LIMITE_OFF) {
%>
						<td>DISSUASIONE_LIMITE_OFF<br/>(Disattivazione Soglia Dissuasione Chiamate in Coda)</td>
						<td><input type="number" name="DISSUASIONE_LIMITE_OFF" min="0" value="<%=DISSUASIONE_LIMITE_OFF%>"></td>
<%
		}
%>
					</tr>
					<tr style="width: 100%" class="listGroupItem" >
<%
		if (fMSGATTESA_LIMITE_ON) {
%>
						<td>MSGATTESA_LIMITE_ON<br/>(Soglia Attivazione Messaggio Attesa in Coda)</td>
						<td><input type="number" name="MSGATTESA_LIMITE_ON" min="0" value="<%=MSGATTESA_LIMITE_ON%>"></td>
<%
		}
		if (fMSGATTESA_LIMITE_OFF) {
%>
						<td>MSGATTESA_LIMITE_OFF<br/>(Soglia Disattivazione Messaggio Attesa in Coda)</td>
						<td><input type="number" name="MSGATTESA_LIMITE_OFF" min="0" value="<%=MSGATTESA_LIMITE_OFF%>"></td>
<%
		}
%>
					</tr>
<%
		if (fTIMEOUT_ATTESA) {
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td>TIMEOUT_ATTESA<br/>(Tempo Max Attesa prima di CallBack)</td>
						<td colspan="3"><input type="number" name="TIMEOUT_ATTESA" min="0" value="<%=TIMEOUT_ATTESA%>"></td>
					</tr>
							</table>
						</td>
					</tr>
<%
		}
		if (fCALLBACK_LIMITE_OFF ||
			fCALLBACK_LIMITE_ON ||
			fDISSUASIONE_CALLBACK ||
			fDISSUASIONE_LIMITE_OFF ||
			fDISSUASIONE_LIMITE_ON ||
			fDISSUASIONE_MANUALE ||
			fFLAG_ATTIVA_CALLBACK ||
			fMSGATTESA_LIMITE_OFF ||
			fMSGATTESA_LIMITE_ON ||
			fSOGLIA_CALLBACK ||
			fT_MAX_ATTESA ||
			fTIMEOUT_ATTESA) {

%>
					<tr>
						<td colspan="2">
							<button type="submit" class="button blue">SALVA</button>
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
		<td style="width: 5%"></td>
	</tr>
</table>
<%
	}
%>
<script>
	$(function() {
		try {
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