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
<title>DissuasionCCCDetail</title>
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
				String p_DISSUASIONE_LIMITE_OFF = request.getParameter("DISSUASIONE_LIMITE_OFF");
				String p_DISSUASIONE_LIMITE_ON = request.getParameter("DISSUASIONE_LIMITE_ON");
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
			<form id="form_without_control" action="DissuasionCCCDetail.jsp" method="post">
				<input type="hidden" name="action" value="SAVE">
				<input type="hidden" name="idesigenza" Value="<%=p_idesigenza%>"> 
				<table id="stickytable_1" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active blue">
						<td colspan='7'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Parametri</div>
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
		boolean fDISSUASIONE_LIMITE_OFF = false;
		int DISSUASIONE_LIMITE_OFF = -1;
		boolean fDISSUASIONE_LIMITE_ON = false;
		int DISSUASIONE_LIMITE_ON = -1;
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
			case "DISSUASIONE_LIMITE_OFF":
				fDISSUASIONE_LIMITE_OFF = true;
				DISSUASIONE_LIMITE_OFF = Integer.parseInt(value);
				break;
			case "DISSUASIONE_LIMITE_ON":
				fDISSUASIONE_LIMITE_ON = true;
				DISSUASIONE_LIMITE_ON = Integer.parseInt(value);
				break;
			}
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
													<div class="title">DISSUASIONE</div>
												</td>
											</tr>
										</table>
									</td>
								</tr>

								<tr style="width: 100%" class="listGroupItem" >
<%
		if (fDISSUASIONE_LIMITE_ON) {
%>
									<td>DISSUASIONE_LIMITE_ON</td>
									<td><input type="number" name="DISSUASIONE_LIMITE_ON" min="0" value="<%=DISSUASIONE_LIMITE_ON%>"></td>
<%
		}
		if (fDISSUASIONE_LIMITE_OFF) {
%>
									<td>DISSUASIONE_LIMITE_OFF</td>
									<td><input type="number" name="DISSUASIONE_LIMITE_OFF" min="0" value="<%=DISSUASIONE_LIMITE_OFF%>"></td>
<%
		}
%>
								</tr>
							</table>
						</td>
					</tr>
<%
		if (fCALLBACK_LIMITE_OFF ||
			fCALLBACK_LIMITE_ON ||
			fDISSUASIONE_LIMITE_OFF ||
			fDISSUASIONE_LIMITE_ON) {

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