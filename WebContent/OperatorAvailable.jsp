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
<title>OperatorAvailable</title>
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
	Connection conn = null;
	CallableStatement cstmt = null;
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
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CNF");
		log.info(session.getId() + " - connection CNF wait...");
		connCnf = ds.getConnection();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "SAVE" :
				int iEsigenze = 0;
				String p_FLAG_ATTIVA_OP_DISP = request.getParameter("FLAG_ATTIVA_OP_DISP");
				if (p_FLAG_ATTIVA_OP_DISP==null) p_FLAG_ATTIVA_OP_DISP = "false";
				p_FLAG_ATTIVA_OP_DISP = (p_FLAG_ATTIVA_OP_DISP.equalsIgnoreCase("ON")) ? "true" : "false";
				String p_transactionname = request.getParameter("transactionname");
				String transactionName = p_transactionname;
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
							if (jkey.equals("FLAG_ATTIVA_OP_DISP")) {
	 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_FLAG_ATTIVA_OP_DISP);
								jajo.put(jkey, p_FLAG_ATTIVA_OP_DISP);
							}
						}
				    }
				}
				log.info(session.getId() + " - "+transactionName+" -> "+jo.toString());
				ConfigurationUtility.saveTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), transactionName, jo);
				break;
			default:
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	} finally {
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
%>
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td style="width: 40%">
			<form id="form_without_control" action="OperatorAvailable.jsp" method="post">
				<input type="hidden" name="action" value="SAVE">
				<table id="stickytable_1" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active blue">
						<td colspan='2'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Operatore Disponibile</div>
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
		String transactionname = "";
		boolean fFLAG_ATTIVA_OP_DISP = false;
		boolean FLAG_ATTIVA_OP_DISP = false;
		log.info(session.getId() + " - dashboard.OperatorAvailable_GetAnnex('" + CodIvr + "')");
		cstmtCnf = connCnf.prepareCall("{ call dashboard.OperatorAvailable_GetAnnex(?)} ");
		cstmtCnf.setString(1, CodIvr);
		rsCnf = cstmtCnf.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rsCnf.next()) {
			int dbid =  rsCnf.getInt("dbid"); 
			String key = rsCnf.getString("key");
			String value = rsCnf.getString("value");
			transactionname = rsCnf.getString("transactionname");
			switch (key) {
			case "FLAG_ATTIVA_OP_DISP":
				fFLAG_ATTIVA_OP_DISP = true;
				FLAG_ATTIVA_OP_DISP = value.equalsIgnoreCase("true");
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td>FLAG_ATTIVA_OP_DISP'</td>
						<td><label class='switch'><input type='checkbox' name='FLAG_ATTIVA_OP_DISP' <%=((FLAG_ATTIVA_OP_DISP)?"checked=true":"")%>><span class='slider round green'></span></label></td>
						<input type="number" name="FLAG_ATTIVA_OP_DISP" min="0" max="10000" value="<%=FLAG_ATTIVA_OP_DISP%>">
					</tr>
<%
				break;
			}
		}
		if (fFLAG_ATTIVA_OP_DISP) {
%>
					<tr>
						<td colspan="2">
							<input type="hidden" id="transactionname" name="transactionname" Value="<%=transactionname%>"> 
							<button type="submit" class="button blue">SALVA</button>
						</td>
					</tr>
<%
		} else {
%>
					<tr>
						<td colspan="2">
							<div class="head_parameter_alert" style="text-align: center;">NESSUN FLAG OPERATORE DISPONIBILE CONFIGURATO</div>
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
			parent.ChangeActivedFooter("#OperatorAvailable","OperatorAvailable.jsp");
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