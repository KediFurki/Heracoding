<!DOCTYPE html>
<%@page import="java.util.ArrayList"%>
<%@page import="java.util.List"%>
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
<title>DistributionCCTDetail</title>
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
	String p_transactionname = request.getParameter("transactionname");
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
				String p_POLO1 = request.getParameter("POLO1");
				String p_POLO2 = request.getParameter("POLO2");
				List<String> p_OVERFLOW = new ArrayList<String>();
				String p_OVERFLOWPOLO1 = request.getParameter("OVERFLOWPOLO1");
				if (p_OVERFLOWPOLO1!=null) p_OVERFLOW.add("POLO1");
				String p_OVERFLOWPOLO2 = request.getParameter("OVERFLOWPOLO2");
				if (p_OVERFLOWPOLO2!=null) p_OVERFLOW.add("POLO2");
				String pc_OVERFLOW = (p_OVERFLOW.isEmpty())?"":String.join(";", p_OVERFLOW);
				String p_TIMEOUT_OVERFLOW = request.getParameter("TIMEOUT_OVERFLOW");
				JSONObject jo = ConfigurationUtility.getTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), p_transactionname);
				log.info(session.getId() + " - "+p_transactionname+" -> "+jo.toString());
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
							if (jkey.equals("POLO1")) {
								if (StringUtils.isNoneBlank(p_POLO1)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_POLO1);
									jajo.put(jkey, p_POLO1);
								}
							}
							if (jkey.equals("POLO2")) {
								if (StringUtils.isNoneBlank(p_POLO2)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_POLO2);
									jajo.put(jkey, p_POLO2);
								}
							}
							if (jkey.equals("OVERFLOW")) {
	 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+pc_OVERFLOW);
								jajo.put(jkey, pc_OVERFLOW);
							}
							if (jkey.equals("TIMEOUT_OVERFLOW")) {
								if (StringUtils.isNoneBlank(p_TIMEOUT_OVERFLOW)) {
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_TIMEOUT_OVERFLOW);
									jajo.put(jkey, p_TIMEOUT_OVERFLOW);
								}
							}
						}
				    }
				}
				log.info(session.getId() + " - "+p_transactionname+" -> "+jo.toString());
				ConfigurationUtility.saveTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), p_transactionname, jo);
				break;
			default:
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
	if (StringUtils.isBlank(p_transactionname)) {
	} else {
%>
<table class="center">
	<tr>
		<td style="width: 5%"></td>
		<td style="width: 90%">
			<form id="form_without_control" action="DistributionSTCDetail.jsp" method="post">
				<input type="hidden" name="action" value="SAVE">
				<input type="hidden" name="transactionname" Value="<%=p_transactionname%>"> 
				<table id="stickytable_1" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active blue">
						<td colspan='7'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Distribuzione</div>
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
		boolean fPOLO1 = false;
		int POLO1 = 0;
		boolean fPOLO2 = false;
		int POLO2 = 0;
		boolean fOVERFLOW = false;
		String OVERFLOW = "";
		boolean fTIMEOUT_OVERFLOW = false;
		int TIMEOUT_OVERFLOW = -1;
		Context ctx = new InitialContext();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CNF");
		log.info(session.getId() + " - connection CNF wait...");
		connCnf = ds.getConnection();
		log.info(session.getId() + " - dashboard.Distribution_GetAnnexTransaction('" + p_transactionname + "')");
		cstmtCnf = connCnf.prepareCall("{ call dashboard.Distribution_GetAnnexTransaction(?)} ");
		cstmtCnf.setString(1, p_transactionname);
		rsCnf = cstmtCnf.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rsCnf.next()) {
			int dbid =  rsCnf.getInt("dbid"); 
			String key = rsCnf.getString("key");
			String value = rsCnf.getString("value");
			switch (key) {
			case "POLO1":
				fPOLO1= true;
				POLO1 = Integer.parseInt(value);
				break;
			case "POLO2":
				fPOLO2 = true;
				POLO2 = Integer.parseInt(value);
				break;
			case "OVERFLOW":
				fOVERFLOW = true;
				OVERFLOW = value;
				OVERFLOW = (StringUtils.isBlank(OVERFLOW)?"":OVERFLOW);
				break;
			case "TIMEOUT_OVERFLOW":
				fTIMEOUT_OVERFLOW = true;
				TIMEOUT_OVERFLOW = Integer.parseInt(value);
				break;
			}
		}
%>
					<tr style="width: 100%" class="listGroupItem" >
						<td colspan="2">POLO1</td>
						<td colspan="2">POLO2</td>
					</tr>
					<tr style="width: 100%" class="listGroupItem" >
						<td>
							<label class="switch">
								<input type="checkbox" name="OVERFLOWPOLO1" <%=((OVERFLOW.contains("POLO1")) ? "checked=true" : "")%>>
								<span class="slider round blue"></span>
							</label>
						</td>
						<td id="POLO1_bg">
							<input type="number" name="POLO1" id="POLO1" min="0" max="100" value="<%=POLO1%>" onchange="delta_onchange('1',this,100)">
						</td>
						<td>
							<label class="switch">
								<input type="checkbox" name="OVERFLOWPOLO2" <%=((OVERFLOW.contains("POLO2")) ? "checked=true" : "")%>>
								<span class="slider round blue"></span>
							</label>
						</td>
						<td id="POLO2_bg">
							<input type="number" name="POLO2" id="POLO2" min="0" max="100" value="<%=POLO2%>" onchange="delta_onchange('2',this,100)">
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
													<div class="title">OVERFLOW</div>
												</td>
											</tr>
										</table>
									</td>
								</tr>
								<tr style="width: 100%" class="listGroupItem" >
									<td colspan="4">TIMEOUT OVERFLOW
										<input type="number" name="TIMEOUT_OVERFLOW" min="0" value="<%=TIMEOUT_OVERFLOW%>">
									</td>
								</tr>
							</table>
						</td>
					</tr>
<%
		if (fPOLO1 ||
			fPOLO2 ||
			fOVERFLOW ||
			fTIMEOUT_OVERFLOW) {

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
 		delta_check();
	})

	window.onclick = function(event) {
		if (event.target.className == 'modal') {
		}
	}
	
	delta_onchange = function(esi,_this,max) {
		if ($.isNumeric($(_this).val())) {
			var value = parseInt($(_this).val(), 10);
			if (value<0) 
				$(_this).val(0);
			else if (value>max) 
				$(_this).val(max);
		} else {
			$(_this).val(0);
		}
		
		delta_check();
	}
	
	delta_check = function() {
		var distot = 0;
		for (var ip = 1; ip <= 2; ip++) {
			if ($.isNumeric($('#POLO'+ip).val()))
				distot += parseInt($('#POLO'+ip).val(), 10);
		}
		var delta = distot-100;
		for (var ip = 1; ip <= 2; ip++) {
			if (delta==0) {
				$('#POLO'+ip+'_bg').removeClass("redthr");
			} else {	
				$('#POLO'+ip+'_bg').addClass("redthr");
			}
		}
	} 
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>