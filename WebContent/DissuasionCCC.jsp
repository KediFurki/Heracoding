<!DOCTYPE html>
<%@page import="java.util.HashMap"%>
<%@page import="java.util.Map"%>
<%@page import="org.json.JSONArray"%>
<%@page import="java.util.Iterator"%>
<%@page import="comapp.Utility"%>
<%@page import="comapp.ConfigurationUtility"%>
<%@page import="org.json.JSONObject"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
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
<title>DissuasionCCC</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/getBrowser.js"></script>
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
	DataSource ds = null;
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	Connection connCnf = null;
	CallableStatement cstmtCnf = null;
	ResultSet rsCnf = null;
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	String dbsystemproperties = (String)session.getAttribute("DBSystemProperties");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+action  );
	try {
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "SAVE" :
				Map<String,Map<String,String>> mTransaction = new HashMap<String,Map<String,String>>();	
				String s_numesi = request.getParameter("numesi");
				int p_numesi = Integer.parseInt(s_numesi);
				String s_value = "";
				String s_key = "";
				String s_trans = "";
				boolean s_flag = false;

				s_trans = request.getParameter("flag_t");
				s_key = request.getParameter("flag_k");
				s_value = request.getParameter("flag_v");
				s_flag = s_value==null?false:true;
				s_value = s_value==null?"false":"true";
				if (StringUtils.isNotBlank(s_trans) && StringUtils.isNotBlank(s_key)) {
					Map<String,String> mKeyValueList = null;
					if (mTransaction.get(s_trans)!=null) {
						mKeyValueList = mTransaction.get(s_trans);
					} else {
						mKeyValueList = new HashMap<String,String>();
					}
					log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
					mKeyValueList.put(s_key, s_value);
					mTransaction.put(s_trans, mKeyValueList);
				}
				
				s_trans = request.getParameter("tmm_t");
				if (StringUtils.isNotBlank(s_trans)) {
					s_key = "TEMPO_MIN";
					s_value = request.getParameter("tmm_min");
					Map<String,String> mKeyValueList = null;
					if (mTransaction.get(s_trans)!=null) {
						mKeyValueList = mTransaction.get(s_trans);
					} else {
						mKeyValueList = new HashMap<String,String>();
					}
					log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
					mKeyValueList.put(s_key, s_value);
					mTransaction.put(s_trans, mKeyValueList);

					s_key = "TEMPO_MAX";
					s_value = request.getParameter("tmm_max");
					mKeyValueList = null;
					if (mTransaction.get(s_trans)!=null) {
						mKeyValueList = mTransaction.get(s_trans);
					} else {
						mKeyValueList = new HashMap<String,String>();
					}
					log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
					mKeyValueList.put(s_key, s_value);
					mTransaction.put(s_trans, mKeyValueList);
				}
				
				if (s_flag) {
					s_trans = request.getParameter("tann_t");
					if (StringUtils.isNotBlank(s_trans)) {
						s_key = "CALLBACK_LIMITE_ON";
						s_value = request.getParameter("clon");
						Map<String,String> mKeyValueList = null;
						if (mTransaction.get(s_trans)!=null) {
							mKeyValueList = mTransaction.get(s_trans);
						} else {
							mKeyValueList = new HashMap<String,String>();
						}
						log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
						mKeyValueList.put(s_key, s_value);
						mTransaction.put(s_trans, mKeyValueList);
	
						s_key = "CALLBACK_LIMITE_OFF";
						s_value = request.getParameter("cloff");
						mKeyValueList = null;
						if (mTransaction.get(s_trans)!=null) {
							mKeyValueList = mTransaction.get(s_trans);
						} else {
							mKeyValueList = new HashMap<String,String>();
						}
						log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
						mKeyValueList.put(s_key, s_value);
						mTransaction.put(s_trans, mKeyValueList);
	
						s_key = "DISSUASIONE_LIMITE_ON";
						s_value = request.getParameter("dlon");
						mKeyValueList = null;
						if (mTransaction.get(s_trans)!=null) {
							mKeyValueList = mTransaction.get(s_trans);
						} else {
							mKeyValueList = new HashMap<String,String>();
						}
						log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
						mKeyValueList.put(s_key, s_value);
						mTransaction.put(s_trans, mKeyValueList);
	
						s_key = "DISSUASIONE_LIMITE_OFF";
						s_value = request.getParameter("dloff");
						mKeyValueList = null;
						if (mTransaction.get(s_trans)!=null) {
							mKeyValueList = mTransaction.get(s_trans);
						} else {
							mKeyValueList = new HashMap<String,String>();
						}
						log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
						mKeyValueList.put(s_key, s_value);
						mTransaction.put(s_trans, mKeyValueList);
					}
				} else {
					for (int ie=1; ie<=p_numesi; ie++) {
						s_trans = request.getParameter("esi_t_"+ie);
						if (StringUtils.isNotBlank(s_trans)) {
							s_key = "CALLBACK_LIMITE_ON";
							s_value = request.getParameter("clon_"+ie);
							Map<String,String> mKeyValueList = null;
							if (mTransaction.get(s_trans)!=null) {
								mKeyValueList = mTransaction.get(s_trans);
							} else {
								mKeyValueList = new HashMap<String,String>();
							}
							log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
							mKeyValueList.put(s_key, s_value);
							mTransaction.put(s_trans, mKeyValueList);
		
							s_key = "CALLBACK_LIMITE_OFF";
							s_value = request.getParameter("cloff_"+ie);
							mKeyValueList = null;
							if (mTransaction.get(s_trans)!=null) {
								mKeyValueList = mTransaction.get(s_trans);
							} else {
								mKeyValueList = new HashMap<String,String>();
							}
							log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
							mKeyValueList.put(s_key, s_value);
							mTransaction.put(s_trans, mKeyValueList);
		
							s_key = "DISSUASIONE_LIMITE_ON";
							s_value = request.getParameter("dlon_"+ie);
							mKeyValueList = null;
							if (mTransaction.get(s_trans)!=null) {
								mKeyValueList = mTransaction.get(s_trans);
							} else {
								mKeyValueList = new HashMap<String,String>();
							}
							log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
							mKeyValueList.put(s_key, s_value);
							mTransaction.put(s_trans, mKeyValueList);
		
							s_key = "DISSUASIONE_LIMITE_OFF";
							s_value = request.getParameter("dloff_"+ie);
							mKeyValueList = null;
							if (mTransaction.get(s_trans)!=null) {
								mKeyValueList = mTransaction.get(s_trans);
							} else {
								mKeyValueList = new HashMap<String,String>();
							}
							log.info(session.getId() + " - trans:"+s_trans+" - key:"+s_key+" -> "+s_value);
							mKeyValueList.put(s_key, s_value);
							mTransaction.put(s_trans, mKeyValueList);
						}
					}
				}
				
				for (String transactionName : mTransaction.keySet()) {
					Map<String,String> mKeyValueList = mTransaction.get(transactionName);
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
	 							if (mKeyValueList.get(jkey) != null) {
	 								String nValue = mKeyValueList.get(jkey);
		 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+nValue);
	 								jajo.put(jkey, nValue);
	 							}
	 						}
	 				    }
	 				}
	 				log.info(session.getId() + " - "+transactionName+" -> "+jo.toString());
	 				ConfigurationUtility.saveTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), transactionName, jo);
				}
				break;
			default:
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
		
	try {			
		if (StringUtils.isBlank(CodIvr)) {
		} else {
			Context ctx = new InitialContext();
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CNF");
			log.info(session.getId() + " - connection CNF wait...");
			connCnf = ds.getConnection();
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
			log.info(session.getId() + " - connection CCC wait...");
			conn = ds.getConnection();
			String bgclass = "";
%>
<table class="center"  >
	<tr>
		<td style="width: 10%"></td>
		<td style="width: 30%">
			<form id="form_dissuasion" action="DissuasionCCC.jsp" method="post">
				<input type="hidden" id="mAction" name="action" Value="SAVE"> 
				<input type="hidden" id="CodIvr" name="CodIvr" Value="<%=CodIvr%>"> 
				<table class="roundedCorners small" >
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan="7">
								<table>
									<tr>
										<td>
											<div class="title">Parametri Dissuasione</div>
										</td>
										<td>
											<div class="container title right">
												<label></label>										
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</thead>
				 	<tbody>
						<tr style="width: 100%" class="listGroupItem" >
							<td>
								<table class="roundedCorners small">
<% 									// ###### HEAD SERVIZI ###### %>
									<tr class="listGroupItem active blue">
										<td colspan="5"><div class="title">per SERVIZI</div></td>
									</tr>
<% 									// ###### ROW SWITCH ###### %>
									<tr class="listGroupItem active lightblue">
<%
			String flag_value = "false";
			String flag_transactionname = "";
			String flag_key = "";
			log.info(session.getId() + " - dashboard.Dissuasion_GetFlagDissuasion('" + CodIvr + "')");
			cstmtCnf = connCnf.prepareCall("{ call dashboard.Dissuasion_GetFlagDissuasion(?)} ");
			cstmtCnf.setString(1, CodIvr);
			rsCnf = cstmtCnf.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			if (rsCnf.next()) {
				flag_transactionname = rsCnf.getString("transactionname");
				flag_key = rsCnf.getString("key");
				flag_value = rsCnf.getString("value");
%>
<%
			}
			try { rsCnf.close(); } catch (Exception e) {}
			try { cstmtCnf.close(); } catch (Exception e) {}
%>
										<td class="nopadding_lr">
											<input type="hidden" name="flag_k" value="<%=flag_key%>">
											<input type="hidden" name="flag_t" value="<%=flag_transactionname%>">
											<label class="switch"><input type="checkbox" id="flag_v" name="flag_v" <%=((flag_value.equalsIgnoreCase("true"))?"checked":"")%> onchange="flag_onchange()"><span class="slider round blue"></span></label>
											<span id="flag_text" style="margin-left: 10px;"><%=((flag_value.equalsIgnoreCase("true"))?"Attivo":"Disattivo")%></span>
										</td>
										<td colspan="4"><div class="title"></div></td>
									</tr>
<% 									// ###### ROW MINMAX ###### %>
									<tr>
										<td><div class="title"></div></td>
<%
			int numEsigenze = 0;
			String tailName = "";
			String transactionnameMinMax = "";
 			boolean fTEMPO_MIN = false;
			int TEMPO_MIN = -1;
 			boolean fTEMPO_MAX = false;
			int TEMPO_MAX = -1;	
			
			log.info(session.getId() + " - dashboard.Dissuasion_GetTimeMinMax('" + CodIvr + "')");
			cstmtCnf = connCnf.prepareCall("{ call dashboard.Dissuasion_GetTimeMinMax(?)} ");
			cstmtCnf.setString(1, CodIvr);
			rsCnf = cstmtCnf.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			while (rsCnf.next()) {
				String key = rsCnf.getString("key");
				String value = rsCnf.getString("value");
				switch (key) {
				case "TEMPO_MIN":
					transactionnameMinMax =  rsCnf.getString("transactionname"); 
					fTEMPO_MIN = true;
					TEMPO_MIN = Integer.parseInt(value);
					break;
				case "TEMPO_MAX":
					transactionnameMinMax =  rsCnf.getString("transactionname"); 
					fTEMPO_MAX = true;
					TEMPO_MAX = Integer.parseInt(value);
					break;
				}
			}
			try { rsCnf.close(); } catch (Exception e) {}
			try { cstmtCnf.close(); } catch (Exception e) {}
%>
										<input type="hidden" name="tmm_t" value="<%=Utility.getNotNull(transactionnameMinMax)%>">
<%
			bgclass = (StringUtils.isNotBlank(transactionnameMinMax) && fTEMPO_MIN)?"":"redthr ";
%>
										<td class="<%=bgclass%>">
											TEMPO_MIN<br>
											<input type="number" name="tmm_min" style="width: 50px;" value="<%=TEMPO_MIN != -1 ? TEMPO_MIN : ""%>" min="0">
										</td>
<%
			bgclass = (StringUtils.isNotBlank(transactionnameMinMax) && fTEMPO_MIN)?"":"redthr ";
%>
										<td class="<%=bgclass%>">
											TEMPO_MAX<br>
											<input type="number" name="tmm_max" style="width: 50px;" value="<%=TEMPO_MAX != -1 ? TEMPO_MAX : ""%>" min="0">
										</td>
										<td colspan="2"><div class="title"></div></td>
									</tr>
<% 									// ###### ROW ANNEX ###### %>
									<tr>
										<td><div class="title"></div></td>
<%
			String transactionnameAnnex = null;
			boolean fCALLBACK_LIMITE_OFF = false;
			int CALLBACK_LIMITE_OFF = -1;
			boolean fCALLBACK_LIMITE_ON = false;
			int CALLBACK_LIMITE_ON = -1;
			boolean fDISSUASIONE_LIMITE_OFF = false;
			int DISSUASIONE_LIMITE_OFF = -1;
			boolean fDISSUASIONE_LIMITE_ON = false;
			int DISSUASIONE_LIMITE_ON = -1;

			log.info(session.getId() + " - dashboard.Dissuasion_GetAnnexIvr('" + CodIvr + "')");
			cstmtCnf = connCnf.prepareCall("{ call dashboard.Dissuasion_GetAnnexIvr(?)} ");
			cstmtCnf.setString(1, CodIvr);
			rsCnf = cstmtCnf.executeQuery();
			while (rsCnf.next()) {
				int dbid =  rsCnf.getInt("dbid"); 
				String key = rsCnf.getString("key");
				String value = rsCnf.getString("value");
				switch (key) {
				case "CALLBACK_LIMITE_OFF":
					transactionnameAnnex = rsCnf.getString("transactionname");
					fCALLBACK_LIMITE_OFF = true;
					CALLBACK_LIMITE_OFF = Integer.parseInt(value);
					break;
				case "CALLBACK_LIMITE_ON":
					transactionnameAnnex = rsCnf.getString("transactionname");
					fCALLBACK_LIMITE_ON = true;
					CALLBACK_LIMITE_ON = Integer.parseInt(value);
					break;
				case "DISSUASIONE_LIMITE_OFF":
					transactionnameAnnex = rsCnf.getString("transactionname");
					fDISSUASIONE_LIMITE_OFF = true;
					DISSUASIONE_LIMITE_OFF = Integer.parseInt(value);
					break;
				case "DISSUASIONE_LIMITE_ON":
					transactionnameAnnex = rsCnf.getString("transactionname");
					fDISSUASIONE_LIMITE_ON = true;
					DISSUASIONE_LIMITE_ON = Integer.parseInt(value);
					break;
				}
			}
			try { rsCnf.close(); } catch (Exception e) {}
			try { cstmtCnf.close(); } catch (Exception e) {}
%>
										<input type="hidden" name="tann_t" value="<%=Utility.getNotNull(transactionnameAnnex)%>">
<%
			bgclass = (StringUtils.isNotBlank(transactionnameAnnex) && fCALLBACK_LIMITE_ON)?"":"redthr ";
%>
										<td class="<%=bgclass%>">
											CALLBACK_LIMITE_ON<br><br>
											<input type="number" name="clon" id="clon" min="0" style="width: 50px;" value="<%=CALLBACK_LIMITE_ON != -1 ? CALLBACK_LIMITE_ON : ""%>">
										</td>
<%
			bgclass = (StringUtils.isNotBlank(transactionnameAnnex) && fCALLBACK_LIMITE_OFF)?"":"redthr ";
%>
										<td class="<%=bgclass%>">
											CALLBACK_LIMITE_OFF<br><br>
											<input type="number" name="cloff" id="cloff" min="0" style="width: 50px;" value="<%=CALLBACK_LIMITE_OFF != -1 ? CALLBACK_LIMITE_OFF : ""%>">
										</td>
<%
			bgclass = (StringUtils.isNotBlank(transactionnameAnnex) && fDISSUASIONE_LIMITE_ON)?"":"redthr ";
%>
										<td class="<%=bgclass%>">
											DISSUASIONE_LIMITE_ON<br>(Attivazione Soglia Dissuasione Chiamate in Coda)<br>
											<input type="number" name="dlon" id="dlon" min="0" style="width: 50px;" value="<%=DISSUASIONE_LIMITE_ON != -1 ? DISSUASIONE_LIMITE_ON : ""%>">
										</td>
<%
			bgclass = (StringUtils.isNotBlank(transactionnameAnnex) && fDISSUASIONE_LIMITE_OFF)?"":"redthr ";
%>
										<td class="<%=bgclass%>">
											DISSUASIONE_LIMITE_OFF<br>(Disattivazione Soglia Dissuasione Chiamate in Coda)<br>
											<input type="number" name="dloff" id="dloff" min="0" style="width: 50px;" value="<%=DISSUASIONE_LIMITE_OFF != -1 ? DISSUASIONE_LIMITE_OFF : ""%>">
										</td>
									</tr>
<% 									// ###### HEAD ESIGENZE ###### %>
									<tr class="listGroupItem active blue">
										<td colspan="5"><div class="title">per ESIGENZE</div></td>
									</tr>
<% 									// ###### ROW HEAD ESIGENZE ###### %>
									<tr class="listGroupItem active lightblue">
										<td class="middle"><div class="title center">ESIGENZA</div></td>
										<td class="middle"><div class="title center">CALLBACK_LIMITE_ON</div></td>
										<td class="middle"><div class="title center">CALLBACK_LIMITE_OFF</div></td>
										<td class="middle"><div class="title center">DISSUASIONE_LIMITE_ON<br>(Attivazione Soglia Dissuasione Chiamate in Coda)</div></td>
										<td class="middle"><div class="title center">DISSUASIONE_LIMITE_OFF<br>(Disattivazione Soglia Dissuasione Chiamate in Coda)</div></td>
									</tr>
<% 									// ###### ROWS ###### %>
<%
			log.info(session.getId() + " - dashboard.Dissuasion_GetEsigenze('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.Dissuasion_GetEsigenze(?)} ");
			cstmt.setString(1, CodIvr);
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			while (rs.next()) {
				numEsigenze++;
				tailName = ""+numEsigenze;
				
				String id_esigenza = rs.getString("id_esigenza");
				
				String transactionnameEsi = null;
				fCALLBACK_LIMITE_OFF = false;
				CALLBACK_LIMITE_OFF = -1;
				fCALLBACK_LIMITE_ON = false;
				CALLBACK_LIMITE_ON = -1;
				fDISSUASIONE_LIMITE_OFF = false;
				DISSUASIONE_LIMITE_OFF = -1;
				fDISSUASIONE_LIMITE_ON = false;
				DISSUASIONE_LIMITE_ON = -1;

				log.info(session.getId() + " - dashboard.Dissuasion_GetAnnexEsigenza('" + id_esigenza + "')");
				cstmtCnf = connCnf.prepareCall("{ call dashboard.Dissuasion_GetAnnexEsigenza(?)} ");
				cstmtCnf.setString(1, id_esigenza);
				rsCnf = cstmtCnf.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
				while (rsCnf.next()) {
					int dbid =  rsCnf.getInt("dbid"); 
					String key = rsCnf.getString("key");
					String value = rsCnf.getString("value");
					switch (key) {
					case "CALLBACK_LIMITE_OFF":
						transactionnameEsi = rsCnf.getString("transactionname");
 						fCALLBACK_LIMITE_OFF = true;
						CALLBACK_LIMITE_OFF = Integer.parseInt(value);
						break;
					case "CALLBACK_LIMITE_ON":
						transactionnameEsi = rsCnf.getString("transactionname");
 						fCALLBACK_LIMITE_ON = true;
						CALLBACK_LIMITE_ON = Integer.parseInt(value);
						break;
					case "DISSUASIONE_LIMITE_OFF":
						transactionnameEsi = rsCnf.getString("transactionname");
 						fDISSUASIONE_LIMITE_OFF = true;
						DISSUASIONE_LIMITE_OFF = Integer.parseInt(value);
						break;
					case "DISSUASIONE_LIMITE_ON":
						transactionnameEsi = rsCnf.getString("transactionname");
 						fDISSUASIONE_LIMITE_ON = true;
						DISSUASIONE_LIMITE_ON = Integer.parseInt(value);
						break;
					}
				}				
%>
									<tr class="listGroupItem">
										<td>
											<div><%=id_esigenza%></div>
											<input type="hidden" name="esi_t_<%=tailName%>" value="<%=Utility.getNotNull(transactionnameEsi)%>">
										</td>
<%
				bgclass = (StringUtils.isNotBlank(transactionnameEsi) && fCALLBACK_LIMITE_ON)?"":"redthr ";
%>
										<td class="<%=bgclass%>">
											<input type="number" id="clon_<%=tailName%>" name="clon_<%=tailName%>" min="0" style="width: 50px;" value="<%=CALLBACK_LIMITE_ON != -1 ? CALLBACK_LIMITE_ON : ""%>">
										</td>
<%
				bgclass = (StringUtils.isNotBlank(transactionnameEsi) && fCALLBACK_LIMITE_OFF)?"":"redthr ";
%>
										<td class="<%=bgclass%>">
											<input type="number" id="cloff_<%=tailName%>" name="cloff_<%=tailName%>" min="0" style="width: 50px;" value="<%=CALLBACK_LIMITE_OFF != -1 ? CALLBACK_LIMITE_OFF : ""%>">
										</td>
<%
				bgclass = (StringUtils.isNotBlank(transactionnameEsi) && fDISSUASIONE_LIMITE_ON)?"":"redthr ";
%>
										<td class="<%=bgclass%>">
											<input type="number" id="dlon_<%=tailName%>" name="dlon_<%=tailName%>" min="0" style="width: 50px;" value="<%=DISSUASIONE_LIMITE_ON != -1 ? DISSUASIONE_LIMITE_ON : ""%>">
										</td>
<%
				bgclass = (StringUtils.isNotBlank(transactionnameEsi) && fDISSUASIONE_LIMITE_OFF)?"":"redthr ";
%>
										<td class="<%=bgclass%>">
											<input type="number" id="dloff_<%=tailName%>" name="dloff_<%=tailName%>" min="0" style="width: 50px;" value="<%=DISSUASIONE_LIMITE_OFF != -1 ? DISSUASIONE_LIMITE_OFF : ""%>">
										</td>
									</tr>
<%
			}
%>
									<input type="hidden" id="numesi" name="numesi" value="<%=tailName%>">
								</table>
							</td>
						</tr>
						<tr>
							<td colspan="2">
								<button type="button" class="button blue" onclick="save_onclick();">SALVA</button>
							</td>
						</tr>
					</tbody>
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
		try { rsCnf.close(); } catch (Exception e) {}
		try { cstmtCnf.close(); } catch (Exception e) {}
		try { connCnf.close(); } catch (Exception e) {}
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>

<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#DissuasionCCC","DissuasionCCC.jsp");
		} catch (e) {}
		aggiornaStato();
	})
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
		}
	}
	
	save_onclick = function() {
		$('#form_dissuasion')[0].submit();
	}

	aggiornaStato = function() {
		var _status = $("#flag_v").is(':checked');
		$("#clon").prop("disabled", !_status);
		$("#cloff").prop("disabled", !_status);
		$("#dlon").prop("disabled", !_status);
		$("#dloff").prop("disabled", !_status);
		$("input[id^='clon_']").prop("disabled", _status);
		$("input[id^='cloff_']").prop("disabled", _status);
		$("input[id^='dlon_']").prop("disabled", _status);
		$("input[id^='dloff_']").prop("disabled", _status);
		$("#flag_text").text(_status?"Attivo":"Disattivo");
	}
	
	flag_onchange = function() {
		aggiornaStato();
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>