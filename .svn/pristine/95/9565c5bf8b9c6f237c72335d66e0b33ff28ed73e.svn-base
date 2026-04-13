<!DOCTYPE html>
<%@page import="java.util.ArrayList"%>
<%@page import="java.util.List"%>
<%@page import="java.util.HashMap"%>
<%@page import="java.util.Map"%>
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
<title>RoutingParameters</title>
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
	ResultSet rs = null;
	Connection connCnf2 = null;
	CallableStatement cstmtCnf2 = null;
	ResultSet rsCnf2 = null;
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
// 	String p_Esigenze = request.getParameter("nEsigenze");
	String environment = (String)session.getAttribute("Environment");
	String dbsystemproperties = (String)session.getAttribute("DBSystemProperties");
	int numPoli = 0;
	boolean actPoli[] = {true,true,true,true,true,true,true};
	for (int ip=0; ip<actPoli.length; ip++) {
		if (actPoli[ip]) numPoli++;
	}
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+action  );
	try {
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "SAVE" :
				Map<String,Map<String,String>> mTransaction = new HashMap<String,Map<String,String>>();	
				// ###### ESIGENZA ######
				String s_numesi = request.getParameter("numesi");
				int p_numesi = Integer.parseInt(s_numesi);
				// ###### PRIORITA' ######
				// 				pri_XX
				// 				pri_k_XX
				// 				pri_t_XX
				for (int ie=1; ie<=p_numesi; ie++) {
					String s_value = request.getParameter("pri_"+ie);
					String s_key = request.getParameter("pri_k_"+ie);
					String s_trans = request.getParameter("pri_t_"+ie);
					if (StringUtils.isNotBlank(s_trans) && StringUtils.isNotBlank(s_key)) {
						Map<String,String> mKeyValueList = null;
						if (mTransaction.get(s_trans)!=null) {
							mKeyValueList = mTransaction.get(s_trans);
						} else {
							mKeyValueList = new HashMap<String,String>();
						}
						mKeyValueList.put(s_key, s_value);
						mTransaction.put(s_trans, mKeyValueList);
					}
				}
				// ###### DISTRIBUZIONE ######
				// 				dispNN_XX
				// 				dispNN_k_XX
				// 				dispNN_t_XX
				for (int ie=1; ie<=p_numesi; ie++) {
					for (int ip=1; ip<=7; ip++) {
						if (actPoli[ip-1]) {
							String s_value = request.getParameter("disp"+ip+"_"+ie);
							String s_key = request.getParameter("disp"+ip+"_k_"+ie);
							String s_trans = request.getParameter("disp"+ip+"_t_"+ie);
							if (StringUtils.isNotBlank(s_trans) && StringUtils.isNotBlank(s_key)) {
								Map<String,String> mKeyValueList = null;
								if (mTransaction.get(s_trans)!=null) {
									mKeyValueList = mTransaction.get(s_trans);
								} else {
									mKeyValueList = new HashMap<String,String>();
								}
								mKeyValueList.put(s_key, s_value);
								mTransaction.put(s_trans, mKeyValueList);
							}
						}
					}
				}
				// ###### OVERFLOW ######
				// 				ovfpNN_XX
				// 				ovf_k_XX
				// 				ovf_t_XX
				for (int ie=1; ie<=p_numesi; ie++) {
					String s_key = request.getParameter("ovf_k_"+ie);
					String s_trans = request.getParameter("ovf_t_"+ie);
					if (StringUtils.isNotBlank(s_trans) && StringUtils.isNotBlank(s_key)) {
						List<String> p_overflow = new ArrayList<String>();
						for (int ip=1; ip<=7; ip++) {
							if (actPoli[ip-1]) {
								String s_value = request.getParameter("ovfp"+ip+"_"+ie);
								if (s_value!=null) p_overflow.add("P"+ip);
							}
						}
						String pc_overflow = (p_overflow.isEmpty())?"":String.join(";", p_overflow);
						Map<String,String> mKeyValueList = null;
						if (mTransaction.get(s_trans)!=null) {
							mKeyValueList = mTransaction.get(s_trans);
						} else {
							mKeyValueList = new HashMap<String,String>();
						}
						mKeyValueList.put(s_key, pc_overflow);
						mTransaction.put(s_trans, mKeyValueList);
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
			<form id="form_routing_parameters" action="RoutingParameters.jsp" method="post">
				<input type="hidden" name="action" value="SAVE">
				<input type="hidden" id="CodIvr" name="CodIvr" Value="<%=CodIvr%>"> 
				<table class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active blue">
						<td colspan='2'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Parametri Routing</div>
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
					<tr style="width: 100%" class="listGroupItem" >
						<td>
							<table class="roundedCorners small">
<% 								// ###### HEAD 1 ###### %>
								<tr class="listGroupItem active lightblue">
<% 									// ###### 1 - ESIGENZA ###### %>
									<td rowspan="4" class="middle"><div class="title center">ESIGENZA</div></td>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>
									
<% 									// ###### 2 - PRIORITA' ###### %>
									<td colspan="2" rowspan="2"><div class="title">Priorità</div></td>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>
									
<% 									// ###### 9 - DISTRIBUZIONE ###### %>
									<td colspan="<%=""+(numPoli+2)%>"><div class="title">% Distribuzione traffico</div></td>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>
									
<% 									// ###### 8 - OVERFLOW ###### %>
									<td colspan="<%=""+(numPoli+1)%>"><div class="title">Partecipazione coda Overflow</div></td>
								</tr>
<% 								// ###### HEAD 2 ###### %>
								<tr class="listGroupItem active lightblue">
<% 									// ###### 1 - ESIGENZA ###### %>
<% 									// 	Vedi HEAD 1 ###### %>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>
									
<% 									// ###### 2 - PRIORITA' ###### %>
<% 									// 	Vedi HEAD 1 ###### %>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>

<% 									// ###### 9 - DISTRIBUZIONE ###### %>
									<td><div class="title">Delta%</div></td>
<%
	for (int ip=0; ip<actPoli.length; ip++) {
		if (actPoli[ip]) {
%>
									<td><div class="title">P<%=""+(ip+1)%></div></td>
<%
		}
	}
%>
									<td><div class="title"></div></td>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>
									
<% 									// ###### 8 - OVERFLOW ###### %>
<%
	for (int ip=0; ip<actPoli.length; ip++) {
		if (actPoli[ip]) {
%>
									<td><div class="title">P<%=""+(ip+1)%></div></td>
<%
		}
	}
%>
									<td><div class="title"></div></td>
								</tr>
<% 								// ###### HEAD 3 DEFAULTs ###### %>
								<tr class="listGroupItem active ">
<% 									// ###### 1 - ESIGENZA ###### %>
<% 									// 	Vedi HEAD 1 ###### %>
									
									<td class="nopadding_lr superlightblue" style="width: 10px;">&nbsp;</td>

<% 									// ###### 2 - PRIORITA' ###### %>
<% 									// 		###### Fields DEFAULT ###### %>
									<td class="nopadding_lr superlightblue">
										<input type="number" style="width:40px;" id="pridef" min="0" max="999" value="0" onchange="checkrange_onchange(this,0,999)">
									</td>
<% 									// 		###### SET DEFAULT ###### %>
									<td class="nopadding_lr superlightblue">
										<button type="button" class="buttonsmall blue" onclick="pridef_onclick()">SET</button>
									</td>
									
									<td class="nopadding_lr superlightblue" style="width: 10px;">&nbsp;</td>
									
<% 									// ###### 9 - DISTRIBUZIONE ###### %>
<% 									// 		###### Info Delta ###### %>
									<td class="nopadding_lr superlightblue" id="deldisdef_bg"><label class="lsmall" id="deldisdef"></label></td>
<% 									// 		###### Fields DEFAULT ###### %>
<%
	for (int ip=0; ip<actPoli.length; ip++) {
		if (actPoli[ip]) {
%>
									<td class="nopadding_lr superlightblue">
										<input type="number" style="width:40px;" id="disdef<%=""+(ip+1)%>" min="0" max="100" value="0" onchange="deltadef_onchange(this,100)">
									</td>
<%
		}
	}
%>
<% 									// 		###### SET DEFAULT ###### %>
									<td class="nopadding_lr superlightblue">
										<button type="button" class="buttonsmall blue" onclick="disdef_onclick()">SET</button>
									</td>
									
									<td class="nopadding_lr superlightblue" style="width: 10px;">&nbsp;</td>
									
<% 									// ###### 8 - OVERFLOW ###### %>
<% 									// 		###### Fields DEFAULT ###### %>
<%
	for (int ip=0; ip<actPoli.length; ip++) {
		if (actPoli[ip]) {
%>
									<td class="nopadding_lr superlightblue">
										<label class="switch"><input type="checkbox" id="ovfdef<%=""+(ip+1)%>"><span class="slider round blue"></span></label>
									</td>
<%
		}
	}
%>
<% 									// 		###### SET DEFAULT ###### %>
									<td class="nopadding_lr superlightblue">
										<button type="button" class="buttonsmall blue" onclick="avfdef_onclick()">SET</button>
									</td>
								</tr>
<% 								// ###### ROW 1 COMMON ###### %>
								<tr class="listGroupItem active ">
<% 									// ###### 1 - ESIGENZA ###### %>
<% 									// 	Vedi HEAD 1 ###### %>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>

<% 									// ###### 2 - PRIORITA' ###### %>
<% 									// 		###### Empty ###### %>
									<td colspan="2" class="nopadding_lr"></td>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>
									
<% 									// ###### 9 - DISTRIBUZIONE ###### %>
<% 									// 		###### Empty ###### %>
									<td colspan="<%=""+(numPoli+2)%>" class="nopadding_lr"></td>
<%
	try {
		Context ctx = new InitialContext();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CNF");
		log.info(session.getId() + " - connection CNF wait...");
		connCnf = ds.getConnection();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CNF");
		log.info(session.getId() + " - connection CNF wait...");
		connCnf2 = ds.getConnection();
		String bgclass = "";
// 		String transactionname = "";
// // 		int dbid = 0; 
// 		String key = "";
// 		String value = "";
// 		String polo = "";
// 		Map<String,String> mTransactionname_tim = new HashMap<String,String>();	
// // 		Map<String,Integer> mDbid = new HashMap<String,Integer>();	
// 		Map<String,String> mKey_tim = new HashMap<String,String>();	
// 		Map<String,String> mValue_tim = new HashMap<String,String>();	

// 		log.info(session.getId() + " - dashboard.RoutingParameters_GetAnnexIvr('" + CodIvr + "')");
// 		cstmtCnf = connCnf.prepareCall("{ call dashboard.RoutingParameters_GetAnnexIvr(?)} ");
// 		cstmtCnf.setString(1, CodIvr);
// 		rsCnf = cstmtCnf.executeQuery();					
// 		log.debug(session.getId() + " - executeCall complete");
// 		while (rsCnf.next()) {
// //			---------------	-------	----------------------	-------
// // 			transactionname	dbid	key						value
// //			---------------	-------	----------------------	-------
// // 			ACEGAS			267393	P1_TIMEOUT_PRIMO_TRAB	60
// // 			ACEGAS			267397	P2_TIMEOUT_PRIMO_TRAB	60
// // 			ACEGAS			267401	P3_TIMEOUT_PRIMO_TRAB	60
// // 			ACEGAS			267405	P4_TIMEOUT_PRIMO_TRAB	60
// // 			ACEGAS			516334	P6_TIMEOUT_PRIMO_TRAB	60
// // 			ACEGAS			516335	P7_TIMEOUT_PRIMO_TRAB	60
// //			---------------	-------	----------------------	-------
// 			transactionname = rsCnf.getString("transactionname");
// // 			dbid =  rsCnf.getInt("dbid"); 
// 			key = rsCnf.getString("key");
// 			value = rsCnf.getString("value");	// --> tim
// 			polo = rsCnf.getString("polo");
// 			mTransactionname_tim.put(polo, transactionname);
// // 			mDbid.put(polo, dbid);
// 			mKey_tim.put(polo, key);
// 			mValue_tim.put(polo, value);
// 		}
%>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>
									
<% 									// ###### 8 - OVERFLOW ###### %>
<% 									// 		###### Empty ###### %>
									<td colspan="<%=""+(numPoli+1)%>" class="nopadding_lr"></td>
								</tr>
<%		

		int numEsigenze = 0;
		String tailName = "";

		String id_esigenza = "";
		int type2 = 0;
		String transactionname2 = "";
// 		int dbid2 = 0; 
		String key2 = "";
		String value2 = "";
		
		String mTransactionname_pri = "";	
		String mKey_pri = "";	
		String mValue_pri = "";	
		Map<String,String> mTransactionname_dis = new HashMap<String,String>();	
		Map<String,String> mKey_dis = new HashMap<String,String>();	
		Map<String,String> mValue_dis = new HashMap<String,String>();	
		String mTransactionname_ovf = "";	
		String mKey_ovf = "";	
		String mValue_ovf = "";	


		log.info(session.getId() + " - dashboard.RoutingParameters_GetEsigenze('" + CodIvr + "')");
		cstmt = conn.prepareCall("{ call dashboard.RoutingParameters_GetEsigenze(?)} ");
		cstmt.setString(1, CodIvr);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rs.next()) {
			numEsigenze++;
			tailName = ""+numEsigenze;
//			---------------
// 			id_esigenza
//			---------------
//			ACEGAS_ACEGAS
//			---------------
			id_esigenza = rs.getString("id_esigenza");

			mTransactionname_pri = "";	
			mKey_pri = "";	
			mValue_pri = "";	
			mTransactionname_dis = new HashMap<String,String>();	
			mKey_dis = new HashMap<String,String>();	
			mValue_dis = new HashMap<String,String>();	
			mTransactionname_ovf = "";	
			mKey_ovf = "";	
			mValue_ovf = "";	

			log.info(session.getId() + " - dashboard.RoutingParameters_GetAnnexDistribution('" + id_esigenza + "')");
			cstmtCnf2 = connCnf2.prepareCall("{ call dashboard.RoutingParameters_GetAnnexDistribution(?)} ");
			cstmtCnf2.setString(1, id_esigenza);
			rsCnf2 = cstmtCnf2.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			while (rsCnf2.next()) {
//				-------	-----------------------	-------	------------------	-------
// 				type	transactionname			dbid	key					value
//				-------	-----------------------	-------	------------------	-------
// 				1		PRIORITA_ESIGENZE		492791	PRIO_ACEGAS_ACEGAS	0
// 				2		PERCEN_ACEGAS_ACEGAS	530696	P1					40
// 				2		PERCEN_ACEGAS_ACEGAS	530697	P2					0
// 				2		PERCEN_ACEGAS_ACEGAS	530698	P3					60
// 				2		PERCEN_ACEGAS_ACEGAS	530699	P4					0
// 				2		PERCEN_ACEGAS_ACEGAS	530700	P6					NULL
// 				2		PERCEN_ACEGAS_ACEGAS	530701	P7					NULL
// 				3		PERCEN_ACEGAS_ACEGAS	530702	OVERFLOW			NULL
//				-------	-----------------------	-------	------------------	-------
				type2 =  rsCnf2.getInt("type"); 
				transactionname2 = rsCnf2.getString("transactionname");
// 				dbid2 =  rsCnf2.getInt("dbid"); 
				key2 = rsCnf2.getString("key");
				value2 = rsCnf2.getString("value");	// --> pri/dis/ovf
				switch (type2) {
				case 1:
					mTransactionname_pri = transactionname2;	
					mKey_pri = key2;	
					mValue_pri = value2;	
					break;
				case 2:
					mTransactionname_dis.put(key2, transactionname2);
					mKey_dis.put(key2, key2);
					mValue_dis.put(key2, value2);
					break;
				case 3:
					mTransactionname_ovf = transactionname2;	
					mKey_ovf = key2;	
					mValue_ovf = value2;
					mValue_ovf = (StringUtils.isBlank(mValue_ovf)?"":mValue_ovf);
					break;
				}
			}
			try { rsCnf2.close(); } catch (Exception e) {}
			try { cstmtCnf2.close(); } catch (Exception e) {}
%>
								<tr>
<% 									// ###### 1 - ESIGENZA ###### %>
									<td class="nopadding_lr"><%=id_esigenza%></td>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>

<% 									// ###### 2 - PRIORITA' ###### %>
<% 									// 		###### Configurazione ###### %>
<%
			bgclass = (StringUtils.isNotBlank(mTransactionname_pri) && StringUtils.isNotBlank(mKey_pri))?"":"redthr ";
%>
									<td class="<%=bgclass%>nopadding_lr">
										<input type="hidden" name="pri_t_<%=tailName%>" value="<%=Utility.getNotNull(mTransactionname_pri)%>">
										<input type="hidden" name="pri_k_<%=tailName%>" value="<%=Utility.getNotNull(mKey_pri)%>">
										<input type="number" style="width:40px;" id="pri_<%=tailName%>" name="pri_<%=tailName%>" min="0" max="999" value="<%=Utility.getNotNull(mValue_pri)%>" onchange="checkrange_onchange(this,0,999)">
									</td>
									<td class="nopadding_lr"></td>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>
									
<% 									// ###### 9 - DISTRIBUZIONE ###### %>
									<td id="deldis_<%=tailName%>_bg"><label class="nopadding_lr lsmall" id="deldis_<%=tailName%>"></label></td>
<% 									// 		###### Configurazione ###### %>
<%
	for (int ip=0; ip<actPoli.length; ip++) {
		if (actPoli[ip]) {
			transactionname2 = mTransactionname_dis.get("P"+(ip+1));
			key2 = mKey_dis.get("P"+(ip+1));
			value2 = mValue_dis.get("P"+(ip+1));
			bgclass = (StringUtils.isNotBlank(transactionname2) && StringUtils.isNotBlank(key2))?"":"redthr ";
%>
									<td class="<%=bgclass%>nopadding_lr">
										<input type="hidden" name="disp<%=""+(ip+1)%>_t_<%=tailName%>" value="<%=Utility.getNotNull(transactionname2)%>">
										<input type="hidden" name="disp<%=""+(ip+1)%>_k_<%=tailName%>" value="<%=Utility.getNotNull(key2)%>">
										<input type="number" style="width:40px;" id="disp<%=""+(ip+1)%>_<%=tailName%>" name="disp<%=""+(ip+1)%>_<%=tailName%>" min="0" max="100" value="<%=Utility.getNotNull(value2)%>" onchange="delta_onchange(<%=tailName%>,this,100)">
									</td>
<%
		}
	}
%>
									<td class="nopadding_lr"></td>
									
									<td class="nopadding_lr" style="width: 10px;">&nbsp;</td>

<% 									// ###### 8 - OVERFLOW ###### %>
<%
			bgclass = (StringUtils.isNotBlank(mTransactionname_pri) && StringUtils.isNotBlank(mKey_pri))?"":"redthr ";
%>
<% 									// 		###### Configurazione ###### %>
<%
	for (int ip=0; ip<actPoli.length; ip++) {
		if (actPoli[ip]) {
%>
									<td class="<%=bgclass%>nopadding_lr">
										<label class="switch"><input type="checkbox" id="ovfp<%=""+(ip+1)%>_<%=tailName%>" name="ovfp<%=""+(ip+1)%>_<%=tailName%>" <%=((mValue_ovf.contains("P"+(ip+1))) ? "checked=true" : "")%>><span class="slider round blue"></span></label>
									</td>
<%
		}
	}
%>
									<td>
										<input type="hidden" name="ovf_t_<%=tailName%>" value="<%=Utility.getNotNull(mTransactionname_ovf)%>">
										<input type="hidden" name="ovf_k_<%=tailName%>" value="<%=Utility.getNotNull(mKey_ovf)%>">
									</td>
								</tr>
<%
		}
%>
								<input type="hidden" id="numesi" name="numesi" value="<%=tailName%>">
<%

	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rsCnf.close(); } catch (Exception e) {}
		try { cstmtCnf.close(); } catch (Exception e) {}
		try { connCnf.close(); } catch (Exception e) {}
		try { rsCnf2.close(); } catch (Exception e) {}
		try { cstmtCnf2.close(); } catch (Exception e) {}
		try { connCnf2.close(); } catch (Exception e) {}
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>
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

<script>

	var actPoli = [true,true,true,true,true,true,true];

	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#RoutingParameters","RoutingParameters.jsp");
		} catch (e) {
		}
		delta_reload();
	})

	window.onclick = function(event) {
		if (event.target.className == 'modal') {
		}
	}
	
	save_onclick = function() {
		var numerr = 0;
		var numesi = $('#numesi').val();
		for (var ie = 1; ie <= numesi; ie++) {
			if ($('#deldis_'+ie).html()!="")
				numerr++;
		}
		if (numerr==0)
			$('#form_routing_parameters')[0].submit();
	}
	
	pridef_onclick = function() {
		var value = $('#pridef').val();
		var numesi = $('#numesi').val();
		for (var ie = 1; ie <= numesi; ie++) {
			$('#pri_'+ie).val(value);
		}
	}
	
	disdef_onclick = function() {
		var distot = 0;
		for (var ip = 1; ip <= 7; ip++) {
			if (actPoli[ip-1]) {
				if ($.isNumeric($('#disdef'+ip).val()))
					distot += parseInt($('#disdef'+ip).val(), 10);
			}
		}
		var delta = distot-100;
		if (delta==0) {
			var numesi = $('#numesi').val();
			for (var ip = 1; ip <= 7; ip++) {
				if (actPoli[ip-1]) {
					var value = $('#disdef'+ip).val();
					for (var ie = 1; ie <= numesi; ie++) {
						$('#disp'+ip+'_'+ie).val(value);
					}
				}
			}
			delta_reload();
		}
	}
	
	timdef_onclick = function() {
		var value = $('#timdef').val();
		for (var ip = 1; ip <= 7; ip++) {
			if (actPoli[ip-1]) {
				$('#timp'+ip).val(value);
			}
		}
	}
	
	avfdef_onclick = function() {
		var numesi = $('#numesi').val();
		for (var ip = 1; ip <= 7; ip++) {
			if (actPoli[ip-1]) {
				var value = $('#ovfdef'+ip).is(':checked');
				for (var ie = 1; ie <= numesi; ie++) {
					$('#ovfp'+ip+'_'+ie).prop("checked", value);
				}
			}
		}
	}
	
	deltadef_onchange = function(_this,max) {
		if ($.isNumeric($(_this).val())) {
			var value = parseInt($(_this).val(), 10);
			if (value<0) 
				$(_this).val(0);
			else if (value>max) 
				$(_this).val(max);
		} else {
			$(_this).val(0);
		}
		
		var distot = 0;
		for (var ip = 1; ip <= 7; ip++) {
			if (actPoli[ip-1]) {
				if ($.isNumeric($('#disdef'+ip).val()))
					distot += parseInt($('#disdef'+ip).val(), 10);
			}
		}
		var delta = distot-100;
		if (delta==0) {
			$('#deldisdef').html("");
			$('#deldisdef_bg').removeClass("redthr");
			$('#deldisdef_bg').addClass("superlightblue");
		} else {	
			$('#deldisdef').html(""+delta+"%");
			$('#deldisdef_bg').removeClass("superlightblue");
			$('#deldisdef_bg').addClass("redthr");
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
		
		var distot = 0;
		for (var ip = 1; ip <= 7; ip++) {
			if (actPoli[ip-1]) {
				if ($.isNumeric($('#disp'+ip+'_'+esi).val()))
					distot += parseInt($('#disp'+ip+'_'+esi).val(), 10);
			}
		}
		var delta = distot-100;
		if (delta==0) {
			$('#deldis_'+esi).html("");
			$('#deldis_'+esi+'_bg').removeClass("redthr");
		} else {	
			$('#deldis_'+esi).html(""+delta+"%");
			$('#deldis_'+esi+'_bg').addClass("redthr");
		}
	}
	
	delta_reload = function() {
		var numesi = parseInt($('#numesi').val(), 10);
		for (var ip = 1; ip <= numesi; ip++) {
			delta_onchange(ip);
		}
	} 

	checkrange_onchange = function(_this,min,max) {
		if ($.isNumeric($(_this).val())) {
			var value = parseInt($(_this).val(), 10);
			if (value<min) 
				$(_this).val(min);
			else if (value>max) 
				$(_this).val(max);
		} else {
			$(_this).val(min);
		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>