<%@page import="java.util.*"%>
<%@page import="java.sql.*"%>
<%@page import="javax.naming.*"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="org.apache.log4j.Logger"%>
<%@page import="org.json.*"%>
<%@page
	import="comapp.Utility, comapp.ConfigurationUtility, comapp.ConfigServlet"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<!DOCTYPE html>
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="UTF-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TimeoutOVSec</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
</head>
<body style="overflow-y: auto;">
<%
	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");
	//== PARAMETRI DI REQUEST ===
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
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String dbsystemproperties = (String) session.getAttribute("DBSystemProperties");
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
				String s_value = "";
				String s_key = "";
				String s_trans = "";
				boolean s_flag = false;

				s_value = request.getParameter("flag_v");
				s_key = request.getParameter("flag_k");
				s_trans = request.getParameter("flag_t");
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
				if (s_flag) {
					for (int ip=1; ip<=7; ip++) {
						if (actPoli[ip-1]) {
							s_value = request.getParameter("tims_"+ip);
							s_key = request.getParameter("tims_"+ip+"_k");
							s_trans = request.getParameter("tims_"+ip+"_t");
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
						}
					}
				} else {
					for (int ie=1; ie<=p_numesi; ie++) {
						for (int ip=1; ip<=7; ip++) {
							if (actPoli[ip-1]) {
								s_value = request.getParameter("time_"+ip+"_"+ie);
								s_key = request.getParameter("time_"+ip+"_k_"+ie);
								s_trans = request.getParameter("time_"+ip+"_t_"+ie);
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
							}
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
	} finally {
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
%>
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td style="width: 40%">
			<form id="form_timeout_ov_sec" action="TimeoutOVSec.jsp" method="post">
				<input type="hidden" name="action" value="SAVE">
				<input type="hidden" id="CodIvr" name="CodIvr" value="<%=CodIvr%>">
				<table class="roundedCorners small" >
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan="7">
								<table>
									<tr>
										<td>
											<div class="title">Timeout Overflow</div>
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
										<td colspan="<%=""+(numPoli+2)%>"><div class="title">per SERVIZI</div></td>
									</tr>
<% 									// ###### ROW SWITCH / POLI ###### %>
									<tr class="listGroupItem active lightblue">
<%
			String flag_value = "false";
			String flag_transactionname = "";
			String flag_key = "";
			log.info(session.getId() + " - dashboard.TimeoutOVSec_GetFlagTimeoutOV('" + CodIvr + "')");
			cstmtCnf = connCnf.prepareCall("{ call dashboard.TimeoutOVSec_GetFlagTimeoutOV(?)} ");
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
<% 									// ###### ROW TIMEOUT ###### %>
									<tr>
										<td><div class="title">TIMEOUT</div></td>
<%
			String bgclass = "";
			String to_transactionname = "";
			String to_key = "";
			String to_value = "";
			String to_polo = "";
			Map<String, String> mTransactionname_tim = new HashMap<>();
			Map<String, String> mKey_tim = new HashMap<>();
			Map<String, String> mValue_tim = new HashMap<>();

			log.info(session.getId() + " - dashboard.TimeoutOVSec_GetAnnexIvr('" + CodIvr + "')");
			cstmtCnf = connCnf.prepareCall("{ call dashboard.TimeoutOVSec_GetAnnexIvr(?)} ");
			cstmtCnf.setString(1, CodIvr);
			rsCnf = cstmtCnf.executeQuery();
			while (rsCnf.next()) {
				to_transactionname = rsCnf.getString("transactionname");
				to_key = rsCnf.getString("key");
				to_value = rsCnf.getString("value");
				to_polo = rsCnf.getString("polo");
				mTransactionname_tim.put(to_polo, to_transactionname);
				mKey_tim.put(to_polo, to_key);
				mValue_tim.put(to_polo, to_value);
			}
			try { rsCnf.close(); } catch (Exception e) {}
			try { cstmtCnf.close(); } catch (Exception e) {}

			for (int ip = 0; ip < actPoli.length; ip++) {
				if (actPoli[ip]) {
					to_transactionname = mTransactionname_tim.get("P" + (ip + 1));
					to_key = mKey_tim.get("P" + (ip + 1));
					to_value = mValue_tim.get("P" + (ip + 1));
					bgclass = (StringUtils.isNotBlank(to_transactionname) && StringUtils.isNotBlank(to_key))?"":"redthr ";
%>
										<td class=" <%=bgclass%>nopadding_lr">
											<input type="hidden" name="tims_<%=""+(ip+1)%>_t" value="<%=Utility.getNotNull(to_transactionname)%>">
											<input type="hidden" name="tims_<%=""+(ip+1)%>_k" value="<%=Utility.getNotNull(to_key)%>">
											<input type="number" style="width: 40px;" id="tims_<%=""+(ip+1)%>" name="tims_<%=""+(ip+1)%>" min="0" max="999" value="<%=Utility.getNotNull(to_value)%>" onchange="checkrange_onchange(this,0,999)">
										</td>
<%
				}
			}
%>
										<td><div class="title"></div></td>
									</tr>
<% 									// ###### HEAD ESIGENZE ###### %>
									<tr class="listGroupItem active blue">
										<td colspan="<%=""+(numPoli+2)%>"><div class="title">per ESIGENZE</div></td>
									</tr>
<% 									// ###### ROW POLI ###### %>
									<tr class="listGroupItem active lightblue">
										<td rowspan="2" class="middle"><div class="title center">ESIGENZA</div></td>
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
<% 									// ###### ROW DEFAULT TITLE ###### %>
									<tr class="listGroupItem">
										<td colspan="<%=""+(numPoli+2)%>" style="padding-left: 8px;">
											<div class="title">Timeout Overflow per esigenze - valore universale</div>
										</td>
									</tr>
<% 									// ###### ROW DEFAULT ###### %>
									<tr class="listGroupItem active lightblue">
<%
			for (int ip=0; ip<actPoli.length; ip++) {
				if (actPoli[ip]) {
%>
										<td class="nopadding_lr superlightblue">
											<input type="number" style="width:40px;" id="timedef<%=""+(ip+1)%>" min="0" max="999" value="0">
										</td>
<%
				}
			}
%>
										<td class="nopadding_lr superlightblue">
											<small>Applica a tutte</small><br>
											<button type="button" class="buttonsmall blue" id="timedef_btn" onclick="timedef_onclick()">SET</button>
										</td>
									</tr>
<% 									// ###### ROWS ###### %>
<%

			int numEsigenze = 0;
			String tailName = "";
			
			log.info(session.getId() + " - dashboard.TimeoutOVSec_GetEsigenze('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.TimeoutOVSec_GetEsigenze(?)} ");
			cstmt.setString(1, CodIvr);
			rs = cstmt.executeQuery();
			while (rs.next()) {
				numEsigenze++;
				tailName = ""+numEsigenze;
				
				String id_esigenza = rs.getString("id_esigenza");
%>
									<tr>
										<td><div class="title"><%=id_esigenza %></div></td>
<%
				mTransactionname_tim = new HashMap<>();
				mKey_tim = new HashMap<>();
				mValue_tim = new HashMap<>();
				log.info(session.getId() + " - dashboard.TimeoutOVSec_GetTimeoutAnnex('" + id_esigenza + "')");
				cstmtCnf = connCnf.prepareCall("{ call dashboard.TimeoutOVSec_GetTimeoutAnnex(?)} ");
				cstmtCnf.setString(1, id_esigenza);
				rsCnf = cstmtCnf.executeQuery();
				while (rsCnf.next()) {
					to_transactionname = rsCnf.getString("transactionname");
					to_key = rsCnf.getString("key");
					to_value = rsCnf.getString("value");
					to_polo = to_key.substring(0,2);
					mTransactionname_tim.put(to_polo, to_transactionname);
					mKey_tim.put(to_polo, to_key);
					mValue_tim.put(to_polo, to_value);
				}
				try { rsCnf.close(); } catch (Exception e) {}
				try { cstmtCnf.close(); } catch (Exception e) {}

				for (int ip = 0; ip < actPoli.length; ip++) {
					if (actPoli[ip]) {
						to_transactionname = mTransactionname_tim.get("P" + (ip + 1));
						to_key = mKey_tim.get("P" + (ip + 1));
						to_value = mValue_tim.get("P" + (ip + 1));
						bgclass = (StringUtils.isNotBlank(to_transactionname) && StringUtils.isNotBlank(to_key))?"":"redthr ";
%>
										<td class=" <%=bgclass%>nopadding_lr">
											<input type="hidden" name="time_<%=""+(ip+1)%>_t_<%=tailName%>" value="<%=Utility.getNotNull(to_transactionname)%>">
											<input type="hidden" name="time_<%=""+(ip+1)%>_k_<%=tailName%>" value="<%=Utility.getNotNull(to_key)%>">
											<input type="number" style="width: 40px;" id="time_<%=""+(ip+1)%>_<%=tailName%>" name="time_<%=""+(ip+1)%>_<%=tailName%>" min="0" max="999" value="<%=Utility.getNotNull(to_value)%>" onchange="checkrange_onchange(this,0,999)">
										</td>
<%
					}
				}
%>
										<td><div class="title"></div></td>
									</tr>
<%
			}
%>
									<input type="hidden" id="numesi" name="numesi" value="<%=tailName%>">
								</table>
							</td>
						</tr>
						<tr>
							<td>
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

	var actPoli = [true,true,true,true,true,true,true];

	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#TimeoutOVSec", "TimeoutOVSec.jsp");
		} catch (e) {
		}
		aggiornaStato();
	});
	
	save_onclick = function() {
		$('#form_timeout_ov_sec')[0].submit();
	}
	
	aggiornaStato = function() {
		var _status = $("#flag_v").is(':checked');
		$("input[id^='tims_']").prop("disabled", !_status);
		$("input[id^='time_']").prop("disabled", _status);
		$("input[id^='timedef']").prop("disabled", _status);
		$("#timedef_btn").prop("disabled", _status);
		$("#flag_text").text(_status?"Attivo":"Disattivo");
	}
	
	flag_onchange = function() {
		aggiornaStato();
	}

	timedef_onclick = function() {
 		var numesi = $('#numesi').val();
		for (var ip = 1; ip <= 7; ip++) {
			if (actPoli[ip-1]) {
				var value = $('#timedef'+ip).val();
				for (var ie = 1; ie <= numesi; ie++) {
					$('#time_'+ip+'_'+ie).val(value);
				}
			}
		}
	}

	checkrange_onchange = function(_this, min, max) {
		if ($.isNumeric($(_this).val())) {
			var value = parseInt($(_this).val(), 10);
			if (value < min)
				$(_this).val(min);
			else if (value > max)
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