<!DOCTYPE html>
<%@page import="java.sql.Types"%>
<%@page import="org.json.JSONArray"%>
<%@page import="java.util.Iterator"%>
<%@page import="comapp.Utility"%>
<%@page import="comapp.ConfigurationUtility"%>
<%@page import="org.json.JSONObject"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Date"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Timestamp"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
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
<title>AbandonedGlobal</title>
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
	//		NONE
	//==============================
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	Connection connCnf = null;
	CallableStatement cstmtCnf = null;
	ResultSet rsCnf = null;
	SimpleDateFormat sdf_day_of_year = new SimpleDateFormat("MM-dd");
	SimpleDateFormat sdf_day_of_year_ext = new SimpleDateFormat("dd MMMM");
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy/MM/dd");
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	String dbsystemproperties = (String)session.getAttribute("DBSystemProperties");
	//==============================
	//==	OTHER PARAMETERS	====
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:" + action);
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CNF");
		log.info(session.getId() + " - connection CNF wait...");
		connCnf = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "save" :
				String p_NUMERO_TENTATIVI = request.getParameter("NUMERO_TENTATIVI");
				String p_PRIORITA = request.getParameter("PRIORITA");
				if (StringUtils.isNoneBlank(p_NUMERO_TENTATIVI) || StringUtils.isNoneBlank(p_PRIORITA)) {
					int NumCodIvr = 0;
					String p_NumCodIvr = request.getParameter("NumCodIvr");
					try { NumCodIvr = Integer.parseInt(p_NumCodIvr); } catch (Exception e) {}
					for (int iCodIvr=1; iCodIvr<=NumCodIvr; iCodIvr++) {
						String p_CodIvr = request.getParameter("CodIvr_"+iCodIvr);
						String p_transactionname = request.getParameter("transactionname_"+iCodIvr);
						String transactionName = p_transactionname;
						String p_status = request.getParameter("Status_"+iCodIvr);
						if (p_status!=null && p_status.equalsIgnoreCase("on")) {
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
										if (jkey.equals("NUMERO_TENTATIVI")) {
											if (StringUtils.isNoneBlank(p_NUMERO_TENTATIVI)) {
					 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_NUMERO_TENTATIVI);
												jajo.put(jkey, p_NUMERO_TENTATIVI);
											}
										}
										if (jkey.equals("PRIORITA")) {
											if (StringUtils.isNoneBlank(p_PRIORITA)) {
					 							log.info(session.getId() + " - key:"+jkey+" - value:"+jvalue+" -> "+p_PRIORITA);
												jajo.put(jkey, p_PRIORITA);
											}
										}
									}
							    }
							}
							log.info(session.getId() + " - "+transactionName+" -> "+jo.toString());
							ConfigurationUtility.saveTransactionJSON(Utility.getSystemProperties(dbsystemproperties), session.getId(), transactionName, jo);
							// HISTORY ---------------
							// ParamIn:	p_CodIvr
							//			p_NUMERO_TENTATIVI
							//			p_PRIORITA
							log.info(session.getId() + " - dashboard.Abandoned_AddHistory('"+p_CodIvr+"','"+transactionName+"','"+p_NUMERO_TENTATIVI+"','"+p_PRIORITA+"')");
							cstmt = conn.prepareCall("{ call dashboard.Abandoned_AddHistory(?,?,?,?)} ");
							cstmt.setString(1, p_CodIvr);
							cstmt.setString(2, transactionName);
							if (p_NUMERO_TENTATIVI==null || p_NUMERO_TENTATIVI.trim().isEmpty()) {
								cstmt.setNull(3, Types.INTEGER);
							} else {
								cstmt.setInt(3, Integer.parseInt(p_NUMERO_TENTATIVI));
							}
							if (p_PRIORITA==null || p_PRIORITA.trim().isEmpty()) {
								cstmt.setNull(4, Types.INTEGER);
							} else {
								cstmt.setInt(4, Integer.parseInt(p_PRIORITA));
							}
	
							cstmt.execute();
							log.debug(session.getId() + " - executeCall complete");
							// -----------------------
						}
					}
				}
				break;
		}
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	} finally {
		try { cstmt.close(); } catch (Exception e) {}
	}
	if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> STOP ACTION");
	//==	ACTION STOP		====
	try {
		log.info(session.getId() + " - dashboard.Service_List('"+environment+"')");
		int i = 0;
%>
<table class="center"  >
	<tr>
		<td style="width: 10%"></td>
		<td>
			<table class="roundedCorners small no_bottom">
				<tr class="listGroupItem active green">
					<td colspan='7'>
						<table>
							<tr>
								<td>
									<div class="title">Priorità su Abbandono</div>
								</td>
								<td valign="top">
									<div class="container title right">
										<button type="button" class="buttonset green white" onclick="setAll()">Tutti</button>
										<button type="button" class="buttonset green white" onclick="setNone()">Nessuno</button>
									</div>
								</td>
							</tr>
						</table>
					</td>
				</tr>
			</table>

			<form id="form_abandonedglobalglobal" name="form_abandonedglobalglobal" action="AbandonedGlobal.jsp" method="post">
				<input type="hidden" id="mAction" name="action" type="hidden" value="save">
				<div id="_left" style="overflow: auto;">
					<table id="_tab" class="roundedCorners small no_top">
<%
		log.info(session.getId() + " - dashboard.Service_List('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.Service_List(?)} ");
		cstmt.setString(1,environment);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rs.next()) {
			String COD_IVR = rs.getString("COD_IVR");
			String Descrizione = rs.getString("DESCRIZIONE");	

			String transactionname = "";
 			boolean fNUMERO_TENTATIVI = false;
 			int NUMERO_TENTATIVI = -1;
 			String sNUMERO_TENTATIVI = "--";
 			boolean fPRIORITA = false;
 			int PRIORITA = -1;
 			String sPRIORITA = "--";
			
			log.info(session.getId() + " - dashboard.Abandoned_GetAnnex('" + COD_IVR + "')");
			cstmtCnf = connCnf.prepareCall("{ call dashboard.Abandoned_GetAnnex(?)} ");
			cstmtCnf.setString(1, COD_IVR);
			rsCnf = cstmtCnf.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			while (rsCnf.next()) {
				int dbid =  rsCnf.getInt("dbid"); 
				String key = rsCnf.getString("key");
				String value = rsCnf.getString("value");
				transactionname = rsCnf.getString("transactionname");
				switch (key) {
				case "NUMERO_TENTATIVI":
					fNUMERO_TENTATIVI = true;
					NUMERO_TENTATIVI = Integer.parseInt(value);
					sNUMERO_TENTATIVI = ""+NUMERO_TENTATIVI;
					break;
				case "PRIORITA":
					fPRIORITA = true;
					PRIORITA = Integer.parseInt(value);
					sPRIORITA = ""+PRIORITA;
					break;
				}
			}
			if (fNUMERO_TENTATIVI || fPRIORITA) {
				i++;
				if (i==1) {
%>
						<tr>
							<td colspan="8">
								<table class="roundedCorners small no_top">
									<tr style="width: 100%" class="listGroupItem" >
										<td>PRIORITA'</td>
										<td><input type="number" name="PRIORITA" min="0" max="10000" value="20"></td>
									</tr>
									<tr style="width: 100%" class="listGroupItem" >
										<td>NUMERO TENTATIVI</td>
										<td><input type="number" name="NUMERO_TENTATIVI" min="0" max="1000" value="2"></td>
									</tr>
								</table>
							</td>
						</tr>
<%
				}
				if (i%2==1) {
%>
						<tr>
<%
				}
%>
							<td>
								<input type="hidden" id="CodIvr_<%=i%>" name="CodIvr_<%=i%>" value="<%=COD_IVR%>">
								<input type="hidden" id="transactionname_<%=i%>" name="transactionname_<%=i%>" Value="<%=transactionname%>"> 
								<label class='switch'><input type='checkbox' id='mCurrentState' name='Status_<%=i%>'><span class='slider round green'></span></label>
							</td>
							<td><label><%=COD_IVR%></label> </td>
							<td><label><%=Descrizione%></label> </td>
							<td><label>(Pr.=<%=sPRIORITA%> / N.Tent.=<%=sNUMERO_TENTATIVI%></label> </td>
<%
				if (i%2==0) {
%>
						</tr>
<%
				}
			}
		}
%>
						<input type="hidden" id="NumCodIvr" name="NumCodIvr" value="<%=i%>">
<%
				if (i>0) {
%>
						<tr>
							<td colspan="8">
								<button type="submit" class="button green">SALVA</button>
							</td>
						</tr>
<%
				}
%>
					</table>
				</div>
			</form>

		</td>
		<td style="width: 10%"></td>
	</tr>
</table>
<%
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

<script type="text/javascript">
	var margintop=0;
	$(function() {
		try {
			parent.ChangeActiveMenu("#AbandonedGlobal");
		} catch (e) {
		}
	})

	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#Error').hide();
		}
	}

	setAll = function() {
		$(':checkbox[id=mCurrentState]').prop("checked", true);
	}
	
	setNone = function() {
		$(':checkbox[id=mCurrentState]').prop("checked", false);
	}
	
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>