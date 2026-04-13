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
<title>MapEsigenze</title>
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
	CallableStatement cstmt = null;
	ResultSet rs = null;
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
		Context ctx = new InitialContext();
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "update" :
				String mCodNod = request.getParameter("mCodNod");
				String mEsigenza = request.getParameter("mEsigenza");
				log.info(session.getId() + " - dashboard.MapEsigenze_Update('" + CodIvr + "','" + mCodNod + "','" + mEsigenza + "')");
				cstmt = conn.prepareCall("{ call dashboard.MapEsigenze_Update(?,?,?)} ");
				cstmt.setString(1, CodIvr);
				cstmt.setString(2, mCodNod);
				cstmt.setString(3, mEsigenza);
				cstmt.execute();					
				log.debug(session.getId() + " - executeCall complete");
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
						<th colspan='3'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title textcenter">ASSOCIAZIONE NODI ESIGENZE</div>
									</td>
								</tr>
							</table>
						</th>
					</tr>
					<tr class='listGroupItem active blue'>
						<th>Codice Nodo</th>
						<th>Descrizione</th>
						<th>Esigenza</th>
					</tr>
			 	</thead>
			 	<tbody>
<%
	try {
		log.info(session.getId() + " - dashboard.MapEsigenze_Get('" + CodIvr + "')");
		cstmt = conn.prepareCall("{ call dashboard.MapEsigenze_Get(?)} ");
		cstmt.setString(1, CodIvr);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		while (rs.next()) {
			String cod_nodo = rs.getString("cod_nodo");
			String descrizione_nodo = rs.getString("descrizione_nodo");
			String uscita = rs.getString("uscita");
			int priorita = rs.getInt("priorita");
			String id_esigenza = rs.getString("id_esigenza");
			String messaggio_chiusura = rs.getString("messaggio_chiusura");
			String messaggio_trasferimento = rs.getString("messaggio_trasferimento");
%>
				<tr onclick="change('<%=cod_nodo%>','<%=Utility.stringJavaToJS(descrizione_nodo)%>','<%=id_esigenza%>')">
					<td><label><%=cod_nodo%></label></td>
					<td><label><%=descrizione_nodo%></label></td>
					<td><label><%=id_esigenza%></label></td>
				</tr>
<%
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>
				</tbody>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>


<div id="Modify" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Modifica</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#Modify').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_modify" action="MapEsigenze.jsp" method="post">
			<input type="hidden" id="mAction" name="action" Value="update">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label for="mCodNod">Codice Nodo</label>
					</td>
					<td>
						<input class="noBorder bold large" type="text" id="mCodNod" name="mCodNod" readonly="readonly" style="border: 0px">
					</td>
				</tr>
				<tr>
					<td>
						<label for="mDescNod">Descrizione</label>
					</td>
					<td>
						<input class="noBorder bold large" type="text" id="mDescNod" name="mDescNod" readonly="readonly" style="border: 0px">
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label for="mEsigenza">Esigenza</label>
					</td>
					<td>
						<input class="noBorder bold large" type="text" id="mEsigenza" name="mEsigenza">
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<button type="submit" id="" class="button blue">SALVA</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#MapEsigenze","MapEsigenze.jsp");
		} catch (e) {
		}
 		$("#stickytable_1").stickyTableHeaders();
	})

	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#Modify').hide();
		}
	}
	
	change = function(cod_nodo,descrizione_nodo,id_esigenza) {
		$("#mCodNod").val(cod_nodo);
		$("#mDescNod").val(descrizione_nodo);
		$("#mEsigenza").val(id_esigenza);
		$('#Modify').show();
	}
	
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>