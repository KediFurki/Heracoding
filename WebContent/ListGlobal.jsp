<!DOCTYPE html>
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
<title>ListGlobal</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
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
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+action  );
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "delete_from_list":
				String d_lista = request.getParameter("delete_from_list_lista");
				String d_ntel = request.getParameter("delete_from_list_ntel");
				log.info(session.getId() + " - dashboard.List_Delete('"+environment+"','" + d_lista + "','"+d_ntel+"')");
				cstmt = conn.prepareCall("{ call dashboard.List_Delete(?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, d_lista);
				cstmt.setString(3, d_ntel);
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add_to_list":
				String a_lista = request.getParameter("add_to_list_lista");
				String a_ntel = request.getParameter("add_to_list_ntel");
				log.info(session.getId() + " - dashboard.List_Add('"+environment+"','" + a_lista + "','"+a_ntel+"')");
				cstmt = conn.prepareCall("{ call dashboard.List_Add(?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, a_lista);
				cstmt.setString(3, a_ntel);
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
			<form id="form_without_control" action="ListGlobal.jsp" method="post">
				<input type="hidden" name="action" value="day_of_year">
				<table id="stickytable_1" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active green">
						<td colspan='7'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Simulazione in Orario</div>
									</td>
									<td>
										<div class="container title right green">
											<label></label>										
										</div>

									</td>
								</tr>
							</table>
						</td>
					</tr>
			 	 </thead>
<%
	try {
		log.info(session.getId() + " - dashboard.List_GetOrario('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.List_GetOrario(?)} ");
		cstmt.setString(1,environment);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		int i = 0;
		while (rs.next()) {
			i++;
			String numero_telefono = rs.getString("NUMERO_TELEFONO");
			String lista = rs.getString("LISTA");
			String ts = rs.getString("TIME_STAMP");
			if (ts == null)
				ts = "";										

			out.println("<tr>");
			out.println("<td><label>" + numero_telefono + " </label> </td> ");
			out.println("<td><label>" + ts + " </label> </td> ");
			out.println("<td  >");
			out.println("   <div  class='container_img grey right'>");
			out.println("	   <img src='images/TrashGrey_.png' onclick='confirmdelete_from_list(\""+lista+"\",\"Simulazione in Orario\",\""+numero_telefono+"\")'  >");
			out.println("    </div>");
			out.println("</td>");
			out.println("</tr>");
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
	}
%>
					<tr>
						<td colspan="7">
							<div class="container_img title left grey">
								<img alt="" src="images/PlusGrey_.png" onclick="confirmadd_to_list('SENZA_CONTROLLO_ORARIO','Simulazione in Orario')">
							</div>
						</td>
					</tr>
				</table>
			</form>
		</td>
		<td style="width: 40%">
			<form id="form_outof_hour" action="ListGlobal.jsp" method="post">
				<input type="hidden" name="action" value="special_day">
				<table id="stickytable_2" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active green">
						<td colspan='7'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Simulazione Fuori Orario</div>
									</td>
									<td valign="top">
										<div class="container title right green">
											<label></label>										
										</div>
									</td>
								</tr>
							</table>
						</td>
					</tr>
				</thead>
<%
	try {
		log.info(session.getId() + " - dashboard.List_GetFuoriOrario('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.List_GetFuoriOrario(?)} ");
		cstmt.setString(1,environment);
		rs = cstmt.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
		int i = 0;
		while (rs.next()) {
			i++;
			String numero_telefono = rs.getString("NUMERO_TELEFONO");
			String lista = rs.getString("LISTA");
			String ts = rs.getString("TIME_STAMP");
			if (ts == null)
				ts = "";								
			
			out.println("<tr>");
			out.println("<td><label>" + numero_telefono + " </label> </td> ");
			out.println("<td><label>" + ts + " </label> </td> ");
			out.println("<td  >");
			out.println("   <div  class='container_img grey right'>");
			out.println("	   <img src='images/TrashGrey_.png' onclick='confirmdelete_from_list(\""+lista+"\",\"Simulazione Fuori Orario\",\""+numero_telefono+"\")'  >");
			out.println("    </div>");
			out.println("</td>");
			out.println("</tr>");
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>
					<tr>
						<td colspan="7">
							<div class="container_img grey">
								<img alt="" src="images/PlusGrey_.png" onclick="confirmadd_to_list('FUORI_ORARIO','Simulazione Fuori Orario')">
							</div>
						</td>
					</tr>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>

<div id="delete_from_list" class="modal">
	<div class="modal-content">
		<div class="modal-header green">						
			<table style="width: 100%">
				<tr>
					<td>	<h2>Elimina</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#delete_from_list').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_delete_from_list" action="ListGlobal.jsp" method="post">
			<input type="hidden" id="dAction" name="action" Value="delete_from_list">
			<input type="hidden" id="delete_from_list_lista" name="delete_from_list_lista" Value="">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="delete_from_list_name">Lista</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_from_list_name" name="delete_from_list_name" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td class="bottom"><label for="delete_from_list_ntel">Numero Telefono</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_from_list_ntel" name="delete_from_list_ntel" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><button type="submit" id="CloseModal" class="button green">Confermi Eleminazione</button></td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="add_to_list" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Aggiungi numero di telefono</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#add_to_list').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add_to_list" action="ListGlobal.jsp" method="post">
			<input type="hidden" id="asAction" name="action" Value="add_to_list">
			<input type="hidden" id="add_to_list_lista" name="add_to_list_lista" Value="">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="add_to_list_name">Lista</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="add_to_list_name" name="add_to_list_name" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><input type="text" id="add_to_list_ntel" name="add_to_list_ntel" class="formInput" placeholder=" Numero di Telefono" required="required"></td>	
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><button type="submit" id="AddCloseModal" class="button green">Conferma</button></td>
				</tr>
			</table>
		</form>
	</div>
</div>

<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#ListGlobal");			
		} catch (e) {
		}
 		$("#stickytable_1").stickyTableHeaders();
 		$("#stickytable_2").stickyTableHeaders();
	})

	confirmdelete_from_list =  function(lista,label,ntel) {
		$("#delete_from_list_ntel").val(ntel);
		$("#delete_from_list_name").val(label);
		$("#delete_from_list_lista").val(lista);
		$("#delete_from_list").show();
	}
	
	confirmadd_to_list =  function(lista,label) {
		$('#add_to_list_name').val(label);
		$('#add_to_list_lista').val(lista);
		$('#add_to_list').show();
	}
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#delete_from_list').hide();
			$('#add_to_list').hide();
		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>