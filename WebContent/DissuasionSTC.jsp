<!DOCTYPE html>
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
<title>DissuasionSTC</title>
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
	//==	ACTION PARAMETERS	====
	//==============================
	//==	ACTION START	====
	//==	ACTION STOP		====
		
	try {			
		if (StringUtils.isBlank(CodIvr)) {
		} else {
%>
<table class="center"  >
	<tr>
		<td style="width: 5%"></td>
		<td style="width: 30%">
			<form id="form_recall_1" action="DissuasionSTC.jsp" method="post">
				<input type="hidden" id="mAction" name="action" Value="SAVE"> 
				<table class="roundedCorners small" >
					<thead>
						<tr class="listGroupItem active blue">
							<td colspan="7">
								<table>
									<tr>
										<td>
											<div class="title">Esigenze</div>
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
<%
			Context ctx = new InitialContext();
			String environment = (String)session.getAttribute("Environment");
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"STC");
			log.info(session.getId() + " - connection STC wait...");
			conn = ds.getConnection();
			log.info(session.getId() + " - dashboard.Dissuasion_GetEsigenze('" + CodIvr + "')");
			cstmt = conn.prepareCall("{ call dashboard.Dissuasion_GetEsigenze(?)} ");
			cstmt.setInt(1, Integer.parseInt(CodIvr));
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			while (rs.next()) {
				String descrizione_nodo = rs.getString("descrizione_nodo");
				String id_esigenza = rs.getString("id_esigenza");
%>
				<tr class='listGroupItem' onclick='clickMe(event,this,"<%=id_esigenza%>","<%=descrizione_nodo%>") '>
					<td><div><%=id_esigenza%></div></td>
					<td><div><%=descrizione_nodo%></div></td>
				</tr>
<%
			}
%>
				</table>
			</form>
		</td>
		<td style="width: 60%">
			<iframe src="DissuasionSTCDetail.jsp" id="iFrame_DissuasionSTCDetail" name="iFrame_DissuasionSTCDetail" style="width: 100%; border: 0 solid grey"></iframe>
		</td>
		<td style="width: 5%"></td>
	</tr>
</table>
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

<div id="dates" class="modal">
	<form id="form_dates" name="form_dates" action="DissuasionSTCDetail.jsp" method="post" target="iFrame_DissuasionSTCDetail" >
		<input type="text" id="dst_idesigenza" name="idesigenza" > 
		<input type="text" id="dst_descrizionenodo" name="descrizionenodo" > 
	</form>
</div>

<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#DissuasionSTC","DissuasionSTC.jsp");
			$("#iFrame_DissuasionSTCDetail").height($(window).height()-10 );
			$(window).resize(function() {
				$("#iFrame_DissuasionSTCDetail").height($(window).height()-10 );		
			});	
		} catch (e) {}
//  		$("#mCodMessaggio").change(function() {
// 			changeCodMessaggio();
// 		});
	})
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
		}
	}

	clickMe = function(event,_this,idesigenza,descrizionenodo){
		$(_this).parent().find("td").removeClass("greyBackGround"); 
		$(_this).children("td").addClass("greyBackGround"); 
		$('#dst_idesigenza').val(idesigenza);
		$('#dst_descrizionenodo').val(descrizionenodo);
		$('#form_dates')[0].submit();		
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>