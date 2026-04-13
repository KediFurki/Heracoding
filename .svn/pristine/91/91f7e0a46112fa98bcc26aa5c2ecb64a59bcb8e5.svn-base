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
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SwitchGlobal</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
</head>
<body>
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
	//==============================
	//==	ACTION START	====
	//==	ACTION STOP		====
%>
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td style="width: 30%">
			<table class="roundedCorners small">
				<tr class="listGroupItem active green">
					<td colspan="7">
						<table>
							<tr>
								<td>
									<div class="title">Switch Generale</div>
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
			</table>
			<div id="_left" style="overflow: auto;">
				<table id="_tab" class="roundedCorners no_top">
<%
	try {
		Context ctx = new InitialContext();
		String environment = (String)session.getAttribute("Environment");
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		log.info(session.getId() + " - dashboard.GlobalSwitch_GetCodNodi('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.GlobalSwitch_GetCodNodi(?)} ");
		cstmt.setString(1,environment);
		rs = cstmt.executeQuery();
		log.debug(session.getId() + " - executeCall complete");
		while (rs.next()) {
			String CodNodo = rs.getString("Cod_Nodo");
			String Etichetta = rs.getString("Etichetta");
%>
					<tr class='listGroupItem' onclick='clickMe(event,this,"<%=CodNodo%>","<%=Etichetta%>") '>
						<td><div><%=Etichetta%></div></td>
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
				</table>
	
			</div>
		</td>
		<td style="width: 50%">
			<iframe src="SwitchGlobalDetail.jsp" id="iFrame_SwitchGlobalDetail" name="iFrame_SwitchGlobalDetail" style="width: 100%; border: 0 solid grey"></iframe>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>

<div id="dates" class="modal">
	<form id="form_dates" name="form_dates" action="SwitchGlobalDetail.jsp" method="post" target="iFrame_SwitchGlobalDetail" >
		<input type="text" id="swi_codnodo" name="codnodo" > 
		<input type="text" id="swi_etichetta" name="etichetta" > 
		<input type="text" id="swi_pos" name="pos" > 
		<input type="text" id="row_height" name="row_height" >
		<input type="text" id="row_number" name="row_number" >
		<input type="text" id="container_height" name="container_height" >
	</form>
</div>
	
<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#SwitchGlobal");
			$("#iFrame_SwitchGlobalDetail").height($(window).height()-10 );
// 			$("#_right").height($(window).height()-10 );
			$("#_left").height($(window).height()-90 );
			$(window).resize(function() {
				$("#iFrame_SwitchGlobalDetail").height($(window).height()-10 );		
// 				$("#_right").height($(window).height()-10 );
				$("#_left").height($(window).height()-90 );
			});	
		} catch (e) {}
	})
		
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
		}
	}

	var tdWidth = 0;
	var tableWidth =0;
	clickMe = function(event,_this,codnodo,etichetta){
		$(_this).parent().find("td").removeClass("greyBackGround"); 
		$(_this).children("td").addClass("greyBackGround"); 
		$('#swi_codnodo').val(codnodo);
		$('#swi_etichetta').val(etichetta);
		$('#swi_pos').val("["+event.x+","+event.y+"]");
		$('#container_height').val($(_left).height());
		$('#row_height').val(_this.clientHeight);
		$('#row_number').val(_this.rowIndex);
		$('#form_dates')[0].submit();		
	}
	
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>