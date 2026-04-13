<!DOCTYPE html>
<%@page import="comapp.Utility"%>
<%@page import="java.sql.Types"%>
<%@page import="java.util.Arrays"%>
<%@page import="java.util.List"%>
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
<title>SwitchGlobal</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
</head>
<body style="overflow-y: auto;">
<%
	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");
	//==	REQUEST PARAMETERS	====
	//		NONE
	//==============================
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	SimpleDateFormat sdf_day_of_year = new SimpleDateFormat("MM-dd");
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy/MM/dd");
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	String codnodo = request.getParameter("codnodo");
	String etichetta = request.getParameter("etichetta");
	String CodIvr = request.getParameter("CodIvr");
	String status = request.getParameter("status");
	//==============================
	//==	OTHER PARAMETERS	====
	String pos = request.getParameter("pos");
	String row_height = request.getParameter("row_height");
	String row_number=request.getParameter("row_number");
	String container_height=	request.getParameter("container_height");
	//==============================
	//==	ACTION START	====
		
	log.info(session.getId() + " - action:" + action + " codnodo: " + codnodo + " CodIvr: " + CodIvr + " status: " + status);
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "update" :
				log.info(session.getId() + " - dashboard.GlobalSwitch_UpdateStatus('"+environment+"','"+CodIvr+ "','"+codnodo+ "','" + status + "')");
				cstmt = conn.prepareCall("{ call dashboard.GlobalSwitch_UpdateStatus(?,?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, CodIvr);
				cstmt.setString(3, codnodo);
				cstmt.setString(4, status);
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "updateall" :
				log.info(session.getId() + " - dashboard.GlobalSwitch_UpdateStatus('"+environment+"','','"+codnodo+ "','" + status + "')");
				cstmt = conn.prepareCall("{ call dashboard.GlobalSwitch_UpdateStatus(?,?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setNull(2, Types.VARCHAR);
				cstmt.setString(3, codnodo);
				cstmt.setString(4, status);
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
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
		log.info(session.getId() + " - dashboard.GlobalSwitch_GetInfoCodNodo('"+environment+"','" + codnodo+ "')");
		cstmt = conn.prepareCall("{ call dashboard.GlobalSwitch_GetInfoCodNodo(?,?)} ");
		cstmt.setString(1,environment);
		if (StringUtils.isNoneBlank(codnodo)){
			cstmt.setString(2, codnodo);
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
		}else{
			log.info(session.getId() + " - codnodo is null or empty");
		}
		int i = 0;
		if (StringUtils.isNoneBlank(codnodo)){
%>
<div id="_top" >
	<table class="roundedCorners small no_bottom" style="width: 99%">
		<tr class="listGroupItem active blue">
			<td colspan='7'>
				<table>
					<tr>
						<td>
							<div class="title">Dettaglio: <%=etichetta%> </div>
						</td>
						<td valign="top">
							<div class="container title right">
									<button type="button" class="buttonset blue white" onclick="setAll()">Tutti</button>
									<button type="button" class="buttonset blue white" onclick="setNone()">Nessuno</button>
							</div>
						</td>
					</tr>
				</table>
			</td>
		</tr> 
	</table>
						
	<form id="form_save_switch" name="form_save_switch" action="SwitchGlobalDetail.jsp" method="post">
		<input type="hidden" id="mAction" name="action"  value="" type="hidden"/>
		<input type="hidden"  id="mCodNodo" name="codnodo" value="<%=codnodo%>" type="hidden"/>
		<input type="hidden"  id="mEtichetta" name="etichetta" value="<%=etichetta%>" type="hidden"/>
		<input type="hidden"  id="mCodIvr" name="CodIvr" value="">
		<input type="hidden"  id="mStatus" name=status value="">
		<input type="hidden"  id="mPos" name="pos" value='<%=pos %>'>
		<div id="_left" style="overflow: auto;">
			<table id="_tab" class="roundedCorners small no_top" style="width: 99%">
<%
			while (rs.next()) {
				i++;
				String COD_IVR = rs.getString("COD_IVR");
				String Descrizione = rs.getString("DESCRIZIONE");									
				String TimeStamp = rs.getString("timestamp");
				String stato = rs.getString("stato");
				
				if (i%2==1)
					out.println("<tr>");
				out.println("<td>");										
				out.println("<label class='switch'>"); 
				out.println("<input type='checkbox' id='mCurrentState' name='Status' "+(stato.equalsIgnoreCase("ON")?"checked=true":"")+" onclick='change(this,\""+COD_IVR+"\")'> ");

				out.println("<span class='slider round blue'></span>");
				out.println("</label>");
				out.println("</td>");										
				out.println("<td><label>"+ COD_IVR + " </label> </td> ");
				out.println("<td><label>"+ Descrizione + " </label> </td> ");
				out.println("<td><label>"+ Utility.getNotNull(TimeStamp) + " </label> </td> ");
				if (i%2==0)
				out.println("</tr>");
			}
%>
			</table>
		</div>
	</form>
</div>
<%
		} else {
%>
<div id="_top" >
	<table class="roundedCorners small no_bottom" style="width: 99%">
		<tr class="listGroupItem active blue">
			<td colspan='7'>
				<table>
					<tr>
						<td>
							<div class="title">Nessuna Switch Selezionata </div>
						</td>
						<td valign="top">
							<div class="container title right">
							</div>
						</td>
					</tr>
				</table>
			</td>
		</tr> 
	</table>
</div>
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

<script type="text/javascript">
	var pos=<%=pos%>;
	
	var  row_height ='<%=row_height%>';
	var  row_number='<%=row_number%>';
	var  container_height='<%=container_height%>';
	var margintop=0;
	$(function() {
		try {
			parent.parent.ChangeActiveMenu("#SwitchGlobal");	
		} catch (e) {}
		try{
			pos[1]=Math.floor(pos[1]);
			
			$(window).resize(function() {
			
				$("#_left").height($(window).height()-90- margintop);
			});	
							
			var element = document.getElementById("_tab");
			var rows_n = element.rows.lenght;
			var _tab_height = $("#_tab").height();
			console.log(" pos="+pos+" row_height ="+row_height+" row_number="+row_number+" container_height="+container_height+" _tab_height:"+_tab_height+" pos:"+pos);

			var alf_tab_height =  Math.floor(_tab_height/3);
			margintop = pos[1]-alf_tab_height;
			
			var div_h = $(window).height() - margintop - 75;
			
			console.log(" $(window).height()="+$(window).height()+" margintop ="+margintop);
			if (div_h <_tab_height){
				console.log("set max");
				
				margintop = $(window).height() - _tab_height - 75;
			}
			
	 		if (margintop > 0  ){
				$("#_top").css("margin-top", margintop +30);
				$("#_left").height($(window).height() - margintop - 72);
				
			}
		} catch (e) {}
	})
	
	change=function(_this,CodIvr){
		$("#mAction").val('update');
		if ($(_this).is(':checked'))
			$("#mStatus").val('ON');
		else
			$("#mStatus").val('OFF');
		$("#mCodIvr").val(CodIvr);
		$("#form_save_switch")[0].submit();
	}

	setAll = function() {
		$(':checkbox[id=mCurrentState]').prop("checked", true);
		$("#mAction").val('updateall');
		$("#mCodIvr").val('');
		$("#mStatus").val('ON');
		$("#form_save_switch")[0].submit();
	}
	
	setNone = function() {
		$(':checkbox[id=mCurrentState]').prop("checked", false);
		$("#mAction").val('updateall');
		$("#mCodIvr").val('');
		$("#mStatus").val('OFF');
		$("#form_save_switch")[0].submit();
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>