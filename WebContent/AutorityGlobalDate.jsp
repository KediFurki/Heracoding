<!DOCTYPE html>
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
<title>Autority</title>
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
	String CodIvr = request.getParameter("CodIvr");
	String date = request.getParameter("date");
	//==============================
	//==	OTHER PARAMETERS	====
	String pos = request.getParameter("pos");
	String row_height = request.getParameter("row_height");
	String row_number=request.getParameter("row_number");
	String container_height=	request.getParameter("container_height");
	//==============================
	Date filterToday = new Date();
	filterToday = sdf_full_day_of_year.parse(sdf_full_day_of_year.format(filterToday));
	String [] options;
	String [] selections;
	List <String> selectionList;
	boolean enableMod = true;
	//==	ACTION START	====
	log.info(session.getId() + " - action:" + action + " date: " + date + " pos:" + pos +" codIvr:"+CodIvr);
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "delete" :
				log.info(session.getId() + " - dashboard.Autority_Delete('"+environment+"','" + CodIvr + "','" + date+ "')");
				cstmt = conn.prepareCall("{ call dashboard.Autority_Delete(?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, CodIvr);
				cstmt.setTimestamp(3, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add" :
				log.info(session.getId() + " - dashboard.Autority_Add('"+environment+"','" + CodIvr + "','" + date+ "')");
				cstmt = conn.prepareCall("{ call dashboard.Autority_Add(?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, CodIvr);
				cstmt.setTimestamp(3, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "deleteall" :
				log.info(session.getId() + " - dashboard.Autority_DelAll('"+environment+"','" + date+ "')");
				cstmt = conn.prepareCall("{ call dashboard.Autority_DelAll(?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "addall" :
				log.info(session.getId() + " - dashboard.Autority_AddAll('"+environment+"','" + date+ "')");
				cstmt = conn.prepareCall("{ call dashboard.Autority_AddAll(?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
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
		log.info(session.getId() + " - dashboard.Autority_GetInfoDate('"+environment+"','" + date+ "')");
		cstmt = conn.prepareCall("{ call dashboard.Autority_GetInfoDate(?,?)} ");
		if (StringUtils.isNoneBlank(date)){
			cstmt.setString(1,environment);
			cstmt.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(date).getTime()));
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
		}else{
			log.info(session.getId() + " - date is null or empty");
		}
		int i = 0;
		if (StringUtils.isNoneBlank(date)){
			if (filterToday.after(sdf_full_day_of_year.parse(date))) enableMod = false;
%>
<div id="_top" >
	<table class="roundedCorners small no_bottom" style="width: 99%">
		<tr class="listGroupItem active blue">
			<td colspan='7'>
				<table>
					<tr>
						<td>
							<div class="title">Dettaglio data:<%=date==null?"":date%> </div>
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
						
	<form id="form_save_autority" name="form_save_autority" action="AutorityGlobalDate.jsp" method="post">
		<input type="hidden" id="mAction" name="action"  value="" type="hidden"/>
		<input type="hidden"  id="mDate" name="date" value="<%=date%>" type="hidden"/>
		<input type="hidden"  id="mCodIvr" name="CodIvr" value="">
		<input type="hidden"  id="mPos" name="pos" value='<%=pos %>'>
		<div id="_left" style="overflow: auto;">
			<table id="_tab" class="roundedCorners small no_top" style="width: 99%">
<%
			while (rs.next()) {
				i++;
				Timestamp ts = rs.getTimestamp("data");
				String COD_IVR = rs.getString("COD_IVR");
				String Descrizione = rs.getString("Descrizione");									
				
				if (i%2==1)
					out.println("<tr>");
				out.println("<td>");										
				out.println("<label class='switch'>"); 
				out.println("<input type='checkbox' id='mCurrentState' name='Status' "+(ts==null?"":"checked=true")+(enableMod?" onclick='change(this,\""+COD_IVR+"\")'":" disabled")+"> ");
				out.println("<span class='slider round blue'></span>");
				out.println("</label>");
				out.println("</td>");										
				out.println("<td><label>"+ COD_IVR + " </label> </td> ");
				out.println("<td><label>"+ Descrizione + " </label> </td> ");
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
							<div class="title">Nessuna Data Selezionata </div>
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

<div id="Error" class="modal">
	<div class="modal-content">
		<div class="modal-header pink">
			<table style="width: 100%">
				<tr>
					<td><h2>Error</h2></td>
					<td>
						<div class="container_img title right pink " onclick="$('#Error').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<input type="hidden" id="asAction" name="action" Value="add_message">
		<table>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="1"><label>Error Code:</label></td>
				<td colspan="1"><label id='ErrorCode'></label></td>
			</tr>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="1"><label>Error Message:</label></td>
				<td colspan="1"><label id='ErrorMessage'></label></td>
			</tr>
			<tr>
				<td colspan="2">
					<button type="button" id="AddCloseModal" class="button pink" onclick="$('#Error').hide();">Ok</button>
				</td>
			</tr>
		</table>
	</div>
</div>

<script type="text/javascript">
	var pos=<%=pos%>;
	
	var  row_height ='<%=row_height%>';
	var  row_number='<%=row_number%>';
	var  container_height='<%=container_height%>';
	var margintop=0;
	$(function() {
		try {
			parent.parent.ChangeActiveMenu("#AutorityGlobal");	
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
// 			console.log('h = h/2:'+h)
// 			if (pos[1]>h){ 
// 				console.log('pos[1]-h:'+pos[1]+" -" +h+" >> "+ (pos[1]-h))
// 				$("#_left").css("margin-top",(pos[1]-h));
// 				$("#_left").height($(window).height()-90 -(pos[1]-h));
// 			}
				
		} catch (e) {
			//alert(e);
		}
	})
	
// 	selectRow = function(element,date,x,y) {
// 		$(element).parent().find("td").removeClass("greyBackGround"); 
// 		$(element).children("td").addClass("greyBackGround");
// 		$('#dates_day').val(date);
// 		alert(x +" - "+y)
// 	}
<%
	String ErrorCode = request.getParameter("ErrorCode");
	String ErrorMessage = request.getParameter("ErrorMessage");
	if (StringUtils.isNotBlank(ErrorCode)) {

		out.println("$('#ErrorCode').html('" + ErrorCode + "');");
		out.println("$('#ErrorMessage').html('" + ErrorMessage + "');");
		out.println("$('#Error').show();");
	}
%>
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#Error').hide();
		}
	}

	change=function(_this,CodIvr){
		if ($(_this).is(':checked'))
			$("#mAction").val('add'); // name="action"  value="" type="hidden"/>
		else
			$("#mAction").val('delete');
		$("#mCodIvr").val(CodIvr); // name="date";
		$("#form_save_autority")[0].submit();
	}

	setAll = function() {
		$(':checkbox[id=mCurrentState]').prop("checked", true);
		$("#mAction").val('addall');
		$("#form_save_autority")[0].submit();
	}
	
	setNone = function() {
		$(':checkbox[id=mCurrentState]').prop("checked", false);
		$("#mAction").val('deleteall');
		$("#form_save_autority")[0].submit();
	}
	
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>