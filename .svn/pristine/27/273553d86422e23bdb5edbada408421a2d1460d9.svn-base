<!DOCTYPE html>
<%@page import="java.util.concurrent.TimeUnit"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Date"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="java.util.ArrayList"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Properties"%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Details</title>
<link rel="stylesheet" href="../css/three.css">
<link rel="stylesheet" type="text/css" href="../css/comapp.css">
<script src="../js/jquery.min.js"></script>
<script>
var radius = 4;
var size = 1;
var color ="#e23ca1";
var style="solid";

// var connections = [];

$.fn.connect = function(to){
	var el1, el2; 
	el1 = $(this);
	el2 = $(to)
	if (el1.offset().top>el2.offset().top){
		el2 = el1;
		el1 =  $(to); 
	}
	var pa = $(el1).offset();
	var pb = $(el2).offset();
	var wa = $(el1).width();
	var wb = $(el2).width();		
	var ha = $(el1).height();
	var hb = $(el2).height();
	var pTop = $(el1).css("padding-top");
	var pBootom = $(el1).css("padding-bottom");
	var paddingBootom =  parseInt(pBootom.replace("px",""));
	var paddingTop =  parseInt(pTop.replace("px",""));
	ha += paddingTop + paddingBootom;		 
	if (pb.left <= pa.left){
		if ( (pa.left - pb.left) < radius)
			rad = 0;
		else
			rad = radius;			
		
		console.log("paddingBootom:"+paddingBootom+" paddingTop: "+paddingTop+" pa.left: " + pa.left + " pa.top:" +pa.top+" pb.left:"+pb.left+" pb.top:"+pb.top+" (wa/2):" + (wa/2)+" (wb/2):"+(wb/2) );
		var offset = (pa.left - pb.left) - (wa/2) + (wb/2);
		console.log("offset: " + offset );			
		var leftconnection = pa.left + (wa/2) - offset
		var topconnection = pa.top + ha;						
		var hconnection = (pb.top - pa.top-ha)/2;
		var wconnection = offset;							
		console.log("topconnection:  "+topconnection + " pa.top: "+pa.top + " ha: "+ ha+"  hconnection: " + hconnection+" wconnection:" + offset);			
		var element = document.createElement("connection");	
		
// 		connections.push(element);
		
		
		var width = wconnection;// - rad - rad;
		if (width<0)
				width = rad;
		
		$(element).css({ 
		    width: wconnection,
		    height: hconnection,						    
		    position: "absolute",				    
		    top: topconnection +1, 
		    left: leftconnection ,	
		    borderBottomWidth : size,
		    borderBottomColor: color,
			borderBottomStyle: style ,
		    borderRightWidth : size, 
		    borderRightColor: color,
			borderRightStyle: style,
		    borderBottomRightRadius : rad, 			 
		    width: width, 
		    height: hconnection
		}).appendTo('body');
		
		topconnection += hconnection;
		console.log("topconnection: "+topconnection);
		width = wconnection - rad -rad;
		if (width<0)
				width = rad;
		
		var element = document.createElement("connection");
// 		connections.push(element);
		$(element).css({ 			    
		    position: "absolute",				    
		    top: topconnection , 
		    left: leftconnection - rad,
		    borderLeftWidth : size,
		    borderLeftColor: color,
			borderLeftStyle: style,
			borderTopWidth : size,
			borderTopColor: color,
			borderTopStyle: style,
		    borderTopLeftRadius : rad, 
		    width: width,
		    height:hconnection		    
		}).appendTo('body'); 
	}else{
		if ( (pb.left - pa.left) < radius)
			rad = 0;
		else
			rad = radius;	
		
		console.log("pa.left: " + pa.left +" pb.left:"+pb.left+" (wa/2)  " + (wa/2)+" (wb/2):"+(wb/2) );
		var offset = (pb.left - pa.left) - (wa/2) + (wb/2);  ///------
		console.log("offset: " + offset );			
		var leftconnection = pa.left + (wa/2) ///+ offset  ///---------
		var topconnection = pa.top + ha;			
		var hconnection = (pb.top - pa.top-ha)/2;
		var wconnection = offset;				
		var   width = wconnection - rad- rad;
		if (width<0)
				width = radius;
		console.log("topconnection:  "+topconnection + " pa.top: "+pa.top + " ha: "+ ha+"  hconnection: " + hconnection+" wconnection:" + offset);	
		var element = document.createElement("connection");
// 		connections.push(element);
		$(element).css({ 
		    position: "absolute",				    
		    top: topconnection +1 , 
		    left: leftconnection ,	
		    borderBottomWidth : size,
		    borderBottomColor: color,
			borderBottomStyle: style ,
			borderLeftWidth : size,
		    borderLeftColor: color,
			borderLeftStyle: style,
		    borderBottomLeftRadius : radius, 			 
		    width: width, 
		    height: hconnection
		}).appendTo('body');
		 width = wconnection - radius;
			if (width<0)
					width = radius;
		topconnection += hconnection;
		console.log("topconnection: "+topconnection);
		var element = document.createElement("connection");
// 		connections.push(element);
		$(element).css({ 
		    position: "absolute",				    
		    top: topconnection, 
		    left: leftconnection + rad,
		    borderRightWidth : size,
		    borderRightColor: color,
			borderRightStyle: style,
			borderTopWidth : size,
			borderTopColor: color,
			borderTopStyle: style,
		    borderTopRightRadius : radius, 
		    width:width,
		    height:hconnection
		}).appendTo('body'); 
	}
}
</script>
<style type="text/css">
</style>
</head>
<body>
<div class="bottom-right">
	<table><tr>
		<td class=""><img style="height: 20px" src='../<%=ConfigServlet.getProperties().getProperty("Logo1")%>'></td>
		<td class="" style="float: right; "><img style="height: 20px" src='../<%=ConfigServlet.getProperties().getProperty("Logo2")%>'></td>
	</tr></table>
</div>
<div class="main">
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
	Properties prop = ConfigServlet.getProperties();
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================
			
	int i = 0;
	String MainMenu = null;
	int errorCode = 0;
	try {
		if (StringUtils.isBlank(CodIvr)) {
		} else {
%>
	<table class="tree" style="width: 100%">
		<tr>
			<td>
				<table style="width: 100%">
					<tr>
						<td align='center'>
							<div id='Father' style='width: 8rem;'>
								<table style="width: 100%">
<%
			java.util.Date Day = 
			new java.util.Date(new java.util.Date().getTime() + TimeUnit.DAYS.toMillis( -60 ));
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); 
			SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"); 
			String strDay = dateFormat.format(Day);  		
			try {
				Context ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
				log.info(session.getId() + " - connection CCC wait...");
				conn = ds.getConnection();
				log.info(session.getId() + " - dashboard.Detail_GetInfoCruscotto('" + CodIvr + "','"+strDay+"')");
				cstmt = conn.prepareCall("{ call dashboard.Detail_GetInfoCruscotto(?,?)} ");
				cstmt.setString(1, CodIvr);
				cstmt.setDate(2, new Date(Day.getTime()));
				rs = cstmt.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
				while (rs.next()) {
					MainMenu = rs.getString("MainMenu");																														
					String res = "<tr><td colspan='2' class='CadrsTitleBlue ' >" + rs.getString("MainMenu")+ "</td>	</tr>";
					res += "<tr class=' '><td colspan='2' class='CadrsCol Blue ' >" + rs.getString("GreenNumber")+ "</td>	</tr>";
					res += "<tr class=' '><td colspan='2' class='CadrsCol Blue ' >"+ rs.getString("MenuDescription") + "</td>	</tr>";
					res += "<tr class=' '><td colspan='1' class='CadrsCol Blue '>Chiamate</td><td colspan='1'  class='CadrsCol Blue '>"+ rs.getString("ActiveCalls") + "</td>";
					out.println(res);
				}
			} catch (Exception e) {
				log.error(session.getId() + " - general: " + e.getMessage(), e);
			} finally {
				try { rs.close(); } catch (Exception e) {}
				try { cstmt.close(); } catch (Exception e) {}
			}
%>
								</table>
							</div>
						</td>
					</tr>
				</table>
			</td>
		</tr>
		<tr>
			<td>
				<table style="width: 100%">
					<tr>
<%
			try {
				log.info(session.getId() + " - dashboard.Detail_GetMenuInfo('" + CodIvr + "','" + MainMenu + "')");
				cstmt = conn.prepareCall("{ call dashboard.Detail_GetMenuInfo(?,?)} ");
				cstmt.setString(1, CodIvr);
				cstmt.setString(2, MainMenu);
				rs = cstmt.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");

				rs.next();
				String tmpChoose = rs.getString("Choose");
				ArrayList<String> nodes = new ArrayList<>();
				nodes.add(rs.getString("node"));
				ArrayList<String> nodesDescription = new ArrayList<>();
				nodesDescription.add(rs.getString("nodeDescription"));
				while (rs.next()) {
					String choose = rs.getString("Choose");
					String node = rs.getString("node");
					String nodeDescription = rs.getString("nodeDescription");
					if (!StringUtils.equalsIgnoreCase(tmpChoose, choose)) {

						String res = "<td align='center' style='vertical-align: top'><br>"; 
						res += "<div id='Child_" + i + "' style='width: 8rem; '>";
						res += "<table style='width: 100%'>";
						res += "<tr><td colspan='2' class='CadrsTitleBlue ' >" + tmpChoose+ "</td>	</tr>";
						for (int k=0; k< nodes.size(); k++){
							res += "<tr><td class='CadrsCol Blue ' colspan='2' style='font-size: x-small;' >"+ nodesDescription.get(k) + "</td>	</tr>";
							res += "<tr><td  class='CadrsCol Blue ' style='font-size: x-small;' >"+ nodes.get(k) + "</td><td class='CadrsCol Blue ' id='a_"+ nodes.get(k) + "' > 0 </td> </tr>";
						} 
// 						res += "<tr class=' '><td colspan='1'  class='CadrsColBlue '>Chiamate</td><td colspan='1'  class='CadrsColBlue '>"															+ rs.getString("ActiveCalls") + "</td>";
						res += "</table>";
						res += "</div></td>";
						out.println(res); 
						i++;
						tmpChoose = rs.getString("Choose");
						nodes = new ArrayList<>();
						nodes.add(rs.getString("node"));
						nodesDescription = new ArrayList<>();
						nodesDescription.add(rs.getString("nodeDescription"));
					}else{
						nodes.add(node);
						nodesDescription.add(nodeDescription);
					}
				}
											
				String res = "<td align='center' style='vertical-align: top'><br>"; 
				res += "<div id='Child_" + i + "' style='width: 8rem; '>";
				res += "<table style='width: 100%'>";
				res += "<tr><td colspan='2' class='CadrsTitleBlue ' >" + tmpChoose+ "</td>	</tr>";
				for (int k=0; k< nodes.size(); k++){
					res += "<tr><td class='CadrsCol Blue ' colspan='2' style='font-size: x-small;' >"+ nodesDescription.get(k) + "</td>	</tr>";
					res += "<tr><td  class='CadrsCol Blue ' style='font-size: x-small;' >"+ nodes.get(k) + "</td><td class='CadrsCol Blue ' id='a_"+ nodes.get(k) + "' > 0 </td> </tr>";
				}
//				res += "<tr class=' '><td colspan='1'  class='CadrsColBlue '>Chiamate</td><td colspan='1'  class='CadrsColBlue '>"															+ rs.getString("ActiveCalls") + "</td>";
				res += "</table>";
				res += "</div></td>";
				i++;   
				out.println(res);
			} catch (Exception e) {
				log.error(session.getId() + " - general: " + e.getMessage(), e);
			} finally {
				try { rs.close(); } catch (Exception e) {}
				try { cstmt.close(); } catch (Exception e) {}
			}
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}	
%>
					</tr>
				</table>
			</td>
		</tr>
	</table>
</div>
	
<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#MenuDetails");
			parent.ChangeActivedFooter("#Details","rt/Details.jsp");
			<%if (!StringUtils.isBlank(CodIvr)) {%>
				parent.EnabledFooterService();
				parent.ChangeActivedFooterService();
			<%}%>
		} catch (e) {
		}
		try {
			<%for (int k = 0; k < i; k++) {%>
				$('#Father').connect('#Child_<%=k%>');
			<%}%>
			$(window).resize(function() {
				$("connection").remove();
				<%for (int k = 0; k < i; k++) {%>
					$('#Father').connect('#Child_<%=k%>');
				<%}%>
			});
			<%if (!StringUtils.isBlank(CodIvr)) {%>
			function worker(){
				$.ajax({ 
			        type: 'GET', 
			        url: 'UpdateDetails.jsp', 
			        data: { CodIvr: '<%=CodIvr%>', CodMenu:'<%=MainMenu%>' }, 
			        success: function (data) { 
			        	var json = jQuery.parseJSON(data);
			        	var i = 0;
			        	for (i=0; i<json.length; i++) {
			        		 $("#a_"+json[i].Node).text(json[i].NumCalls);
			        		 if (json[i].NumCalls > 100){
			        			 	var a = $("#a_"+json[i].Node).closest('.CadrsCol');
				        			a.removeClass('Blue Yellow Green');					        			
				        			a.addClass('Red');
			        		 }else{
				        		 if (json[i].NumCalls > 20){
				        			var a = $("#a_"+json[i].Node).closest('.CadrsCol');
				        			a.removeClass('Blue Red Green');					        			
				        			a.addClass('Yellow');
				        		 }else{
				        			var a = $("#a_"+json[i].Node).closest('.CadrsCol');						        		 
				        			a.removeClass('Blue Yellow Red');						        		
				        			a.addClass('Green');
				        		 }				        			 
			        		 }				        		 
			        	} 
			        	setTimeout(worker, 2000);
			        }
			    });
			}
			worker();
			<%}%>
		} catch (e) {
		}
	})
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>