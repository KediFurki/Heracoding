<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="java.util.Properties"%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Survey Web</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
</head>
<body style="overflow-y: auto;">
	<%
		Logger log = Logger.getLogger("comapp." + this.getClass().getName());
		log.info(session.getId() + " - ******* new request ***** ");
		String user = (String)session.getAttribute("UserName");
		if (StringUtils.isBlank(user))
			user = "-";
		user = Character.toUpperCase(user.charAt(0)) + user.substring(1);
		//==	REQUEST PARAMETERS	====
		//		NONE
		//==============================
// 		Properties prop = ConfigServlet.getProperties();
		Connection conn = null;
		CallableStatement cs = null;
		ResultSet rs = null;
		//==	ACTION PARAMETERS	====
		//		NONE
		//==============================
	%>
	<form id="save_daytime" action="SurveyWebQuery.jsp" method="post">
		<table id="stickytable" class="roundedCorners" >
			<thead>
				<tr class="listGroupItem active green">
					<td colspan="7">
						<label class="container title ">Griorni di esecuzione</label>
					</td>
				</tr>
			</thead>
			<tr>
				<td>
					<label class="switch">
						<input type="checkbox" id="Lunedi" name="1" onclick=""> 
						<span class="slider round green"></span>
					</label>
					<label>Lunedi</label>
				</td>
				<td>
					<label class="switch">
						<input type="checkbox" id="Martedi" name="2" onclick=""> 
						<span class="slider round green"></span>
					</label>
					<label>Martedi</label>
				</td>
				<td>
					<label class="switch">
						<input type="checkbox" id="Mercoledi" name="3" onclick=""> 
						<span class="slider round green"></span>
					</label>
					<label>Mercoledi</label>
				</td>
				<td>
					<label class="switch">
						<input type="checkbox" id="Giovedi" name="4" onclick=""> 
						<span class="slider round green"></span>
					</label>
					<label>Giovedi</label>
				</td>
				<td>
					<label class="switch">
						<input type="checkbox" id="Venerdi" name="5" onclick=""> 
						<span class="slider round green"></span>
					</label>
					<label>Venerdi</label>
				</td>
				<td>
					<label class="switch">
						<input type="checkbox" id="Sabato" name="6" onclick=""> 
						<span class="slider round green"></span>
					</label>
					<label>Sabato</label>
				</td>
				<td>
					<label class="switch">
						<input type="checkbox" id="Domenica" name="7" onclick=""> 
						<span class="slider round green"></span>
					</label>
					<label>Domenica</label>
				</td>
			</tr>
			<tr>
				<td colspan="7">
					<table style="width: 100%; text-align: center;">
						<tr>
							<td style="border: none;">
								Ora inizio Chiamate <input type="time" id="start_hour" name="start_hour" />
							</td>
							<td style="border: none;">
								Ora fine Chiamate <input type="time" id="end_hour" name="end_hour" />
							</td>
						</tr>
					</table>
				</td>
			</tr>
			<tr>
	 			<td style="border: none;"></td>
				<td colspan="5" style="border: none;">
					<table style="width:100% ;text-align: center;">
						<tr>
							<td style="border: none;">
								<input type="button" class="button green" value="Salva" name="Salva" onclick="SaveDateTime();" />
							</td>
							<td style="border: none;">
								<input type="button" class="button green" value="Annulla" name="Annulla" />
							</td>
						</tr>
					</table>
				</td>
		 		<td style="border: none;"></td>
			</tr>
		</table>
	</form>
	
	<div id="add_to_list" class="modal" >
			<div class="modal-content" >
				<div class="modal-header green">
					<table style="width: 100%">
						<tbody><tr>
							<td><h2 id="titolo_add_list"></h2></td>
							<td>
								<div class="container_img title right green " onclick="$('#add_to_list').hide();">
									<img alt="" class="" src="images/CloseWhite_.png">
								</div>
							</td>
						</tr>
					</tbody></table>
				</div>
						<table>
						<tbody>
							<tr>
								<td colspan="3"><H2 id="titolo_content_add_list"></H2></td>
							</tr>
							<tr>
								<td ><div id="lista_numeri"></div></td>
								<td ></td>
								<td></td>	
							</tr>
							<tr ><td colspan="3"><br></td></tr>
							<tr>
								<td colspan="3">
									 
								</td>
							</tr>
						</tbody>
					</table>
			</div>
	</div>
	
	
	<div id="Error" class="modal">
		<div class="modal-content">
			<div class="modal-header pink">
				<table style="width: 100%">
					<tbody><tr>
						<td><h2>Error</h2></td>
						<td>
							<div class="container_img title right pink " onclick="$('#Error').hide();">
								<img alt="" class="" src="images/CloseWhite_.png">
							</div>
						</td>
					</tr>
				</tbody></table>
			</div>
			<input type="hidden" id="asAction" name="action" value="add_message">
			<table>
				<tbody><tr><td><br></td></tr>
				<tr>
					<td colspan="1"><label>Error Code:</label></td>
					<td colspan="1"><label id="ErrorCode"></label></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="1"><label>Error Message:</label></td>
					<td colspan="1"><label id="ErrorMessage"></label></td>
				</tr>
				<tr>
					<td colspan="2">
						<button type="button" id="AddCloseModal" class="button pink" onclick="$('#Error').hide();">Ok</button>
					</td>
				</tr>
			</tbody></table>
		</div>
	</div>
	
	
<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#SurveyWeb");
		} catch (e) {}
 		$("#stickytable").stickyTableHeaders(); 
	});
	
	
	SaveDateTime = function(){
 
		var start_hour = jQuery("#start_hour").val();
		var end_hour = jQuery("#end_hour").val();
		
//		 	jQuery('#ErrorCode').html("000000x5");
//			jQuery('#ErrorMessage').html("Selezionare un orario in cui attivare le survey");
//			jQuery('#Error').show();

		console.log("start_hour " + start_hour);
		console.log("end_hour " + end_hour );

		if((start_hour=="")||(start_hour==null)){
			jQuery('#ErrorCode').html("000000x5");
			jQuery('#ErrorMessage').html("Selezionare un orario di inizio");
			jQuery('#Error').show();
		}else{
			
			if((end_hour=="")||(end_hour==null)){
				jQuery('#ErrorCode').html("000000x5");
				jQuery('#ErrorMessage').html("Selezionare un orario di fine");
				jQuery('#Error').show();
			}else{
			 	var action = "SurveyWebQuery.jsp?command=SaveDateTime";
				$.ajax({
		            url: action,
		            type: 'GET',
		            data: jQuery("#save_daytime").serialize(),
		            success: function(data) {
		            	jQuery("body").append(data);
		            	window.location.reload();
		            },
		            error:function(data) {
		            	console.log(data);
		            } 
		    	});
				 
			}
		}
	};
	
	
	
</script>

<%
		try {
			Context ctx = new InitialContext();
			DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
			log.info(session.getId() + " - connection CCTE wait...");
			conn = ds.getConnection();
			log.info(session.getId() + " - dashboard.SurveyWeb_GetDateTime()");
			cs = conn.prepareCall("{call dashboard.SurveyWeb_GetDateTime()}");
			rs = cs.executeQuery(); 					
			log.debug(session.getId() + " - executeCall complete");
			boolean first = true;
			
			String _script= "<script> ";
			String _DALLE = "";
			String _ALLE = "";
			if(rs.next() == false){
				
			}else{
				do{
					String _GIORNO = rs.getString("GIORNO");
					_DALLE = rs.getString("DALLE");
					_ALLE = rs.getString("ALLE");
					log.info(session.getId() + " - " +_GIORNO+" - "+ _DALLE +" - "+_ALLE );
					switch(_GIORNO){
					case "1":
						_script +="jQuery('#Lunedi').prop('checked', true);";
						break;
					case "2":
						_script +="jQuery('#Martedi').prop('checked', true);";
						break;
					case "3":
						_script +="jQuery('#Mercoledi').prop('checked', true);";
						break;
					case "4":
						_script +="jQuery('#Giovedi').prop('checked', true);";
						break;
					case "5":
						_script +="jQuery('#Venerdi').prop('checked', true);";
						break;
					case "6":
						_script +="jQuery('#Sabato').prop('checked', true);";
						break;
					case "7":
						_script +="jQuery('#Domenica').prop('checked', true);";
						break;
							
					}
					
					first = false;
				}while(rs.next());

			}
			_script +="jQuery('#start_hour').val('"+_DALLE+"');";
			_script +="jQuery('#end_hour').val('"+_ALLE+"');";
			_script += " </script>";
			out.println(_script);		
			log.info(_script);
		} catch (Exception e) {
			log.error(session.getId() + " - general: " + e.getMessage(), e);
		} finally {
			try { rs.close(); } catch (Exception e) { }
			try { cs.close(); } catch (Exception e) { }
			try { conn.close(); } catch (Exception e) { }
		}
	%>

</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>