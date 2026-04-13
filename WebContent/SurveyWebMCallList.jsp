<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.text.DateFormat"%>
<%@page import="java.util.Date"%>
<%@page import="java.util.Calendar"%>
<%@page import="java.sql.Statement"%>
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
	//==	REQUEST PARAMETERS	====
	//		NONE
	//==============================
// 	Properties prop = ConfigServlet.getProperties();
	Connection conn = null;
	CallableStatement cs = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================
	
	Date lastModify = Calendar.getInstance().getTime();
	DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    String today = formatter.format(lastModify);

%>

	<form id="upload_calllist" action="UploadCallList" method="post" enctype="multipart/form-data">
		<table id="stickytable_1" class="roundedCorners"  style="width: 100%">
			<thead>
				<tr class="listGroupItem active green">
					<td colspan=2 >
						<label class="container title ">Caricamento liste di chiamata</label>
					</td>
				</tr>
			</thead>
			<tr>
				<td style="width: 25%">
					<label>Applicazione Survey</label>
				</td>
				<td>
					<select id="applicationList_charge" name="applicationList_charge">
						<option> </option>
						</select>
				</td>
			</tr>
			<tr>
				<td>
					<label>Selezione lista</label>
				</td>				
				<td>
					<input type="file" name="docpicker" id="docpicker" accept=".csv,text/csv" class="compress"/>
					<button type="button" id="choosefile" class="button green" onclick="chooseFile();">Sfoglia&hellip;</button>
					<label for="docpicker"><span id="docpickername" class="bold" style="display: inline-block;"></span></label>
				</td>
			</tr>
			<tr>
				<td style="border: none;">
					<input type="button" class="button green" value="Attiva" name="Attiva" name="Attiva"  onclick="AttivaCallList();"  />
				</td>
				<td style="border: none;">
					<input type="button" class="button green" value="Annulla" name="Attiva" name="Annulla" onclick="AnnullaAttivaCallList();"  />
				</td>
			</tr>
		</table>
	</form>

		
	<form id="delete_calllist" action="SurveyWebQuery.jsp" method="post">
		<table id="stickytable_2" class="roundedCorners">
			<thead>
				<tr class="listGroupItem active green">
					<td  colspan=2 >
						<label class="container title ">Eliminazione liste di chiamata</label>
					</td>
				</tr>
			</thead>
			<tr >
				<td style="width: 25%">
					<label>Applicazione Survey</label>
				</td>
				<td style="vertical-align: middle;">
					<select id="applicationList_delete">
						<option> </option>
					</select>
				</td>
			</tr>
			<tr>
				<td>
					<label>Data Caricamento lista</label>
				</td>
				<td style="vertical-align: middle;">
					<table style="width: 100%; border:1px solid #ccc;">
						<tr>
							<td>
								Dal
							</td>
							<td>
								Al
							</td>
						</tr>
						<tr>
							<td>
								<input id="start_time" type="date" name="start_time" min="2000-01-02" value="<%= today%>"><br><br>
							</td>
							<td>
								<input id="end_time" type="date" name="end_time" min="2000-01-02" value="<%= today%>"><br><br>
							</td>	
						</tr>
					</table>
				</td>
			</tr>
			<tr  >
				<td style="border: none;">
					<input type="button" class="button green" value="Elimina" name="Elimina" onclick="EliminaCallList();"  />
				</td>
				<td style="border: none;">
					<input type="button" class="button green" value="Annulla" name="Annulla" onclick="AnnullaEliminaCallList();" />
				</td>
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
	<div id="confirm_delete" class="modal" >
		<div class="modal-content" >
			<div class="modal-header green">
				<table style="width: 100%">
					<tbody><tr>
						<td><h2 id="titolo_confirm_delete"></h2></td>
						<td>
							<div class="container_img title right green " onclick="$('#confirm_delete').hide();">
								<img alt="" class="" src="images/CloseWhite_.png">
							</div>
						</td>
					</tr>
				</tbody></table>
			</div>
				<table>
					<tbody>
						<tr>
							<td colspan="3"><H2 id="content_confirm_delete"></H2></td>
						</tr>
						<tr>
							<td></td>
							<td></td>
							<td></td>	
						</tr>
						<tr><td colspan="3"><br></td></tr>
						<tr>
							<td colspan="3">
							</td>
						</tr>
						<tr>
							<td style="border: none;">
								<input type="button" class="button green" value="Elimina" id="Elimina" name="Elimina" onclick="ConfermaEliminaCallList();"  />
							</td>
							<td></td>
							<td style="border: none;">
								<input type="button" class="button green" id="Annulla"  value="Annulla" name="Annulla" onclick="jQuery('#confirm_delete').hide();" />
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
 
 
<%
	try {
//		String sqlSurvey = "select id, titolo from ivr_realtime_cct.STC_configurazione.elenco_survey order by titolo";
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
		log.info(session.getId() + " - connection CCTE wait...");
		conn = ds.getConnection();
		log.info(session.getId() + " - dashboard.SurveyWeb_GetList()");
		cs = conn.prepareCall("{call dashboard.SurveyWeb_GetList()}");
		cs.execute();					
		log.debug(session.getId() + " - executeCall complete");
 		rs = cs.getResultSet(); 
		String _script = "<script> ";
		if(rs.next() == false){
			
		}else{
			do{
				String titolo =  rs.getString("titolo");
				String id = rs.getString("id");
				_script += "jQuery('#applicationList_charge').append(new Option('"+titolo+"', '"+id+"'));  jQuery('#applicationList_delete').append(new Option('"+titolo+"', '"+id+"'));";
				
			}while(rs.next());
		}
		_script +=" </script>";
		out.println(_script);
		
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cs.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
%>
 
<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#SurveyWeb");
 		} catch (e) {}
 		$("#stickytable_1").stickyTableHeaders();
 		$("#stickytable_2").stickyTableHeaders();
	})
	
	AnnullaAttivaCallList = function(){
		$('#applicationList_charge').val('');
		$("#docpicker").val('')
		$("#docpickername").html('');
	}
	
	AttivaCallList = function(){
	 	var id = jQuery('#applicationList_charge').val();
	 	console.log(id);
		if((id=="")||(id==null)){
		 	jQuery('#ErrorCode').html("000000x1");
			jQuery('#ErrorMessage').html("Per caricare una lista di chiamata è necessario specificare l'applicazione survey");
			jQuery('#Error').show();
		}else{
			if(jQuery('#docpicker').val()==""){
				jQuery('#ErrorCode').html("000000x2");
				jQuery('#ErrorMessage').html("Selezionare il file .CSV contenente la lista di chiamata");
				jQuery('#Error').show();
				
			}else{
				var formData = new FormData();
				formData.append('file', $('#docpicker')[0].files[0]);
				formData.append('id', jQuery('#applicationList_charge option:selected').val());
				$.ajax({
				       url : 'UploadCallList',
				       type : 'POST',
				       data : formData,
				       processData: false,  // tell jQuery not to process the data
				       contentType: false,  // tell jQuery not to set contentType
				       success : function(data) {
				           	console.log(data);
				           	
				           	jQuery('#titolo_add_list').html($("<span />").html("Caricati <label id='numero_caricati'></label> numeri, scartati <label id='numero_scartati'></label>"));
				           	jQuery('#titolo_content_add_list').html($("<span />").html("Numeri di telefono scartati dal caricamento"));
				           	jQuery("body").find("#script_add_to_list").remove();
				           	$("body").append($("<script />", {
				           		id: 'script_add_to_list',
				        	   	html: data
							}));
				           	jQuery('#add_to_list').show();
				       }
				});
			}
		}
	};
	
	
	AnnullaEliminaCallList = function(){
		jQuery('#start_time').val('<%= today%>');
		jQuery('#end_time').val('<%= today%>');
	}
	
	EliminaCallList = function(){
		jQuery('#Elimina').show();
		var id = jQuery('#applicationList_delete').val();
		if((id=="")||(id==null)){
		 	jQuery('#ErrorCode').html("000000x3");
			jQuery('#ErrorMessage').html("Per eliminare una lista di chiamata è necessario specificare l'applicazione survey");
			jQuery('#Error').show();
		}else{
			var start_time= new Date(jQuery('#start_time').val());
			var end_time= new Date(jQuery('#end_time').val());
			if(start_time > end_time){
				jQuery('#ErrorCode').html("000000x4");
				jQuery('#ErrorMessage').html("la data di inizio deve essere inferiore alla data di fine periodo");
				jQuery('#Error').show();
			}else{
				var formData = new FormData();
				var action = "SurveyWebQuery.jsp?command=VerifyContactToDelete&id="+ jQuery('#applicationList_delete option:selected').val()
				$.ajax({
		            url: action,
		            type: 'post',
		            data: jQuery("#delete_calllist").serialize(),
		            success: function(data) {
		            	jQuery('body').find('#scrtmp').remove();
		            	jQuery("body").append(data);
		            	jQuery('#confirm_delete').show();
		            	
		            },
		            error:function(data) {
		            	console.log(data);
		            } 
		    	});
 			}
		} 
	}
	
	ConfermaEliminaCallList = function(){
		var id = jQuery('#applicationList_delete').val();
		if((id=="")||(id==null)){
		 	jQuery('#ErrorCode').html("000000x3");
			jQuery('#ErrorMessage').html("Per eliminare una lista di chiamata è necessario specificare l'applicazione survey");
			jQuery('#Error').show();
		}else{
			var start_time= new Date(jQuery('#start_time').val());
			var end_time= new Date(jQuery('#end_time').val());
			if(start_time > end_time){
				jQuery('#ErrorCode').html("000000x4");
				jQuery('#ErrorMessage').html("la data di inizio deve essere inferiore alla data di fine periodo");
				jQuery('#Error').show();
			}else{
				var formData = new FormData();
				var action = "SurveyWebQuery.jsp?command=DeleteContactList&id="+ jQuery('#applicationList_delete option:selected').val()
				$.ajax({
		            url: action,
		            type: 'post',
		            data: jQuery("#delete_calllist").serialize(),
		            success: function(data) {
		            	jQuery('#confirm_delete').hide();
		            	jQuery('body').find('#scrtmp').remove(); 
		            	jQuery("body").append(data);
			          
		            },
		            error:function(data) {
		            	console.log(data);
		            } 
		    	});
 			}
		}
	}
	
	loadfile = function() {
		$("#docpicker").val('')
		$("#docpickername").html('');
		$("#LoadFile").show();
	}

	chooseFile = function() {
		$("#docpicker").click();
	}

	$("#docpicker").on('change', function(e) {
		var fileName = '';
		if( e.target.value )
			fileName = e.target.value.split( '\\' ).pop();
		if( fileName )
			$("#docpickername").html( fileName );
		else
			$("#docpickername").html( '' );
	});
	
	
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>