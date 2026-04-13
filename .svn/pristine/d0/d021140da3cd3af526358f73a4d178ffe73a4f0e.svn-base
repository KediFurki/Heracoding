<%@page import="comapp.ConfigServlet"%>
<%@page import="org.apache.catalina.Session"%>
<%@page import="javax.security.auth.message.callback.PrivateKeyCallback.Request"%>
<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<!DOCTYPE html>
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
<title>Mail Web</title>
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
	//Properties prop = ConfigServlet.getProperties();
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================

	session.setAttribute("caselle_mail","Clienti.Web@gruppohera.it;supportoapp@gruppohera.it;helpdesk.sportelli@gruppohera.it;Clienti.Web@energiabase.it");
// 	session.setAttribute("caselle_mail",ConfigServlet.getProperties().getProperty("caselle_mail","Clienti.Web@gruppohera.it;supportoapp@gruppohera.it;helpdesk.sportelli@gruppohera.it;Clienti.Web@energiabase.it"));
%>
	<table class="center">
		<tbody>
			<tr>
				<td style="width: 10%">
				<form id="MailWebForm" action="MailWebQuery.jsp" target="iFrame_MailWebFindMail">
					<input type="hidden" name="command" id="command" value="Search">
					<input type="hidden" name="Download" id="Download" value="NO">
					<input type="hidden" name="Selected" id="Selected" value="">
						<table id="stickytable" class="roundedCorners smallnb no_bottom">
							<thead>
								<tr class="listGroupItem active green">
									<td>
										<table>
											<tr>
												<td style="width: 45%">
													<div class="title">Ricerca Email</div>
												</td>
												<td>
													<div class="container_img title right "><img alt="" class="" src="images/RefreshWhite_.png" onclick="window.location.reload(false); "></div>
												</td>
											</tr>
										</table>
									</td>
								</tr>
							</thead>
							<tbody>
								<tr>
									<td><strong>Filtro</strong></td>
								</tr>
								<tr>
									<td><input class="fullwidth" type="text" id="Filtro" name="Filtro" value="" /></td>
								</tr>
								<tr>
									<td><strong>E-Mail</strong></td>
								</tr>
								<tr>
									<td>
										<select id="Email" name="Email">
<!-- 											<option >Non_Associato</option> -->
										</select>
									</td>
								</tr>

								<tr>
									<td><strong>Data</strong></td>
								</tr>
								<tr>
					 				<td>
										<select id="StatoData" name="StatoData">
											<option >Attiva </option>
											<option >Disattiva</option>
										</select>
									</td>
								</tr>
								<tr>
									<td>
										<table class="center roundedCorners"  style="margin-top:0;" >
											<tr>
												<td><strong>Dal</strong></td>
											</tr>
											<tr>
												<td>
													<input type="date" class="fullwidth"  id="Data_Dal" name="Data_Dal" value=""/>
												</td>
											</tr>
											<tr>
												<td><strong>Al</strong></td>
											</tr>
											<tr>
												<td>
													<input type="date" class="fullwidth"  id="Data_Al" name="Data_Al" value=""/>
												</td>
											</tr>
										</table>
									</td>
								</tr>
 								<tr>
									<td>Mittente</td>
								</tr>
								<tr>
									<td><input class="fullwidth" type="text" id="Mittente" name="Mittente" value="" /></td>
								</tr>
								<tr>
									<td>Destinatario</td>
								</tr>
								<tr>
									<td><input class="fullwidth" type="text" id="Destinatario" name="Destinatario" value="" /></td>
								</tr>
								<tr>
									<td>Oggetto</td>
								</tr>
								<tr>
									<td><input class="fullwidth" type="text" id="Oggetto" name="Oggetto" value="" /> </td>
								</tr>
								<tr>
									<td>Testo</td>
								</tr>
								<tr>
									<td><textarea rows="" cols="" id="Testo" name="Testo"></textarea></td>
								</tr>
								<tr>
									<td> 
										<input class="button green" type="button" id="Cerca" name="Cerca" value="Cerca" onclick="sendform();"/>   
										<input class="button green" type="button" id="Annulla" name="Annulla" value="Annulla" onclick="" />  
									</td>
								</tr>
								<tr>
									<td>
<!-- 										<a class="button" href="MailWebQuery.jsp?command=Export">Esporta</a>  -->
<!-- 									 	<a class="button" href="MailWebQuery.jsp?command=Save">Salva</a> -->
										<a class="button green" id="Esporta" name="Esporta" href="">Esporta</a> 
									 	<a class="button green" id="Salva" name="Salva" href="">Salva</a>
								 	</td>
								</tr>
							</tbody>
						</table>
					</form>
				</td>
				<td style="width: 90%">
					<table class="center">
						<tbody>
							<tr>
								<td style="width: 80%">
 									<iframe src="MailWebSearchList.jsp" id="iFrame_MailWebFindMail" name="iFrame_MailWebFindMail" style="width: 100%; border: 0px solid grey; height: 800px;"></iframe>
								</td>
				 			</tr>
						</tbody>
					</table>
				</td>
				<td>
 					
				</td>
 
			</tr>
		</tbody>
	</table>
 	
 	
 	
 	<div id="Result" class="modal" >
	<div class="modal-content" style ="margin: 3% auto;width: 80%; ">
 			<div>
 				<iframe src="" id="iFrame_MailWeb" name="iFrame_MailWeb" style="width: 100%; border: 0px solid grey; min-height: 600px; "></iframe>
 			</div>
			<div id="controlli" class="title">
				<input type="button" class="button green" id="Chiudi" name="Chiudi" value="Chiudi" onclick="hideResult('#Result');">
				
			</div>
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
			parent.ChangeActiveMenu("#MailWeb");
		} catch (e) {
		}
 		$("#stickytable").stickyTableHeaders();
		$("#_iframe").height($(window).height() - 30);
		$(window).resize(function() {
			$("#_iframe").height($(window).height() - 30);
		});
		
		$('#Salva').click(function() {
			jQuery("#MailWebForm").attr('action','MailWebQuery.jsp?command=Save');
			jQuery("#MailWebForm").attr('method','POST');
	        $('#MailWebForm').submit();
	    });
		
// 	    $("#Esporta").click(function(){
// 	    	var favorite = [];
// 	    	$("#iFrame_MailWebFindMail").contents().find("input:checkbox").each(function(index, element){
// 	    		if(element.checked){
// 	    			favorite.push($(this).val());
// 	    		}
// 	    	});
// 	    	console.log("Selected: " + favorite.join(","));
// 	    	var src = 'MailWebQuery.jsp?command=Search&Download=SI&Selected='+favorite.join(",")+'&Filtro='+$('#Filtro').val() +'&Email='+$('#Email').val() +'&StatoData='+$('#StatoData').val() +'&Data_Dal='+$('#Data_Dal').val() +'&Data_Al='+$('#Data_Al').val() +'&Mittente='+$('#Mittente').val() +'&Destinatario='+$('#Destinatario').val() +'&Oggetto='+$('#Oggetto').val() +'&Testo='+$('#Testo').val() +''
// 	    	$('#Esporta').attr('href',src)
// 	    });
		
	   
	})
	
 	$(document).on("click", "#Esporta", function(){
 		var favorite = [];
    	$("#iFrame_MailWebFindMail").contents().find("input:checkbox").each(function(index, element){
    		if(element.checked){
    			favorite.push($(this).val());
    		}
    	});
    	
    	var _selected = favorite.join(",");
    	
    	console.log("Selected: " + _selected);
    	if(_selected.lenght > 0 ){
     		var src = 'MailWebQuery.jsp?command=Search&Download=SI&Selected='+favorite.join(",")+'&Filtro='+$('#Filtro').val() +'&Email='+$('#Email').val() +'&StatoData='+$('#StatoData').val() +'&Data_Dal='+$('#Data_Dal').val() +'&Data_Al='+$('#Data_Al').val() +'&Mittente='+$('#Mittente').val() +'&Destinatario='+$('#Destinatario').val() +'&Oggetto='+$('#Oggetto').val() +'&Testo='+$('#Testo').val() +''
     		$('#Esporta').attr('href',src);
 	 	}else{
    		jQuery("#ErrorCode").html("0x000000015"); jQuery("#ErrorMessage").html("Non sono state selezionate e-mail per l'esportazione ");jQuery("#Error").show()
    		return false;
    	}
    });
	
	
	var caselle_mail= '<%= session.getAttribute("caselle_mail") %>';
	var a_mail = caselle_mail.split(";");
	
	a_mail.forEach(myFunction);

	function myFunction(value) {
		$('#Email').append('<option value="'+value+'">'+value+'</option>');
	}
 
	
	var exportResult = function(a){
		var favorite = [];
    	$("#iFrame_MailWebFindMail").contents().find("input:checkbox").each(function(index, element){
    		if(element.checked){
    			favorite.push($(this).val());
    		}
    	});
    	
    	var _selected = favorite.join(",");
    	
    	console.log("Selected: " + _selected);
    	if(_selected !== "" ){
     		var src = 'MailWebQuery.jsp?command=Search&Download=SI&Selected='+favorite.join(",")+'&Filtro='+$('#Filtro').val() +'&Email='+$('#Email').val() +'&StatoData='+$('#StatoData').val() +'&Data_Dal='+$('#Data_Dal').val() +'&Data_Al='+$('#Data_Al').val() +'&Mittente='+$('#Mittente').val() +'&Destinatario='+$('#Destinatario').val() +'&Oggetto='+$('#Oggetto').val() +'&Testo='+$('#Testo').val() +''
    		a.href= src;
 			a.click();
     	}else{
    		jQuery("#ErrorCode").html("0x000000015"); jQuery("#ErrorMessage").html("Non sono state selezionate e-mail per l'esportazione ");jQuery("#Error").show()
    		a.href="#";
    		 
    	}
	}
	
	var sendform = function(){
		
		if(jQuery('#StatoData').val() === 'Attiva'){
		 	if(jQuery('#Data_Dal').val() === '' || jQuery('#Data_Al').val() === '' ){
				showError('0x00000000','Compilare i campi data');
				return false;
			} 
	 	} 
		
    	$("#iFrame_MailWebFindMail").contents().find('#SearchTable').empty();
    	$("#iFrame_MailWebFindMail").contents().find('#SearchTable').append("<img style='position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);' src='images/Wait.gif'/>");
		
		var action = "MailWebQuery.jsp?command=Search";
 		$.ajax({
            url: action,
            type: 'post',
            data: jQuery("#MailWebForm").serialize(),
            success: function(data) {
            	$("#iFrame_MailWebFindMail").contents().find('#SearchTable').empty();
            	$("#iFrame_MailWebFindMail").contents().find('#SearchTable').append(data);
            },
            error:function(data) {
            	$("#iFrame_MailWebFindMail").contents().find('#SearchTable').empty();
            	console.log(data);
            } 
    	});
		
 	}
 
	var hideResult = function(elem){
		$("#iFrame_MailWeb").attr('src','');
		$(elem).hide();
	}
	
	var openConversation = function(id){
		console.log(id);
		var link = 'MailWebConversation.jsp?command=OpenConversation&threadid='+ id;
		$("#iFrame_MailWeb").attr('src',link);
		$("#openThread").remove();
		$("#Result").show();
	}
	
	var openMail = function(id){
		console.log(id);
		var link = 'MailWebEmail.jsp?command=OpenMail&id='+ id;
		$("#iFrame_MailWeb").attr('src',link);
		$("#Result").show();
	}
	
	
	var showError = function (error_code, error_message ){
		jQuery('#ErrorCode').html(error_code); 
		jQuery('#ErrorMessage').html(error_message);
		jQuery('#Error').show()
	}
	
	var addButton = function (nome, id){
		 
		var funct = "openConversation('"+id+"')";
		$('#openThread').remove();
		$('#controlli').append('<input type="button" class="button green" id="openThread" name="openThread" value="'+nome+'" onclick="'+funct+'">');
	}
	
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>