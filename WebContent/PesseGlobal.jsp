<%@page import="comapp.ConfigServlet"%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<!-- <link rel="icon" rel="images/red.ico"> -->
<link rel="shortcut icon" href="images/favicon.ico" />
<title>Pesse</title>
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
		<table class="center"  >
			<tr>
				<td style="width: 10%"></td>
				<td>
					<form id="FormPesseIM" action="" method="post" enctype="multipart/form-data">
					<input type="hidden" id="TerritorioIM" name="Terrotorio" value="IM" />
			 		<table id="stickytable_1" class="roundedCorners"  style="width: 100%">
						<thead>
							<tr class="listGroupItem active green">
								<td colspan="3"><label class="container title ">Gestione lista chiamanti IMOLA</label></td>
					 	 	</tr>
					 	 </thead>
				 	 	<tr>
				 	 		<td style="width: 25%"><label>Ultima modifica </label></td>	
				 	 		<td colspan="2"><label id="EsitoIM"></label></td>
				 	 	</tr>
				 	 	<tr>
				 	 		<td >Inizio Distacco </td>
				 	 		<td colspan="2"><input id="InizioDistaccoIM" name="InizioDistacco" type="date" value=""></td>
				 	 	</tr>
				 	 	<tr>
				 	 		<td >Fine validità lista </td>
				 	 		<td colspan="2"><input id="FineValidIM" name="FineValid" type="date" value=""></td>
				 	 	</tr>
				 	 	<tr>
				 	 		<td >Livello</td>
				 	 		<td colspan="2">
					 	 		<select id="LivelloIM" name="Livello">
									<option value="0"'>Annullata</option>
									<option value="1">1</option>
									<option value="2">2</option>
									<option value="3">3</option>
									<option value="4">4</option>
									<option value="5">5</option>
								</select>
				 	 		</td>
				 	 	</tr>
				 	 	<tr>
				 	 		<td >Fuso Orario</td>
				 	 		<td colspan="2">
					 	 		<select id="FusoOrarioIM" name="FusoOrario">
									<option Selected></option>
									<option value="L">Ora Legale</option>
									<option value="S">Ora Solare</option>
								</select>
				 	 		</td>
				 	 
				 	 	</tr>
				 	 	<tr>
				 	 		<td >Seleziona Lista</td>
				 	 		<td colspan="2">
								<input type="file" name="File" id="UploadFileIM" accept=".csv" class="compress"/>
								<button type="button" id="choosefile" class="button green" onclick="chooseFileIM();">Sfoglia&hellip;</button>
								<label for="UploadFileIM"><span id="UploadFileIMname" class="bold" style="display: inline-block;"></span></label>
				 	 		 </td>
				 	 	</tr>
				 	 	<tr  >
<!-- 				 	 		<td calspan="3"> -->
<!-- 				 	 			<table> -->
<!-- 				 	 				<tr> -->
				 	 					<td><input class="button green" type="button" id="attiva" name="attiva" value="attiva" onclick="attivaPESSE('IM')"/></td> 
										<td><input class="button green" type="button" id="annulla" name="annulla" value="annulla" onclick="annullaClick('IM')"/></td> 
										<td><input class="button green" type="button" id="svuota" name="svuota" value="svuota" onclick="svuotaTabella('IM')"/> </td>
<!-- 				 	 				</tr> -->
<!-- 				 	 			</table>	 -->
<!-- 				 	 		</td> -->
			 	 		</tr>
				 	 </table>
			 	 </form>
			 	 
			 	 <form id="FormPesseMO" action="" method="post" enctype="multipart/form-data">
			 	 	<input type="hidden" id="TerritorioMO" name="Terrotorio" value="MO" />
			 	 	<table id="stickytable_2" class="roundedCorners" style="width: 100%">
						<thead>
							<tr class="listGroupItem active green">
								<td colspan="3"><label class="container title ">Gestione lista chiamanti MODENA</label></td>
					 	 	</tr>
					 	 </thead>
				 	 	<tr>
				 	 		<td style="width: 25%"><label>Ultima modifica </label></td>	
				 	 		<td colspan="2"><label id="EsitoMO"></label></td>
				 	 	</tr>
				 	 	<tr>
				 	 		<td >Inizio Distacco </td>
				 	 		<td colspan="2"><input id="InizioDistaccoMO" name="InizioDistacco" type="date" value=""></td>
				 	 	</tr>
				 	 	<tr>
				 	 		<td >Fine validità lista </td>
				 	 		<td colspan="2"><input id="FineValidMO" name="FineValid" type="date" value=""></td>
				 	 	</tr>
				 	 	<tr>
				 	 		<td >Livello</td>
				 	 		<td colspan="2">
					 	 		<select id="LivelloMO" name="Livello">
									<option value="0"'>Annullata</option>
									<option value="1">1</option>
									<option value="2">2</option>
									<option value="3">3</option>
									<option value="4">4</option>
									<option value="5">5</option>
								</select>
				 	 		</td>
				 	 	</tr>
				 	 	<tr>
				 	 		<td >Fuso Orario</td>
				 	 		<td colspan="2">
					 	 		<select  id="FusoOrarioMO" name="FusoOrario">
									<option Selected></option>
									<option value="L">Ora Legale</option>
									<option value="S">Ora Solare</option>
								</select>
				 	 		</td> 
				 	 	</tr>
				 	 	<tr>
				 	 		<td >Seleziona Lista</td>
				 	 		<td colspan="2">
								<input type="file" name="UploadFileMO" id="UploadFileMO" accept=".csv" class="compress"/>
								<button type="button" id="choosefile" class="button green" onclick="chooseFileMO();">Sfoglia&hellip;</button>
								<label for="UploadFileMO"><span id="UploadFileMOname" class="bold" style="display: inline-block;"></span></label>
				 	 		</td>
				 	 		 
				 	 	</tr>
				 	 	<tr  >
<!-- 				 	 		<td calspan="2"> -->
<!-- 				 	 			<table> -->
<!-- 				 	 				<tr> -->
				 	 					<td><input class="button green" type="button" id="attivaMO" name="attivaMO" value="attiva" onclick="attivaPESSE('MO')"/></td> 
										<td><input class="button green" type="button" id="annullaMO" name="annullaMO" value="annulla" onclick="annullaClick('MO')"/></td> 
										<td><input class="button green" type="button" id="svuotaMO" name="svuotaMO" value="svuota" onclick="svuotaTabella('MO')"/> </td>
<!-- 				 	 				</tr> -->
<!-- 				 	 			</table>	 -->
<!-- 				 	 		</td> -->
			 	 		</tr>
				 	 </table>
			 	 </form>
		 	 	</td>				 
				<td style="width: 10%"></td>
			</tr>
		</table>
 
 	<div id="modal_confirm" class="modal" >
		<div class="modal-content" >
			<div class="modal-header green">
				<table style="width: 100%">
					<tbody><tr>
						<td><h2 id="titolo_confirm"></h2></td>
						<td>
							<div class="container_img title right green " onclick="$('#modal_confirm').hide();$('#titolo_content_confirm').html('');$('#content_confirm').html('');$('#content_list').html('')">
								<img alt="" class="" src="images/CloseWhite_.png">
							</div>
						</td>
					</tr>
				</tbody></table>
			</div>
				<table>
					<tbody>
						<tr>
							<td colspan="3"><H3  id="titolo_content_confirm"></H3></td>
						</tr>
						<tr>
							<td></td>
							<td><label id="content_confirm"></label></td>
							<td></td>	
						</tr>
						<tr >
							<td colspan="3"></td>
						</tr>
						<tr>
							<td></td>
							<td><label id="content_list"></label></td>
							<td></td>	
						</tr>
					</tbody>
				</table>
		</div>
	</div>
	<div id="Error" class="modal">
		<div class="modal-content">
			<div class="modal-header pink">
				<table style="width: 100%">
					<tbody>
						<tr>
							<td><h2>Error</h2></td>
							<td>
								<div class="container_img title right pink " onclick="$('#Error').hide();">
									<img alt="" class="" src="images/CloseWhite_.png">
								</div>
							</td>
						</tr>
					</tbody>
				</table>
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
				</tbody>
			</table>
		</div>
	</div>
	
	<script type="text/javascript">
		$(function() {
			try {
				parent.ChangeActiveMenu("#PesseGlobal");		
				caricaPesse("IM");
				caricaPesse("MO");
			} catch (e) {
				jQuery('#ErrorCode').html("000000x9");
				jQuery('#ErrorMessage').html("Errore nel caricamento dei dati");
				jQuery('#Error').show();
			}
	 		$("#stickytable_1").stickyTableHeaders();
	 		$("#stickytable_2").stickyTableHeaders();
		}) 


		
		caricaPesse = function(data){
			console.log('caricaPesse ' + data);
			var action = "PesseQuery.jsp?command=caricaPesse&territorio="+data;
			$.ajax({
		        url: action,
	            type: 'GET',
	            success: function(carica) {
	            	$('body').append($('<script />', {
	           		   	html: carica,
					}));
	            },
	            error : function(error){
					console.log('caricaPesseMO ' + error);
	            	$('#ErrorCode').html("000000x8");
    				$('#ErrorMessage').html("Errore nel caricamento dei dati");
    				$('#Error').show();
	            }
			});
		}
	
		checkField = function(data){
			var InizioDistacco = "#InizioDistacco" + data;
			var FineValid 	= "#FineValid" + data;
			var Livello 	= "#Livello" + data;
			var FusoOrario 	= "#FusoOrario" + data;
			var file 		= "#UploadFile" + data;
			var check = 0;
			if((jQuery(InizioDistacco).val() !== null)&&(jQuery(InizioDistacco).val() !== "")&&(jQuery(InizioDistacco).val() !== undefined)){
				check = check + 1;
			}
			
			if((jQuery(FineValid).val() !== null)&&(jQuery(FineValid).val() !== "")&&(jQuery(FineValid).val() !== undefined)){
				check = check + 1;
			}
			
			if((jQuery(Livello).val() !== null)&&(jQuery(Livello).val() !== "")&&(jQuery(Livello).val() !== undefined)){
				check = check + 1;
			}
			
			if((jQuery(FusoOrario).val() !== null)&&(jQuery(FusoOrario).val() !== "")&&(jQuery(FusoOrario).val() !== undefined)){
				check = check + 1;
			}

			if (check === 4) return true;
			else return false;
		}

		attivaPESSE = function(data){
			var territorio;
			switch(data){
				case "IM": 
					territorio = "IMOLA"; 
					break;
				case "MO": 
					territorio = "MODENA"; 
					break;
			}
			var action = "PesseQuery.jsp?command=recordPesseCaricato&territorio="+data;
			$.ajax({
	            url: action,
	            type: 'GET',
	            success: function(data1) {
	            	var cont = (data1.replace(/(\r\n|\n|\r)/gm, "") === "true") ;
	              	if (cont === true){
	            		jQuery('#ErrorCode').html("000000x8");
        				jQuery('#ErrorMessage').html("ATTENZIONE: un contatto della lista PESSE di "+ territorio +" risulta in lavorazione, prima di eseguire modifiche fermare la campagna outbound");
        				jQuery('#Error').show();
        				return;
	            	}else{
	            		var controllo = checkField(data);
	        			console.log("controllo : " + controllo );
	        			if(controllo === false){
	        			 	jQuery('#ErrorCode').html("000000x7");
	        				jQuery('#ErrorMessage').html("Per caricare una lista PESSE è necessario impostare tutte le opzioni");
	        				jQuery('#Error').show();
	        				return;
	        			}
  	        			var form_id = "FormPesse"+data;
	        			var file_id ="#UploadFile"+data;
	         			if((jQuery(file_id).val() === null)||(jQuery(file_id).val() === "")||(jQuery(file_id).val() === undefined)){
							console.log('file_id nullo');
	         				var action = "PesseQuery.jsp?command=svuotaTabellaImpostazioniPesse&territorio="+data;
	         				$.ajax({
	         		            url: action,
	         		           	type: 'post',
	         		            data: jQuery('#'+form_id).serialize(),
	         		            success: function(data3) {
	         		            	cont = (data3.replace(/(\r\n|\n|\r)/gm, '') === 'true') ;
									console.log('data3 : ' + cont);
	         		              	if (cont === true){
	         		              		var action = "PesseQuery.jsp?command=salvaImpostazioniPesse&territorio="+data;
		    	         				$.ajax({
		    	         		            url: action,
		    	         		           	type: 'post',
		    	         		            data: jQuery('#'+form_id).serialize(),
		    	         		            success: function(data4) {
		    		         		           	$('body').append($('<script />', {
		    	        			           		 html: data4
		    	        						}));
		    		         		           caricaPesse(data);
		             		            	},
		             		            	error : function(data4){
		             		            		jQuery('#ErrorCode').html("000000x12");
		            	        				jQuery('#ErrorMessage').html("Errore durante il salvataggio delle impostazioni");
		            	        				jQuery('#Error').show();
		             		            	}
		    	         		        });
	         		              	}else{
	         		              		jQuery('#ErrorCode').html("000000x13");
	        	        				jQuery('#ErrorMessage').html("Errore durante la cancellazione delle impostazioni");
	        	        				jQuery('#Error').show();
	         		              	}
	     		            	},
         		            	error : function(data3){
         		            		jQuery('#ErrorCode').html("000000x10");
        	        				jQuery('#ErrorMessage').html("Errore durante la cancellazione delle impostazioni");
        	        				jQuery('#Error').show();
         		            	}
	          		        });
 	        				return;
	        			}
	        			console.log('upload file_id');
	        			var formData = new FormData(document.getElementById(form_id));
	        			formData.append('file', $(file_id)[0].files[0]);
	        			
	        			$.ajax({
	        			       url : 'UploadPesse',
	        			       type : 'POST',
	        			       data : formData,
	        			       processData: false,  // tell jQuery not to process the data
	        			       contentType: false,  // tell jQuery not to set contentType
	        			       success : function(data2) {
	        			           	//console.log(data2);
	         			           	jQuery('body').append(jQuery('<script />', {
	         			           		 html: data2,
	         						}));
									var action = "PesseQuery.jsp?command=svuotaTabellaImpostazioniPesse&territorio="+data;
									$.ajax({
										url: action,
										type: 'post',
										data: jQuery('#'+form_id).serialize(),
										success: function(data3) {
											cont = (data3.replace(/(\r\n|\n|\r)/gm, '') === 'true') ;
											console.log('data3 : ' + cont);
											if (cont === true){
												var action = "PesseQuery.jsp?command=salvaImpostazioniPesse&territorio="+data;
												$.ajax({
													url: action,
													type: 'post',
													data: jQuery('#'+form_id).serialize(),
													success: function(data4) {
														$('body').append($('<script />', {
															 html: data4
														}));
													   caricaPesse(data);
													},
													error : function(data4){
														jQuery('#ErrorCode').html("000000x12");
														jQuery('#ErrorMessage').html("Errore durante il salvataggio delle impostazioni");
														jQuery('#Error').show();
													}
												});
											}else{
												jQuery('#ErrorCode').html("000000x13");
												jQuery('#ErrorMessage').html("Errore durante la cancellazione delle impostazioni");
												jQuery('#Error').show();
											}
										},
										error : function(data3){
											jQuery('#ErrorCode').html("000000x10");
											jQuery('#ErrorMessage').html("Errore durante la cancellazione delle impostazioni");
											jQuery('#Error').show();
										}
									});
									
 	        			       },
							   error : function(data2){
         		            		jQuery('#ErrorCode').html("000000x11");
        	        				jQuery('#ErrorMessage').html("Errore durante l'upload delle impostazioni");
        	        				jQuery('#Error').show();
         		            	}
	        			});
	            	}
	            },
	            error:function(data1) {
	            	console.log(" --e " +data1);
	            } 
	    	});
 		}
		
		svuotaTabella = function(data) {
 			var action = "PesseQuery.jsp?command=recordPesseCaricato&territorio="+data;
			$.ajax({
	            url: action,
	            type: 'GET',
	            success: function(data1) {
	            	var cont = (data1.replace(/(\r\n|\n|\r)/gm, "") === "true") ;
	              	if (cont === true){
	            		jQuery('#ErrorCode').html("000000x8");
        				jQuery('#ErrorMessage').html("ATTENZIONE: un contatto della lista PESSE di "+ territorio +" risulta in lavorazione, prima di eseguire modifiche fermare la campagna outbound");
        				jQuery('#Error').show();
        				return;
	            	}else{
						action = "PesseQuery.jsp?command=svuotaTabellaImpostazioniPesse&territorio="+data;
						$.ajax({
				            url: action,
				            type: 'GET',
				            success: function(carica) {
				            	jQuery('body').append(jQuery('<script />', {
				           		   	html: carica.replace(/(\r\n|\n|\r)/gm, '')
								}));
				            },
				            error : function(error){
				            	jQuery('#ErrorCode').html("000000x16");
			    				jQuery('#ErrorMessage').html("Errore nello svuotamento della tabella impostazioni");
			    				jQuery('#Error').show();
				            }
						});
						 
						action = "PesseQuery.jsp?command=svuotaTabellaRecordPesse&territorio="+data;
						$.ajax({
				            url: action,
				            type: 'GET',
				            success: function(carica) {
				            	jQuery('body').append(jQuery('<script />', {
				           		   	html: carica.replace(/(\r\n|\n|\r)/gm, "")
								}));
				            },
				            error : function(error){
				            	jQuery('#ErrorCode').html("000000x15");
			    				jQuery('#ErrorMessage').html("Errore nello svuotamento della tabella record Pesse");
			    				jQuery('#Error').show();
				            }
						});
						
						action = "PesseQuery.jsp?command=salvaImpostazioniPesse&territorio="+data+"&Livello=0&FusoOrario=";
         				$.ajax({
         		            url: action,
         		           	type: 'GET',
         		            success: function(data4) {
	         		           	$('body').append($('<script />', {
        			           		 html: data4.replace(/(\r\n|\n|\r)/gm, "")
        						}));
	         		           caricaPesse(data);
     		            	},
     		            	error : function(data4){
     		            		jQuery('#ErrorCode').html("000000x12");
    	        				jQuery('#ErrorMessage').html("Errore durante il salvataggio delle impostazioni");
    	        				jQuery('#Error').show();
     		            	}
         		        });
                	}
	            }
			});
		}
		
		annullaClick = function(data){
			$("#UploadFile"+data).val('')
			$("#UploadFile"+data+"name").html('');
			caricaPesse(data);
		}

		chooseFileIM = function() {
			$("#UploadFileIM").click();
		}

		$("#UploadFileIM").on('change', function(e) {
			var fileName = '';
			if( e.target.value )
				fileName = e.target.value.split( '\\' ).pop();
			if( fileName )
				$("#UploadFileIMname").html( fileName );
			else
				$("#UploadFileIMname").html( '' );
		});

		chooseFileMO = function() {
			$("#UploadFileMO").click();
		}

		$("#UploadFileMO").on('change', function(e) {
			var fileName = '';
			if( e.target.value )
				fileName = e.target.value.split( '\\' ).pop();
			if( fileName )
				$("#UploadFileMOname").html( fileName );
			else
				$("#UploadFileMOname").html( '' );
		});
		
		
		
	</script>
	
</body>
</html>