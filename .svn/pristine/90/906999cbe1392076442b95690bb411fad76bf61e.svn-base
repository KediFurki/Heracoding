<!DOCTYPE html>
<%@page import="java.sql.Types"%>
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
<title>TechniciansList</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
<script src="js/jquery.paging.js"></script>
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
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	String sPage = request.getParameter("page");
	int numPage = 1;
	try {
		numPage = Integer.parseInt(sPage);
	} catch (Exception e) {}
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:"+action  );
	try {
		Context ctx = new InitialContext();
		DataSource ds = null;
		switch (environment) {
		case "CCT-F":
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTF");
			log.info(session.getId() + " - connection CCTF wait...");
			break;
		}
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "delete":
				String d_ntel = request.getParameter("delete_ntel");
				log.info(session.getId() + " - dashboard.TechniciansList_Delete('"+CodIvr+"','"+d_ntel+"')");
				cstmt = conn.prepareCall("{ call dashboard.TechniciansList_Delete(?,?)} ");
				cstmt.setInt(1, Integer.parseInt(CodIvr));
				cstmt.setString(2, d_ntel);
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add":
				String a_ntel = request.getParameter("add_ntel");
				String a_nomcog = request.getParameter("add_nomcog");
				String a_esig = request.getParameter("add_esig");
		 		log.info(session.getId() + " - dashboard.TechniciansList_Add('"+CodIvr+"'"+
		 															",'"+a_ntel+"'"+							// NUMERO_TELEFONO
													 				",'"+a_nomcog+"'"+							// NOM_COG
													 				",'"+a_esig+"'"+							// ESIGENZA
													 				")");
				cstmt = conn.prepareCall("{ call dashboard.TechniciansList_Add(?,?,?,?)} ");
				cstmt.setInt(1, Integer.parseInt(CodIvr));
				cstmt.setString(2, a_ntel);
				cstmt.setString(3, a_nomcog);
				cstmt.setString(4, a_esig);
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
<div class="pagination inline_cmd"></div>
<table class="center">
	<tr>
		<td style="width: 10%"></td>
		<td style="width: 80%">
			<form id="form_without_control" action="TechniciansList.jsp" method="post">
				<input type="hidden" name="action" value="day_of_year">
				<input type="hidden" name="page" id="page" value="<%=numPage%>">
				<table id="stickytable" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active blue">
						<td colspan='9'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Lista Tecnici</div>
									</td>
									<td>
										<div class="container_img title right blue " onclick="loadfile();">
											<img alt="" class="" src="images/MenuPlusWhite_.png">
										</div>
									</td>
								</tr>
							</table>
						</td>
					</tr>
			 	 </thead>
<%
	int numRows = 0;
	try {
		log.info(session.getId() + " - dashboard.TechniciansList_GetNumRows('"+CodIvr+"')");
		cstmt = conn.prepareCall("{ call dashboard.TechniciansList_GetNumRows(?)} ");
		cstmt.setInt(1, Integer.parseInt(CodIvr));
		rs = cstmt.executeQuery();
		if (rs.next()) {
			numRows = rs.getInt("NUMROWS");			
		}
		log.debug(session.getId() + " - executeCall complete");
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}

%>
			 	 <tbody id="pagbody">
			 	 </tbody>
			 	 <tfoot>
					<tr>
						<td colspan="9">
							<div class="container_img title left grey">
								<img alt="" src="images/PlusGrey_.png" onclick="confirmadd()">
							</div>
						</td>
					</tr>
				</tfoot>
				</table>
			</form>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>
<div class="pagination inline_cmd"></div>

<div id="delete" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">						
			<table style="width: 100%">
				<tr>
					<td>	<h2>Elimina</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#delete').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_delete" action="TechniciansList.jsp" method="post">
			<input type="hidden" id="dAction" name="action" Value="delete">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="delete_ntel">Numero Telefono</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_ntel" name="delete_ntel" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td class="bottom"><label for="delete_nomcog">Nome Cognome</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_nomcog" name="delete_nomcog" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr>
					<td><label for="delete_esig">Esigenza</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_esig" name="delete_esig" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><button type="submit" id="CloseModal" class="button blue">Confermi Eliminazione</button></td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="add" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Aggiungi Tecnico</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#add').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add" action="TechniciansList.jsp" method="post">
			<input type="hidden" id="aAction" name="action" Value="add">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><input type="text" id="add_ntel" name="add_ntel" class="formInput bold_imp" placeholder=" Numero Telefono" required="required"></td>	
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><input type="text" id="add_nomcog" name="add_nomcog" class="formInput bold_imp" placeholder=" Nome Cognome" required="required"></td>	
				</tr>
				<tr>
					<td colspan="3"><input type="text" id="add_esig" name="add_esig" class="formInput bold_imp" placeholder=" Esigenza"></td>	
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><button type="submit" id="AddCloseModal" class="button blue">Conferma</button></td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="LoadFile" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Carica Lista Tecnici</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#LoadFile').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_load_file" enctype="multipart/form-data">
			<input type="hidden" id="asAction" name="action" Value="upload">
			<table style="width: 100%">
				<tr><td><br></td></tr>
				<tr>
				 	<td colspan="2">
						<input type="file" name="uploadfile" id="uploadfile" accept=".csv" class="compress"/>
						<button type="button" id="choosefile" class="button blue" onclick="chooseFile();">Sfoglia&hellip;</button>
						<label for="uploadfile"><span id="uploadfilename" class="bold" style="display: inline-block;"></span></label>
				 	</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label class='switch'> <input type='checkbox' id='mDeleteAll' name='deleteall'><span class='slider round blue'></span></label>
					</td>
					<td><label>Cancella Lista Tecnici presenti</label></td>
				</tr>
				<tr>
					<td>
						<label class='switch'> <input type='checkbox' id='mWithHeader' name='withheader'><span class='slider round blue'></span></label>
					</td>
					<td><label>File con Intestazione</label></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="button" id="Carica" class="button blue" onclick="caricaTechList();">Carica</button>
					</td>						
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="Wait" class="modal">
	<img id="wait_image" class="imagehidden" style='position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);' src='images/Wait.gif'/>
</div>

<div id="Error" class="modal">
	<div class="modal-content">
		<div class="modal-header pink">
			<table style="width: 100%">
				<tbody>
					<tr>
						<td><h2>Error</h2></td>
						<td>
							<div class="container_img title right pink " onclick="$('#form_reload')[0].submit();">
								<img alt="" class="" src="images/CloseWhite_.png">
							</div>
						</td>
					</tr>
				</tbody>
			</table>
		</div>
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
						<button type="button" id="errorButton" class="button pink" onclick="$('#form_reload')[0].submit();">Ok</button>
					</td>
				</tr>
			</tbody>
		</table>
	</div>
</div>

<div id="Info" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tbody>
					<tr>
						<td><h2>Risultato</h2></td>
						<td>
							<div class="container_img title right blue " onclick="$('#form_reload')[0].submit();">
								<img alt="" class="" src="images/CloseWhite_.png">
							</div>
						</td>
					</tr>
				</tbody>
			</table>
		</div>
		<table style="width: 100%">
			<tbody><tr><td><br></td></tr>
				<tr>
					<td><label class="bold">Record Letti</label></td>
					<td><label id="Inforead"></label></td>
				</tr>
				<tr>
					<td colspan="2">
						<button type="button" id="infoButton" class="button blue" onclick="$('#form_reload')[0].submit();">Ok</button>
					</td>
				</tr>
			</tbody>
		</table>
	</div>
</div>

<div id="Reload" class="modal">
	<form id="form_reload" action="TechniciansList.jsp" method="post">
	</form>
</div>

<script>
	$(function() { 
		try {
			parent.ChangeActiveMenu("#ServiceDetails");
			parent.ChangeActivedFooter("#TechniciansList","TechniciansList.jsp");
		} catch (e) {
		}
 		$("#stickytable").stickyTableHeaders();
	})

	confirmdelete =  function(ntel,nomcog,esig) {
		$("#delete_nomcog").val(nomcog);
		$("#delete_ntel").val(ntel);
		$("#delete_esig").val(esig);
		$("#delete").show();
	}
	
	confirmadd =  function() {
		$('#add').show();
	}
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#delete').hide();
			$('#add').hide();
			$('#LoadFile').hide();
		}
	}

	loadfile = function() {
		$("#uploadfile").val('')
		$("#uploadfilename").html('');
		$("#Carica").prop("disabled", true);
		$("#LoadFile").show();
	}

	chooseFile = function() {
		$("#uploadfile").click();
	}

	$("#uploadfile").on('change', function(e) {
		var fileName = '';
		if( e.target.value )
			fileName = e.target.value.split( '\\' ).pop();
		if( fileName ) {
			$("#uploadfilename").html( fileName );
			$("#Carica").prop("disabled", false);
		} else {
			$("#uploadfilename").html( '' );
			$("#Carica").prop("disabled", true);
		}
	});
	
	caricaTechList = function() {
		$("#Wait").fadeIn(500);
		$("#wait_image").removeClass("imagehidden");
		$("#wait_image").addClass("imagevisible");
		var formData = new FormData(document.getElementById("form_load_file"));
		$.ajax({
			url : 'UploadTechniciansList',
			type : 'POST',
			data : formData,
			contents: "json",
			processData: false,  // tell jQuery not to process the data
			contentType: false,  // tell jQuery not to set contentType
			success : function(result) {
				var data = JSON.parse(result);
				if (data.res == "OK"){
					$('#LoadFile').hide();
					$('#Inforead').html(data.read);
					$('#Info').show();
				} else {
					$('#LoadFile').hide();
					$('#ErrorCode').html(data.errcode);
					$('#ErrorMessage').html(data.err);
					$('#Error').show();
				}
				$("#Wait").fadeOut(500);
				$("#wait_image").removeClass("imagevisible");
				$("#wait_image").addClass("imagehidden");
				return;
			},
			error: function(error) {
				$("#Wait").fadeOut(500);
				$("#wait_image").removeClass("imagevisible");
				$("#wait_image").addClass("imagehidden");
				$('#ErrorCode').html("ServLet Exception");
				$('#ErrorMessage').html(error);
				$('#Error').show();
				return;
			}
		});
	}
	
	var numRows = <%=numRows%>;
	$(".pagination").paging(<%=numRows%>, { // make 1337 elements navigatable
		format: '[< nncnn! >]', // define how the navigation should look like and in which order onFormat() get's called
		perpage: 100, // show 10 elements per page
		lapping: 0, // don't overlap pages for the moment
		page: <%=numPage%>, // start at page, can also be "null" or negative
		onSelect: function(page) {
			$("#page").val(page);
			$("#Wait").fadeIn(500);
			$("#wait_image").removeClass("imagehidden");
			$("#wait_image").addClass("imagevisible");
			$.ajax({
				url : 'DownloadTechniciansList?action=paging&page='+page,
				type : 'POST',
				contents: "json",
				processData: false,  // tell jQuery not to process the data
				contentType: false,  // tell jQuery not to set contentType
				success : function(result) {
					var data = JSON.parse(result);
					if (data.res == "OK") {
						var rows = data.rows;						
						$('#pagbody').empty();
						for (var i=0; i<rows.length ; i++) {
							$('#pagbody').append('<tr>'+
													'<td>'+
													  	'<div class="container_img grey right"><img src="images/TrashGrey_.png" onclick="confirmdelete(\''+rows[i].numero_telefono+'\',\''+rows[i].nom_cog+'\',\''+rows[i].esigenza+'\')"></div>'+
													'</td>'+
													'<td><label>'+rows[i].numero_telefono+'</label></td>'+
													'<td><label>'+rows[i].nom_cog+'</label></td>'+
													'<td><label>'+rows[i].esigenza+'</label></td>'+
													'<td><label>'+rows[i].ts+'</label></td>'+
												'</tr>');
						}
					} else {
						$('#ErrorCode').html(data.errcode);
						$('#ErrorMessage').html(data.err);
						$('#Error').show();
					}
					$("#Wait").fadeOut(500);
					$("#wait_image").removeClass("imagevisible");
					$("#wait_image").addClass("imagehidden");
					return;
				},
				error: function(error) {
					$("#Wait").fadeOut(500);
					$("#wait_image").removeClass("imagevisible");
					$("#wait_image").addClass("imagehidden");
					$('#ErrorCode').html("ServLet Exception");
					$('#ErrorMessage').html(error);
					$('#Error').show();
					return;
				}
			});
		},
		onFormat: function (type) { // Gets called for each character of "format" and returns a HTML representation
			switch (type) {
			case 'block': // n and c
				if (numRows>0) {
					if (this.page === this.value) {
						return '<a href="#"><button type="button" class="button thin_margin blue">' + this.value + '</button></a>';
					} else {
						return '<a href="#"><button type="button" class="button thin_margin blue">' + this.value + '</button></a>';
					}
				}
				return '';
			case 'next': // >
				return '<a href="#"><button type="button" class="button thin_margin blue">&gt;</button></a>';
			case 'prev': // <
				return '<a href="#"><button type="button" class="button thin_margin blue">&lt;</button></a>';
			case 'first': // [
				return '<a href="#"><button type="button" class="button thin_margin blue">first</button></a>';
			case 'last': // ]
				return '<a href="#"><button type="button" class="button thin_margin blue">last</button></a>';
			}
		}
	});
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>