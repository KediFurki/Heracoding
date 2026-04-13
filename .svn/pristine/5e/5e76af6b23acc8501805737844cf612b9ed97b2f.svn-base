<!DOCTYPE html>
<%@page import="org.apache.commons.lang3.RandomStringUtils"%>
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
<title>AmbienteSpecialGlobal</title>
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
	//		NONE
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
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
		switch (action) {
			case "delete":
				String d_ntel = request.getParameter("delete_ntel");
				log.info(session.getId() + " - dashboard.AmbienteSpecial_Delete('"+environment+"','"+d_ntel+"')");
				cstmt = conn.prepareCall("{ call dashboard.AmbienteSpecial_Delete(?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, d_ntel);
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add":
				String a_ntel = request.getParameter("add_ntel");
				String a_descrizione = request.getParameter("add_descr");
				log.info(session.getId() + " - dashboard.AmbienteSpecial_Add('"+environment+"','" + a_ntel + "','"+a_descrizione+"')");
				cstmt = conn.prepareCall("{ call dashboard.AmbienteSpecial_Add(?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, a_ntel);
				cstmt.setString(3, a_descrizione);
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
			<form id="form_without_control" action="AmbienteSpecialGlobal.jsp" method="post">
				<input type="hidden" name="action" value="day_of_year">
				<input type="hidden" name="page" id="page" value="<%=numPage%>">
				<table id="stickytable" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active green">
						<td colspan='7'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Ambiente Special</div>
									</td>
									<td>
										<div class="container_img title right green " onclick="loadfile();">
											<img alt="" class="" src="images/MenuPlusWhite_.png">
										</div>
									</td>
									<td>
										<div class="container_img title right green " onclick="downloadfile();">
											<img alt="" class="" src="images/DownloadWhite_.png">
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
		log.info(session.getId() + " - dashboard.AmbienteSpecial_GetNumRows('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.AmbienteSpecial_GetNumRows(?)} ");
		cstmt.setString(1,environment);
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
						<td colspan="7">
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
		<div class="modal-header green">						
			<table style="width: 100%">
				<tr>
					<td>	<h2>Elimina</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#delete').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_delete" action="AmbienteSpecialGlobal.jsp" method="post">
			<input type="hidden" id="dAction" name="action" Value="delete">
			<input type="hidden" id="dPage" name="page" value="">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td class="bottom"><label for="delete_ntel">Numero Telefono</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_ntel" name="delete_ntel" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="delete_descr">Descrizione</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_descr" name="delete_descr" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><button type="submit" id="CloseModal" class="button green">Confermi Eliminazione</button></td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="add" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Aggiungi numero di telefono</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#add').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add" action="AmbienteSpecialGlobal.jsp" method="post">
			<input type="hidden" id="aAction" name="action" Value="add">
			<input type="hidden" id="aPage" name="page" value="">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><input type="text" id="add_ntel" name="add_ntel" class="formInput bold_imp" placeholder=" Numero di Telefono" required="required"></td>	
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><input type="text" id="add_descr" name="add_descr" class="formInput bold_imp large" placeholder=" Descrizione" required="required"></td>	
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><button type="submit" id="AddCloseModal" class="button green">Conferma</button></td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="LoadFile" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Carica Ambiente Special</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#LoadFile').hide();">
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
						<button type="button" id="choosefile" class="button green" onclick="chooseFile();">Sfoglia&hellip;</button>
						<label for="uploadfile"><span id="uploadfilename" class="bold" style="display: inline-block;"></span></label>
				 	</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label class='switch'> <input type='checkbox' id='mDeleteAll' name='deleteall'><span class='slider round green'></span></label>
					</td>
					<td><label>Cancella numeri presenti</label></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="button" id="Carica" class="button green" onclick="caricaAmbienteSpecial();">Carica</button>
					</td>						
				</tr>
			</table>
		</form>
	</div>
</div>

<%
	String RndToken = RandomStringUtils.random(10, true, true);
%>

<div id="DownloadFile" class="modal">
	<form id="form_download_file" name="form_download_file" action="DownloadAmbienteSpecial" method="post">
		<input type="text" id="downAction" name="action" Value="download">
		<input type="text" id="downToken" name="downloadToken" value="<%=RndToken%>">
	</form>
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
		<div class="modal-header green">
			<table style="width: 100%">
				<tbody>
					<tr>
						<td><h2>Risultato</h2></td>
						<td>
							<div class="container_img title right green " onclick="$('#form_reload')[0].submit();">
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
						<button type="button" id="infoButton" class="button green" onclick="$('#form_reload')[0].submit();">Ok</button>
					</td>
				</tr>
			</tbody>
		</table>
	</div>
</div>

<div id="Reload" class="modal">
	<form id="form_reload" action="AmbienteSpecialGlobal.jsp" method="post">
	</form>
</div>

<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#AmbienteSpecialGlobal");			
		} catch (e) {
		}
 		$("#stickytable").stickyTableHeaders();
	})

	confirmdelete =  function(ntel,descr) {
		$("#dPage").val($("#page").val());
		$("#delete_ntel").val(ntel);
		$("#delete_descr").val(descr);
		$("#delete").show();
	}
	
	confirmadd =  function() {
		$("#aPage").val($("#page").val());
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
	
	caricaAmbienteSpecial = function() {
		$("#Wait").fadeIn(500);
		$("#wait_image").removeClass("imagehidden");
		$("#wait_image").addClass("imagevisible");
		var formData = new FormData(document.getElementById("form_load_file"));
		$.ajax({
			url : 'UploadAmbienteSpecial',
			type : 'POST',
			data : formData,
			contents: "json",
			processData: false,  // tell jQuery not to process the data
			contentType: false,  // tell jQuery not to set contentType
			success : function(result) {
				var data = JSON.parse(result);
				if (data.res == "OK") {
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
	
	var downloadToken = '<%=RndToken%>';
		
	downloadfile = function() {
		$("#Wait").fadeIn(500);
		$("#wait_image").removeClass("imagehidden");
		$("#wait_image").addClass("imagevisible");
		$("#form_download_file").submit();
		var attempts = 120;
		var downloadTimer = window.setInterval(function() {
				try {
					var token;
					var parts = document.cookie.split("downloadToken=");
					if (parts.length == 2)
						token = parts.pop().split(";").shift();
					if ((token == downloadToken) || (attempts == 0)) {
						$("#Wait").fadeOut(500);
						$("#wait_image").removeClass("imagevisible");
						$("#wait_image").addClass("imagehidden");
						window.clearInterval(downloadTimer);
						downloadToken = Math.random().toString(20).substr(2, 10);
						$("#downToken").val(downloadToken);
					}
					attempts--;
				} catch (e) {
					console.log("exception ", e);
				}
			}, 1000);
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
				url : 'DownloadAmbienteSpecial?action=paging&page='+page,
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
													'<td><label>'+rows[i].numero_telefono+'</label></td>'+
													'<td><label>'+rows[i].descrizione+'</label></td>'+
													'<td><label>'+rows[i].ts+'</label></td>'+
													'<td>'+
														'<div class="container_img grey right"><img src="images/TrashGrey_.png" onclick="confirmdelete(\''+rows[i].numero_telefono+'\',\''+rows[i].descrizione+'\')"></div>'+
													'</td>'+
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
						return '<a href="#"><button type="button" class="button thin_margin green">' + this.value + '</button></a>';
					}
				}
				return '';
			case 'next': // >
				return '<a href="#"><button type="button" class="button thin_margin green">&gt;</button></a>';
			case 'prev': // <
				return '<a href="#"><button type="button" class="button thin_margin green">&lt;</button></a>';
			case 'first': // [
				return '<a href="#"><button type="button" class="button thin_margin green">first</button></a>';
			case 'last': // ]
				return '<a href="#"><button type="button" class="button thin_margin green">last</button></a>';
			}
		}
	});
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>