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
<title>TcpGlobal</title>
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
				String d_codcli = request.getParameter("delete_codcli");
				log.info(session.getId() + " - dashboard.Tcp_Delete('"+environment+"','"+d_codcli+"')");
				cstmt = conn.prepareCall("{ call dashboard.Tcp_Delete(?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, d_codcli);
				cstmt.execute();
				log.debug(session.getId() + " - executeCall complete");
				break;
			case "add":
				String a_codcli = request.getParameter("add_codcli");
				String a_descrizione = request.getParameter("add_descr");
				String a_ntel1 = request.getParameter("add_ntel1");
				String a_ntel2 = request.getParameter("add_ntel2");
				String a_ntel3 = request.getParameter("add_ntel3");
				String a_ntel4 = request.getParameter("add_ntel4");
				String a_ntel5 = request.getParameter("add_ntel5");
		 		log.info(session.getId() + " - dashboard.Tcp_Add('"+environment+"'"+
													 				",'"+a_codcli+"'"+							// COD_CLI
													 				",'"+a_ntel1+"'"+							// NUMERO_TELEFONO_1
													 				((StringUtils.isNoneBlank(a_ntel2))?",'"+a_ntel2+"'":"")+	// NUMERO_TELEFONO_2
													 				((StringUtils.isNoneBlank(a_ntel3))?",'"+a_ntel3+"'":"")+	// NUMERO_TELEFONO_3
													 				((StringUtils.isNoneBlank(a_ntel4))?",'"+a_ntel4+"'":"")+	// NUMERO_TELEFONO_4
													 				((StringUtils.isNoneBlank(a_ntel5))?",'"+a_ntel5+"'":"")+	// NUMERO_TELEFONO_5
													 				",'"+a_descrizione+"'"+							// DESCRIZIONE
													 				")");
				cstmt = conn.prepareCall("{ call dashboard.Tcp_Add(?,?,?,?,?,?,?,?)} ");
				cstmt.setString(1,environment);
				cstmt.setString(2, a_codcli);
				cstmt.setString(3, a_ntel1);
				if (StringUtils.isNoneBlank(a_ntel2)) { cstmt.setString(4, a_ntel2); } else { cstmt.setNull(4, Types.VARCHAR); }
				if (StringUtils.isNoneBlank(a_ntel3)) { cstmt.setString(5, a_ntel3); } else { cstmt.setNull(5, Types.VARCHAR); }
				if (StringUtils.isNoneBlank(a_ntel4)) { cstmt.setString(6, a_ntel4); } else { cstmt.setNull(6, Types.VARCHAR); }
				if (StringUtils.isNoneBlank(a_ntel5)) { cstmt.setString(7, a_ntel5); } else { cstmt.setNull(7, Types.VARCHAR); }
				cstmt.setString(8, a_descrizione);
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
			<form id="form_without_control" action="TcpGlobal.jsp" method="post">
				<input type="hidden" name="action" value="day_of_year">
				<input type="hidden" name="page" id="page" value="<%=numPage%>">
				<table id="stickytable" class="roundedCorners small">
				<thead>
					<tr class="listGroupItem active green">
						<td colspan='9'>
							<table>
								<tr>
									<td style="width: 45%">
										<div class="title">Clienti Tariffa TCP</div>
									</td>
									<td>
										<div class="container_img title right green " onclick="loadfile();">
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
		log.info(session.getId() + " - dashboard.Tcp_GetNumRows('"+environment+"')");
		cstmt = conn.prepareCall("{ call dashboard.Tcp_GetNumRows(?)} ");
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
		<form id="form_delete" action="TcpGlobal.jsp" method="post">
			<input type="hidden" id="dAction" name="action" Value="delete">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td class="bottom"><label for="delete_codcli">Codice Cliente</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_codcli" name="delete_codcli" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="delete_descr">Descrizione</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_descr" name="delete_descr" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr>
					<td><label for="delete_ntel1">Numero Telefono 1</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_ntel1" name="delete_ntel1" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr>
					<td><label for="delete_ntel2">Numero Telefono 2</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_ntel2" name="delete_ntel2" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr>
					<td><label for="delete_ntel3">Numero Telefono 3</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_ntel3" name="delete_ntel3" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr>
					<td><label for="delete_ntel4">Numero Telefono 4</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_ntel4" name="delete_ntel4" readonly="readonly" style="border: 0px"></td>
				</tr>
				<tr>
					<td><label for="delete_ntel5">Numero Telefono 5</label></td>
					<td>&nbsp;</td>
					<td><input class="bold" type="text" id="delete_ntel5" name="delete_ntel5" readonly="readonly" style="border: 0px"></td>
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
					<td>	<h2>Aggiungi Cliente TCP</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#add').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_add" action="TcpGlobal.jsp" method="post">
			<input type="hidden" id="aAction" name="action" Value="add">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><input type="text" id="add_codcli" name="add_codcli" class="formInput bold_imp" placeholder=" Codice Cliente" required="required"></td>	
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3"><input type="text" id="add_descr" name="add_descr" class="formInput bold_imp large" placeholder=" Descrizione" required="required"></td>	
				</tr>
				<tr>
					<td colspan="3"><input type="text" id="add_ntel1" name="add_ntel1" class="formInput bold_imp" placeholder=" Numero Telefono 1" required="required"></td>	
				</tr>
				<tr>
					<td colspan="3"><input type="text" id="add_ntel2" name="add_ntel2" class="formInput bold_imp" placeholder=" Numero Telefono 2"></td>	
				</tr>
				<tr>
					<td colspan="3"><input type="text" id="add_ntel3" name="add_ntel3" class="formInput bold_imp" placeholder=" Numero Telefono 3"></td>	
				</tr>
				<tr>
					<td colspan="3"><input type="text" id="add_ntel4" name="add_ntel4" class="formInput bold_imp" placeholder=" Numero Telefono 4"></td>	
				</tr>
				<tr>
					<td colspan="3"><input type="text" id="add_ntel5" name="add_ntel5" class="formInput bold_imp" placeholder=" Numero Telefono 5"></td>	
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
					<td>	<h2>Carica Clienti TCP</h2></td>
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
					<td><label>Cancella Clienti TCP presenti</label></td>
				</tr>
				<tr>
					<td>
						<label class='switch'> <input type='checkbox' id='mWithHeader' name='withheader'><span class='slider round green'></span></label>
					</td>
					<td><label>File con Intestazione</label></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="button" id="Carica" class="button green" onclick="caricaTcp();">Carica</button>
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
	<form id="form_reload" action="TcpGlobal.jsp" method="post">
	</form>
</div>

<script>
	$(function() {
		try {
			parent.ChangeActiveMenu("#TcpGlobal");			
		} catch (e) {
		}
 		$("#stickytable").stickyTableHeaders();
	})

	confirmdelete =  function(codcli,descr,ntel1,ntel2,ntel3,ntel4,ntel5) {
		$("#delete_codcli").val(codcli);
		$("#delete_descr").val(descr);
		$("#delete_ntel1").val(ntel1);
		$("#delete_ntel2").val(ntel2);
		$("#delete_ntel3").val(ntel3);
		$("#delete_ntel4").val(ntel4);
		$("#delete_ntel5").val(ntel5);
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
	
	caricaTcp = function() {
		$("#Wait").fadeIn(500);
		$("#wait_image").removeClass("imagehidden");
		$("#wait_image").addClass("imagevisible");
		var formData = new FormData(document.getElementById("form_load_file"));
		$.ajax({
			url : 'UploadTcp',
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
				url : 'DownloadTcp?action=paging&page='+page,
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
													  	'<div class="container_img grey right"><img src="images/TrashGrey_.png" onclick="confirmdelete(\''+rows[i].cod_cli+'\',\''+rows[i].descrizione+'\',\''+rows[i].numero_telefono_1+'\',\''+rows[i].numero_telefono_2+'\',\''+rows[i].numero_telefono_3+'\',\''+rows[i].numero_telefono_4+'\',\''+rows[i].numero_telefono_5+'\')"></div>'+
													'</td>'+
													'<td><label>'+rows[i].cod_cli+'</label></td>'+
													'<td><label>'+rows[i].descrizione+'</label></td>'+
													'<td><label>'+rows[i].numero_telefono_1+'</label></td>'+
													'<td><label>'+rows[i].numero_telefono_2+'</label></td>'+
													'<td><label>'+rows[i].numero_telefono_3+'</label></td>'+
													'<td><label>'+rows[i].numero_telefono_4+'</label></td>'+
													'<td><label>'+rows[i].numero_telefono_5+'</label></td>'+
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