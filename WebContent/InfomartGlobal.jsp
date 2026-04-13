<!DOCTYPE html>
<%@page import="java.sql.Timestamp"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.util.Date"%>
<%@page import="org.apache.commons.text.StringEscapeUtils"%>
<%@page import="org.json.JSONArray"%>
<%@page import="org.json.JSONObject"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="comapp.Utility"%>
<%@page import="comapp.ConfigServlet"%>
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
<title>InfomartGlobal</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/getBrowser.js"></script>
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
 	Connection connInf = null;
	CallableStatement cstmtInf = null;
	ResultSet rsInf = null;
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");
	SimpleDateFormat sdf_full_day_of_year_YM = new SimpleDateFormat("yyyy-MM");
	//==	ACTION PARAMETERS	====
	String action = request.getParameter("action");
	if (StringUtils.isBlank(action)) {
		action = "";
	}
	String environment = (String)session.getAttribute("Environment");
	//==============================
	//==	FILTER PARAMETERS	====
	String sFilterDay = request.getParameter("change_filter_info");
	if (StringUtils.isBlank(sFilterDay))
		sFilterDay = (String) session.getAttribute("change_filter_info");
	if (StringUtils.isBlank(sFilterDay)) {
		Date filterDay = new Date();
		filterDay.setDate(1);
		sFilterDay = sdf_full_day_of_year.format(filterDay);
	} else {
		Date dayConv = sdf_full_day_of_year_YM.parse(sFilterDay);
		dayConv.setDate(1);
		sFilterDay = sdf_full_day_of_year.format(dayConv);
	}
	session.setAttribute("change_filter_info", sFilterDay);
	Date filterToday = new Date();
	filterToday.setDate(1);
	filterToday = sdf_full_day_of_year.parse(sdf_full_day_of_year.format(filterToday));
	//==============================
	//==	ACTION START	====
	log.info(session.getId() + " - action:" + action + " filter_day:"+sFilterDay);
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"INF");
		log.info(session.getId() + " - connection INF wait...");
		connInf = ds.getConnection();
 		if (StringUtils.isNoneBlank(action)) log.info(session.getId() + " -> START ACTION: "+action);
	} catch (Exception e) {
		log.error(session.getId() + " - action error: " + e.getMessage(), e);
	} finally {
		try { cstmtInf.close(); } catch (Exception e) {}
	}
%>
<script type="text/javascript">
	var listMsg = new Array();
</script>
<table class="center"  >
	<tr>
		<td style="width: 10%"></td>
		<td colspan="2" >
			<table id="stickytable" class="roundedCorners ">
				<thead>
					<tr class="listGroupItem active green">
						<th colspan="5">
							<table style="width: 100%" >
								<tr>
									<td>
										<div class="title">Lista</div>
									</td>
									<td style="width: 10%">
										<div class="inline_cmd">
											<div class="container_img title right green " onclick="codifica();">
												<img alt="" class="" src="images/MenuWhite_.png">
											</div>
											<div>Codifica</div>
										</div>
									</td>
									<td style="width: 10%">
										<div class="inline_cmd">
											<div class="inline_cmd container_img title right green " onclick="mappatura();">
												<img alt="" class="" src="images/MenuWhite_.png">
											</div>
											<div>Mappatura</div>
										</div>
									</td>
									<td>
										<div class="container_img title right green " onclick="loadfile();">
											<img alt="" class="" src="images/MenuPlusWhite_.png">
										</div>
									</td>
									<td valign="top">
										<div class="container_img title green right" onclick="confirmchange_filter('<%=sdf_full_day_of_year_YM.format(sdf_full_day_of_year.parse(sFilterDay))%>')">
											<img alt="" class="" src="images/PointWhite_.png">
										</div>
									</td>
								</tr>
							</table>
						</th>
					</tr>
					<tr class='listGroupSubItem active green'>
					 	<th style='width:190px'>GQ</th>
					 	<th>Data</th>
					 	<th>AM</th>
					 	<th>PM</th>
					 	<th>Intera Giornata</th>
					</tr>
				</thead>
				<tbody id="tListGiornate">
<%
	try {
		
//	ivr_realtime
//	+	dashboard.Infomart_GetCodifiche()
//		dashboard.Infomart_ModCodifica(VARCHAR servizio_partech, VARCHAR servizio_genesys)
//		dashboard.Infomart_DelCodifica(VARCHAR servizio_partech, VARCHAR servizio_genesys)
//	Infomart
//	+	dashboard.Infomart_GetMonth(DATE date)
//	+	dashboard.Infomart_RemoveMonth(DATE month)
//	+	dashboard.Infomart_AddDay(VARCHAR gq, DATE data, VARCHAR am, VARCHAR pm, VARCHAR intera_giornata)
 		log.info(session.getId() + " - dashboard.Infomart_GetMonth('"+environment+"','" + sFilterDay + "')");
		cstmtInf = connInf.prepareCall("{ call dashboard.Infomart_GetMonth(?,?)} ");
		cstmtInf.setString(1,environment);
		cstmtInf.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
		rsInf = cstmtInf.executeQuery();					
		log.debug(session.getId() + " - executeCall complete");
 		while (rsInf.next()) {
			String GQ = rsInf.getString("GQ");
			String DATA = rsInf.getString("DATA");
			String AM = rsInf.getString("AM");
			String PM = rsInf.getString("PM");
			String INTERA_GIORNATA = rsInf.getString("INTERA_GIORNATA");
%>
					<tr class='listGroupItem'>
					 	<td style='width:190px' ><%=GQ%></td>
					 	<td                     ><%=DATA%></td>
					 	<td                     ><%=(AM.equals("1")?"SI":"NO")%></td>
					 	<td                     ><%=(PM.equals("1")?"SI":"NO")%></td>
					 	<td                     ><%=(INTERA_GIORNATA.equals("1")?"SI":"NO")%></td>
		 			</tr>
<%
 		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rsInf.close(); } catch (Exception e) {}
		try { cstmtInf.close(); } catch (Exception e) {}
		try { connInf.close(); } catch (Exception e) {}
		try {rs.close();} catch (Exception e) {}
		try {cstmt.close();} catch (Exception e) {}
		try {conn.close();} catch (Exception e) {}
	}
%>
				</tbody>
			</table>
		</td>
		<td style="width: 10%"></td>
	</tr>
</table>

<div id="LoadFile" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Carica Giornate da Escludere</h2></td>
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
				 	<td colspan="1">
						<input type="file" name="uploadfile" id="uploadfile" accept=".csv" class="compress"/>
						<button type="button" id="choosefile" class="button green" onclick="chooseFile();">Sfoglia&hellip;</button>
						<label for="uploadfile"><span id="uploadfilename" class="bold" style="display: inline-block;"></span></label>
				 	</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="1">
						<button type="button" id="Carica" class="button green" onclick="caricaGiornate();">Carica</button>
					</td>						
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="Codifica" class="modal">
	<div class="modal-content big thin">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Codifica Servizi</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#Codifica').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<table>
			<tbody id="tListCodifiche">
			</tbody>
		</table>
	</div>
</div>

<div id="Mappatura" class="modal">
	<div class="modal-content big thin">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Mappatura Servizi</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#Mappatura').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<table>
			<tbody id="tListMappature">
			</tbody>
		</table>
	</div>
</div>

<div id="change_filter" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td><h2>Cambia filtro di visualizzazione</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#change_filter').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_change_filter" action="InfomartGlobal.jsp" method="post">
			<input type="hidden" id="chAction" name="action" Value="change_filter">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="change_filter_info">Mese</label></td>
					<td><input type="month" id="change_filter_info" name="change_filter_info"></td>
					<td><button type="button" class="buttonsmall green" onclick="$('#change_filter_info').val('<%=sdf_full_day_of_year_YM.format(filterToday)%>')">Questo Mese</button></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="3">
						<button type="submit" id="CloseModal" class="button green">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
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
						<button type="button" id="errorButton" class="button pink" onclick="$('#Error').hide();">Ok</button>
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
							<div class="container_img title right green " onclick="$('#Info').hide();">
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
					<td><label class="bold">Giornata Intera</label></td>
					<td><label id="Infointeragiornata"></label></td>
				</tr>
				<tr>
					<td colspan="2"></td>
					<td><label class="bold">AM</label></td>
					<td><label id="Infoam"></label></td>
				</tr>
				<tr>
					<td colspan="2"></td>
					<td><label class="bold">PM</label></td>
					<td><label id="Infopm"></label></td>
				</tr>
				<tr>
					<td colspan="2"></td>
					<td><label class="bold">Scarti</label></td>
					<td><label id="Infoscarti"></label></td>
				</tr>
				<tr>
					<td><label class="bold">Info Scarti</label></td>
					<td colspan="3"><textarea id="Infoinfoscarti" name="Infoinfoscarti" maxlength="400" rows=10 class="messageBox"></textarea></td>
				</tr>
				<tr>
					<td colspan="4">
						<button type="button" id="infoButton" class="button green" onclick="$('#Info').hide();">Ok</button>
					</td>
				</tr>
			</tbody>
		</table>
	</div>
</div>

<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#InfomartGlobal");
		} catch (e) {}
 		$("#stickytable").stickyTableHeaders();
	})
	
	confirmchange_filter = function(id) {
		$("#change_filter").show();
		$("#change_filter_info").val(id);

	}

	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#change_filter').hide();
			$('#LoadFile').hide();
			$('#Codifica').hide();
			$('#Mappatura').hide();
			$('#Error').hide();
			$('#Info').hide();
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
	
	caricaGiornate = function() {
		var formData = new FormData(document.getElementById("form_load_file"));
		$.ajax({
			url : 'UploadInfomart',
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
					$('#Infoam').html(data.am);
					$('#Infopm').html(data.pm);
					$('#Infointeragiornata').html(data.interagiornata);
					$('#Infoscarti').html(data.scarti);
					$('#Infoinfoscarti').val(data.infoscarti);
					$('#Info').show();
					loadGiornate();
					return;
				}
				$('#LoadFile').hide();
				$('#ErrorCode').html(data.errcode);
				$('#ErrorMessage').html(data.err);
				$('#Error').show();
				loadGiornate();
			},
			error: function(error) {
				$('#ErrorCode').html("ServLet Exception");
				$('#ErrorMessage').html(error);
				$('#Error').show();
				loadGiornate();
			}
		});
	}

	codifica = function() {
		loadCodifica();
	}

	loadCodifica = function() {
		var formData = new FormData();
 		formData.append('action', 'loadcodifica');
		$.ajax({
			url : 'UploadInfomart',
			type : 'POST',
			data : formData,
			contents: "json",
			processData: false,  // tell jQuery not to process the data
			contentType: false,  // tell jQuery not to set contentType
			success : function(result) {
				var data = JSON.parse(result);
				if (data.res == "OK"){
					var tabList = $("#tListCodifiche");
					tabList.empty();
					tabList.append('<tr><td><br></td></tr>');
					tabList.append('<tr><td><label class="bold">Par-Tec</label></td><td><label class="bold">Genesys</label></td><td colspan="2"></td></tr>');
				    tabList.append(	'<tr>'+
										'<td class="green"><input class="bold small" type="text" id="cPar0" value=""></td>'+
										'<td class="green"><input class="bold small" type="text" id="cGen0" value=""></td>'+
										'<td style="width: 60px"><div class="container_img green right" onclick="modCodifica(0)"><img src="images/PlusWhite_.png"></div></td>'+
										'<td style="width: 60px"></td>'+
									'</tr>');
					var arr = data.arr;
					for (var i=0; i<arr.length; i++) {
					    var obj = arr[i];
					    tabList.append(	'<tr>'+
											'<td><input class="bold small" type="text" id="cPar'+obj.id+'" value="'+obj.partech+'" onchange="evidence(this,\''+obj.partech+'\');"></td>'+
											'<td><input class="bold small" type="text" id="cGen'+obj.id+'" value="'+obj.genesys+'" onchange="evidence(this,\''+obj.genesys+'\');"></td>'+
											'<td style="width: 60px"><div class="container_img green right" onclick="modCodifica('+obj.id+')"><img src="images/SaveWhite_.png"></div></td>'+
											'<td style="width: 60px"><div class="container_img green right" onclick="delCodifica('+obj.id+')"><img src="images/TrashWhite_.png"></div></td>'+
										'</tr>');
					}
					$("#Codifica").show();
					return;
				}
				$('#ErrorCode').html(data.errcode);
				$('#ErrorMessage').html(data.err);
				$('#Error').show();
			},
			error: function(error) {
				$('#ErrorCode').html("ServLet Exception");
				$('#ErrorMessage').html(error);
				$('#Error').show();
			}
		});
	}
	
	evidence = function(th,pr) {
		if ($(th).val() == pr) $(th).css("background-color", "#ffffff");
		else				   $(th).css("background-color", "#ffd5d5");
	}
	
	modCodifica = function(ind) {
		var formData = new FormData();
 		formData.append('action', 'modcodifica');
 		formData.append('partech', $("#cPar"+ind).val());
 		formData.append('genesys', $("#cGen"+ind).val());
		$.ajax({
			url : 'UploadInfomart',
			type : 'POST',
			data : formData,
			contents: "json",
			processData: false,  // tell jQuery not to process the data
			contentType: false,  // tell jQuery not to set contentType
			success : function(result) {
				var data = JSON.parse(result);
				if (data.res == "OK"){
					loadCodifica();
					return;
				}
				$('#ErrorCode').html(data.errcode);
				$('#ErrorMessage').html(data.err);
				$('#Error').show();
			},
			error: function(error) {
				$('#ErrorCode').html("ServLet Exception");
				$('#ErrorMessage').html(error);
				$('#Error').show();
			}
		});
	}

	delCodifica = function(ind) {
		var formData = new FormData();
 		formData.append('action', 'delcodifica');
 		formData.append('partech', $("#cPar"+ind).val());
 		formData.append('genesys', $("#cGen"+ind).val());
		$.ajax({
			url : 'UploadInfomart',
			type : 'POST',
			data : formData,
			contents: "json",
			processData: false,  // tell jQuery not to process the data
			contentType: false,  // tell jQuery not to set contentType
			success : function(result) {
				var data = JSON.parse(result);
				if (data.res == "OK"){
					loadCodifica();
					return;
				}
				$('#ErrorCode').html(data.errcode);
				$('#ErrorMessage').html(data.err);
				$('#Error').show();
			},
			error: function(error) {
				$('#ErrorCode').html("ServLet Exception");
				$('#ErrorMessage').html(error);
				$('#Error').show();
			}
		});
	}

	mappatura = function() {
		loadMappatura();
	}

	loadMappatura = function() {
		var formData = new FormData();
 		formData.append('action', 'loadmappatura');
		$.ajax({
			url : 'UploadInfomart',
			type : 'POST',
			data : formData,
			contents: "json",
			processData: false,  // tell jQuery not to process the data
			contentType: false,  // tell jQuery not to set contentType
			success : function(result) {
				var data = JSON.parse(result);
				if (data.res == "OK"){
					var tabList = $("#tListMappature");
					tabList.empty();
					tabList.append('<tr><td><br></td></tr>');
					tabList.append('<tr><td><label class="bold">In</label></td><td><label class="bold">Out</label></td><td colspan="2"></td></tr>');
				    tabList.append(	'<tr>'+
										'<td class="green"><input class="bold small" type="text" id="cIn0" value=""></td>'+
										'<td class="green"><input class="bold small" type="text" id="cOut0" value=""></td>'+
										'<td style="width: 60px"><div class="container_img green right" onclick="modMappatura(0)"><img src="images/PlusWhite_.png"></div></td>'+
										'<td style="width: 60px"></td>'+
									'</tr>');
					var arr = data.arr;
					for (var i=0; i<arr.length; i++) {
					    var obj = arr[i];
					    tabList.append(	'<tr>'+
											'<td><input class="bold small" type="text" id="cIn'+obj.id+'" value="'+obj.in+'" onchange="evidence(this,\''+obj.in+'\');"></td>'+
											'<td><input class="bold small" type="text" id="cOut'+obj.id+'" value="'+obj.out+'" onchange="evidence(this,\''+obj.out+'\');"></td>'+
											'<td style="width: 60px"><div class="container_img green right" onclick="modMappatura('+obj.id+')"><img src="images/SaveWhite_.png"></div></td>'+
											'<td style="width: 60px"><div class="container_img green right" onclick="delMappatura('+obj.id+')"><img src="images/TrashWhite_.png"></div></td>'+
										'</tr>');
					}
					$("#Mappatura").show();
					return;
				}
				$('#ErrorCode').html(data.errcode);
				$('#ErrorMessage').html(data.err);
				$('#Error').show();
			},
			error: function(error) {
				$('#ErrorCode').html("ServLet Exception");
				$('#ErrorMessage').html(error);
				$('#Error').show();
			}
		});
	}
	
	modMappatura = function(ind) {
		var formData = new FormData();
 		formData.append('action', 'modmappatura');
 		formData.append('in', $("#cIn"+ind).val());
 		formData.append('out', $("#cOut"+ind).val());
		$.ajax({
			url : 'UploadInfomart',
			type : 'POST',
			data : formData,
			contents: "json",
			processData: false,  // tell jQuery not to process the data
			contentType: false,  // tell jQuery not to set contentType
			success : function(result) {
				var data = JSON.parse(result);
				if (data.res == "OK"){
					loadMappatura();
					return;
				}
				$('#ErrorCode').html(data.errcode);
				$('#ErrorMessage').html(data.err);
				$('#Error').show();
			},
			error: function(error) {
				$('#ErrorCode').html("ServLet Exception");
				$('#ErrorMessage').html(error);
				$('#Error').show();
			}
		});
	}

	delMappatura = function(ind) {
		var formData = new FormData();
 		formData.append('action', 'delmappatura');
 		formData.append('in', $("#cIn"+ind).val());
 		formData.append('out', $("#cOut"+ind).val());
		$.ajax({
			url : 'UploadInfomart',
			type : 'POST',
			data : formData,
			contents: "json",
			processData: false,  // tell jQuery not to process the data
			contentType: false,  // tell jQuery not to set contentType
			success : function(result) {
				var data = JSON.parse(result);
				if (data.res == "OK"){
					loadMappatura();
					return;
				}
				$('#ErrorCode').html(data.errcode);
				$('#ErrorMessage').html(data.err);
				$('#Error').show();
			},
			error: function(error) {
				$('#ErrorCode').html("ServLet Exception");
				$('#ErrorMessage').html(error);
				$('#Error').show();
			}
		});
	}

	loadGiornate = function(ind) {
		var formData = new FormData();
 		formData.append('action', 'loadgiornate');
 		formData.append('change_filter_info', '<%=sFilterDay%>');
		$.ajax({
			url : 'UploadInfomart',
			type : 'POST',
			data : formData,
			contents: "json",
			processData: false,  // tell jQuery not to process the data
			contentType: false,  // tell jQuery not to set contentType
			success : function(result) {
				var data = JSON.parse(result);
				if (data.res == "OK"){
					var tabList = $("#tListGiornate");
					tabList.empty();
					var arr = data.arr;
					for (var i=0; i<arr.length; i++) {
					    var obj = arr[i];
					    tabList.append(	'<tr class="listGroupItem">'+
											'<td style="width:190px">'+obj.gq+'</td>'+
											'<td>'+obj.data+'</td>'+
											'<td>'+obj.am+'</td>'+
											'<td>'+obj.pm+'</td>'+
											'<td>'+obj.intera_giornata+'</td>'+
										'</tr>');
					}
					return;
				}
				$('#ErrorCode').html(data.errcode);
				$('#ErrorMessage').html(data.err);
				$('#Error').show();
			},
			error: function(error) {
				$('#ErrorCode').html("ServLet Exception");
				$('#ErrorMessage').html(error);
				$('#Error').show();
			}
		});
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>