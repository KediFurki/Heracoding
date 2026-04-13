<!DOCTYPE html>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="javax.naming.Context"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Date"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en" style="height: 100%;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Graphs</title>
<link rel="stylesheet" type="text/css" href="../css/comapp.css">
<!-- <link rel="stylesheet" href="css/graph.css"> -->
<script src="../js/jquery.min.js"></script>
<script src="../js/Chart.js"></script>
</head>
<body id="bodygraph" style="height: 100%;">
<div class="bottom-right">
	<table><tr>
		<td class=""><img style="height: 20px" src='../<%=ConfigServlet.getProperties().getProperty("Logo1")%>'></td>
		<td class="" style="float: right; "><img style="height: 20px" src='../<%=ConfigServlet.getProperties().getProperty("Logo2")%>'></td>
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
	SimpleDateFormat sdf_day_of_year = new SimpleDateFormat("MM-dd");
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================
	//==	FILTER PARAMETERS	====
	String sFilterDay = request.getParameter("change_filter_day");
	if (StringUtils.isBlank(sFilterDay))
		sFilterDay = (String) session.getAttribute("change_filterRT_day");
	if (StringUtils.isBlank(sFilterDay)){		
		Date filterDay = new Date();
		sFilterDay = sdf_full_day_of_year.format(filterDay);
	}
	session.setAttribute("change_filterRT_day",sFilterDay);
	Date filterToday = new Date();
	//==============================
	
	int i = 0;
	String MainMenu = null;
	int errorCode = 0;

	int numConfOS = 0;
	String confIdObject[] = new String[5];
	String confSigla[] = new String[5];
	String confNome[] = new String[5];
	
	try {
		if (StringUtils.isBlank(CodIvr)) {
		} else {
			Context ctx = new InitialContext();
			DataSource ds = null;
			String environment = (String)session.getAttribute("Environment");
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
			log.info(session.getId() + " - connection CCC wait...");
			conn = ds.getConnection();
			log.info(session.getId() + " - dashboard.Graph_ReadConfig('"+environment+"','"+CodIvr+"')");
			cstmt = conn.prepareCall("{ call dashboard.Graph_ReadConfig(?,?)}");
			cstmt.setString(1,environment);
			cstmt.setString(2,CodIvr);
			rs = cstmt.executeQuery();
			while (rs.next()) {
				confIdObject[numConfOS] = rs.getString("IDOBJECT");
				confSigla[numConfOS] = rs.getString("SIGLA");
				confNome[numConfOS] = rs.getString("NOME");
				numConfOS++;
			}
			
%>
<div id="ContainerFlex" class="ContainerFlex">
	<div class="FlexTitle">
		<table id="title_he" class="roundedCorners smallnb no_top" style="border-spacing: 0; padding: 0px;">
				<tr class="listGroupItem active blue">
					<td colspan='20'>
						<table>
							<tr>
								<td style="width: 48%">
									<div class="title">Grafici Previsionali - Data <%=sFilterDay%></div>
								</td>
								<td style="width: 4%" valign="top" class="">
									<div class="container_img blue title  " onclick="loadfile();">
										<img alt="" class="" src="../images/MenuPlusWhite_.png">
									</div>
								</td>
								<td valign="top" class="">
									<div class="container_img blue title right" onclick="confirmchange_filter('<%=sFilterDay%>')">
										<img alt="" class="" src="../images/PointWhite_.png">
									</div>
								</td>
							</tr>
						</table>
					</td>
				</tr>
			</table>
<!-- 	</div> -->
	<div id="title_tt" class="FlexTitle">
		<div class="roundedCorners lightblue" onclick="switchOnOff('#divcanvas_tt',0)">ANDAMENTO TOTALE</div>
	</div>
	<div class="FlexGraph" id="divcanvas_tt">
		<div id="idcanvas_tt" style="position: relative; "><canvas id="canvas_tt"></canvas></div>
	</div>
<%
			for (int iLoop=0; iLoop<numConfOS; iLoop++) {
%>
	<div id="title_<%=confSigla[iLoop]%>" class="FlexTitle">
		<div class="roundedCorners lightblue" onclick="switchOnOff('#divcanvas_<%=confSigla[iLoop]%>',<%=(iLoop+1)%>)"><%=confNome[iLoop]%></div>
	</div>
	<div class="FlexGraph" id="divcanvas_<%=confSigla[iLoop]%>">
		<div id="idcanvas_<%=confSigla[iLoop]%>" style="position: relative; "><canvas id="canvas_<%=confSigla[iLoop]%>"></canvas></div>
	</div>
<%
			}
%>
<!-- 	<div id="title_ln" class="FlexTitle"> -->
<!-- 		<div class="roundedCorners lightblue" onclick="switchOnOff('#divcanvas_ln',1)">LINETECH</div> -->
<!-- 	</div> -->
<!-- 	<div class="FlexGraph" id="divcanvas_ln"> -->
<!-- 		<div id="idcanvas_ln" style="position: relative; "><canvas id="canvas_ln"></canvas></div> -->
<!-- 	</div> -->
<!-- 	<div id="title_kn" class="FlexTitle"> -->
<!-- 		<div class="roundedCorners lightblue" onclick="switchOnOff('#divcanvas_kn',2)">KOINE</div> -->
<!-- 	</div> -->
<!-- 	<div class="FlexGraph" id="divcanvas_kn"> -->
<!-- 		<div id="idcanvas_kn" style="position: relative; "><canvas id="canvas_kn"></canvas></div> -->
<!-- 	</div> -->
<!-- 	<div id="title_vk" class="FlexTitle"> -->
<!-- 		<div class="roundedCorners lightblue" onclick="switchOnOff('#divcanvas_vk',3)">KOINE VENEZIA</div> -->
<!-- 	</div> -->
<!-- 	<div class="FlexGraph" id="divcanvas_vk"> -->
<!-- 		<div id="idcanvas_vk" style="position: relative; "><canvas id="canvas_vk"></canvas></div> -->
<!-- 	</div> -->
</div>
	
<div id="change_filter" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td><h2>Cambia Data di Visualizzazione</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#change_filter').hide();">
							<img alt="" class="" src="../images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_change_filter" action="Graphs.jsp" method="post">
			<input type="hidden" id="chAction" name="action" Value="change_filter">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td><label for="change_filter_day">Giorno</label></td>
					<td><input type="date" id="change_filter_day" name="change_filter_day"></td>
					<td><button type="button" class="buttonsmall blue" onclick="$('#change_filter_day').val('<%=sdf_full_day_of_year.format(filterToday)%>')">Oggi</button></td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="submit" id="CloseModal" class="button blue">Conferma</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>

<div id="upload_forecast" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td><h2>Carica File Previsionali</h2></td>
					<td>
						<div class="container_img title right blue "  onclick="$('#upload_forecast').hide();">
							<img alt="" class="" src="../images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<table style="width: 100%">
			<tr><td><br></td></tr>
			<tr>
				<td>
					<label for="ForecastOutSourcer">OutSourcer</label>
				</td>
				<td>
					<select id="ForecastOutSourcer" name="ForecastOutSourcer" readonly=true>
						<option value=""></option>
<%
			for (int iLoop=0; iLoop<numConfOS; iLoop++) {
%>
						<option value="<%=confIdObject[iLoop]%>"><%=confNome[iLoop]%></option>
<%
			}
%>
<!-- 						<option value="LINETECH">LINETECH</option> -->
<!-- 						<option value="KOINE">KOINE</option> -->
<!-- 						<option value="KV">KOINE VENEZIA</option> -->
					</select>
				</td>
			</tr>
			<tr>
				<td>
					<label for="ForecastData">Giorno</label>
				</td>
				<td>
					<input type="date" id="ForecastData" name="ForecastData" value='<%=sdf_full_day_of_year.format(filterToday)%>'>
					<button type="button" class="buttonsmall blue" onclick="$('ForecastData').val('<%=sdf_full_day_of_year.format(filterToday)%>')">Oggi</button>
				</td>
			</tr>
		 	<tr>
<!-- 				<td class="middle"> -->
<!-- 					<label for="ForecastFile">Scelta File</label> -->
<!-- 				</td> -->
				<td colspan="2">
					<input type="file" name="ForecastFile" id="ForecastFile" accept=".xls,.xlsx" class="compress"/>
					<button type="button" id="choosefile" class="button blue" onclick="chooseFile();">Sfoglia&hellip;</button>
					<label for="ForecastFile"><span id="ForecastFilename" class="bold" style="display: inline-block;"></span></label>
				</td>
	 	 	</tr>
			<tr><td><br></td></tr>
			<tr>
				<td>
					<button type="button" id="CloseModal" class="button blue" onclick='UploadForecast()'>Conferma</button>
				</td>
			</tr>
		</table>
	</div>
</div>
<%
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) { }
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}	
%>

<div id="Info" class="modal">
	<div class="modal-content">
		<div class="modal-header blue">
			<table style="width: 100%">
				<tr>
					<td><h2>Info</h2></td>
					<td>
						<div class="container_img title right blue " onclick="$('#Info').hide();">
							<img alt="" class="" src="../images/Close_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<input type="hidden" id="asAction" name="action" Value="add_message">
		<table>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="1"><label>Info Message:</label></td>
				<td colspan="1"><label id='InfoMessage'></label></td>
			</tr>
			<tr><td><br></td></tr>
			<tr>
				<td colspan="2">
					<button type="button" id="AddCloseModal" class="button blue" onclick="$('#Info').hide();">Ok</button>
				</td>
			</tr>
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
							<img alt="" class="" src="../images/CloseWhite_.png">
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

<%	if (!StringUtils.isBlank(CodIvr)) {%>
<script>
	var HOURS =  [ '0','','','',  '1','','','',  '2','','','',  '3','','','',  '4','','','', '5','','','', 
				   '6','','','',  '7','','','',  '8','','','',  '9','','','', '10','','','', '11','','','', 
				  '12','','','', '13','','','', '14','','','', '15','','','', '16','','','', '17','','','', 
				  '18','','','', '19','','','', '20','','','', '21','','','', '22','','','', '23','','','','24'];
	var configtemplate = {
		type: 'line',
		data: {
			labels: HOURS,
			datasets: [{
				label: 'Previsioni',
				fill: false,
				backgroundColor: '#238acf',
				borderColor: '#238acf',
				data: []
			}, {
				label: 'Max Previsioni',
				fill: false,
				backgroundColor: '#f282c2', 
				borderColor: '#f282c2',
				data: []
			}, {
				label: 'Consuntivi',
				fill: false,
				backgroundColor: '#0ebe72', 
				borderColor: '#0ebe72',
				data: []
			}]
		},
		options: {
			responsive: true,
			maintainAspectRatio: false,
			title: {
				display: true,
				lineHeight: 0.1,
				text: ''
			},
			elements: {
				line: {
					borderWidth: 1,
				}
			},
			legend: {
				position: 'bottom'
			},
			tooltips: {
				mode: 'index',
				intersect: false,
			},
			hover: {
				mode: 'nearest',
				intersect: true
			},
			scales: {
				xAxes: [{
					display: true,
					scaleLabel: {
						display: false,
						labelString: ''
					}
				}],
				yAxes: [{
					display: true,
					scaleLabel: {
						display: true,
						labelString: 'Chiamate'
					},
					ticks: {
				    	suggestedMin: 0,
				    	suggestedMax: 10
				    }
				}]
			}
		}
	};

</script>
<%	}%>
<script type="text/javascript">
	$(function() {
		try {
			parent.ChangeActiveMenu("#MenuDetails");
			parent.ChangeActivedFooter("#Graphs","rt/Graphs.jsp");
<%	if (!StringUtils.isBlank(CodIvr)) { %>
			parent.EnabledFooterService();
			parent.ChangeActivedFooterService();
<%	} %>
		} catch (e) {}
		
		var ctx1 = document.getElementById('canvas_tt').getContext('2d');
		var configTOT = JSON.parse(JSON.stringify(configtemplate));
		window.lines_tt = new Chart(ctx1, configTOT);
		
<%
		for (int iLoop=0; iLoop<numConfOS; iLoop++) {
%>
		var ctx<%=(iLoop+1)%> = document.getElementById('canvas_<%=confSigla[iLoop]%>').getContext('2d');
		var config<%=confSigla[iLoop].toUpperCase()%> = JSON.parse(JSON.stringify(configtemplate));
		window.lines_<%=confSigla[iLoop]%> = new Chart(ctx<%=(iLoop+1)%>, config<%=confSigla[iLoop].toUpperCase()%>);

<%
		}
%>
// 		var ctx2 = document.getElementById('canvas_ln').getContext('2d');
// 		var configLN = JSON.parse(JSON.stringify(configtemplate));
// 		window.lines_ln = new Chart(ctx2, configLN);
		
// 		var ctx3 = document.getElementById('canvas_kn').getContext('2d');
// 		var configKN = JSON.parse(JSON.stringify(configtemplate));
// 		window.lines_kn = new Chart(ctx3, configKN);

// 		var ctx4 = document.getElementById('canvas_vk').getContext('2d');
// 		var configVK = JSON.parse(JSON.stringify(configtemplate));
// 		window.lines_vk = new Chart(ctx4, configVK);
<%
		for (int iLoop=0; iLoop<numConfOS; iLoop++) {
%>
<%
		}
%>
	
		resizegraph();
		
		try {
<%	if (!StringUtils.isBlank(CodIvr)) { %>
			function worker(){
				$.ajax({ 
			        type: 'GET', 
			        url: 'UpdateGraphs.jsp', 
			        data: { CodIvr: '<%=CodIvr%>', Data: '<%=sFilterDay%>' }, 
			        success: function (data) { 
			        	var json = jQuery.parseJSON(data);
						window.lines_tt.data.datasets[0].data = json[0].prev;
						window.lines_tt.data.datasets[1].data = json[0].maxp;
						window.lines_tt.data.datasets[2].data = json[0].curr;
						window.lines_tt.update();
<%
		for (int iLoop=0; iLoop<numConfOS; iLoop++) {
%>
						window.lines_<%=confSigla[iLoop]%>.data.datasets[0].data = json[<%=(iLoop+1)%>].prev;
						window.lines_<%=confSigla[iLoop]%>.data.datasets[1].data = json[<%=(iLoop+1)%>].maxp;
						window.lines_<%=confSigla[iLoop]%>.data.datasets[2].data = json[<%=(iLoop+1)%>].curr;
						window.lines_<%=confSigla[iLoop]%>.update();
<%
		}
%>
// 						window.lines_ln.data.datasets[0].data = json[1].prev;
// 						window.lines_ln.data.datasets[1].data = json[1].maxp;
// 						window.lines_ln.data.datasets[2].data = json[1].curr;
// 						window.lines_ln.update();
// 						window.lines_kn.data.datasets[0].data = json[2].prev;
// 						window.lines_kn.data.datasets[1].data = json[2].maxp;
// 						window.lines_kn.data.datasets[2].data = json[2].curr;
// 						window.lines_kn.update();
// 						window.lines_vk.data.datasets[0].data = json[3].prev;
// 						window.lines_vk.data.datasets[1].data = json[3].maxp;
// 						window.lines_vk.data.datasets[2].data = json[3].curr;
// 						window.lines_vk.update();
			        },
			        error: function (data) {
			        }
			    });
<%		if (sdf_full_day_of_year.format(filterToday).equalsIgnoreCase(sFilterDay)) { %>
	        	setTimeout(worker, 5000);
<%		} %>
			}
			worker();
<%	} %>
		} catch (e) {}
		$(window).resize(function() {
			resizegraph();
		});	
	})

 	var view = [1<% for (int iLoop=0; iLoop<numConfOS; iLoop++) { %><%=",1"%><% } %>];
	var view_totali = <%=(numConfOS+1)%>;
// 	var view = [1,1,1,1];
// 	var view_totali = 4;
	
	resizegraph = function() {
		var t_he_height = $(title_he).height();
		var t_tt_height = $(title_tt).height();
<%
		for (int iLoop=0; iLoop<numConfOS; iLoop++) {
%>
		var t_<%=confSigla[iLoop]%>_height = $(title_<%=confSigla[iLoop]%>).height();
<%
		}
%>
// 		var t_ln_height = $(title_ln).height();
// 		var t_kn_height = $(title_kn).height();
// 		var t_vk_height = $(title_vk).height();
		var t_height = t_he_height+t_tt_height<% for (int iLoop=0; iLoop<numConfOS; iLoop++) { %><%=("+t_"+confSigla[iLoop]+"_height")%><% } %>;
// 		var t_height = t_he_height+t_tt_height+t_ln_height+t_kn_height+t_vk_height;
// 		console.log("t_height:"+t_height+" t_he_height:"+t_he_height+" t_tt_height:"+t_tt_height+" t_ln_height:"+t_ln_height+" t_kn_height:"+t_kn_height+" t_vk_height:"+t_vk_height);
		if (view_totali>0) {
			$("#bodygraph").css("height","100%");
		} else {
			$("#bodygraph").css("height",t_height);
		}

		var cnt_f_height = $(ContainerFlex).height();
		var cnt_f_width = $(ContainerFlex).width()
			
// 		console.log("new size height:"+cnt_f_height+"  width:"+cnt_f_width);
		
		var cnv_height;
		if (view_totali>0) {
			cnv_height = Math.floor((cnt_f_height-t_height)/view_totali);
			$("#bodygraph").css("height","100%");
		} else {
			cnv_height = 1;
			$("#bodygraph").css("height",t_height);
		}

// 		console.log("cnv_height:"+cnv_height);
		
		$("#idcanvas_tt").css("height",(view[0]=0)?1:cnv_height);
		$("#idcanvas_tt").css("width",cnt_f_width);
<%
		for (int iLoop=0; iLoop<numConfOS; iLoop++) {
%>
		$("#idcanvas_<%=confSigla[iLoop]%>").css("height",(view[<%=(iLoop+1)%>]=0)?1:cnv_height);
		$("#idcanvas_<%=confSigla[iLoop]%>").css("width",cnt_f_width);
<%
		}
%>
// 		$("#idcanvas_ln").css("height",(view[1]=0)?1:cnv_height);
// 		$("#idcanvas_ln").css("width",cnt_f_width);
// 		$("#idcanvas_kn").css("height",(view[2]=0)?1:cnv_height);
// 		$("#idcanvas_kn").css("width",cnt_f_width);
// 		$("#idcanvas_vk").css("height",(view[3]=0)?1:cnv_height);
// 		$("#idcanvas_vk").css("width",cnt_f_width);
	}
	
	switchOnOff = function(cnv,id) {
		if ($(cnv).css('display')=='none'){
			$(cnv).css('display', 'block');
			$(cnv).css('visibility', 'visible');
			view[id] = 1;
			view_totali++;
		} else {
			$(cnv).css('display', 'none');
			$(cnv).css('visibility', 'hidden');
			view[id] = 0;
			view_totali--;
		}
// 		console.log("view_totali:"+view_totali);
		resizegraph();
	}

	confirmchange_filter = function(id) {
		$("#change_filter").show();
		$("#change_filter_day").val(id);
	}
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#change_filter').hide();
			$('#upload_forecast').hide();
			$('#Info').hide();
			$('#Error').hide();
		}
	}

	loadfile = function() {
		$("#ForecastFile").val('')
		$("#ForecastFilename").html('');
		$("#upload_forecast").show();
	}

	chooseFile = function() {
		$("#ForecastFile").click();
	}

	$("#ForecastFile").on('change', function(e) {
		var fileName = '';
		if( e.target.value )
			fileName = e.target.value.split( '\\' ).pop();
		if( fileName )
			$("#ForecastFilename").html( fileName );
		else
			$("#ForecastFilename").html( '' );
	});
	
	UploadForecast = function(){
	 	var codivr = '<%=CodIvr%>';
// 	 	console.log(codivr);
		if ($('#ForecastFile').val() == '') {
// 		 	console.log("IS NULL");
			$('#ErrorCode').html("1");
			$('#ErrorMessage').html("Selezionare il file .CSV da caricare");
			$('#Error').show();
		} else {
 			var formData = new FormData();
			formData.append('ForecastFile', $('#ForecastFile')[0].files[0]);
			formData.append('CodIvr', codivr);
			formData.append('ForecastData', $('#ForecastData').val());
			formData.append('ForecastOutSourcer', $('#ForecastOutSourcer').val());
			$.ajax({
				url : '../UploadForecast',
				type : 'POST',
				data : formData,
				processData: false,
				contentType: false,
				success : function(data) {
// 					console.log(data);
					var obj = JSON.parse(data);
					if (obj.res.toLowerCase() == "ok"){
						$('#InfoMessage').html(obj.msg);
						$('#Info').show();
					} else {
						$('#ErrorCode').html("2");
						$('#ErrorMessage').html(obj.msg);
						$('#Error').show();
					}
				}
			});
		}
	};
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>