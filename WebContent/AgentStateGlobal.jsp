<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.json.JSONObject"%>
<%@page import="comapp.Utility"%>
<%@page import="comapp.ConfigurationUtility"%>
<%@page import="org.json.JSONArray"%>
<%@page import="org.apache.log4j.Logger"%>
<html lang="en" style="overflow: inherit;">
<head >
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AgentStatus</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
<script src="js/jquery.sortElements.js"></script>
</head>
<body style="overflow-y: auto;">
<div class="bottom-right">
	<table>
		<tr>
			<td class=""><img style="height: 20px" src='<%=ConfigServlet.getProperties().getProperty("Logo1")%>'></td>
			<td class="" style="float: right; "><img style="height: 20px" src='<%=ConfigServlet.getProperties().getProperty("Logo2")%>'></td>
		</tr>
	</table>
</div>
<%
	Logger log = Logger.getLogger("comappTS." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");
	String PlaceGroup= (String)session.getAttribute("PlaceGroup");
	String environment = (String)session.getAttribute("Environment");
	String dbsystemproperties = (String)session.getAttribute("DBSystemProperties");
%>
<div id="_left" style="overflow: auto;">
	<table class="center" >
		<tr>
			<td style="width: 10%"></td>
			<td>
				<table id="stickytable_1" class="roundedCorners">
					<thead>
						<tr class="listGroupItem active green">
							<th>
								<table>
									<tr>
										<td>
											<div class="title">Stato Agenti</div>
										</td>
										<td valign="top">
											 
										</td>
									</tr>
								</table>
							</th>
						</tr>
					</thead>
					<tr>
						<td>
							<table id='stickytable_2' class="roundedCorners smallnb" style="width:100%;">
								<thead>
									<tr>
										<th class='lightgreen' id='agentid_header'>Agent ID</th>
										<th class='lightgreen' id='thisdn_header'>Postazione</th>
										<th class='lightgreen'>Stato</th>
										<th class='lightgreen'>Reason</th>
										<th class='lightgreen'>NotReady</th>
										<th class='lightgreen'>LogOut</th>
									</tr>
								</thead>
								<tbody id="table_AgentState">
									<img id="wait_image" class="imagevisible" style='position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);' src='images/Wait.gif'/>
								</tbody>
							 </table>
						</td>
					</tr>
				</table>
	 		</td>
			<td style="width: 10%"></td>
		</tr>
	</table>
</div>

<div id="SetReason" class="modal">
	<div class="modal-content">
		<div class="modal-header green">
			<table style="width: 100%">
				<tr>
					<td>	<h2>Reason</h2></td>
					<td>
						<div class="container_img title right green "  onclick="$('#SetReason').hide();">
							<img alt="" class="" src="images/CloseWhite_.png">
						</div>
					</td>
				</tr>
			</table>
		</div>
		<form id="form_setreason">
			<input type="hidden" id="mThisDN" name="mThisDN" Value="">
			<table>
				<tr><td><br></td></tr>
				<tr>
					<td>
						<label for="Reason">NotReady Reason</label>
					</td>
					<td>
						<select id="mReason" name="Reason" readonly=true>
							<option value=""></option>
<%
	try {
		JSONArray jaac = ConfigurationUtility.getCfgActionCode(Utility.getSystemProperties(dbsystemproperties), session.getId());
		log.info(session.getId() + " - actioncode Value:" + jaac);
		for (int i = 0; i < jaac.length(); i++) {
			JSONObject ac = jaac.getJSONObject(i);
%>
		<option value="<%=ac.getString("Code")%>"><%=ac.getString("Name")%></option>
<%
		}
	} catch (Exception e) {
		log.error(session.getId() + " -Error on getActionCode: ", e);
	}		
%>							
						</select>
					</td>
				</tr>
				<tr><td><br></td></tr>
				<tr>
					<td colspan="2">
						<button type="button" id="" class="button green" onclick="sendNotReadyReason();">Invia</button>
					</td>
				</tr>
			</table>
		</form>
	</div>
</div>



<script type="text/javascript">
	var placeList = new Map();
	var webSocket;
	
	function openSocket() {
		if (webSocket !== undefined	&& webSocket.readyState !== WebSocket.CLOSED) {
			console.log("WebSocket already created");
			return;
		}
		console.log("WebSocket opening");
	
		webSocket = new WebSocket("ws://" + location.host+ "/DashBoard/WebSocketAgentState");
		console.log("WebSocket completed");
		
		webSocket.onopen = function(event) {
			console.log("WebSocket event onopen");
			var js = {};
			js['command'] = 'register';
			js['Group'] = '<%=PlaceGroup%>';
			console.log("WebSocket sent "+JSON.stringify(js));
			webSocket.send(JSON.stringify(js));
		};
	
		webSocket.onmessage = function(event) {
			console.log("onmessage" + event.data);		
			var obj = JSON.parse(event.data); 
			var AgentID = obj.agentID;
	
			if(AgentID !== undefined) {
				var ThisDN = obj.thisDN;
				var MessageName = obj.MessageName;
				var Reasons = ''; 
				var ReasonsCode = ''; 
	
				if (obj.reasons === undefined || obj.reasons[0].key ==='INTERACTION_WORKSPACE') {
					Reasons = '----';
				} else {
					Reasons = obj.reasons[0].key;
					ReasonsCode = obj.reasons[0].value;
				}
	
				var AgentStatus = '';
				var nr_enable = '';
				var nr_checked = '';
				var lo_enable = '';
				var lo_checked = 'checked="true"';
	
				obj.extensions.forEach(function(o) { 
					if (o.key === 'AgentStatus') {
// 						console.log("AgentStatus -> " + o.value);		
						switch (o.value) {
						case 1 : 
							AgentStatus = "Logout";
							break;
						case 2 : 
							AgentStatus = "<label style=\"color: green;\">Ready</label>";
							nr_checked  ='checked="true"';
							break;
						case 3 :
							AgentStatus = "<label style=\"color: red;\">NotReady</label>";
// 							nr_enable = 'disabled';
							break;
						case 4 : 
							AgentStatus = "AfterCallWork";
							break;
						}
					}
				});
	
				if (AgentStatus === '') {
// 					console.log("MessageName -> " + MessageName);		
					switch (MessageName) {
					case 'EventAgentNotReady' : 
						AgentStatus = "<label style=\"color: red;\">NotReady</label>";
// 						nr_enable = 'disabled';
						break;
					case 'EventAgentReady' :
						AgentStatus = "<label style=\"color: green;\">Ready</label>";
						nr_checked  ='checked="true"';
						break;
					default :
						break;
					}
				}
	
				switch (MessageName) {
				case 'EventAgentLogout':
					if (placeList.has(AgentID)) {
						placeList.delete(AgentID); 
					} 
					break;
				case 'EventLinkConnected':
					break;
				default :
					if (AgentStatus !== '') {
						var val = "<td>"+ ThisDN +"</td>";
						val += "<td>"+ AgentStatus +"</td>";
						val += "<td>"+ Reasons+"</td>";
						val += "<td><label class=\"switch\"><input type=\"checkbox\" id=\"notready_dn\" name=\"notready\"  onclick=\"return setNotReadyReason("+ ThisDN +","+ReasonsCode+");\" "+nr_enable+" "+nr_checked+"><span class=\"slider round green\"></span></label></td>";
						val += "<td><label class=\"switch\"><input type=\"checkbox\" id=\"logoff_dn\" name=\"logoff\" onclick=\"SendLogoff(this,"+ ThisDN +")\" "+lo_enable+" "+lo_checked+"><span class=\"slider round green\"></span></label></td>";
						placeList.set(AgentID,val);
					}
					break;
				}
	
				var table = "";
				placeList.forEach(function(value, key) {
					table += "<tr><td>"+key + "</td>" + value +"</tr>";
				}, placeList);
				if (table === '') {
					$("#wait_image").removeClass('imagehidden');						        		
					$("#wait_image").addClass('imagevisible');
				} else {
					$("#wait_image").removeClass('imagevisible');						        		
					$("#wait_image").addClass('imagehidden');
				}
//             	$("#table_AgentState").empty();
//             	$("#table_AgentState").append(table);
				document.getElementById("table_AgentState").innerHTML = table;
			}
		}
	
		webSocket.onclose = function(event) {
			console.log("Connection closed");
			alert("Connection closed, server: " + location.host);
		};
	} 
	openSocket();
	
	
	sendNotReadyReason = function () {
		var ThisDN = $("#mThisDN").val();
		var reason = $( "#mReason option:selected" ).text();
		var reasoncode = $("#mReason").val();
		var js = {};
		js['command'] = 'NotReady';
		js['thisDN'] = ''+ThisDN;
		js['reason'] = ''+reason;
		js['reasoncode'] = ''+reasoncode;
		console.log("WebSocket sent "+JSON.stringify(js));
		webSocket.send(JSON.stringify(js));
		$('#SetReason').hide();
	}
	
	setNotReadyReason = function (ThisDN,reasoncode) {
		$("#mThisDN").val(ThisDN);
		$("#mReason").val(reasoncode);
		$('#SetReason').show();
		return false;
	}
	
// 	var SendNotReady = function (elem, ThisDN, reason, reasoncode) {
// // 		console.log("Press NotReady");
// 		if (elem.checked !== true){
// 			var js = {};
// 			js['command'] = 'NotReady';
// 			js['thisDN'] = ''+ThisDN;
// 			js['reason'] = ''+reason;
// 			js['reasoncode'] = ''+reasoncode;
// 			console.log("WebSocket sent "+JSON.stringify(js));
// 			webSocket.send(JSON.stringify(js));
// 		}
// 	}
	
	var SendLogoff = function (elem, ThisDN) {
// 		console.log("Press Logoff");
		if (elem.checked !== true){
			var js = {};
			js['command'] = 'Logoff';
			js['thisDN'] = ''+ThisDN;
			console.log("WebSocket sent "+JSON.stringify(js));
			webSocket.send(JSON.stringify(js));
		}
	}

	var table = $('#stickytable_2');

	$('#agentid_header, #thisdn_header').each(function() {
		var th = $(this);
		var thIndex = th.index();
		var inverse = false;

		th.click(function() {
// 			console.log("Sort");
			table.find('td').filter(function() {
				return $(this).index() === thIndex;
			}).sortElements(function(a, b) {
				return $.text([a]) > $.text([b])
						? inverse ? -1 : 1
						: inverse ? 1 : -1;
			}, function() {
				return this.parentNode; 
			});
			inverse = !inverse;
		});
	});
	
	$(function() {
		try {
			parent.ChangeActiveMenu("#AgentStateGlobal");
		} catch (e) {}
 		$("#stickytable_1").stickyTableHeaders();
 		$("#stickytable_2").stickyTableHeaders();
	})
	
	window.onclick = function(event) {
		if (event.target.className == 'modal') {
			$('#SetReason').hide();
		}
	}
</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>