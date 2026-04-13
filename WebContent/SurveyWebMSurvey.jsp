<%@page import="comapp.ConfigServlet"%>
<%@page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<%@page import="java.sql.Statement"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="java.util.Properties"%>
<%@page import="org.apache.log4j.Logger"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<html lang="en" style="overflow: inherit;">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Survey Web</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
<script src="js/jquery.stickytableheaders.js"></script>
<style type="text/css">
.hidden {
	display: none;
}
table.padding td {
	padding: 3px;
}
table.noborder td {
	border: 0px;
}
</style>
</head>
<body style="overflow: auto;">
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

	<form id="DataSurvey" action="">
		<table id="stickytable_1" class="roundedCorners" style="width: 100%;">
			<thead>
				<tr class="listGroupItem active green">
					<td colspan="7">
						<label class="container title "> Applicazione Survey</label>
					</td>
				</tr>
			</thead>
			<tr class="listGroupItem" id="a1" class="show">
				<td>
					<table id="table1" class="padding noborder" style="width: 100%; ">
						<tr>
							<td >
								<select id="applicationList" name="applicationList" style="width: 95%;" onChange="distdisplay()">
									<option></option>
								</select>
								
							</td>
							<td > 
								<input name="SurveyTitle" id="SurveyTitle" class="fullwidth" type="text" value=""  />
							</td>
						</tr>
						<tr>
							<td >
								 <label  id="ultima_modifica" >&nbsp;</label>
							</td>
						</tr>
					</table>
				</td>
			</tr>

		</table>
		<table id="stickytable_2" class="roundedCorners" id="mainSurvey" style="width: 100%;">
			<thead>
				<tr class="listGroupItem white">
					<td style="width: auto;  padding: 0px !important">  
						<table style="width: auto; padding: 0px !important;">
							<tr>
								<td style="padding: 0px !important">
									<div class="button green active" style="margin: 0px" onclick="$('table[id^=_]').addClass('hidden'); $('#_a').removeClass('hidden');$('div.button').removeClass('active');$(this).addClass('active');">Benvenuto</div>
								</td>
								<td style="padding: 0px !important">
									<div class="button green" style="margin: 0px" onclick="$('table[id^=_]').addClass('hidden'); $('#_b').removeClass('hidden');$('div.button').removeClass('active');$(this).addClass('active');">Domanda 1</div>
								</td>
								<td style="padding: 0px !important">
									<div class="button green" style="margin: 0px"  onclick="$('table[id^=_]').addClass('hidden'); $('#_c').removeClass('hidden');$('div.button').removeClass('active');$(this).addClass('active');">Domanda 2</div>
								</td>
								<td style="padding: 0px !important">
									<div class="button green" style="margin: 0px" onclick="$('table[id^=_]').addClass('hidden'); $('#_d').removeClass('hidden');$('div.button').removeClass('active');$(this).addClass('active');">Domanda 3</div>
								</td>
								<td style="padding: 0px !important">
									<div class="button green" style="margin: 0px" onclick="$('table[id^=_]').addClass('hidden'); $('#_e').removeClass('hidden');$('div.button').removeClass('active');$(this).addClass('active');">Domanda 4</div>
								</td>
								<td style="padding: 0px !important">
									<div class="button green" style="margin: 0px" onclick="$('table[id^=_]').addClass('hidden'); $('#_f').removeClass('hidden');$('div.button').removeClass('active');$(this).addClass('active');">Domanda 5</div>
								</td>
								<td style="padding: 0px !important">
									<div class="button green" style="margin: 0px" onclick="$('table[id^=_]').addClass('hidden'); $('#_g').removeClass('hidden');$('div.button').removeClass('active');$(this).addClass('active');">Arrivederci</div>
								</td>
							</tr>
						</table>
					</td>
				</tr>
			</thead>
			<tr>
				<td id="tabTd" style="height: 100%; ">

					<table class="roundedCorners " style="margin-top: 5px; " id="_a">
						<tr class="listGroupItem active green" >
							<td colspan="7">
								<label class="container title ">Benvenuto</label>
							</td>
						</tr>
						<tr id="a2">
							<td>
								<table style="width: 100%; margin: auto;"> 
									<tr>
										<td colspan="2">
											<div>
												<textarea id="WellcomeMsg" name="WellcomeMsg"  ></textarea>
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</table>
					
					<table class="roundedCorners hidden" style=" margin-top: 5px;" id="_b">
						<tr class="listGroupItem active green" onclick=" $('#a3').toggle('show');">
							<td colspan="7">
								<label class="container title ">Domanda 1</label>
							</td>
						</tr>
						<tr id="a3"  >
							<td>
								<table style="width: 100%; ">
									<tr class="listGroupItem">
										<td colspan="4">
											<div>
												<textarea id="Question1Msg" name="Question1Msg"  ></textarea>
											</div>
										</td>
									</tr>
									<tr class="listGroupItem">
										<td>Descrizione</td>
										<td colspan="3">
											<input id="Question1Desc" name="Question1Desc" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr class="listGroupItem">
										<td >
											<label class="switch"> <input type="checkbox" id="Question1Answer0ck" name="Question1Answer0ck" onclick="" value="0"> <span class="slider round green"></span>
											</label> <label for="Question1Answer0ck">0</label>
										</td>
										<td >
											<input id="Question1Answer0Msg" name="Question1Answer0Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question1Answer1ck" name="Question1Answer1ck" onclick="" value="1"> <span class="slider round green"></span>
											</label> <label for="Question1Answer1ck">1</label>
										</td>
										<td >
											<input id="Question1Answer1Msg" name="Question1Answer1Msg" type="text" class="fullwidth"  />
										</td> 
									 </tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question1Answer2ck" name="Question1Answer2ck" onclick="" value="2"> <span class="slider round green"></span>
											</label> <label for="Question1Answer2ck">2</label>
										</td> 
										<td >
											<input id="Question1Answer2Msg" name="Question1Answer2Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question1Answer3ck" name="Question1Answer3ck" onclick="" value="3"> <span class="slider round green"></span>
											</label> <label for="Question1Answer3ck">3</label>
										</td>
										<td >
											<input id="Question1Answer3Msg" name="Question1Answer3Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question1Answer4ck" name="Question1Answer4ck" onclick="" value="4"> <span class="slider round green"></span>
											</label> <label for="Question1Answer4ck">4</label>
										</td>
										<td >
											<input id="Question1Answer4Msg" name="Question1Answer4Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question1Answer5ck" name="Question1Answer5ck" onclick="" value="5"> <span class="slider round green"></span>
											</label> <label for="Question1Answer5ck">5</label>
										</td>
										<td >
											<input id="Question1Answer5Msg" name="Question1Answer5Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question1Answer6ck" name="Question1Answer6ck" onclick="" value="6"> <span class="slider round green"></span>
											</label> <label for="Question1Answer6ck">6</label>
										</td>
										<td >
											<input id="Question1Answer6Msg" name="Question1Answer6Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question1Answer7ck" name="Question1Answer7ck" onclick="" value="7"> <span class="slider round green"></span>
											</label> <label for="Question1Answer7ck">7</label>
										</td>
										<td >
											<input id="Question1Answer7Msg" name="Question1Answer7Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question1Answer8ck" name="Question1Answer8ck" onclick="" value="8"> <span class="slider round green"></span>
											</label> <label for="Question1Answer8ck">8</label>
										</td>
										<td >
											<input id="Question1Answer8Msg" name="Question1Answer8Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question1Answer9ck" name="Question1Answer9ck" onclick="" value="9"> <span class="slider round green"></span>
											</label> <label for="Question1Answer9ck">9</label>
										</td>
										<td >
											<input id="Question1Answer9Msg" name="Question1Answer9Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</table>
					<table class="roundedCorners hidden" style="margin-top: 5px;" id="_c">
						<tr class="listGroupItem active green" onclick=" $('#a4').toggle('show');">
							<td colspan="7">
								<label class="container title ">Domanda 2</label>
							</td>
						</tr>
						<tr id="a4">
							<td>
								<table style="width: 100%; margin: auto;">
									<tr>
										<td colspan="4">
											<div> 
												<textarea id="Question2Msg" name="Question2Msg" ></textarea>
											</div>
										</td>
									</tr>
									<tr>
										<td >Descrizione</td>
										<td colspan="3">
											<input id="Question2Desc" name="Question2Desc" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question2Answer0ck" name="Question2Answer0ck" onclick="" value="0"> <span class="slider round green"></span>
											</label> <label for="Question2Answer0ck">0</label>
										</td>
										<td >
											<input id="Question2Answer0Msg" name="Question2Answer0Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question2Answer1ck" name="Question2Answer1ck" onclick="" value="1"> <span class="slider round green"></span>
											</label> <label for="Question2Answer1ck">1</label>
										</td>
										<td >
											<input id="Question2Answer1Msg" name="Question2Answer1Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question2Answer2ck" name="Question2Answer2ck" onclick="" value="2"> <span class="slider round green"></span>
											</label> <label for="Question2Answer2ck">2</label>
										</td>
										<td >
											<input id="Question2Answer2Msg" name="Question2Answer2Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question2Answer3ck" name="Question2Answer3ck" onclick="" value="3"> <span class="slider round green"></span>
											</label> <label for="Question2Answer3ck">3</label>
										</td>
										<td >
											<input id="Question2Answer3Msg" name="Question2Answer3Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question2Answer4ck" name="Question2Answer4ck" onclick="" value="4"> <span class="slider round green"></span>
											</label> <label for="Question2Answer4ck">4</label>
										</td>
										<td >
											<input id="Question2Answer4Msg" name="Question2Answer4Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question2Answer5ck" name="Question2Answer5ck" onclick="" value="5"> <span class="slider round green"></span>
											</label> <label for="Question2Answer5ck">5</label>
										</td>
										<td >
											<input id="Question2Answer5Msg" name="Question2Answer5Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question2Answer6ck" name="Question2Answer6ck" onclick="" value="6"> <span class="slider round green"></span>
											</label> <label for="Question2Answer6ck">6</label>
										</td>
										<td >
											<input id="Question2Answer6Msg" name="Question2Answer6Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question2Answer7ck" name="Question2Answer7ck" onclick="" value="7"> <span class="slider round green"></span>
											</label> <label for="Question2Answer7ck">7</label>
										</td>
										<td >
											<input id="Question2Answer7Msg" name="Question2Answer7Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question2Answer8ck" name="Question2Answer8ck" onclick="" value="8"> <span class="slider round green"></span>
											</label> <label for="Question2Answer8ck">8</label>
										</td>
										<td >
											<input id="Question2Answer8Msg" name="Question2Answer8Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question2Answer9ck" name="Question2Answer9ck" onclick="" value="9"> <span class="slider round green"></span>
											</label> <label for="Question2Answer9ck">9</label>
										</td>
										<td >
											<input id="Question2Answer9Msg" name="Question2Answer9Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</table>
					<table class="roundedCorners hidden" style="margin-top: 5px;" id="_d">
						<tr class="listGroupItem active green" >
							<td colspan="7">
								<label class="container title ">Domanda 3</label>
							</td>
						</tr>
						<tr id="a5">
							<td>
								<table style="width: 100%; margin: auto;">
									<tr>
										<td colspan="4">
											<div>
												<textarea id="Question3Msg" name="Question3Msg" ></textarea>
											</div>
										</td>
									</tr>
									<tr>
										<td >Descrizione</td>
										<td colspan="3">
											<input id="Question3Desc" name="Question3Desc" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question3Answer0ck" name="Question3Answer0ck" onclick="" value="0"> <span class="slider round green"></span>
											</label> <label for="Question3Answer0ck">0</label>
										</td>
										<td >
											<input id="Question3Answer0Msg" name="Question3Answer0Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question3Answer1ck" name="Question3Answer1ck" onclick="" value="1"> <span class="slider round green"></span>
											</label> <label for="Question3Answer1ck">1</label>
										</td>
										<td >
											<input id="Question3Answer1Msg" name="Question3Answer1Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question3Answer2ck" name="Question3Answer2ck" onclick="" value="2"> <span class="slider round green"></span>
											</label> <label for="Question3Answer2ck">2</label>
										</td>
										<td >
											<input id="Question3Answer2Msg" name="Question3Answer2Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question3Answer3ck" name="Question3Answer3ck" onclick="" value="3"> <span class="slider round green"></span>
											</label> <label for="Question3Answer3ck">3</label>
										</td>
										<td >
											<input id="Question3Answer3Msg" name="Question3Answer3Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question3Answer4ck" name="Question3Answer4ck" onclick="" value="4"> <span class="slider round green"></span>
											</label> <label for="Question3Answer4ck">4</label>
										</td>
										<td >
											<input id="Question3Answer4Msg" name="Question3Answer4Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question3Answer5ck" name="Question3Answer5ck" onclick="" value="5"> <span class="slider round green"></span>
											</label> <label for="Question3Answer5ck">5</label>
										</td>
										<td >
											<input id="Question3Answer5Msg" name="Question3Answer5Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question3Answer6ck" name="Question3Answer6ck" onclick="" value="6"> <span class="slider round green"></span>
											</label> <label for="Question3Answer6ck">6</label>
										</td>
										<td >
											<input id="Question3Answer6Msg" name="Question3Answer6Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question3Answer7ck" name="Question3Answer7ck" onclick="" value="7"> <span class="slider round green"></span>
											</label> <label for="Question3Answer7ck">7</label>
										</td>
										<td >
											<input id="Question3Answer7Msg" name="Question3Answer7Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question3Answer8ck" name="Question3Answer8ck" onclick="" value="8"> <span class="slider round green"></span>
											</label> <label for="Question3Answer8ck">8</label>
										</td>
										<td >
											<input id="Question3Answer8Msg" name="Question3Answer8Msg" type="text" class="fullwidth"  />
										</td>
								
										<td >
											<label class="switch"> <input type="checkbox" id="Question3Answer9ck" name="Question3Answer9ck" onclick="" value="9"> <span class="slider round green"></span>
											</label> <label for="Question3Answer9ck">9</label>
										</td>
										<td >
											<input id="Question3Answer9Msg" name="Question3Answer9Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</table>
					<table class="roundedCorners hidden" style="margin-top: 5px;" id="_e">
						<tr class="listGroupItem active green" onclick=" $('#a6').toggle('show');">
							<td colspan="7">
								<label class="container title ">Domanda 4</label>
							</td>
						</tr>
						<tr id="a6">
							<td>
								<table style="width: 100%; margin: auto;">
									<tr>
										<td colspan="4">
											<div>
												<textarea id="Question4Msg" name="Question4Msg"></textarea>
											</div>
										</td>
									</tr>
									<tr>
										<td >Descrizione</td>
										<td colspan="3">
											<input id="Question4Desc" name="Question4Desc" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question4Answer0ck" name="Question4Answer0ck" onclick="" value="0"> <span class="slider round green"></span>
											</label> <label for="Question4Answer0ck">0</label>
										</td>
										<td >
											<input id="Question4Answer0Msg" name="Question4Answer0Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question4Answer1ck" name="Question4Answer1ck" onclick="" value="1"> <span class="slider round green"></span>
											</label> <label for="Question4Answer1ck">1</label>
										</td>
										<td >
											<input id="Question4Answer1Msg" name="Question4Answer1Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question4Answer2ck" name="Question4Answer2ck" onclick="" value="2"> <span class="slider round green"></span>
											</label> <label for="Question4Answer2ck">2</label>
										</td>
										<td >
											<input id="Question4Answer2Msg" name="Question4Answer2Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question4Answer3ck" name="Question4Answer3ck" onclick="" value="3"> <span class="slider round green"></span>
											</label> <label for="Question4Answer3ck">3</label>
										</td>
										<td >
											<input id="Question4Answer3Msg" name="Question4Answer3Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question4Answer4ck" name="Question4Answer4ck" onclick="" value="4"> <span class="slider round green"></span>
											</label> <label for="Question4Answer4ck">4</label>
										</td>
										<td >
											<input id="Question4Answer4Msg" name="Question4Answer4Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question4Answer5ck" name="Question4Answer5ck" onclick="" value="5"> <span class="slider round green"></span>
											</label> <label for="Question4Answer5ck">5</label>
										</td>
										<td >
											<input id="Question4Answer5Msg" name="Question4Answer5Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question4Answer6ck" name="Question4Answer6ck" onclick="" value="6"> <span class="slider round green"></span>
											</label> <label for="Question4Answer6ck">6</label>
										</td>
										<td >
											<input id="Question4Answer6Msg" name="Question4Answer6Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question4Answer7ck" name="Question4Answer7ck" onclick="" value="7"> <span class="slider round green"></span>
											</label> <label for="Question4Answer7ck">7</label>
										</td>
										<td >
											<input id="Question4Answer7Msg" name="Question4Answer7Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question4Answer8ck" name="Question4Answer8ck" onclick="" value="8"> <span class="slider round green"></span>
											</label> <label for="Question4Answer8ck">8</label>
										</td>
										<td >
											<input id="Question4Answer8Msg" name="Question4Answer8Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question4Answer9ck" name="Question4Answer9ck" onclick="" value="9"> <span class="slider round green"></span>
											</label> <label for="Question4Answer9ck">9</label>
										</td>
										<td >
											<input id="Question4Answer9Msg" name="Question4Answer9Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</table>
					<table class="roundedCorners hidden" style="margin-top: 5px;" id="_f">
						<tr class="listGroupItem active green" onclick=" $('#a7').toggle('show');">
							<td colspan="7">
								<label class="container title ">Domanda 5</label>
							</td>
						</tr>
						<tr id="a7">
							<td>
								<table style="width: 100%; margin: auto;">
									<tr>
										<td colspan="4">
											<div>
												<textarea id="Question5Msg" name="Question5Msg" ></textarea>
											</div>
										</td>
									</tr>
									<tr>
										<td >Descrizione</td>
										<td colspan="3">
											<input id="Question5Desc" name="Question5Desc" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question5Answer0ck" name="Question5Answer0ck" onclick="" value="0"> <span class="slider round green"></span>
											</label> <label for="Question5Answer0ck">0</label>
										</td>
										<td >
											<input id="Question5Answer0Msg" name="Question5Answer0Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question5Answer1ck" name="Question5Answer1ck" onclick="" value="1"> <span class="slider round green"></span>
											</label> <label for="Question5Answer1ck">1</label>
										</td>
										<td >
											<input id="Question5Answer1Msg" name="Question5Answer1Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question5Answer2ck" name="Question5Answer2ck" onclick="" value="2"> <span class="slider round green"></span>
											</label> <label for="Question5Answer2ck">2</label>
										</td>
										<td >
											<input id="Question5Answer2Msg" name="Question5Answer2Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question5Answer3ck" name="Question5Answer3ck" onclick="" value="3"> <span class="slider round green"></span>
											</label> <label for="Question5Answer3ck">3</label>
										</td>
										<td >
											<input id="Question5Answer3Msg" name="Question5Answer3Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question5Answer4ck" name="Question5Answer4ck" onclick="" value="4"> <span class="slider round green"></span>
											</label> <label for="Question5Answer4ck">4</label>
										</td>
										<td >
											<input id="Question5Answer4Msg" name="Question5Answer4Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question5Answer5ck" name="Question5Answer5ck" onclick="" value="5"> <span class="slider round green"></span>
											</label> <label for="Question5Answer5ck">5</label>
										</td>
										<td >
											<input id="Question5Answer5Msg" name="Question5Answer5Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question5Answer6ck" name="Question5Answer6ck" onclick="" value="6"> <span class="slider round green"></span>
											</label> <label for="Question5Answer6ck">6</label>
										</td>
										<td >
											<input id="Question5Answer6Msg" name="Question5Answer6Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question5Answer7ck" name="Question5Answer7ck" onclick="" value="7"> <span class="slider round green"></span>
											</label> <label for="Question5Answer7ck">7</label>
										</td>
										<td >
											<input id="Question5Answer7Msg" name="Question5Answer7Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
									<tr>
										<td >
											<label class="switch"> <input type="checkbox" id="Question5Answer8ck" name="Question5Answer8ck" onclick="" value="8"> <span class="slider round green"></span>
											</label> <label for="Question5Answer8ck">8</label>
										</td>
										<td >
											<input id="Question5Answer8Msg" name="Question5Answer8Msg" type="text" class="fullwidth"  />
										</td>
									
										<td >
											<label class="switch"> <input type="checkbox" id="Question5Answer9ck" name="Question5Answer9ck" onclick="" value="9"> <span class="slider round green"></span>
											</label> <label for="Question5Answer9ck">9</label>
										</td>
										<td >
											<input id="Question5Answer9Msg" name="Question5Answer9Msg" type="text" class="fullwidth"  />
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</table>

					<table class="roundedCorners hidden" style="margin-top: 5px;" id="_g">
						<tr class="listGroupItem active green" onclick=" $('#a8').toggle('show');">
							<td colspan="7">
								<label class="container title ">Arrivederci</label>
							</td>
						</tr>
					<tr>

							<td>
								<table style="width: 100%; margin: auto;">
									<tr>
										<td colspan="2">
											<div>
												<textarea id="GoodbyeMsg" name="GoodbyeMsg" ></textarea>
											</div>
										</td>
									</tr>
								</table>
							</td>
						</tr>
					</table>
				</td>
			</tr>

			<tr>
				<td colspan=3 style="border: none;">
					<table style="width: 100%; text-align: center;">
						<tr>
							<td style="border: none;">
								<input type="button" class="button green fullwidth" value="Salva" name="Salva" onclick="sendform();" />
							</td>

							<td style="border: none;">
								<input type="button" class="button green fullwidth" value="Annulla" name="Annulla" />
							</td>
						</tr>
					</table>
				</td>
			</tr>
		</table>
	</form>

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
		} catch (e) {}
 		$("#stickytable_1").stickyTableHeaders();
 		$("#stickytable_2").stickyTableHeaders();
		$("#mainSurvey").height($(window).height() -180);
		$(window).resize(function() {
			$("#mainSurvey").height($(window).height() - 180);				 
		});
	})
	
	var distdisplay = function(){
        var id = jQuery('#applicationList option:selected').val();
        
        if(id==""){
        	jQuery('#SurveyTitle').val('');
        	jQuery('#ultima_modifica').html('');
        	clearInput();
        }else{
	        $.ajax({
				type: 'GET',
				url: 'SurveyWebQuery.jsp?command=SurveyDetails&id='+id,
				success: function(data) {
					jQuery('script[id*=scrtmp]').remove();
					jQuery("body").append(data);
				},
				error: function() {
					console.log("load empty");
				}            
			});
	     
	        $.ajax({
				type: 'GET',
				url: 'SurveyWebQuery.jsp?command=SurveyDetailsFields&id='+id,
				success: function(data) {
					jQuery('script[id*=scrtmp]').remove();
					jQuery("body").append(data);
				},
				error: function() {
					console.log("load empty");
				}            
			});
        }
	}
	
	var sendform = function(){
		var action = "SurveyWebQuery.jsp?command=SaveSurvey&id="+ jQuery('#applicationList option:selected').val()
 		
		$.ajax({
            url: action,
            type: 'post',
            data: jQuery("#DataSurvey").serialize(),
            success: function(data) {
            	 distdisplay();
            },
            error:function(data) {
            	console.log(data);
            } 
    	});
 
	}
</script>

	<%
		try {
			Context ctx = new InitialContext();
			DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
			log.info(session.getId() + " - connection CCTE wait...");
			conn = ds.getConnection();
			//String sqlSurvey = "select id, titolo from ivr_realtime_cct.STC_configurazione.elenco_survey order by titolo";
			log.info(session.getId() + " - dashboard.SurveyWeb_GetList()");
			cs = conn.prepareCall("{call dashboard.SurveyWeb_GetList()}");
			cs.execute();										
			log.debug(session.getId() + " - executeCall complete");
	 		rs = cs.getResultSet(); 
			boolean first = true;
			
			String _script= "<script> ";
			
			if(rs.next() == false){
				
			}else{
				do{
					String titolo = rs.getString("titolo");
					String id = rs.getString("id");
					_script +="jQuery('#applicationList').append(new Option('"+titolo+"', '"+id+"',"+first+"));";
					first = false;
				}while(rs.next());

			}
			_script += " </script>";
			out.println(_script);		
			
		} catch (Exception e) {
			log.error(session.getId() + " - general: " + e.getMessage(), e);
		} finally {
			try { rs.close(); } catch (Exception e) { }
			try { cs.close(); } catch (Exception e) { }
			try { conn.close(); } catch (Exception e) { }
		}
	%>
	<script>
		clearInput = function() {

			jQuery('#WellcomeMsg').val('');
			jQuery('#GoodbyeMsg').val('');

			for (i = 1; i <= 5; i++) {
				jQuery('#Question' + i + 'Msg').val('');
				jQuery('#Question' + i + 'AnswerDesc').val('');
				for (j = 0; j <= 9; j++) {
					jQuery('#Question' + i + 'Answer' + j + 'Msg').val('');
					jQuery('#Question' + i + 'Answer' + j + 'ck').prop('checked', false);
				}
			}

		}
	</script>
</body>
<%
	log.info(session.getId() + " - ******* end page res ***** ");
%>
</html>