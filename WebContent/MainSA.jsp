<!DOCTYPE html>
<%@page import="comapp.ConfigServlet"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<!-- <link rel="icon" rel="images/red.ico"> -->
<link rel="shortcut icon" href="images/favicon.ico" />
<title>Hera DashBoard</title>
<link rel="stylesheet" type="text/css" href="css/comapp.css">
<script src="js/jquery.min.js"></script>
</head>
<body>
<table style="width:100%; top:-3px" >
	<tr>
		<td>
			<table id="Header" class="navbar green">
				<tr>
					<td style="width: 48%" >
						<table class="left">
							<tr>
								<td><a class="green" id="AgentStateGlobal" target="_MainIframe">Stato Agenti</a></td>								
							</tr>
						</table>
					</td>
					<td style="width: 4%">
						<table class="center">
							<tr>
								<td>
									<div class="container_img green title" onclick="location.href='index.jsp'"> 
										<img alt=""  src="images/ShutDownWhite_.png">
									</div>
								</td>
							</tr>
						</table>
					</td>
					<td>
						<div class="left"><a class="green bold "><%=ConfigServlet.applVersion%></a></div>
						<div class="right"></div>
					</td>
				</tr>
			</table>
		</td>
	</tr>
	<tr>
		<td >	
			<iframe src="AgentStateGlobal.jsp" id="MainIframe" name="_MainIframe" style="width: 100%;  border: 0 solid grey"></iframe>
		</td>
	</tr>
	<tr >
		<td>
			<table id="Footer" class="navbar bottom blue">
				<tr>
					<td style="width: 60%">
						<table class="left">
							<tr>
								<td>
									
								</td>
								<td>
									
								</td>
							</tr>
						</table>
					</td>
					<td >
						<table class="right" >
							<tr>
								<td>
									
								</td>
							</tr>
						</table>
					</td>
				</tr>
			</table>
		</td>
	</tr>
</table>
	
<script type="text/javascript">
	$(function() {
		$("#MainIframe").height($(window).height() - $("#Footer").height()-$("#Header").height()-10);
		$("#Footer").width($(window).width() - 5);
		$(window).resize(function() {
			$("#MainIframe").height($(window).height() - $("#Footer").height()-$("#Header").height()-10);
			$("#Footer").width($(window).width() - 5);
			
		});
	})

	ChangeActiveMenu = function(menu) {
		$("#AgentStateGlobal").removeClass("active");
		
		$(menu).addClass("active");
		$("#Footer").css('display', 'none');
	}
	
	ChangeActivedFooter = function(menu,href) {
		$("#Footer").css('display', 'inline-table');				
		if (menu != undefined) {
			$(menu).addClass("active");
			if (href != undefined) {
				$("#ServiceDetails").attr("href",href);
			}
		}
	}
</script>
</body>
</html>
