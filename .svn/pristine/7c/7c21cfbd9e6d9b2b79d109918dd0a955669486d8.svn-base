<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<!DOCTYPE html>
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
<div id="_top" >
	<table class="center" style="width: 100%">
<!-- 		<tr> -->
<!-- 			<td> -->
<!-- 				<table> -->
<!-- 					<tr> -->
<!-- 					</tr> -->
<!-- 				</table> -->
<!-- 			</td> -->
<!-- 		</tr> -->
		<tr >
			<td>
				<div id="SearchTable" style="width: 100%; border: 0px solid grey; " >
					
				</div>
			</td>
		</tr> 
	</table>
</div>
 <script>
	var selectAll = function(ck) {
		if (ck.checked) {
			jQuery(".abc").prop('checked', true);
		} else {
			jQuery(".abc").prop('checked', false);
		}
	}

	var Esporta = function() {
		var action = "MailWebQuery.jsp?command=Export";
		$.ajax({
			url : action,
			type : 'post',
			data : jQuery("#MailWebForm").serialize(),
			success : function(data) {
				$("#iFrame_MailWebFindMail").contents().find('#SearchTable').append(data);
			},
			error : function(data) {
				console.log(data);
			}
		});
	}

	var Salva = function() {
		var action = "MailWebQuery.jsp?command=Save";
		$.ajax({
			url : action,
			type : 'post',
			data : jQuery("#MailWebForm").serialize(),
			success : function(data) {
				$("#iFrame_MailWebFindMail").contents().find('#SearchTable').append(data);
			},
			error : function(data) {
				console.log(data);
			}
		});
	}
</script>
</body>
</html>