<%@ page language="java" contentType="text/html; charset=UTF-8"    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<link rel="stylesheet" href="css/global.css">
<script src="js/jquery.min.js"></script>
</head>
<body > 

 
	
	<div class="main" style="overflow:visible;">
		<div class="container">
			<div class="listGroup" style="width: 100%">
				<table class='CadrsTitleBlue ' style="width: 100%">	
				<tr><th colspan="2" width="36%">Descrizione</th><th width="15%">Totale</th><th width="16%">Out 1</th><th width="16%">Out 2</th><th width="16%">Out 3</th></tr></table>
				<table style="width: 100%" >				
				</table>
			</div>
		</div>
	</div>
<script type="text/javascript">
		$(function() {
			try {
				parent.ChangeActiveMenu("#MenuDetails");
				parent.EnabledFooter("#MainGraph");	
			} catch (e) {
			}
		})

		
	</script>
</body>
</html>