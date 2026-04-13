<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="comapp.ConfigServlet"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<link rel="shortcut icon" href="images/favicon.ico" />
<title>Hera DashBoard</title>
</head>
<link rel="stylesheet" type="text/css" href="css/comapp.css">

<body>
<%
	try {
		session.invalidate();
	} catch (Exception e) {}

	String ERROR = request.getParameter("ERROR");
%>
	<table class="center">
		<tr>
			<td class="container_left"></td>
			<td colspan="1">
				<form action="Environment.jsp" method="post">
					<table class="roundedCorners login">
						<tr class="listGroupItem active green ">
							<td colspan="7">
								<table>
									<tr>
										<td>
											<div class="title  LoginTitle">Login</div>
										</td>
									</tr>
								</table>
							</td>

						</tr>
						<tr>
							<td colspan="7">
								<table style="width: 100%;">
									<tr>
										<td colspan="2">
											<div class="formGroupInput">
												<div class="formIconInput">
													<span class=" ca-icon-green ui-icon-check "></span>
												</div>
												<input name="user" type="text" class="formInput" placeholder=" Username" required="required">
											</div>
										</td>
									</tr>
									<tr>
										<td colspan="2">
											<div class="formGroupInput">
												<div class="formIconInput">
													<span class=" ca-icon-green ui-icon-check "></span>
												</div>
												<input name="password" type="password" class="formInput" placeholder=" Password">
											</div>
										</td>
									</tr>
<%
	if (!StringUtils.isBlank(ERROR)) {
%>
									<tr>
										<td colspan="2">
											<label style="color: red;">Errore: <%=ERROR%></label>
										</td>
									</tr>
<%
	}
%>
									<tr>
										<td colspan="2">
											<button type="submit" class="buttonLogin">Login</button>
										</td>
									</tr>
									<tr>
										<td class=""><img style="height: 20px" src='<%=ConfigServlet.getProperties().getProperty("Logo1")%>'></td>
										<td class="" style="float: right; "><img style="height: 20px" src='<%=ConfigServlet.getProperties().getProperty("Logo2")%>'></td>
									</tr>
								</table>
							</td>
						</tr>
					</table>
				</form>
			</td>
			<td class="container_right"></td>
		</tr>
	</table>

</body>
</html>