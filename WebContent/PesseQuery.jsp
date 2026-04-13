<%@page import="comapp.ConfigServlet"%>
<%@page import="java.sql.Types"%>
<%@page import="java.sql.SQLType"%>
<%@page import="java.sql.Timestamp"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.text.DateFormat"%>
<%@page import="java.util.Date"%>
<%@page import="java.util.Calendar"%>

<%@page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1"%>
<%@page import="java.sql.Statement"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>

<%@page import="java.util.Properties"%>
<%@page import="org.apache.log4j.Logger"%>
<%@page import = "java.util.Map" %>
<%@page import="org.apache.commons.lang3.StringUtils"%>

<%
	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");

	String user  = (String) session.getAttribute("UserName");


	//==	REQUEST PARAMETERS	====
	//		NONE
	//==============================
// 	Properties prop = ConfigServlet.getProperties();
	Connection connCct = null;
	Connection connOcs = null;
	CallableStatement cstmtOcs = null;
	CallableStatement cstmtCct = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================

	String _Command = request.getParameter("command"); 
	String _Id =request.getParameter("id");
	String _Territorio =request.getParameter("territorio");
	
	String _FusoOrario =request.getParameter("FusoOrario") != null ? request.getParameter("FusoOrario") : "";
	String _Livello =request.getParameter("Livello") != null ? request.getParameter("Livello") : "0";
 	String _InizioDistacco =request.getParameter("InizioDistacco") != null ? request.getParameter("InizioDistacco") : "";
 	String _FineValid =request.getParameter("FineValid") != null ? request.getParameter("FineValid") : "";

 	String _Aggiornamento ="";
	int Record_Count = 0;

	Map<String, String[]> parameters = request.getParameterMap();
 
	SimpleDateFormat sdf_origin = new SimpleDateFormat("dd/MM/yyyy");
	SimpleDateFormat sdf_destination = new SimpleDateFormat("yyyy-MM-dd");
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
		log.info(session.getId() + " - connection CCTE wait...");
		connCct = ds.getConnection();
		
		 ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"OCS");
		log.info(session.getId() + " - connection OCS wait...");
		connOcs = ds.getConnection();
		
		String query ="";
 		String _Script = "<script id=\"scrtmp\" type=\"text/javascript\">";
 		String nome_territorio ="";
		switch(_Territorio){
			case "IM": 
				nome_territorio = "IMOLA"; 
				break;
			case "MO": 
				nome_territorio = "MODENA"; 
				break;
		}

		log.info(session.getId() + " - command:" + _Command+")");
 		
		switch(_Command){
			case "recordPesseCaricato":
				log.info(session.getId() + " - dashboard.Pesse_recordPesseCaricato('" + _Territorio+ "')");
				cstmtOcs = connOcs.prepareCall("{call dashboard.Pesse_recordPesseCaricato(?)}");
				cstmtOcs.setString(1, _Territorio);
				rs = cstmtOcs.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
		 		if (rs.next()) {
					Record_Count = rs.getInt("TOTALE");
				} else {
					Record_Count = 0;
				}
				if (Record_Count != 0){
					out.println(true);
				}else{
					out.println(false);
				}
				break;
			case "caricaPesse":
				log.info(session.getId() + " - dashboard.Pesse_estraiImpostazioniPesse('" + _Territorio+ "')");
				cstmtCct = connCct.prepareCall("{call dashboard.Pesse_estraiImpostazioniPesse(?)}");
				cstmtCct.setString(1, _Territorio);
				rs = cstmtCct.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
				String res ="";
				String utente = "";
				String aggiornamento = "";
				String fuso_orario = "";
				String livello = "";
				String data_inizio_distacco ="";
				String data_fine_validita ="";
		 		if(rs.next() == false){
					
				}else {
					do{
						utente = rs.getString("utente");
						aggiornamento = rs.getString("aggiornamento");
						fuso_orario = rs.getString("fuso_orario");
						livello = rs.getString("livello");
						data_inizio_distacco = rs.getString("inizio_distacco");
						data_fine_validita = rs.getString("data_fine_validita");
					}while(rs.next());
				}
		 		if(StringUtils.isEmpty(utente) || StringUtils.isEmpty(data_inizio_distacco) || StringUtils.isEmpty(data_fine_validita) || StringUtils.isEmpty(livello)){
			 		res += "jQuery('#InizioDistacco"+_Territorio+"').val('aaaa-mm-gg');";
			 		res += "jQuery('#FineValid"+_Territorio+"').val('aaaa-mm-gg');";
			 		res += "jQuery('#Livello"+_Territorio+"').val('0');";
			 		res += "jQuery('#FusoOrario"+_Territorio+"').val('');";
			 		res += "jQuery('#Esito"+_Territorio+"').html('');";
		 	 		out.println(res);
		 		}else{
			  		String distac = sdf_destination.format(sdf_origin.parse(data_inizio_distacco));
			  		String valid = sdf_destination.format(sdf_origin.parse(data_fine_validita));
			 		res += "jQuery('#InizioDistacco"+_Territorio+"').val('"+distac+"');";
			 		res += "jQuery('#FineValid"+_Territorio+"').val('"+valid+"');";
			 		res += "jQuery('#Livello"+_Territorio+"').val('"+livello+"');";
			 		res += "jQuery('#FusoOrario"+_Territorio+"').val('"+fuso_orario+"');";
			 		res += "jQuery('#Esito"+_Territorio+"').html('" + utente +" il "+aggiornamento+"');";
		 	 		out.println(res);
		 		}
		 		break;
			case "salvaImpostazioniPesse":
				SimpleDateFormat sdf_agg = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
				_Aggiornamento = sdf_agg.format(new Date());
				String iniz_distac = "";
				String fine_valid = "";
				if(!_InizioDistacco.isEmpty()){
					iniz_distac = sdf_origin.format(sdf_destination.parse(_InizioDistacco));
				}else{
					iniz_distac = sdf_origin.format(new Date());
				}
				if(!_FineValid.isEmpty()){
					fine_valid = sdf_origin.format(sdf_destination.parse(_FineValid));
				}else{
					fine_valid = sdf_origin.format(new Date());
				}
				log.info(session.getId() + " - dashboard.Pesse_salvaImpostazioniPesse('" +user+"','"+_FusoOrario+"',"+_Livello+",'"+_Aggiornamento+"','"+iniz_distac+"','"+fine_valid+"','"+_Territorio+"')");
				cstmtCct = connCct.prepareCall("{call dashboard.Pesse_salvaImpostazioniPesse(?,?,?,?,?,?,?)}");
				cstmtCct.setString(1, user);
				cstmtCct.setString(2, _FusoOrario);
				cstmtCct.setInt(3, Integer.parseInt(_Livello));
				cstmtCct.setString(4, _Aggiornamento);
				cstmtCct.setString(5, iniz_distac);
				cstmtCct.setString(6, fine_valid);
				cstmtCct.setString(7, _Territorio);
				cstmtCct.execute();					
				log.debug(session.getId() + " - executeCall complete");
				out.println("jQuery('#content_confirm').html('Impostazioni PESSE per "+nome_territorio+" salvate correttamente'); jQuery('#titolo_confirm').html('Salvataggio completato');jQuery('#modal_confirm').show();");
				break;
			case "svuotaTabellaImpostazioniPesse":
				log.info(session.getId() + " - dashboard.Pesse_svuotaTabellaImpostazioniPesse('" + _Territorio+ "')");
				cstmtCct = connCct.prepareCall("{call dashboard.Pesse_svuotaTabellaImpostazioniPesse(?)}");
				cstmtCct.setString(1, _Territorio);
				rs = cstmtCct.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
		 		if (rs.next()) {
					Record_Count = rs.getInt("TOTALE");
				} else {
					Record_Count = 0;
				}
				if (Record_Count != 0){
					out.println(false);
				}else{
					out.println(true);
				}
				break;
			case "svuotaTabellaRecordPesse":
				log.info(session.getId() + " - dashboard.Pesse_svuotaTabellaRecordPesse('" + _Territorio+ "')");
				cstmtOcs = connOcs.prepareCall("{call dashboard.Pesse_svuotaTabellaRecordPesse(?)}");
				cstmtOcs.setString(1, _Territorio);
				cstmtOcs.execute();					
				log.debug(session.getId() + " - executeCall complete");
				out.println(true);
				break;
		}
	}catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
		out.println("jQuery('#ErrorCode').html("+e.hashCode()+"); jQuery('#ErrorMessage').html('"+e.getMessage()+"');jQuery('#Error').show()");
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmtOcs.close(); } catch (Exception e) {}
		try { cstmtCct.close(); } catch (Exception e) {}
		try { connCct.close(); } catch (Exception e) {}
		try { connOcs.close(); } catch (Exception e) {}
	}
	log.info(session.getId() + " - ******* end page res ***** ");
%>