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

	String user = (String)session.getAttribute("UserName");
	if (StringUtils.isBlank(user))
		user = "-";
	user = Character.toUpperCase(user.charAt(0)) + user.substring(1);


	//==	REQUEST PARAMETERS	====
	//		NONE
	//==============================
// 	Properties prop = ConfigServlet.getProperties();
	Connection connCct = null;
	Connection connUcs = null;
	CallableStatement cstmtCct = null;
	CallableStatement cstmtUcs = null;
	ResultSet rs = null;
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================

	String _Command = request.getParameter("command"); 
	String _Id =request.getParameter("id");
	
	
	int RECORD_COUNT = 0;

	Map<String, String[]> parameters = request.getParameterMap();
 
	try {
		log.info(session.getId() + " - (command : "+_Command+")");
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
		log.info(session.getId() + " - connection CCTE wait...");
		connCct = ds.getConnection();
		
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"UCS");
		log.info(session.getId() + " - connection UCS wait...");
		connUcs = ds.getConnection();
		
		String query ="";
 		String _Script = "<script id=\"scrtmp\" type=\"text/javascript\">";
		switch(_Command){
			case"GetDateTime":
			
				break;
			case "SaveDateTime":
	 			try{
	 				String start_hour = request.getParameter("start_hour");
	 				String end_hour = request.getParameter("end_hour");
	 				
	 				log.info(session.getId() + " -  start_hour : " + start_hour); 
	 				log.info(session.getId() + " -  end_hour : " + end_hour);
	 				log.info(session.getId() + " - dashboard.SurveyWeb_DeleteGiorniOrari()");
	 				cstmtCct = connCct.prepareCall("{call dashboard.SurveyWeb_DeleteGiorniOrari()}");
	 				cstmtCct.execute();
	 				log.debug(session.getId() + " - executeCall complete");
	 			 	cstmtCct.close();
	 			 	
					String regex = "[0-9]+";
					String query_insert = "";
					
					cstmtCct = connCct.prepareCall("{call dashboard.SurveyWeb_InsertGiorniOrari(?,?,?)}");
					
					for (Map.Entry<String, String[]> entry : parameters.entrySet()) {
						if(entry.getValue()[0] != null){
							if(entry.getKey().matches(regex)){
								log.info(session.getId() + " - dashboard.SurveyWeb_InsertGiorniOrari("+entry.getKey()+",'"+start_hour+"','"+end_hour+"')" );
								cstmtCct.setInt(1,Integer.parseInt(entry.getKey()));
								cstmtCct.setString(2, start_hour);
								cstmtCct.setString(3, end_hour);
								cstmtCct.execute();
								log.debug(session.getId() + " - executeCall complete");
						 	}
						}
// 						System.out.println(entry.getKey() + "/" + entry.getValue());
					}
					
// 					String q_check= "SELECT count(*) as DAY_OPEN FROM ivr_realtime_cct.STC_configurazione.giorni_orari_survey";
// 	 				rs = stmt.executeQuery(q_check);
// 					if (rs.next() == false) {
						
// 			    	}else {
// 			    		do {
// 			    			RECORD_COUNT = rs.getInt("DAY_OPEN");
// 			    		}while(rs.next());
// 					}	
// 					rs.close();
// 					if(RECORD_COUNT>0){
// 						_Script += "jQuery('#titolo_add_list').html($('<span />').html('Survey attivata'));";
// 						_Script += "jQuery('#titolo_content_add_list').html($('<span />').html(''));";
// 						_Script += "jQuery('#add_to_list').show()";
// 					}else{
// 						_Script += "jQuery('#titolo_add_list').html($('<span />').html('Survey disattivata'));";
// 						_Script += "jQuery('#titolo_content_add_list').html($('<span />').html(''));";
// 						_Script += "jQuery('#add_to_list').show()";
// 					}
				}catch(Exception e){
					session.setAttribute("Error","Error");
					_Script += "jQuery('#ErrorCode').html("+e.hashCode()+"); jQuery('#ErrorMessage').html('"+e.getMessage()+"');jQuery('#Error').show()";
				}
				break;
			case "DeleteContactList":
			case "VerifyContactToDelete":
				String start_time = request.getParameter("start_time");	
				String end_time = request.getParameter("end_time");	
				String start_data ="";
				String end_data = "";
		        Calendar c_start = Calendar.getInstance();
		        Calendar c_end = Calendar.getInstance();
		        Date start = new Date();
		        Date end = new Date();
				if((start_time!= null)&&(end_time!= null)){
					DateFormat originalFormat = new SimpleDateFormat("yyyy-MM-dd");
					start = originalFormat.parse(start_time);
			        end = originalFormat.parse(end_time);
				}
				log.info(session.getId() + " -  start_data : " + start); 
				log.info(session.getId() + " -  end_data : " + end);
				switch(_Command){
				case "DeleteContactList":
					try{
						log.info(session.getId() + " - dashboard.SurveyWeb_DeleteCallingListSTC("+_Id+",'"+new Timestamp(start.getTime())+"','"+new Timestamp(end.getTime())+"')"); 
						cstmtUcs = connUcs.prepareCall("{call dashboard.SurveyWeb_DeleteCallingListSTC(?,?,?)}");
						cstmtUcs.setInt(1, Integer.parseInt(_Id));
						cstmtUcs.setTimestamp(2,new Timestamp(start.getTime()));
						cstmtUcs.setTimestamp(3,new Timestamp(end.getTime()));
						cstmtUcs.execute();
						log.debug(session.getId() + " - executeCall complete");
						_Script += "jQuery('#titolo_confirm_delete').html($('<span />').html('Conferma eliminazione '));";
						_Script += "jQuery('#content_confirm_delete').html($('<span />').html('Calling List ripulita per il perodo dal "+start_time+" al "+end_time+"</label>'));";
						_Script += "jQuery('#numero_da_eliminare').html('"+RECORD_COUNT+"'); ";
						_Script += "jQuery('#Elimina').hide();";
						_Script += "jQuery('#Annulla').prop('value','Chiudi');";
						_Script += "jQuery('#confirm_delete').show()";
					}catch(Exception e){
						session.setAttribute("Error","Error");
						_Script += "jQuery('#ErrorCode').html("+e.hashCode()+"); jQuery('#ErrorMessage').html("+e.getMessage()+");jQuery('#Error').show()";
					}
					break;
				case "VerifyContactToDelete":
 					try{
 						log.info(session.getId() + " - dashboard.SurveyWeb_CheckBeforeDeleteCallingListSTC('"+_Id+"','"+start+"','"+end+"')"); 
 						cstmtUcs = connUcs.prepareCall("{call dashboard.SurveyWeb_CheckBeforeDeleteCallingListSTC(?,?,?)}");
 						cstmtUcs.setString(1, _Id);
 						cstmtUcs.setTimestamp(2,new Timestamp(start.getTime()));
 						cstmtUcs.setTimestamp(3,new Timestamp(end.getTime()));
						rs = cstmtUcs.executeQuery();
						log.debug(session.getId() + " - executeCall complete");
				 		if(rs.next() == false){
							
						}else {
							do{
								RECORD_COUNT = rs.getInt("TOT_TO_DELETE");
							}while(rs.next());
						}
				 		log.info(session.getId() + " -  result : " + RECORD_COUNT);
						if(RECORD_COUNT>0){
							_Script += "jQuery('#titolo_confirm_delete').html($('<span />').html('Conferma eliminazione '));";
							_Script += "jQuery('#content_confirm_delete').html($('<span />').html('Eliminare <label id=\"numero_da_eliminare\"></label> numeri?</label>'));";
							_Script += "jQuery('#numero_da_eliminare').html('"+RECORD_COUNT+"'); ";
							_Script += "jQuery('#confirm_delete').show()";
						}else{
							_Script += "jQuery('#titolo_confirm_delete').html($('<span />').html('Conferma eliminazione'));";
							_Script += "jQuery('#content_confirm_delete').html($('<span />').html('Non ci sono elementi da eliminare per il periodo indicato'));";
							_Script += "jQuery('#Elimina').hide();";
							_Script += "jQuery('#confirm_delete').show();";
						}
					}catch(Exception ex){
						log.error(session.getId() + " - error : " + ex);
						session.setAttribute("Error","Error");
						_Script += "jQuery('#ErrorCode').html("+ex.hashCode()+"); jQuery('#ErrorMessage').html('"+ex.getMessage()+"');jQuery('#Error').show()";
					}
 					break;
				}
				break;
			case "SaveSurvey":
				try{
					String vWellcome = request.getParameter("WellcomeMsg");	
					String vGoodBye = request.getParameter("GoodbyeMsg");	
// 					vGoodBye = "'"+vGoodBye.replaceAll("'","''")+"'";
// 					vWellcome = "'"+vWellcome.replaceAll("'","''")+"'";
					String vTitle = request.getParameter("SurveyTitle");
					Date lastModify = Calendar.getInstance().getTime();
					DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
			        String today = formatter.format(lastModify);
			        
					log.info(session.getId() + " - dashboard.SurveyWeb_UpdateDescription("+Integer.valueOf(_Id)+",'"+vTitle+"','"+today+"','"+user+"')");
			        cstmtCct = connCct.prepareCall("{call dashboard.SurveyWeb_UpdateDescription(?,?,?,?)}");
			        cstmtCct.setInt(1, Integer.valueOf(_Id));
			        cstmtCct.setString(2, vTitle);
			        cstmtCct.setString(3, today);
			        cstmtCct.setString(4,	user);
			        cstmtCct.execute();
			        log.debug(session.getId() + " - executeCall complete");
					cstmtCct.close();
	 
					log.info(session.getId() + " - dashboard.SurveyWeb_Set("+Integer.valueOf(_Id)+",'A',null,null,'"+vGoodBye+"')");
			    	cstmtCct = connCct.prepareCall("{call dashboard.SurveyWeb_Set(?,?,?,?,?)}");
			    	cstmtCct.setInt(1, Integer.valueOf(_Id));//id_survey
			    	cstmtCct.setString(2, "A");//id_messaggio
			    	cstmtCct.setNull(3, Types.INTEGER);//option
			    	cstmtCct.setNull(4, Types.VARCHAR);//description
			    	cstmtCct.setString(5, vGoodBye);//testo
			    	cstmtCct.execute();
			    	log.debug(session.getId() + " - executeCall complete");
					cstmtCct.close();
 
					log.info(session.getId() + " - dashboard.SurveyWeb_Set("+Integer.valueOf(_Id)+",'B',null,null,'"+vWellcome+"')");
			    	cstmtCct = connCct.prepareCall("{call dashboard.SurveyWeb_Set(?,?,?,?,?)}");
					cstmtCct.setInt(1, Integer.valueOf(_Id));//id_survey
					cstmtCct.setString(2, "B");//id_messaggio
					cstmtCct.setNull(3, Types.INTEGER);//option
					cstmtCct.setNull(4, Types.VARCHAR);//description
					cstmtCct.setString(5, vWellcome);//testo
					cstmtCct.execute();
					log.debug(session.getId() + " - executeCall complete");
					cstmtCct.close();
			
					
				}catch(NullPointerException npe) {
					log.error(session.getId() + " - Insert A/B error : " +npe);
				}
				for(int i=1; i<=5; i++){
					for(int j=0; j<=9; j++){
						try{
							String ck = "Question"+i+"Answer"+j+"ck";
							String msg = "Question"+i+"Answer"+j+"Msg";
							String vCk= request.getParameter(ck);
							String vMsg = request.getParameter(msg);
// 							vMsg = "'"+vMsg.replaceAll("'","''")+"'";
 							if(vCk != null){
 								log.info(session.getId() + " - dashboard.SurveyWeb_Set("+Integer.valueOf(_Id)+",'D_"+i+"',"+j+",'"+vMsg+"',null)");
						    	cstmtCct = connCct.prepareCall("{call dashboard.SurveyWeb_Set(?,?,?,?,?)}");
								cstmtCct.setInt(1, Integer.valueOf(_Id));//id_survey
								cstmtCct.setString(2, "D_"+i );//id_messaggio
								cstmtCct.setInt(3, j);//option
								if(vMsg == null){
									cstmtCct.setNull(4, Types.VARCHAR);
								}else{
									cstmtCct.setString(4, vMsg);//description	
								}
								
								cstmtCct.setNull(5, Types.VARCHAR);//testo
								cstmtCct.execute();
								log.debug(session.getId() + " - executeCall complete");
								cstmtCct.close();
					 		} else {
								log.info(session.getId() + " - dashboard.SurveyWeb_Del("+Integer.valueOf(_Id)+",'D_"+i+"',"+j+")");
						    	cstmtCct = connCct.prepareCall("{call dashboard.SurveyWeb_Del(?,?,?)}");
								cstmtCct.setInt(1, Integer.valueOf(_Id));//id_survey
								cstmtCct.setString(2, "D_"+i );//id_messaggio
								cstmtCct.setInt(3, j);//option
								cstmtCct.execute();
								log.debug(session.getId() + " - executeCall complete");
								cstmtCct.close();
					 		}
						}catch (Exception e){
							log.error(session.getId() + " - Insert answer error : " + e);
						}
					}
					try{
						String qest = "Question"+i+"Msg";
						String desc = "Question"+i+"Desc";
 						String vQuest = request.getParameter(qest)!= null ?request.getParameter(qest) : null;
 						String vDesc = request.getParameter(desc)!= null ?request.getParameter(desc) : null;	
 						log.info(session.getId() + " - dashboard.SurveyWeb_Set("+Integer.valueOf(_Id)+",'D_"+i+"',null,'"+vDesc+"','"+vQuest+"')");
 				    	cstmtCct = connCct.prepareCall("{call dashboard.SurveyWeb_Set(?,?,?,?,?)}");
						
 						cstmtCct.setInt(1, Integer.valueOf(_Id));//id_survey
 						cstmtCct.setString(2, "D_"+i );//id_messaggio
 						cstmtCct.setNull(3, Types.INTEGER);//option
						if((vDesc == null)||(vDesc == "")){
							cstmtCct.setNull(4, Types.NVARCHAR);
						}
						else{
							cstmtCct.setString(4, vDesc);//description
						}
						if((vQuest == null)||(vQuest == "")){
							cstmtCct.setNull(5, Types.NVARCHAR);
						}else{
							cstmtCct.setString(5, vQuest);//testo
						}
						cstmtCct.execute();
						log.debug(session.getId() + " - executeCall complete");
						cstmtCct.close();
							
 					}catch (Exception npe){
						log.error(session.getId() + " - Insert answer description : " + npe);
					}
				}
				break;
			case "SurveyDetails":
				log.info(session.getId() + " - dashboard.SurveyWeb_GetSurveyInfo("+_Id+")");
				cstmtCct = connCct.prepareCall("{call dashboard.SurveyWeb_GetSurveyInfo(?)}");
				cstmtCct.setInt(1, Integer.valueOf(_Id));//id_survey
				rs = cstmtCct.executeQuery();
				log.debug(session.getId() + " - executeCall complete");
				if (rs.next() == false) {
					_Script = setEmptyFields(_Script);
		    	}else {
		    		do {
			        	if((rs.getString("data_ultima_modifica")== null)||(rs.getString("login_ultima_modifica")==null)){
			        		_Script += "clearInput();";
			        	}
			        	_Script +=" jQuery('#SurveyTitle').val('"+ rs.getString("descrizione") + "');";
						_Script +=" jQuery('#ultima_modifica').html('Modificata il "+ rs.getString("data_ultima_modifica") + " da " + rs.getString("login_ultima_modifica") +"' );";
			        }while(rs.next());
				}	
				break;
			case "SurveyDetailsFields":
				log.info(session.getId() + " - dashboard.SurveyWeb_GetSurvey("+_Id+")");
				cstmtCct = connCct.prepareCall("{call dashboard.SurveyWeb_GetSurvey(?)}");
				cstmtCct.setInt(1, Integer.valueOf(_Id));//id_survey
				rs = cstmtCct.executeQuery();
				log.debug(session.getId() + " - executeCall complete");
				
			 	if (rs.next() == false) {
					_Script += "clearInput();";
				} else {
					String question = "";
					_Script += "clearInput();";
					do {
			        	String _Testo = rs.getString("testo");
			        	String _Descrizione = rs.getString("descrizione");
			        	Integer answer = rs.getInt("opzione");
			    		switch (rs.getString("id_messaggio")){
							case "B":
								_Script += " jQuery(\"#WellcomeMsg\").val(\""+ _Testo + "\");";
// 								question = "-1";
								break;
							case "A":
								_Script += " jQuery(\"#GoodbyeMsg\").val(\""+ _Testo + "\");";
// 								question = "-1";
								break;
							case "D_1":
								question = "1";
								break;
							case "D_2":
								question = "2";
								break;
							case "D_3":
								question = "3";
								break;
							case "D_4":
								question = "4";
								break;
							case "D_5":
								question = "5";
								break;
						}
						try{
							if(question!= ""){
								int opt = Integer.valueOf(question);
								if(opt >= 0){
									if((_Testo!= null)&&(_Testo!= "")&&(_Testo!= "null")){
										_Script += " jQuery(\"#Question"+question+"Msg\").val(\""+  _Testo  + "\"); ";
										if((_Descrizione!= null)&&(_Descrizione!= "")&&(_Descrizione!= "null")){
											_Script += " jQuery(\"#Question"+question+"AnswerDesc\").val(\""+  _Descrizione  + "\");";
										}
									}else{
									  	if(answer != null){
									 		if((_Descrizione!= null)&&(_Descrizione!= "")&&(_Descrizione!= "null")){
									 			_Script += " jQuery(\"#Question"+question+"Answer"+answer+"Msg\").val(\""+  _Descrizione  + "\");";
									 			_Script += " jQuery(\"#Question"+question+"Answer"+answer+"ck\").prop(\"checked\", true);";
											}
									 		
										}
									}
								}else{
									
								}
							}else{
								switch (rs.getString("id_messaggio")){
								case "A":
									_Script += " jQuery(\"#GoodbyeMsg\").val(\""+  _Testo + "\");";
									break;
								case "B":
									_Script += " jQuery(\"#WellcomeMsg\").val(\""+  _Testo + "\");";
									break;
							}
							}
						}catch(Exception e){
							log.info(session.getId() + " - Error parse question " + e);
						}
				    } while (rs.next());
		      	}
				break;
			default:
		}
        _Script += "</script>";
        out.println(_Script);
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
		out.println("<script>jQuery('#ErrorCode').html("+e.hashCode()+"); jQuery('#ErrorMessage').html('"+e.getMessage()+"');jQuery('#Error').show()</script>");
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmtCct.close(); } catch (Exception e) {}
		try { cstmtUcs.close(); } catch (Exception e) {}
		try { connCct.close(); } catch (Exception e) {}
		try { connUcs.close(); } catch (Exception e) {}
	}
	log.info(session.getId() + " - ******* end page res ***** ");
%>

<%!
	String setEmptyFields(String str){
		String a = "";
		String b = "<script>";
		a += "<script> jQuery('#WellcomeMsg').val('');";
		a += " jQuery(''#GoodbyeMsg').val('');";
		for(int i = 1; i <=5; i++){
			a +=" jQuery('#Question"+i+"Msg').val('');";
			a +=" jQuery('#Question"+i+"AnswerDesc').val('');";
			for(int j = 0; j <=9; j++){
				b +=" jQuery('#Question"+i+"Answer"+j+"Msg').val('');";
				b +=" jQuery('#Question"+i+"Answer"+j+"ck').prop('checked', false);";
			}
		}
		a += "</script>";
		b += "</script>";
		return  a + b ;
	}
 
%>

<%!
// 	public void ExecuteQuery(Logger log, Connection conn, String count, String Field,String Id_survey, String Id_Messaggio, String Descrizione, String Testo, String Opzione){
		
// 		try{
		
// 			CallableStatement cs = conn.prepareCall("{call [dashboard].[SurveyWeb_Set](?,?,?,?,?)]}");
// 			cs.setString(1, Id_survey);
// 			cs.setString(2, Id_Messaggio);
// 			cs.setString(3, Opzione);
// 			cs.setString(4, Descrizione);
// 			cs.setString(5, Testo);
// 			cs.execute();
	
	
	
		
// 			log.debug("count "+ Field +" : " +count);
// 			String query = "";
// 			ResultSet _TotA = (conn.createStatement()).executeQuery(count);
// 			if (_TotA.next() == false) {
// 				log.debug("_TotA : empty");
// 			}else {
// 				do{
// 					int totA = _TotA.getInt("TOTAL");
// 					log.debug("totA : " + totA);
// 					if(totA == 0){
// 				 		query ="INSERT INTO ivr_realtime_cct.stc_configurazione.dettaglio_survey ( id_survey, id_messaggio,descrizione, testo, opzione ) values ( "+Id_survey +",'"+Id_Messaggio+"',"+Descrizione+","+Testo+","+Opzione+")"; 
// 					}else{
// 						String isNull = "IS NULL"; 
// 						String W_Id_survey = Id_survey != null ? "id_survey = "+Id_survey : "id_survey "+isNull;
// 						String W_Id_Messaggio = Id_Messaggio != null ? "id_messaggio = '"+Id_Messaggio+"'" : "id_messaggio "+isNull;
// 						String W_Descrizione = Descrizione != null ? "descrizione = '"+Descrizione+"'" : "descrizione "+isNull;
// 						String W_Testo = Testo != null ? "testo = '"+Testo+"'" : "testo "+isNull;
// 						String W_Opzione = Opzione != null ? "opzione = "+Opzione : "opzione "+isNull;
// 				 		query = "UPDATE ivr_realtime_cct.stc_configurazione.dettaglio_survey SET descrizione = "+Descrizione+", testo ="+Testo+" WHERE "+ W_Id_survey +" AND "+W_Id_Messaggio+" AND "+W_Opzione;
// 				 	}
// 					log.debug("Query : " +query);
// 					(conn.createStatement()).executeUpdate(query);
// 				}while (_TotA.next());
// 			}
// 			_TotA.close();
// 		}catch(Exception e){
// 			log.error(e);
// 		}
// 	}
%>