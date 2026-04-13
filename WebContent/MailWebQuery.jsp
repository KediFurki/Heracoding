<%@page import="comapp.ConfigServlet"%>
<%@page import="comapp.Utility"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.File"%>
<%@page import="java.io.FileInputStream"%>
<%@page import="java.io.FileOutputStream"%>
<%@page import="java.io.IOException"%>
<%@page import="java.io.InputStreamReader"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.Statement"%>
<%@page import="java.sql.Types"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.util.Date"%>
<%@page import="java.util.HashMap"%>
<%@page import="java.util.List"%>
<%@page import="java.util.Map"%>
<%@page import="java.util.Properties"%>
<%@page import="java.util.zip.ZipEntry"%>
<%@page import="java.util.zip.ZipOutputStream"%>
<%@page import="javax.activation.DataHandler"%>
<%@page import="javax.activation.FileDataSource"%>
<%@page import="javax.mail.Message"%>
<%@page import="javax.mail.Multipart"%>
<%@page import="javax.mail.Session"%>
<%@page import="javax.mail.internet.InternetAddress"%>
<%@page import="javax.mail.internet.MimeBodyPart"%>
<%@page import="javax.mail.internet.MimeMessage"%>
<%@page import="javax.mail.internet.MimeMultipart"%>
<%@page import="javax.mail.util.ByteArrayDataSource"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="org.apache.log4j.Logger"%>
<%@page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@page trimDirectiveWhitespaces="true"%>
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
	Connection conn = null;
	Statement stmt = null;
	CallableStatement cs = null;
	ResultSet rs = null;
	
	Map<String, String> persons = new HashMap<String, String>();
	
 	
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================
	String environment = (String)session.getAttribute("Environment");
	String dbsystemproperties = (String)session.getAttribute("DBSystemProperties");
	String _Command = request.getParameter("command"); 
	String _Id =request.getParameter("id");
	String _Download = ""; 
	String _Selected = ""; 
	
	
	int RECORD_COUNT = 0;

	Map<String, String[]> parameters = request.getParameterMap();
 
	try {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"UCS");
		log.info(session.getId() + " - connection UCS wait...");
		conn = ds.getConnection();

		String download_location ="";
		Properties prop = Utility.getSystemProperties(dbsystemproperties);
		
		download_location = prop.getProperty("file-download-location");
		
		File _Dir_download = new File(download_location);
		if (!_Dir_download.exists()) {
			_Dir_download.mkdirs();
		}  
		
		String query ="";
		String csv ="";
 		String _Script = "<script id=\"scrtmp\" type=\"text/javascript\">";
 		
 		
 		String _Filtro = "";
		String _Email = "";
		String _StatoData = "";
		String _Data_Dal = "";
		String _Data_Al = "";
		String _Mittente = "";
		String _Destinatario = "";
		String _Oggetto = "";
		String _Testo = "";
 
		String id = "";
		String threadId = ""; 
		String affidabilita = "";
		String startData = "";
		String endData = "";
		String user_name = "";
		String MAIL_FROM = "";
		String MAIL_TO = "";
		String MAIL_CC = "";
		String Subject = "";
		String documentId = "";
		String TempoLav = "";
		String structuredText = "";
		String allegati = "";
		String _Threadid = "";
		String attachId = "";
 		
		log.info(session.getId() + " - command:" + _Command+")");

		switch(_Command){
			case "Search":
				_Filtro = request.getParameter("Filtro");
				_Email = request.getParameter("Email");
				_StatoData = request.getParameter("StatoData");
				_Data_Dal = request.getParameter("Data_Dal");
				_Data_Al = request.getParameter("Data_Al");
				_Mittente = request.getParameter("Mittente");
				_Destinatario = request.getParameter("Destinatario");
				_Oggetto = request.getParameter("Oggetto");
				_Testo = request.getParameter("Testo");
				_Selected = request.getParameter("Selected"); 

				_Download = request.getParameter("Download"); 
				log.info(session.getId() + " - Download:" + _Download+")");

				persons =  Utility.readPersons(session, log);
				log.info(session.getId() + " - dashboard.MailWeb_Search('" +_Filtro + "','" +_Email +"','" +_StatoData +"','" +_Data_Dal +"','" +_Data_Al +"','" +_Mittente +"','" +_Destinatario +"','" +_Oggetto + "','" + _Testo+")");
				cs = conn.prepareCall("{call dashboard.MailWeb_Search(?,?,?,?,?,?,?,?,?)}");
				if(_Filtro.isEmpty())		{ cs.setNull(1, Types.NVARCHAR); }else { cs.setNString(1, _Filtro); 		}
				if(_Email.isEmpty()) 		{ cs.setNull(2, Types.NVARCHAR); }else { cs.setNString(2, _Email); 		}
				if(_StatoData.isEmpty())	{ cs.setNull(3, Types.NVARCHAR); }else { cs.setNString(3, _StatoData);	}
	   			if(_Data_Dal.isEmpty())		{ cs.setNull(4, Types.NVARCHAR); }else { cs.setNString(4, _Data_Dal); 	}
	   			if(_Data_Al.isEmpty())		{ cs.setNull(5, Types.NVARCHAR); }else { cs.setNString(5, _Data_Al); 		}
	   			if(_Mittente.isEmpty())		{ cs.setNull(6, Types.NVARCHAR); }else { cs.setNString(6, _Mittente); 	}
				if(_Destinatario.isEmpty())	{ cs.setNull(7, Types.NVARCHAR); }else { cs.setNString(7, _Destinatario); }
				if(_Oggetto.isEmpty())		{ cs.setNull(8, Types.NVARCHAR); }else { cs.setNString(8, _Oggetto); 		}
				if(_Testo.isEmpty())		{ cs.setNull(9, Types.NVARCHAR); }else { cs.setNString(9, _Testo); 		}
				rs = cs.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
				boolean first = true;
				String _table="";
		 		if(rs.next() == false){
// 					out.println("<script>jQuery('#ErrorCode').html('0x0000000'); jQuery('#ErrorMessage').html('la ricerca non ha restituito risultati');jQuery('#Error').show()</script>");
					out.println("<script>parent.showError('0x0000000','la ricerca non ha restituito risultati');</script>");

		 		}else{
					_table =  "<table id=\"stickytable_s\" class=\"roundedCorners small no_bottom\"> ";
					_table += "<thead>";
					
					_table += "<tr class=\"listGroupItem white\" >";
	  				_table += "		<td colspan=\"12\" style=\"border-bottom: 0px;\">";
					_table += "			<div class=\"container_img title right \"><a onclick=\"parent.exportResult(this)\"><img alt=\"\" class=\"\" src=\"images/Save_.png\" ></a></div>"; 
					_table += "		</td>"; 
					_table += "</tr>";
					
					_table += "<tr class=\"listGroupItem white\" >";
					_table += "		<td colspan=\"11\"><div class=\"title right \">Seleziona tutti</div></td>";
					_table += "		<td>";
					_table += "			<label class=\"switch right\">  ";
					_table += "				<input class =\"selectAll\" type=\"checkbox\" value=\"All\" onclick=\"selectAll(this);\"> ";
					_table += "				<span class=\"slider round green\"></span>";
					_table += "			</label>";
					_table += " 	</td>"; 	
					_table += "</tr>";
					
					_table += "<tr class=\"listGroupItem active green\" ><td>E-mail</th><td>Inizio Lavorazione</td><td>Fine Lavorazione</td><td>Tempo Lav.</td><td>Utente</td><td>Mittente</td><td>Destinatario</td><td>Oggetto</td><td>Allegati</td><td>Conv.</td><td>Perc.</td><td></td></tr>";

					_table += "</thead>";
					_table += "<tbody>";
					
					csv ="Inizio Lavorazione,Fine Lavorazione,Tempo Lavorazione,Utente,Mittente,Destinatario,Oggetto,Allegati,Conversazione";
		 			
					do{
						
						id= rs.getString("id");
						threadId =rs.getString("ThreadId") ;
						affidabilita = rs.getString("Affidabilita");
						startData = rs.getString("StartDate");
						endData = rs.getString("EndDate");
						String dbid = rs.getString("OwnerId");
						user_name = Utility.getPerson(persons, dbid);
						MAIL_FROM = rs.getString("MAIL_FROM");
						MAIL_TO = rs.getString("MAIL_TO");
						Subject = rs.getString("Subject");
						documentId = rs.getString("documentId");
						TempoLav = rs.getString("TempoLav");
						if (StringUtils.isBlank(TempoLav)) TempoLav = "0";

						if(_Selected.isEmpty()){
							csv += "\r\n";
							csv += startData+","+endData+","+String.format("%02d:%02d:%02d", (Long.parseLong(TempoLav)/3600),(Long.parseLong(TempoLav)%3600)/60 , Long.parseLong(TempoLav)%60)+","+user_name+","+MAIL_FROM+","+MAIL_TO+","+Subject+","+documentId+","+threadId;
						}else{
							if(_Selected.toLowerCase().contains(id.toLowerCase())){
								csv += "\r\n";
								csv += startData+","+endData+","+String.format("%02d:%02d:%02d", (Long.parseLong(TempoLav)/3600),(Long.parseLong(TempoLav)%3600)/60 , Long.parseLong(TempoLav)%60)+","+user_name+","+MAIL_FROM+","+MAIL_TO+","+Subject+","+documentId+","+threadId;
							}
						}
						
						_table += "<tr>";
						_table += "<td><input type=\"button\" class=\"button green\"  onclick=\"parent.openMail('"+id+"');\" value=\"Apri\" /></td>" +
									"<td>"+startData+"</td>" +
									"<td>"+endData+"</td>" +
									"<td>"+String.format("%02d:%02d:%02d", (Long.parseLong(TempoLav)/3600),(Long.parseLong(TempoLav)%3600)/60 , Long.parseLong(TempoLav)%60)+"</td>" +
									"<td>"+user_name+"</td>" +
									"<td>"+MAIL_FROM+"</td>" +
									"<td>"+MAIL_TO+"</td>" +
									"<td>"+Subject+"</td>" +
									"<td>"+documentId+"</td>" +
									"<td><input type=\"button\" class=\"button green\"  onclick=\"parent.openConversation('"+threadId+"');\" value=\"Apri\" /></td>" +
									"<td>"+affidabilita+"%</td><td><label class=\"switch\">";
						_table += "		<input class =\"abc\" type=\"checkbox\" value=\""+id+"\" onclick=\"\"> ";
						_table += "		<span class=\"slider round green\"></span>";
						_table += "	</label>  </td>"; 					
						_table += "</tr>";
 						first = false;
					}while(rs.next());
					_Script += "$(\"#stickytable_s\").stickyTableHeaders();";
				}
				_table += "</tbody></table>";
				if(!_Download.isEmpty() && _Download.toLowerCase().contains("si")){
					try{
						
						SimpleDateFormat sdf = new SimpleDateFormat("ddMMyy");
				        Date date = new Date();
						
						response.setContentType("text/csv");
						response.setHeader("Content-Disposition", "attachment; filename=\"Export_"+sdf.format(date)+".csv\"");
						out.write(csv);
					}catch(Exception ex){
// 						out.println("<script>jQuery('#ErrorCode').html("+ex.hashCode()+"); jQuery('#ErrorMessage').html('"+ex.getMessage()+"');jQuery('#Error').show()</script>");
						out.println("<script>parent.showError('"+ex.hashCode()+"','"+ex.getMessage()+"');</script>");
					}finally{
						 
					}
				}else{
					out.println(  _table );	
				}
				break;
			case "OpenMail":
				_Download = request.getParameter("Download"); 
				log.info(session.getId() + " - Download:" + _Download+")");

				log.info(session.getId() + " - dashboard.MailWeb_OpenMail('" + _Id + "')");
				cs = conn.prepareCall("{call dashboard.MailWeb_OpenMail(?)}");
				if(_Id.isEmpty())		{ cs.setNull(1, Types.VARCHAR); }else { cs.setString(1, _Id); 		}
				rs = cs.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
				_table="";
				if(rs.next() == false){
// 					out.println("<script>jQuery('#ErrorCode').html('0x0000000'); jQuery('#ErrorMessage').html('Errore nell'apertura della mail');jQuery('#Error').show()</script>");
					out.println("<script>parent.showError('0x0000000','Errore nell'apertura della mail');</script>");
				}else{
					threadId = rs.getString("ThreadId");
					startData = rs.getString("StartDate");
					endData = rs.getString("EndDate");
					MAIL_FROM = rs.getString("MAIL_FROM");
					MAIL_TO = rs.getString("MAIL_TO");
					MAIL_CC = rs.getString("MAIL_CC")!= null ? rs.getString("MAIL_CC") : ""; ;
					Subject = rs.getString("Subject");
					structuredText = rs.getString("structuredText");
					allegati = rs.getString("allegati")!= null ? rs.getString("allegati") : "";
					attachId = rs.getString("documentId")!= null ? rs.getString("documentId") : ""; 
					String[] allegati_list = allegati.split(",");
					String[] attach_list = attachId.split(",");
					 
					_table =  "<table class=\"roundedCorners small no_bottom\"> ";
					_table += "<tbody>";
					_table += "<tr class=\"listGroupItem active green\">";
	  				_table += "		<td colspan=\"2\">";
					_table += "			<div class=\"container_img title right\"><a id=\"download\" href=\"UploadDownloadEmail?command=DownloadMail&id="+_Id+"&Download=SI\"><img alt=\"\" class=\"\" src=\"images/SaveWhite_.png\" ></a></div>"; 
					_table += "		</td>"; 
					_table += "</tr>";
					_table += "<tr><td class='title'>Oggetto : </td><td class='title'>"+Subject+"</td></tr>";
					_table += "<tr><td class='title'>Da : </td><td class='title'>"+MAIL_FROM+"</td></tr>";
					_table += "<tr><td class='title'>A  : </td><td class='title'>"+MAIL_TO+"</td></tr>";
 					_table += "<tr><td class='title'>CC : </td><td class='title'>"+MAIL_CC+"</td></tr>";
 					_table += "<tr><td class='title'>Allegati : </td><td class='title'>";
 					
 					for(String name: allegati_list){
 						_table += name+"</br>"; 
 					}
 							
 							
 					
 					_table += "</td></tr>";
 					_table += "<tr><td colspan=\"2\">Ricezione: "+startData+" (inizio lavorazione: "+startData+", fine lavorazione "+endData+")</td></tr>";
 			 		_table += "<tr> <td colspan=\"2\">"+structuredText+"</td></tr>"; 
					_table += "</tbody></table>";
					
					if( _Download.toLowerCase().contains("si")){
						_Download = "si";
						
 						Message message = new MimeMessage(Session.getInstance(System.getProperties()));
				        message.setFrom(new InternetAddress(MAIL_FROM));
				        message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(MAIL_TO));
				        message.setSubject(Subject);
				        MimeBodyPart content = new MimeBodyPart();
				        Multipart multipart = new MimeMultipart();
				        
				        content.setDataHandler(new DataHandler(new ByteArrayDataSource(structuredText, "text/html")));
				        multipart.addBodyPart(content);
				        
				        log.info(session.getId() + " - dashboard.MailWeb_getAttachedFile('" + attachId + "')");
						CallableStatement cs_attach = conn.prepareCall("{call dashboard.MailWeb_getAttachedFile(?)}");
						if(attachId.isEmpty())		{ cs_attach.setNull(1, Types.VARCHAR); }else { cs_attach.setString(1, attachId); 		}
						ResultSet rs_attach = cs_attach.executeQuery();					
						log.debug(session.getId() + " - executeCall complete");
						if(rs_attach.next() == false) {
							
						}else {
							do {
								String TheName = rs_attach.getString("TheName");
								byte[] TheContent = rs_attach.getBytes("Content"); 
								String TheMimeType = rs_attach.getString("MimeType");
								int TheSize = rs_attach.getInt("TheSize");
								String a = "";
								
								File TheContentFile = new File(download_location +File.separator +TheName);
								FileOutputStream attach = new FileOutputStream(TheContentFile);
								attach.write(TheContent,0,TheSize);
//								
						        content = new MimeBodyPart();
						        javax.activation.DataSource source = new FileDataSource(new File(download_location +File.separator +TheName));
		 			            content.setDataHandler(new DataHandler(source));
		 			            content.setFileName(TheName);
		 			           	multipart.addBodyPart(content);
		 			           	attach.close();
//							
							}while(rs_attach.next());
						}
 						
 						
 			           	content = new MimeBodyPart();
 			            content.setDataHandler(new DataHandler(new ByteArrayDataSource(structuredText, "text/html")));
				        multipart.addBodyPart(content);
 	        
				        multipart.addBodyPart(content);
				        message.setContent(multipart, "text/html");
				        message.writeTo(new FileOutputStream(new File(download_location +File.separator +_Id+".eml")));
				        try( BufferedReader br = new BufferedReader( new InputStreamReader(new java.io.FileInputStream(download_location +File.separator +_Id+".eml"), "UTF-8" )))
				        {
				        	response.setContentType("application/octet-stream");
							response.setHeader("Content-Disposition", "attachment; filename=\"EMAIL_"+_Id+".eml\"");
				          	StringBuilder sb = new StringBuilder();
				           	String line;
				           	while(( line = br.readLine()) != null ) {
				           	   sb.append( line );
				           	   sb.append( '\n' );
				           	}
				           	out.write(sb.toString());
				        }
					}else{
						out.println("<script>parent.addButton('Conversazione','"+threadId+"');</script>");
						out.println(_table);	
					}
				}
				break;
			case "OpenConversation":
				_Threadid =request.getParameter("threadid");
				_Download = request.getParameter("Download");
				log.info(session.getId() + " - Download:" + _Download+")");

				persons =  Utility.readPersons(session, log);
				log.info(session.getId() + " - dashboard.MailWeb_OpenConversation('" + _Threadid+ "')");
				cs = conn.prepareCall("{call dashboard.MailWeb_OpenConversation(?)}");
				if(_Threadid.isEmpty())	{ cs.setNull(1, Types.VARCHAR); }else { cs.setString(1, _Threadid); }
				rs = cs.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
				_table="";
				if(rs.next() == false){
// 					out.println("<script>jQuery('#ErrorCode').html('0x0000000'); jQuery('#ErrorMessage').html('Errore nell'apertura della mail');jQuery('#Error').show()</script>");
					out.println("<script>parent.showError('0x0000000','Errore nel caricamento della conversazione');</script>");
				}else{
					_table =  "<table id=\"stickytable_c\" class=\"roundedCorners small no_bottom\">";
					_table += "<thead>";
					_table += "<tr class=\"listGroupItem active green\"  >";
					_table += "		<td colspan='9' style=\"border-bottom: 0px;\">";
					_table += "			<div class='title'></div>";
					_table += "		</td>"; 
					_table += "		<td style=\"border-bottom: 0px;\">";
					_table += "			<div class=\"container_img title right\"><a id=\"download\" href=\"UploadDownloadEmail?command=DownloadConversation&threadid="+ _Threadid+"&Download=SI\"><img alt=\"\" src=\"images/SaveWhite_.png\" ></a></div>"; 
					_table += "		</td>"; 
					_table += "</tr>";
					_table += "<tr class='listGroupItem active green'><td></td><td>StartDate</td><td>EndDate</td><td>TEMPOLAV</td><td>user_name</td><td>MAIL_FROM</td><td>MAIL_TO</td><td>MAIL_CC</td><td>Subject</td><td>Allegati</td></tr>";
					_table += "</thead>";
					_table += "<tbody>";
					do{
						id = rs.getString("id");
						startData = rs.getString("StartDate");
						endData = rs.getString("EndDate");
						MAIL_FROM = rs.getString("MAIL_FROM");
						MAIL_TO = rs.getString("MAIL_TO");
						MAIL_CC = rs.getString("MAIL_CC")!= null ? rs.getString("MAIL_CC") : ""; 
						Subject = rs.getString("Subject");
						String dbid = rs.getString("OwnerId");
						user_name = Utility.getPerson(persons, dbid);
						TempoLav = rs.getString("TEMPOLAV");
						documentId = rs.getString("allegati")!= null ? rs.getString("allegati") : "";
						_table += "<tr>";
						_table += "<td><input type='button' class='button green' onclick=\"parent.hideResult('#Result'); parent.openMail('"+id+"');\" value='Apri'/></td>";
						_table += "<td>"+startData+"</td>";
						_table += "<td>"+endData+"</td>";
						_table += "<td>"+TempoLav+"</td>";
	 					_table += "<td>"+user_name+"</td>";
	 					_table += "<td>"+MAIL_FROM+"</td>";
	 					_table += "<td>"+MAIL_TO+"</td>";
	 					_table += "<td>"+MAIL_CC+"</td>";
						_table += "<td>"+Subject+"</td>";
						_table += "<td>"+documentId+"</td>";
						_table += "</tr>";
					}while(rs.next());
					_Script += "$(\"#stickytable_c\").stickyTableHeaders();";
				} 
				_table += "</tbody></table>";
				out.println( _table );	
				break;	
			default:
				break;
		}
		
		if(_Download.isEmpty() || _Download == null || _Download.toLowerCase().contains("no")){
			_Script += "</script>";
        	out.println(_Script);
		}
	} catch (Exception e) {
		log.error(session.getId() + " - general: " + e.getMessage(), e);
// 		out.println("<script>jQuery('#ErrorCode').html("+e.hashCode()+"); jQuery('#ErrorMessage').html('"+e.getMessage()+"');jQuery('#Error').show()</script>");
		out.println("<script>parent.showError('"+e.hashCode()+"','"+e.getMessage()+"');</script>");
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { stmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
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


public static File zip(List<File> files, String filename) {
    File zipfile = new File(filename);
    // Create a buffer for reading the files
    byte[] buf = new byte[4096];
    try {
        // create the ZIP file
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipfile));
        // compress the files
        for(int i=0; i<files.size(); i++) {
            FileInputStream in = new FileInputStream(files.get(i).getCanonicalFile().getName());
            // add ZIP entry to output stream
            out.putNextEntry(new ZipEntry(files.get(i).getName()));
            // transfer bytes from the file to the ZIP file
            int len;
            while((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            // complete the entry
            out.closeEntry();
            out.finish();
            out.flush();
            in.close();
        }
        // complete the ZIP file
        
        out.close();
        return zipfile;
    } catch (IOException ex) {
        System.err.println(ex.getMessage());
    }
    return null;
}

 

%>