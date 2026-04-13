package comapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.Part;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

@WebServlet("/UploadTechniciansList")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class UploadTechniciansList extends HttpServlet {

	private static final long serialVersionUID = 1L;

	Logger log = Logger.getLogger("comapp." + this.getClass().getName());

	public UploadTechniciansList() {
		super();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		log.info(session.getId() + " - ******* new request ***** ");
		String action = request.getParameter("action");
		String deleteall = request.getParameter("deleteall");
		deleteall = deleteall==null?"OFF":deleteall.toUpperCase();
		String withheader = request.getParameter("withheader");
		withheader = withheader==null?"OFF":withheader.toUpperCase();
		String environment = (String)session.getAttribute("Environment");
		Context ctx = null;
		DataSource ds = null;
		Connection conn = null;
		CallableStatement cstmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		log.info(session.getId() + " action: " + action);
		try {
			switch (action) {
			case "upload":
				Part filePart = request.getPart("uploadfile");
				String username = (String) session.getAttribute("UserName");
				Properties cs = ConfigServlet.getProperties();
				String upload_location = cs.getProperty("file-technicianslist-location");
				upload_location += File.separator + username;
				File _Dir_upload = new File(upload_location);
				if (!_Dir_upload.exists()) {
					_Dir_upload.mkdirs();
				}  
				log.info(session.getId() + " - username : " + username + " - file-technicianslist-location : " + upload_location);
				SimpleDateFormat sdf_dest = new SimpleDateFormat("yyyyMMddhhmmss");
				SimpleDateFormat sdf_from = new SimpleDateFormat("dd/MM/yyyy");
				int nRecordLetti = 0;
				try {
					ctx = new InitialContext();
					switch (environment) {
					case "CCT-F":
						ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTF");
						log.info(session.getId() + " - connection CCTF wait...");
						break;
					}
					conn = ds.getConnection();
					
					conn.setAutoCommit(false);

					try {
						String CodIvr =(String)session.getAttribute("CodIvr");
						
						log.info(session.getId() + " - Upload : inizio upload");
						if (filePart != null) { 
							String file_name = filePart.getSubmittedFileName();
							upload_location += File.separator + sdf_dest.format(new Date())+ "_" + file_name;
							InputStream is = filePart.getInputStream();
							FileOutputStream os = new FileOutputStream(new File(upload_location));
							IOUtils.copy(is, os);
							is.close();
							os.close();
						}
						log.info(session.getId() + " - Upload : File salvato in : " + upload_location);
						log.info(session.getId() + " - Upload : fine upload");
	
						long startTime = System.currentTimeMillis();
			 			log.info(session.getId() + " - File : inizio caricamento csv in memoria");
						Map<String,String> mapRecords = new HashMap<String,String>();
						boolean skipheader = withheader.equalsIgnoreCase("ON");
						try (BufferedReader br = new BufferedReader(new FileReader(upload_location))) {
							String line;
							while ((line = br.readLine()) != null) {
								if (skipheader) {
									skipheader = false;
									continue;
								}
								if (!StringUtils.isBlank(line)) {
									String[] record = line.split(";");
									mapRecords.put(record[0], line);
								}
							}
						} catch (Exception ex) {}
						nRecordLetti = mapRecords.size();
						log.info(session.getId() + " - File : numero record caricati: "+nRecordLetti);
						log.info(session.getId() + " - File : fine caricamento csv in memoria");
						long endTime   = System.currentTimeMillis();
						long totalTime = endTime - startTime;
						log.info(session.getId() + " - DB : Caricato in "+totalTime+" ms");					
						
						if (deleteall.equalsIgnoreCase("ON")) {
							log.info(session.getId() + " - DB : inizio cancellazione");
					 		log.info(session.getId() + " - dashboard.TechniciansList_DeleteAll('"+CodIvr+"')");
							cstmt = conn.prepareCall("{ call dashboard.TechniciansList_DeleteAll(?)} ");
							cstmt.setInt(1, Integer.parseInt(CodIvr));
							cstmt.execute();					
							log.debug(session.getId() + " - executeCall complete");
							try {cstmt.close();} catch (Exception e) {}
							log.info(session.getId() + " - DB : fine cancellazione");
						} else {
							startTime = System.currentTimeMillis();
							log.info(session.getId() + " - DB : inizio purging record");
							pstmt = conn.prepareStatement("DELETE FROM ivr_configurazione.lista_tecnici" +
														  " WHERE COD_IVR = '"+CodIvr+"'" +
														  " AND NUMERO_TELEFONO = ?");
							for (Map.Entry<String,String> entry : mapRecords.entrySet()) {
								pstmt.setString(1, entry.getKey());
								pstmt.addBatch();
							}
							int rd[] = pstmt.executeBatch();
							try {pstmt.close();} catch (Exception e) {}
							endTime   = System.currentTimeMillis();
							totalTime = endTime - startTime;
							log.info(session.getId() + " - DB : Purging in "+totalTime+" ms");					
						}
						
						startTime = System.currentTimeMillis();
						log.info(session.getId() + " - DB : inizio inserimeto record");
						Calendar cal = Calendar.getInstance();
						pstmt = conn.prepareStatement("INSERT INTO ivr_configurazione.lista_tecnici (" +
														"NUMERO_TELEFONO,NOM_COG,ESIGENZA,TIME_STAMP,COD_IVR" +
													  ") VALUES	(" +
													  	"?,?,?,?,'"+CodIvr+"'" +
													  ")");
						for (Map.Entry<String,String> entry : mapRecords.entrySet()) {
							String line = entry.getValue();
							String[] record = line.split(";");
							pstmt.setString(1, entry.getKey());
							pstmt.setString(2, record[1]);
							pstmt.setString(3, record[2]);
							pstmt.setTimestamp(4, new Timestamp(cal.getTime().getTime()));
							pstmt.addBatch();
						}
						int ri[] = pstmt.executeBatch();
						endTime   = System.currentTimeMillis();
						totalTime = endTime - startTime;
						log.info(session.getId() + " - DB : Inserimento in "+totalTime+" ms");					
						log.info(session.getId() + " - DB : fine inserimeto");
						
						conn.commit();
					} catch (Exception e1) {
						log.error(session.getId() + " - action error: " + e1.getMessage(), e1);
						log.info(session.getId() + " - DB : RollBack");
						conn.rollback();
						throw e1;
					}
					conn.setAutoCommit(true);
					
					// JSON Return OK ==================================		
					JSONObject obj = new JSONObject();
					obj.put("res","OK");
					obj.put("read",nRecordLetti);
					String re = obj.toString();
					log.info(session.getId() + " - File Uploaded Successfully ->" + re);
					response.getOutputStream().print(re);
				} catch (Exception e) {
					log.error(session.getId() + " : " + e.getMessage(), e);
					// JSON Return KO ==================================		
					JSONObject obj = new JSONObject();
					try {
						obj.put("res","KO");
						obj.put("errcode","1");
						obj.put("err",e.getMessage());
					} catch (JSONException e1) {}
					String re = obj.toString();
					log.error(session.getId() + " - File Uploaded Error ->" + re);
					response.getOutputStream().print(re);
				} finally {
					try {rs.close();} catch (Exception e) {}
					try {cstmt.close();} catch (Exception e) {}
					try {pstmt.close();} catch (Exception e) {}
					try {conn.close();} catch (Exception e) {}
				}
				break;
			}
		} catch (Exception e) {
			log.error(session.getId() + " : " + e.getMessage(), e);
			// JSON Return KO ==================================		
			JSONObject obj = new JSONObject();
			try {
				obj.put("res","KO");
				obj.put("errcode","1");
				obj.put("err",e.getMessage());
			} catch (JSONException e1) {}
			String re = obj.toString();
			log.error(session.getId() + " - Servlet Error ->" + re);
			response.getOutputStream().print(re);
		}		
	}
}
