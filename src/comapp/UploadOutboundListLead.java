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
//import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
//import java.util.Hashtable;
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
//import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

//import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
//import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
//import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

@WebServlet("/UploadOutboundListLead")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class UploadOutboundListLead extends HttpServlet {

	private static final long serialVersionUID = 1L;

	Logger log = Logger.getLogger("comapp." + this.getClass().getName());

	public UploadOutboundListLead() {
		super();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		log.info(session.getId() + " - ******* new request ***** ");
		String action = request.getParameter("action");
		String environment = (String)session.getAttribute("Environment");
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		log.info(session.getId() + " action: " + action);
		try {
			switch (action) {
			case "upload":
				Part filePart = request.getPart("uploadfile");
				String username = (String) session.getAttribute("UserName");
				Properties cs = ConfigServlet.getProperties();
				String upload_location = cs.getProperty("file-outbound-location");
				upload_location += File.separator + username;
				File _Dir_upload = new File(upload_location);
				if (!_Dir_upload.exists()) {
					_Dir_upload.mkdirs();
				}  
				log.info(session.getId() + " - username : " + username + " - file-outbound-location : " + upload_location);
				SimpleDateFormat sdf_dest = new SimpleDateFormat("yyyyMMddhhmmss");
				SimpleDateFormat sdf_from = new SimpleDateFormat("dd/MM/yyyy");
				int nRecordLetti = 0;
				try {
					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"OCS");
					log.info(session.getId() + " - connection OCS wait...");
					conn = ds.getConnection();
					
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
					try (BufferedReader br = new BufferedReader(new FileReader(upload_location))) {
						String line;
						while ((line = br.readLine()) != null) {
							if (!StringUtils.isBlank(line)) {
								String[] record = line.split(";");
								mapRecords.put(record[0], (record.length>1)?record[1]:"");
							}
						} 
					} catch (Exception ex) {}
					nRecordLetti = mapRecords.size();
					log.info(session.getId() + " - File : numero record caricati: "+nRecordLetti);
					log.info(session.getId() + " - File : fine caricamento csv in memoria");
					long endTime   = System.currentTimeMillis();
					long totalTime = endTime - startTime;
					log.info(session.getId() + " - DB : Caricato in "+totalTime+" ms");

					startTime = System.currentTimeMillis();
					log.info(session.getId() + " - DB : inizio inserimeto record");
					for (Map.Entry<String,String> entry : mapRecords.entrySet()) {
						log.info(session.getId() + " - dashboard.OutboundListLead_InsertCallingList('"+entry.getKey()+"','"+entry.getValue()+"')");
						cstmt = conn.prepareCall("{ call dashboard.OutboundListLead_InsertCallingList(?,?)} ");
		 				cstmt.setString(1, entry.getKey());
		 				cstmt.setString(2, entry.getValue());
		 				cstmt.execute();
		 				log.debug(session.getId() + " - executeCall complete");
						try {cstmt.close();} catch (Exception e) {}
					}
					endTime   = System.currentTimeMillis();
					totalTime = endTime - startTime;
					log.info(session.getId() + " - DB : Inserimento in "+totalTime+" ms");					
					log.info(session.getId() + " - DB : fine inserimeto");
					
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
