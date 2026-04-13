package comapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
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

@WebServlet("/UploadWhiteList")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class UploadWhiteList extends HttpServlet {

	private static final long serialVersionUID = 1L;

	Logger log = Logger.getLogger("comapp." + this.getClass().getName());

	public UploadWhiteList() {
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
				String upload_location = cs.getProperty("file-whitelist-location");
				upload_location += File.separator + username;
				File _Dir_upload = new File(upload_location);
				if (!_Dir_upload.exists()) {
					_Dir_upload.mkdirs();
				}  
				log.info(session.getId() + " - username : " + username + " - file-whitelist-location : " + upload_location);
				SimpleDateFormat sdf_dest = new SimpleDateFormat("yyyyMMddhhmmss");
				SimpleDateFormat sdf_from = new SimpleDateFormat("dd/MM/yyyy");
				int nRecordLetti = 0;
				try {
					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
					log.info(session.getId() + " - connection CCC wait...");
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
					
		 			log.info(session.getId() + " - File : inizio caricamento csv in memoria");
					List<String> records = new ArrayList<>();
					try (BufferedReader br = new BufferedReader(new FileReader(upload_location))) {
						String line;
						while ((line = br.readLine()) != null) {
							records.add(line);
						}
					} catch (Exception ex) {}
					nRecordLetti = records.size();
					log.info(session.getId() + " - File : numero record caricati: "+nRecordLetti);
					log.info(session.getId() + " - File : fine caricamento csv in memoria");
					
					log.info(session.getId() + " - DB : inizio cancellazione");
					if (deleteall.equalsIgnoreCase("ON")) {
				 		log.info(session.getId() + " - dashboard.WhiteList_DeleteAll('"+environment+"')");
						cstmt = conn.prepareCall("{ call dashboard.WhiteList_DeleteAll(?)} ");
						cstmt.setString(1,environment);
						cstmt.execute();					
						log.debug(session.getId() + " - executeCall complete");
						try {cstmt.close();} catch (Exception e) {}
					}
					log.info(session.getId() + " - DB : fine cancellazione");

					log.info(session.getId() + " - DB : inizio inserimeto record");
					for (int x = 0; x < records.size(); x++) {
						log.debug(session.getId() + " - records:"+records.get(x));
						String cRecord = records.get(x);
						String[] record = cRecord.split(";");
				 		log.info(session.getId() + " - dashboard.WhiteList_Add('"+environment+"','"+record[0]+"',"+((record.length>1)?"'"+record[1]+"'":"null")+")");
						cstmt = conn.prepareCall("{ call dashboard.WhiteList_Add(?,?,?)} ");
						cstmt.setString(1,environment);
						cstmt.setString(2, record[0]);
						if (record.length>1) {
							cstmt.setString(3, record[1]);
						} else {
							cstmt.setNull(3, Types.VARCHAR);
						}
						cstmt.execute();
						log.debug(session.getId() + " - executeCall complete");
					}
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
