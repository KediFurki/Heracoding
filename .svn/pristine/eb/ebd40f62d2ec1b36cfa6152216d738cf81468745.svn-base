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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.apache.commons.io.IOUtils;
import java.util.Properties;

@WebServlet("/UploadPesse")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class UploadPesse extends HttpServlet {

	private static final long serialVersionUID = 1L;

	Logger log = Logger.getLogger("comapp." + this.getClass().getName());

	public UploadPesse() {
		super();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		Connection connOcs = null;
		CallableStatement cstmtOcs = null;
		ResultSet ocs_ResultSet = null;
		String upload_location = "";
		String file_name = "";
		String username = "";
		String Territorio = "";
		username = (String) session.getAttribute("UserName");
		Properties cs = ConfigServlet.getProperties();
		upload_location = cs.getProperty("file-upload-location");
		Part filePart = request.getPart("file");
		Territorio = request.getParameter("Terrotorio");
		String Livello =request.getParameter("Livello") != null ? request.getParameter("Livello") : "0";
	 	String FineValid =request.getParameter("FineValid") != null ? request.getParameter("FineValid") : "";
	 	String InizioDistacco =request.getParameter("InizioDistacco") != null ? request.getParameter("InizioDistacco") : "";
		SimpleDateFormat sdf_dest = new SimpleDateFormat("dd-MM-yyyy");
		SimpleDateFormat sdf_orig = new SimpleDateFormat("yyyy-MM-dd");
		upload_location += File.separator + username;
		File _Dir_upload = new File(upload_location);
		if (!_Dir_upload.exists()) {
			_Dir_upload.mkdirs();
		}  
		log.info(session.getId() + " - USERNAME : " + username);
		log.info(session.getId() + " - UPLOAD_LOCATION : " + upload_location);
		try {
			Context context = new InitialContext();
			DataSource ocs_dataSource = (DataSource) context.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"OCS");
			log.info(session.getId() + " - connection OCS wait...");
			connOcs = ocs_dataSource.getConnection();
			if (filePart != null) { 
				file_name = filePart.getSubmittedFileName();
				try {
					log.info(session.getId() + " - Upload : inizio upload");
					upload_location += File.separator + Territorio +"_"+file_name;
					InputStream is = filePart.getInputStream();
					FileOutputStream os = new FileOutputStream(new File(upload_location));
					IOUtils.copy(is, os);
					is.close();
					os.close();
					log.info(session.getId() + " - Upload : File Name : " + file_name);
					log.info(session.getId() + " - Upload : File Path : " + upload_location);
				} catch (Exception e) {
					log.warn("upload error", e);
				}
			}
			log.info(session.getId() + " - Upload : File salvato in  : " + upload_location);
			log.info(session.getId() + " - Upload : fine upload");
 			log.info(session.getId() + " - File : inizio caricamento csv in memoria");
			List<String> records = new ArrayList<>();
			try (BufferedReader br = new BufferedReader(new FileReader(upload_location))) {
				String line;
				while ((line = br.readLine()) != null) {
					records.add(line);
				}
			} catch (Exception ex) {}
			log.info(session.getId() + " - File : fine caricamento csv in memoria");
			log.info(session.getId() + " - DB : inizio svuotamento tabella TABCallingListPesse" + Territorio);
			log.info(session.getId() + " - dashboard.Pesse_svuotaTabellaRecordPesse('" + Territorio+ "')");
			cstmtOcs = connOcs.prepareCall("{call dashboard.Pesse_svuotaTabellaRecordPesse(?)}");
			cstmtOcs.setString(1, Territorio  );
			cstmtOcs.execute();
			cstmtOcs.close();
			log.info(session.getId() + " - DB : fine svuotamento tabella TABCallingListPesse" + Territorio);
			
			log.info(session.getId() + " - DB : inizio inserimeto record nella tabella TABCallingListPesse" + Territorio);
			int record_id = 1;
		 	
			for (int x = 0; x < records.size(); x++) {
				int chain_number = 0;
				String[] record = records.get(x).split(";");
				if (record.length < 7) {
					response.getOutputStream().print("jQuery('#ErrorCode').html('00000000x11'); jQuery('#ErrorMessage').html('Lista PESSE non correttamente formattata');jQuery('#Error').show();");
				}else {
					if(!record[0].contains("POD")) {
						for (int i = 2; i <= 5; i++) {
							if(!record[i].isEmpty()) {
								log.info(session.getId() + " - dashboard.Pesse_inserisciRecordPesse("+record_id+",'"+record[i]+"',"+x+","+chain_number+",'"+record[1]+"','"+record[0]+"',"+record[6]+","+Livello+",'"+sdf_dest.format(sdf_orig.parse(InizioDistacco))+"','"+sdf_dest.format(sdf_orig.parse(FineValid))+"','"+Territorio+"')");
								cstmtOcs = connOcs.prepareCall("{call dashboard.Pesse_inserisciRecordPesse(?,?,?,?,?,?,?,?,?,?,?)}");
								cstmtOcs.setInt(1, record_id  );
								cstmtOcs.setString(2, record[i]);
								cstmtOcs.setInt(3, x );
								cstmtOcs.setInt(4, chain_number );
								cstmtOcs.setString(5, record[1] );
								cstmtOcs.setString(6, record[0] );
								cstmtOcs.setInt(7, Integer.parseInt(record[6]));
								cstmtOcs.setInt(8, Integer.parseInt(Livello));
								cstmtOcs.setString(9, sdf_dest.format(sdf_orig.parse(InizioDistacco)) );
								cstmtOcs.setString(10, sdf_dest.format(sdf_orig.parse(FineValid)) );
								cstmtOcs.setString(11, Territorio );
								cstmtOcs.execute();
								record_id++;
								chain_number++;
							}
						}	
					}
				}
			}
			log.info(session.getId() + " - DB : fine inserimeto");
			log.info(session.getId() + " - File : inizio lettura file di scarti");
			log.info(session.getId() + " - File : fine lettura file");
			request.setAttribute("message", "File Uploaded Successfully");
			response.setContentType("text/javascript");
			String nome_territorio ="";
			switch(Territorio){
				case "IM": 
					nome_territorio = "IMOLA"; 
					break;
				case "MO": 
					nome_territorio = "MODENA"; 
					break;
			}
			
			response.getOutputStream().print("jQuery('#content_confirm').html('Lista PESSE di "+nome_territorio+" caricata correttamente');jQuery('#content_list').html('Caricati " + (record_id-1) + " contatti per "+nome_territorio+" dal file " + file_name +"'); jQuery('#titolo_confirm').html('Caricamento completato');jQuery('#modal_confirm').show();");
			log.info(session.getId() + " - File Uploaded Successfully");
		} catch (Exception e) {
			response.getOutputStream().print("jQuery('#ErrorCode').html(" + e.hashCode() + "); jQuery('#ErrorMessage').html(" + e.getMessage() + ");jQuery('#Error').show();");
			log.error(session.getId() + " : " + e.getMessage(), e);
		} finally {
			try {ocs_ResultSet.close();} catch (Exception e) {}
			try {cstmtOcs.close();} catch (Exception e) {}
			try {connOcs.close();} catch (Exception e) {}
		}
	}
}
