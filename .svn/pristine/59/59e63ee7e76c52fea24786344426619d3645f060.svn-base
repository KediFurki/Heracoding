package comapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

@WebServlet("/UploadCallList")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class UploadCallList extends HttpServlet {

	private static final long serialVersionUID = 1L;

	Logger log = Logger.getLogger("comapp." + this.getClass().getName());

	public UploadCallList() {
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
		String scarto_location = "";
		String file_name = "";
		String username = "";
		String survey_Id = "";
		
		
		int numero_caricati = 0;
		int numero_scartati = 0;
		int maxRecord_id = 0;

		username = (String) session.getAttribute("UserName");

		Properties cs = ConfigServlet.getProperties();
		
		upload_location = cs.getProperty("file-upload-location");
		scarto_location = cs.getProperty("file-scarto-location");

		Part filePart = request.getPart("file");
		survey_Id = request.getParameter("id");
		
		upload_location += File.separator + username;
		File _Dir_upload = new File(upload_location);
		if (!_Dir_upload.exists()) {
			_Dir_upload.mkdirs();
		}  

		scarto_location += File.separator + username;
		File _Dir_scarto = new File(scarto_location);
		if (!_Dir_scarto.exists()) {
			_Dir_scarto.mkdirs();
 		}  
		
		log.info(session.getId() + " - USERNAME : " + username);
		log.info(session.getId() + " - UPLOAD_LOCATION : " + upload_location);
		log.info(session.getId() + " - SCARTO_LOCATION : " + scarto_location);

		try {
			Context context = new InitialContext();
			DataSource ocs_dataSource = (DataSource) context.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"OCS");
			log.info(session.getId() + " - connection OCS wait...");
			connOcs = ocs_dataSource.getConnection();
			
			if (filePart != null) {
				file_name = filePart.getSubmittedFileName();

				try {
					log.info(session.getId() + " - Upload : inizio upload");

					upload_location += File.separator + file_name;
					InputStream is = filePart.getInputStream();
					FileOutputStream os = new FileOutputStream(new File(upload_location));
					IOUtils.copy(is, os);
					is.close();
					os.close();

					log.info(session.getId() + " - Upload : File Name : " + file_name);
					log.info(session.getId() + " - Upload : File Path : " + upload_location);
					scarto_location += File.separator + file_name;
					File scarto = new File(scarto_location);
					if ((scarto.exists())) {
						scarto.delete();
					}
					log.info(session.getId() + " - Upload : File Scarto : " + scarto_location);
				} catch (Exception e) {
					log.warn("upload error", e);
				}
			}

			log.info(session.getId() + " - Upload : File salvato in  : " + upload_location);
			log.info(session.getId() + " - Upload : fine upload");
			log.info(session.getId() + " - DB : inizio database SELECT MAX_RECORD_ID");

			log.info(session.getId() + " - dashboard.SurveyWeb_GetMaxRecordCallingListSTC('" +survey_Id+ "')");
			cstmtOcs = connOcs.prepareCall("{call dashboard.SurveyWeb_GetMaxRecordCallingListSTC(?)}");
			cstmtOcs.setString(1, survey_Id);
			ocs_ResultSet = cstmtOcs.executeQuery();

 			if (ocs_ResultSet.next() == false) {} else {
				do {
					maxRecord_id = ocs_ResultSet.getInt("MAX_RECORD_ID");
					log.info(session.getId() + " - DB : MAX_RECORD_ID : " + maxRecord_id);
				} while (ocs_ResultSet.next());
			}
 			ocs_ResultSet.close();
			log.info(session.getId() + " - DB : fine database SELECT");
			log.info(session.getId() + " - File : inizio lettura csv");
 
			maxRecord_id = maxRecord_id + 1;
			int iChainId = maxRecord_id;
			int iChainNumber = 1;

			List<String> records = new ArrayList<>();
			try (BufferedReader br = new BufferedReader(new FileReader(upload_location))) {
				String line;
				while ((line = br.readLine()) != null) {
					records.add(line);
				}
			} catch (Exception ex) {

			}
			log.info(session.getId() + " - File : fine lettura csv");
			log.info(session.getId() + " - DB : inizio inserimeto record nella tabella TABCallingListSTCSurvey_" + survey_Id);
			SimpleDateFormat sdfDate = new SimpleDateFormat("dd/MM/yyyy");
			Date now = new Date();
			try { cstmtOcs.close(); } catch (Exception e) {}
			
			cstmtOcs = connOcs.prepareCall("{call dashboard.SurveyWeb_InsertCallingListSTC(?,?,?,?,?,?)}");
			cstmtOcs.setNString(1, survey_Id);

			for (int x = 0; x < records.size(); x++) {
				String number = records.get(x);
				boolean numero_valido = ScartoNumero(number, scarto_location);
				if (numero_valido == false) {
					log.info(session.getId() + " - dashboard.SurveyWeb_InsertCallingListSTC('" + survey_Id + "'," + maxRecord_id + ",'" + number + "'," + iChainId + "," +  (iChainNumber + x)  + ",'" + sdfDate.format(now)+ "')");
					cstmtOcs.setInt(2, maxRecord_id);
					cstmtOcs.setNString(3, number);
					cstmtOcs.setInt(4, iChainId);
					cstmtOcs.setInt(5, (iChainNumber + x));
					cstmtOcs.setNString(6, sdfDate.format(now));
					cstmtOcs.execute();
					numero_caricati++;
				} else {
					log.info(session.getId() + " - numero scartato : " + number);
				}
				maxRecord_id++;
			}
			log.info(session.getId() + " - DB : fine inserimeto");
			log.info(session.getId() + " - File : inizio lettura file di scarti");
			String return_list = "";

			if ((new File(scarto_location)).exists()) {
				try (BufferedReader br = new BufferedReader(new FileReader(scarto_location))) {
					String line;
					while ((line = br.readLine()) != null) {
						numero_scartati++;
						return_list += " " + line + " </br>";
					}
				}
			}
			return_list += "";
			log.info(session.getId() + " - File : fine lettura file");
			request.setAttribute("message", "File Uploaded Successfully");
			response.setContentType("text/javascript");
			response.getOutputStream().print("jQuery('#numero_caricati').html(" + numero_caricati + "); jQuery('#numero_scartati').html(" + numero_scartati + ");jQuery('#lista_numeri').find('label').remove(); jQuery('#lista_numeri').html('" + return_list + "');");

			log.info(session.getId() + " - File Uploaded Successfully");

		} catch (Exception e) {
			response.getOutputStream().print("jQuery('#ErrorCode').html(" + e.hashCode() + "); jQuery('#ErrorMessage').html(" + e.getMessage() + ");jQuery('#Error').show();");
			log.error(session.getId() + " : " + e.getMessage(), e);
		} finally {
			try { ocs_ResultSet.close(); } catch (Exception e) {}
			try { cstmtOcs.close(); } catch (Exception e) {}
			try { connOcs.close(); } catch (Exception e) {}
		}

	}

	protected boolean ScartoNumero(String number, String _Path) {
		boolean scarto = false;
		if (!number.startsWith("0") && !number.startsWith("3")) {
			scarto = true;
		}
		if (number.startsWith("00")) {
			scarto = true;
		}
		try {
			Double.parseDouble(number);
		} catch (NumberFormatException e) {
			scarto = true;
		}
		if (scarto == true) {
			SalvataggioScarti(number, _Path);
		}
		return scarto;
	}

	protected void SalvataggioScarti(String number, String _Path) {

		File f = new File(_Path);
		PrintWriter out = null;

		try {
			if (f.exists() && !f.isDirectory()) {
				out = new PrintWriter(new FileOutputStream(new File(_Path), true));
			} else {
				out = new PrintWriter(_Path);
			}
			out.append(number + "\n");
			out.close();
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

}
