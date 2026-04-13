package comapp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
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
import javax.servlet.http.Cookie;
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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@WebServlet("/DownloadAmbienteSpecial")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class DownloadAmbienteSpecial extends HttpServlet {

	private static final long serialVersionUID = 1L;

	Logger log = Logger.getLogger("comapp." + this.getClass().getName());

	public DownloadAmbienteSpecial() {
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
		Context ctx = null;
		DataSource ds = null;
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		log.info(session.getId() + " action: " + action);
		try {
			switch (action) {
			case "download":
				log.info(session.getId() + " - Start Download AmbienteSpecial");
				String downloadToken = request.getParameter("downloadToken");
				log.info("Token("+downloadToken+")");

				File csvOut = File.createTempFile("spc", ".csv");
				File zipOut = File.createTempFile("spc", ".zip");
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvOut, true)));
				
				ctx = new InitialContext();
				ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
				log.info(session.getId() + " - connection CCC wait...");
				conn = ds.getConnection();

				log.info(session.getId() + " - dashboard.AmbienteSpecial_Get('"+environment+"')");
				cstmt = conn.prepareCall("{ call dashboard.AmbienteSpecial_Get(?)} ");
				cstmt.setString(1,environment);
				rs = cstmt.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
				int i = 0;
				while (rs.next()) {
					i++;
					String numero_telefono = rs.getString("NUMERO_TELEFONO");
					String descrizione = rs.getString("DESCRIZIONE");
					String line = ((numero_telefono!=null)?numero_telefono:"")+";"+((descrizione!=null)?descrizione:"");
					bw.write(line);
					bw.newLine();
				}				
				bw.close();
				
				boolean zipOk = zipping(log, session, csvOut, "special.csv", zipOut);
				
				response.setContentType("application/zip");
				response.setHeader("Content-Disposition", "attachment;filename=\"special.zip\"");
				Cookie cookie = new Cookie("downloadToken", downloadToken);
				cookie.setPath("/");
				response.addCookie(cookie);
				OutputStream out = response.getOutputStream();
				FileInputStream fis = new FileInputStream(zipOut);
				int bytes;
				while ((bytes = fis.read()) != -1) {
					out.write(bytes);
				}
				fis.close();
				out.close();
				response.flushBuffer();
				csvOut.delete();
				zipOut.delete();
				log.info(session.getId() + " - End Download AmbienteSpecial");
				break;
			case "paging":
				log.info(session.getId() + " - Start Paging AmbienteSpecial");
				String page = request.getParameter("page");
				log.info("Page("+page+")");
				JSONObject obj = new JSONObject();
				JSONArray rows = new JSONArray();

				ctx = new InitialContext();
				ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
				log.info(session.getId() + " - connection CCC wait...");
				conn = ds.getConnection();
				
				log.info(session.getId() + " - dashboard.AmbienteSpecial_GetPages(100,"+page+",'"+environment+"')");
				cstmt = conn.prepareCall("{ call dashboard.AmbienteSpecial_GetPages(?,?,?)} ");
				cstmt.setInt(1,100);
				cstmt.setInt(2,Integer.parseInt(page));
				cstmt.setString(3,environment);
				rs = cstmt.executeQuery();
				while (rs.next()) {
		 			String numero_telefono = rs.getString("NUMERO_TELEFONO");
		 			String descrizione = rs.getString("DESCRIZIONE");
		 			String ts = rs.getString("TIME_STAMP");
		 			if (ts == null)
		 				ts = "";										
					JSONObject row = new JSONObject();
					row.put("numero_telefono", numero_telefono);
					row.put("descrizione", descrizione);
					row.put("ts", ts);
					rows.put(row);
				}
				obj.put("rows", rows);
				log.debug(session.getId() + " - executeCall complete");
				// JSON Return OK ==================================		
				obj.put("res","OK");
				String re = obj.toString();
				log.info(session.getId() + " - End Paging Successfully ->" + re);
				response.getOutputStream().print(re);
				break;
			}
		} catch (Exception e) {
			log.error(session.getId() + " : " + e.getMessage(), e);
		} finally {
			try { rs.close(); } catch (Exception e) {}
			try { cstmt.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}		
	}

	private static boolean zipping(Logger log, HttpSession session, File file, String namedfile, File zipfile) {
		log.info(session.getId() + " - Start zipping");
		byte[] buf = new byte[4096];
		try {
			ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipfile));
			log.info(session.getId() + " - zipping file:"+namedfile);
			FileInputStream in = new FileInputStream(file.getPath());
			out.putNextEntry(new ZipEntry(namedfile));
			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}
			out.closeEntry();
			in.close();
			out.finish();
			out.flush();
			out.close();
			log.info(session.getId() + " - End zipping");
			return true;
		} catch (IOException ex) {
			log.error(session.getId() + " - zipping Exception:" + ex);
			ex.printStackTrace();
		}
		return false;
	}
}
