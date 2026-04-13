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

@WebServlet("/DownloadTop")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class DownloadTop extends HttpServlet {

	private static final long serialVersionUID = 1L;

	Logger log = Logger.getLogger("comapp." + this.getClass().getName());

	public DownloadTop() {
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
			case "paging":
				log.info(session.getId() + " - Start Paging Top");
				String page = request.getParameter("page");
				log.info("Page("+page+")");
				JSONObject obj = new JSONObject();
				JSONArray rows = new JSONArray();

				ctx = new InitialContext();
				ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
				log.info(session.getId() + " - connection CCC wait...");
				conn = ds.getConnection();
				
				log.info(session.getId() + " - dashboard.Top_GetPages(100,"+page+",'"+environment+"')");
				cstmt = conn.prepareCall("{ call dashboard.Top_GetPages(?,?,?)} ");
				cstmt.setInt(1,100);
				cstmt.setInt(2,Integer.parseInt(page));
				cstmt.setString(3,environment);
				rs = cstmt.executeQuery();
				while (rs.next()) {
		 			String cod_cli = rs.getString("COD_CLI");
		 			String descrizione = rs.getString("DESCRIZIONE");
		 			String numero_telefono_1 = rs.getString("NUMERO_TELEFONO_1");
		 			String numero_telefono_2 = rs.getString("NUMERO_TELEFONO_2");
		 			String numero_telefono_3 = rs.getString("NUMERO_TELEFONO_3");
		 			String numero_telefono_4 = rs.getString("NUMERO_TELEFONO_4");
		 			String numero_telefono_5 = rs.getString("NUMERO_TELEFONO_5");
		 			numero_telefono_2 = ((StringUtils.isBlank(numero_telefono_2))?"":numero_telefono_2);
		 			numero_telefono_3 = ((StringUtils.isBlank(numero_telefono_3))?"":numero_telefono_3);
		 			numero_telefono_4 = ((StringUtils.isBlank(numero_telefono_4))?"":numero_telefono_4);
		 			numero_telefono_5 = ((StringUtils.isBlank(numero_telefono_5))?"":numero_telefono_5);
		 			String ts = rs.getString("TIME_STAMP");
		 			if (ts == null)
		 				ts = "";
					JSONObject row = new JSONObject();
					row.put("cod_cli", cod_cli);
					row.put("descrizione", descrizione);
					row.put("numero_telefono_1", numero_telefono_1);
					row.put("numero_telefono_2", numero_telefono_2);
					row.put("numero_telefono_3", numero_telefono_3);
					row.put("numero_telefono_4", numero_telefono_4);
					row.put("numero_telefono_5", numero_telefono_5);
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
}
