package comapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.commons.io.IOUtils;
import java.util.Properties;

@WebServlet("/UploadForecast")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class UploadForecast extends HttpServlet {

	private static final long serialVersionUID = 1L;

	Logger log = Logger.getLogger("comapp." + this.getClass().getName());

	public UploadForecast() {
		super();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		HttpSession session = request.getSession();
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); 
		
		Part ForecastFile = request.getPart("ForecastFile");
		String ForecastOutSourcer = request.getParameter("ForecastOutSourcer");
		String ForecastData = request.getParameter("ForecastData");
		log.info(session.getId() + " ForecastOutSourcer: " + ForecastOutSourcer + " ForecastData: " + ForecastData + " ForecastFile: " + ForecastFile.getSubmittedFileName());
		
		try {
			Context context = new InitialContext();
			DataSource ds = (DataSource) context.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
			log.info(session.getId() + " - connection CCC wait...");
			conn = ds.getConnection();
			
			if (!ForecastOutSourcer.isEmpty() && ForecastFile != null) {
				List<String> records = new ArrayList<>();
				InputStream is = ForecastFile.getInputStream();
	            HSSFWorkbook workbook = new HSSFWorkbook(is);

	    		Date ForecastDay = dateFormat.parse(ForecastData);
	    		Date Monday = getMonday(ForecastDay);

	    		log.info(session.getId() + " - Lunedi: "+dateFormat.format(Monday));

	    		String SheetName;
	            String CodIvr;
	            int Graph;
	            HSSFSheet sheet;
	            	
	            log.info(session.getId() + " - dashboard.Graph_ReadImportConfig('" + ForecastOutSourcer + "')");
				cstmt = conn.prepareCall("{call dashboard.Graph_ReadImportConfig(?)}");
				cstmt.setString(1, ForecastOutSourcer);
				rs = cstmt.executeQuery();
				log.debug(session.getId() + " - executeCall complete");
				while (rs.next()) {
		            SheetName = rs.getString("SheetName");
		            CodIvr = rs.getString("CodIvr");
		            Graph = rs.getInt("Graph");
		            sheet = workbook.getSheet(SheetName);
		            if (sheet != null) {
						log.info(session.getId() + " - Read Sheet: "+SheetName);
		            	ReadForecastValue(session, conn, sheet, Monday, CodIvr, Graph);
		            }
				}
		        is.close();
		        workbook.close();
				
				JSONObject obj = new JSONObject();
				obj.put("res","OK");
				obj.put("msg","File Caricato Correttamente");
				String re = obj.toString();
		        response.getWriter().write(re);
		        response.getWriter().flush();
		        response.getWriter().close();
//		        response.setContentType("text/javascript");
//				response.getOutputStream().print("File Caricato Correttamente");
			} else {
				JSONObject obj = new JSONObject();
				obj.put("res","KO");
				obj.put("msg","Errore caricamento File o OutSources non Scelto");
				String re = obj.toString();
		        response.getWriter().write(re);
		        response.getWriter().flush();
		        response.getWriter().close();
//				response.setContentType("text/javascript");
//				response.getOutputStream().print("Errore caricamento File");
			}
		} catch (Exception e) {
			log.error(session.getId() + " : " + e.getMessage(), e);
			JSONObject obj = new JSONObject();
			try {
				obj.put("res","KO");
				obj.put("msg","Errore Caricamento file: "+e.getMessage());
			} catch (JSONException e1) {}
			String re = obj.toString();
	        response.getWriter().write(re);
	        response.getWriter().flush();
	        response.getWriter().close();
//			response.setContentType("text/javascript");
//			response.getOutputStream().print("Errore Caricamento file: "+e.getMessage());
		} finally {
			try { rs.close(); } catch (Exception e) {}
			try { cstmt.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}
	}
	
	private int RI_HOURSTART = 3;
	private int RI_MAXHOUREND = 98;
	private int CI_HOURSTART = 1;
	private int CI_MONDAY = 2;
	private int CI_SUNDAY = 8;
	
	
	private void ReadForecastValue(HttpSession session, Connection conn, HSSFSheet sheet, Date Monday, String CodIvr, int Graph) throws Exception {
		String Hours;
		String Value;
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm");
		CallableStatement cstmt = conn.prepareCall("{call dashboard.Graph_PutForecastValues(?,?,?,?)}");
		cstmt.setString(1, CodIvr);
		cstmt.setInt(2, Graph);

		for (int startRow = RI_HOURSTART; startRow <= RI_MAXHOUREND; startRow++) {
	        HSSFRow row = sheet.getRow(startRow);
	        if (row==null) { return; }

	        HSSFCell cell_datetime = row.getCell(CI_HOURSTART);
	        Hours = cell_datetime.getStringCellValue();
	        if (!Hours.equalsIgnoreCase("Totale")) {
	        	log.info(session.getId() + " - Hour:"+cell_datetime);
	    		for (int iDow = CI_MONDAY; iDow <= CI_SUNDAY; iDow++) {
	    			Date currDate = addDays(Monday,iDow-CI_MONDAY);
	    			currDate = setHours(currDate,Hours);
	    	        HSSFCell cell_dow = row.getCell(iDow);
	    	        double cell_hour = cell_dow.getNumericCellValue();
	    	        Value = ""+(Math.round(cell_hour/4));
					cstmt.setString(3, Value);
	    	        
					String strDay = dateFormat.format(currDate);  		
					log.info(session.getId() + " - dashboard.Graph_PutForecastValues('" + CodIvr + "','"+Graph+"','" + Value + "'," + strDay + "')");
					cstmt.setTimestamp(4, new Timestamp(currDate.getTime()));
					cstmt.execute();
					
					currDate = add15min(currDate);
					strDay = dateFormat.format(currDate);  		
					log.info(session.getId() + " - dashboard.Graph_PutForecastValues('" + CodIvr + "','"+Graph+"','" + Value + "'," + strDay + "')");
					cstmt.setTimestamp(4, new Timestamp(currDate.getTime()));
					cstmt.execute();
					
					currDate = add15min(currDate);
					strDay = dateFormat.format(currDate);  		
					log.info(session.getId() + " - dashboard.Graph_PutForecastValues('" + CodIvr + "','"+Graph+"','" + Value + "'," + strDay + "')");
					cstmt.setTimestamp(4, new Timestamp(currDate.getTime()));
					cstmt.execute();
					
					currDate = add15min(currDate);
					strDay = dateFormat.format(currDate);  		
					log.info(session.getId() + " - dashboard.Graph_PutForecastValues('" + CodIvr + "','"+Graph+"','" + Value + "'," + strDay + "')");
					cstmt.setTimestamp(4, new Timestamp(currDate.getTime()));
					cstmt.execute();
	    		}
	        }
		}
	}
	
    private static Date getMonday(Date pi_Day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(pi_Day);
        int dow = cal.get(Calendar.DAY_OF_WEEK);
        if (dow != Calendar.MONDAY) {
        	if (dow == Calendar.SUNDAY) dow += 7;
        	dow = dow - Calendar.MONDAY;
            cal.add(Calendar.DAY_OF_YEAR, -dow);
        }
        return cal.getTime();
    }	
	
    private static Date addDays(Date pi_Date, int pi_NumDays) {
		Calendar c = Calendar.getInstance(); 
		c.setTime(pi_Date); 
		c.add(Calendar.DATE, pi_NumDays);
		return (c.getTime());
    }

    private static Date setHours(Date pi_Date, String pi_Hours) throws Exception {
		SimpleDateFormat dateFormatIn = new SimpleDateFormat("dd/MM/yyyy");
		SimpleDateFormat dateFormatOut = new SimpleDateFormat("dd/MM/yyyy HH:mm");
		return dateFormatOut.parse(dateFormatIn.format(pi_Date)+" "+pi_Hours);
    }

    private static Date add15min(Date pi_Date) {
		Calendar c = Calendar.getInstance(); 
		c.setTime(pi_Date); 
		c.add(Calendar.MINUTE, 15);
		return (c.getTime());
    }
}
