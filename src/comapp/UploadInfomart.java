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

@WebServlet("/UploadInfomart")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class UploadInfomart extends HttpServlet {

	private static final long serialVersionUID = 1L;

	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	Hashtable<String,String> CodificaPartech = new Hashtable<String,String>();
	Hashtable<String,String> MappaturaInOut = new Hashtable<String,String>();

	public UploadInfomart() {
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
		String partech = request.getParameter("partech");
		String genesys = request.getParameter("genesys");
		String in = request.getParameter("in");
		String out = request.getParameter("out");
		String sFilterDay = request.getParameter("change_filter_info");
		Connection connInf = null;
		CallableStatement cstmtInf = null;
		ResultSet rsInf = null;
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
				String upload_location = cs.getProperty("file-infomart-location");
				upload_location += File.separator + username;
				File _Dir_upload = new File(upload_location);
				if (!_Dir_upload.exists()) {
					_Dir_upload.mkdirs();
				}  
				log.info(session.getId() + " - username : " + username + " - file-infomart-location : " + upload_location);
				SimpleDateFormat sdf_dest = new SimpleDateFormat("yyyyMMddhhmmss");
				SimpleDateFormat sdf_from = new SimpleDateFormat("dd/MM/yyyy");
				int nRecordLetti = 0;
				int nAm = 0;
				int nPm = 0;
				int nInteraGiornata = 0;
				int nScarti = 0;
				String sScarti = "";
				try {
					readCodificaPartech(session);
					readMappaturaInOut(session);
		
					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"INF");
					log.info(session.getId() + " - connection INF wait...");
					connInf = ds.getConnection();
					
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
							if (line.contains("Partner")) continue;
							records.add(line);
						}
					} catch (Exception ex) {}
					nRecordLetti = records.size();
					log.info(session.getId() + " - File : numero record caricati: "+nRecordLetti);
					log.info(session.getId() + " - File : fine caricamento csv in memoria");
					
					log.info(session.getId() + " - DB : inizio cancellazione mese");
					for (int x = 0; x < records.size(); x++) {
						String[] record = records.get(0).split(";");
						if (record.length >= 4) {
							Date monthDate = sdf_from.parse(record[3]);
					 		log.info(session.getId() + " - dashboard.Infomart_RemoveMonth('"+environment+"','" + record[3] + "')");
							cstmtInf = connInf.prepareCall("{ call dashboard.Infomart_RemoveMonth(?,?)} ");
							cstmtInf.setString(1,environment);
							cstmtInf.setTimestamp(2, new Timestamp(monthDate.getTime()));
							cstmtInf.execute();					
							log.debug(session.getId() + " - executeCall complete");
							break;
						}
					}
					log.info(session.getId() + " - DB : fine cancellazione");

					log.info(session.getId() + " - DB : inizio inserimeto record");
					for (int x = 0; x < records.size(); x++) {
						log.debug(session.getId() + " - records:"+records.get(x));
						String cRecord = records.get(x);
						String[] record = cRecord.split(";");
						if (record.length >= 4) {
							String pService = codificaService(record[0]);
							if (!pService.isEmpty()) {
								String pGq;
								Date pData;
								int pAm = 0;
								int pPm = 0;
								int pInteraGiornata = 0;
								boolean bValido = true;
								pGq = pService + "_" + codificaPartech(record[1]) + "_GQ";
								pData = sdf_from.parse(record[3]);
								switch(record[2]) {
									case "08-14":
										pAm = 1; pPm = 0; pInteraGiornata = 0;
										nAm++;
										break;
									case "14-22":
										pAm = 0; pPm = 1; pInteraGiornata = 0;
										nPm++;
										break;
									case "TUTTE":
										pAm = 0; pPm = 0; pInteraGiornata = 1;
										nInteraGiornata++;
										break;
									default:
										bValido = false;
										break;
								}
								if (bValido) {
							 		log.info(session.getId() + " - dashboard.Infomart_AddDay('"+environment+"','" + pGq + "','"+record[3]+"',"+pAm+","+pPm+","+pInteraGiornata+")");
									cstmtInf = connInf.prepareCall("{ call dashboard.Infomart_AddDay(?,?,?,?,?,?)} ");
									cstmtInf.setString(1,environment);
									cstmtInf.setString(2, pGq);
									cstmtInf.setTimestamp(3, new Timestamp(pData.getTime()));
									cstmtInf.setInt(4, pAm);
									cstmtInf.setInt(5, pPm);
									cstmtInf.setInt(6, pInteraGiornata);
									cstmtInf.execute();
									log.debug(session.getId() + " - executeCall complete");
								} else {
									log.debug(session.getId() + " - Scarto per errore periodo giornata:"+cRecord);
									nScarti++;
									sScarti += cRecord+"\n";
								}
							} else {
								log.debug(session.getId() + " - Scarto per errore servizio:"+cRecord);
								nScarti++;
								sScarti += cRecord+"\n";
							}
						} else {
							log.debug(session.getId() + " - Scarto per errore riga:"+cRecord);
							nScarti++;
							sScarti += cRecord+"\n";
						}
					}
					log.info(session.getId() + " - DB : fine inserimeto");
					// JSON Return OK ==================================		
					JSONObject obj = new JSONObject();
					obj.put("res","OK");
					obj.put("read",nRecordLetti);
					obj.put("am",nAm);
					obj.put("pm",nPm);
					obj.put("interagiornata",nInteraGiornata);
					obj.put("scarti",nScarti);
					obj.put("infoscarti",sScarti);
					String re = obj.toString();
					log.info(session.getId() + " - File Uploaded Successfully ->" + re);
					response.getOutputStream().print(re);
				} catch (Exception e) {
					log.error(session.getId() + " : " + e.getMessage(), e);
					// JSON Return KO ==================================		
					JSONObject obj = new JSONObject();
					try {
						obj.put("res","KO");
						obj.put("errcode","2");
						obj.put("err",e.getMessage());
					} catch (JSONException e1) {}
					String re = obj.toString();
					log.error(session.getId() + " - File Uploaded Error ->" + re);
					response.getOutputStream().print(re);
				} finally {
					try {rsInf.close();} catch (Exception e) {}
					try {cstmtInf.close();} catch (Exception e) {}
					try {connInf.close();} catch (Exception e) {}
				}
				break;
			case "loadgiornate":
				try {
					SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");

					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"INF");
					log.info(session.getId() + " - connection INF wait...");
					connInf = ds.getConnection();
					JSONArray arr = new JSONArray();

			 		log.info(session.getId() + " - dashboard.Infomart_GetMonth('"+environment+"','" + sFilterDay + "')");
					cstmtInf = connInf.prepareCall("{ call dashboard.Infomart_GetMonth(?,?)} ");
					cstmtInf.setString(1,environment);
					cstmtInf.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
					rsInf = cstmtInf.executeQuery();					
					log.debug(session.getId() + " - executeCall complete");
			 		while (rsInf.next()) {
						String GQ = rsInf.getString("GQ");
						String DATA = rsInf.getString("DATA");
						String AM = rsInf.getString("AM");
						String PM = rsInf.getString("PM");
						String INTERA_GIORNATA = rsInf.getString("INTERA_GIORNATA");
						JSONObject cod = new JSONObject();
						cod.put("gq",GQ);
						cod.put("data",DATA);
						cod.put("am",(AM.equals("1")?"SI":"NO"));
						cod.put("pm",(PM.equals("1")?"SI":"NO"));
						cod.put("intera_giornata",(INTERA_GIORNATA.equals("1")?"SI":"NO"));
						arr.put(cod);
			 		}
					// JSON Return OK ==================================		
					JSONObject obj = new JSONObject();
					obj.put("res","OK");
					obj.put("arr",arr);
					String re = obj.toString();
					log.info(session.getId() + " - Lista Codifiche Successfully ->" + re);
					response.getOutputStream().print(re);
				} catch (Exception e) {
					log.error(session.getId() + " : " + e.getMessage(), e);
					// JSON Return KO ==================================		
					JSONObject obj = new JSONObject();
					try {
						obj.put("res","KO");
						obj.put("errcode","5");
						obj.put("err",e.getMessage());
					} catch (JSONException e1) {}
					String re = obj.toString();
					log.error(session.getId() + " - Lista Codifiche Error ->" + re);
					response.getOutputStream().print(re);
				} finally {
					try {rsInf.close();} catch (Exception e) {}
					try {cstmtInf.close();} catch (Exception e) {}
					try {connInf.close();} catch (Exception e) {}
				}
		 		break;
			case "loadcodifica":
				try {
					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
					log.info(session.getId() + " - connection CCC wait...");
					conn = ds.getConnection();
					JSONArray arr = new JSONArray();

			 		log.info(session.getId() + " - dashboard.Infomart_GetCodifiche('"+environment+"')");
					cstmt = conn.prepareCall("{ call dashboard.Infomart_GetCodifiche(?)} ");
					cstmt.setString(1,environment);
					rs = cstmt.executeQuery();					
					log.debug(session.getId() + " - executeCall complete");
					int iLoop = 1;
			 		while (rs.next()) {
						String servizio_partech = rs.getString("servizio_partech");
						String servizio_genesys = rs.getString("servizio_genesys");
						JSONObject cod = new JSONObject();
						cod.put("id",iLoop);
						cod.put("partech",servizio_partech);
						cod.put("genesys",servizio_genesys);
						arr.put(cod);
						iLoop++;
			 		}
					// JSON Return OK ==================================		
					JSONObject obj = new JSONObject();
					obj.put("res","OK");
					obj.put("arr",arr);
					String re = obj.toString();
					log.info(session.getId() + " - Lista Codifiche Successfully ->" + re);
					response.getOutputStream().print(re);
				} catch (Exception e) {
					log.error(session.getId() + " : " + e.getMessage(), e);
					// JSON Return KO ==================================		
					JSONObject obj = new JSONObject();
					try {
						obj.put("res","KO");
						obj.put("errcode","5");
						obj.put("err",e.getMessage());
					} catch (JSONException e1) {}
					String re = obj.toString();
					log.error(session.getId() + " - Lista Codifiche Error ->" + re);
					response.getOutputStream().print(re);
				} finally {
					try {rs.close();} catch (Exception e) {}
					try {cstmt.close();} catch (Exception e) {}
					try {conn.close();} catch (Exception e) {}
				}
				break;
			case "delcodifica":
				log.info(session.getId() + " - partech: " + partech + " - genesys: " + genesys);
				try {
					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
					log.info(session.getId() + " - connection CCC wait...");
					conn = ds.getConnection();
					
			 		log.info(session.getId() + " - dashboard.Infomart_DelCodifica('"+environment+"','"+partech+"','"+genesys+"')");
					cstmt = conn.prepareCall("{ call dashboard.Infomart_DelCodifica(?,?,?)} ");
					cstmt.setString(1,environment);
			 		cstmt.setString(2, partech);
			 		cstmt.setString(3, genesys);
					cstmt.execute();					
					log.debug(session.getId() + " - executeCall complete");
					// JSON Return OK ==================================		
					JSONObject obj = new JSONObject();
					obj.put("res","OK");
					String re = obj.toString();
					log.info(session.getId() + " - Del Codifica Successfully ->" + re);
					response.getOutputStream().print(re);
				} catch (Exception e) {
					log.error(session.getId() + " : " + e.getMessage(), e);
					// JSON Return KO ==================================		
					JSONObject obj = new JSONObject();
					try {
						obj.put("res","KO");
						obj.put("errcode","3");
						obj.put("err",e.getMessage());
					} catch (JSONException e1) {}
					String re = obj.toString();
					log.error(session.getId() + " - Del Codifica Error ->" + re);
					response.getOutputStream().print(re);
				} finally {
					try {cstmt.close();} catch (Exception e) {}
					try {conn.close();} catch (Exception e) {}
				}
				break;
			case "modcodifica":
				log.info(session.getId() + " - partech: " + partech + " - genesys: " + genesys);
				try {
					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
					log.info(session.getId() + " - connection CCC wait...");
					conn = ds.getConnection();
					
			 		log.info(session.getId() + " - dashboard.Infomart_ModCodifica('"+environment+"','"+partech+"','"+genesys+"')");
					cstmt = conn.prepareCall("{ call dashboard.Infomart_ModCodifica(?,?,?)} ");
					cstmt.setString(1,environment);
			 		cstmt.setString(2, partech);
			 		cstmt.setString(3, genesys);
					cstmt.execute();					
					log.debug(session.getId() + " - executeCall complete");
					// JSON Return OK ==================================		
					JSONObject obj = new JSONObject();
					obj.put("res","OK");
					String re = obj.toString();
					log.info(session.getId() + " - Mod Codifica Successfully ->" + re);
					response.getOutputStream().print(re);
				} catch (Exception e) {
					log.error(session.getId() + " : " + e.getMessage(), e);
					// JSON Return KO ==================================		
					JSONObject obj = new JSONObject();
					try {
						obj.put("res","KO");
						obj.put("errcode","4");
						obj.put("err",e.getMessage());
					} catch (JSONException e1) {}
					String re = obj.toString();
					log.error(session.getId() + " - Mod Codifica Error ->" + re);
					response.getOutputStream().print(re);
				} finally {
					try {cstmt.close();} catch (Exception e) {}
					try {conn.close();} catch (Exception e) {}
				}
				break;
			case "loadmappatura":
				try {
					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
					log.info(session.getId() + " - connection CCC wait...");
					conn = ds.getConnection();
					JSONArray arr = new JSONArray();

			 		log.info(session.getId() + " - dashboard.Infomart_GetMappature('"+environment+"')");
					cstmt = conn.prepareCall("{ call dashboard.Infomart_GetMappature(?)} ");
					cstmt.setString(1,environment);
					rs = cstmt.executeQuery();					
					log.debug(session.getId() + " - executeCall complete");
					int iLoop = 1;
			 		while (rs.next()) {
						String servizio_partech = rs.getString("servizio_in");
						String servizio_genesys = rs.getString("servizio_out");
						JSONObject cod = new JSONObject();
						cod.put("id",iLoop);
						cod.put("in",servizio_partech);
						cod.put("out",servizio_genesys);
						arr.put(cod);
						iLoop++;
			 		}
					// JSON Return OK ==================================		
					JSONObject obj = new JSONObject();
					obj.put("res","OK");
					obj.put("arr",arr);
					String re = obj.toString();
					log.info(session.getId() + " - Lista Mappature Successfully ->" + re);
					response.getOutputStream().print(re);
				} catch (Exception e) {
					log.error(session.getId() + " : " + e.getMessage(), e);
					// JSON Return KO ==================================		
					JSONObject obj = new JSONObject();
					try {
						obj.put("res","KO");
						obj.put("errcode","5");
						obj.put("err",e.getMessage());
					} catch (JSONException e1) {}
					String re = obj.toString();
					log.error(session.getId() + " - Lista Mappature Error ->" + re);
					response.getOutputStream().print(re);
				} finally {
					try {rs.close();} catch (Exception e) {}
					try {cstmt.close();} catch (Exception e) {}
					try {conn.close();} catch (Exception e) {}
				}
				break;
			case "delmappatura":
				log.info(session.getId() + " - in: " + in + " - out: " + out);
				try {
					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
					log.info(session.getId() + " - connection CCC wait...");
					conn = ds.getConnection();
					
			 		log.info(session.getId() + " - dashboard.Infomart_DelMappatura('"+environment+"','"+in+"','"+out+"')");
					cstmt = conn.prepareCall("{ call dashboard.Infomart_DelMappatura(?,?,?)} ");
					cstmt.setString(1,environment);
			 		cstmt.setString(2, in);
			 		cstmt.setString(3, out);
					cstmt.execute();					
					log.debug(session.getId() + " - executeCall complete");
					// JSON Return OK ==================================		
					JSONObject obj = new JSONObject();
					obj.put("res","OK");
					String re = obj.toString();
					log.info(session.getId() + " - Del Mappatura Successfully ->" + re);
					response.getOutputStream().print(re);
				} catch (Exception e) {
					log.error(session.getId() + " : " + e.getMessage(), e);
					// JSON Return KO ==================================		
					JSONObject obj = new JSONObject();
					try {
						obj.put("res","KO");
						obj.put("errcode","3");
						obj.put("err",e.getMessage());
					} catch (JSONException e1) {}
					String re = obj.toString();
					log.error(session.getId() + " - Del Mappatura Error ->" + re);
					response.getOutputStream().print(re);
				} finally {
					try {cstmt.close();} catch (Exception e) {}
					try {conn.close();} catch (Exception e) {}
				}
				break;
			case "modmappatura":
				log.info(session.getId() + " - in: " + in + " - out: " + out);
				try {
					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
					log.info(session.getId() + " - connection CCC wait...");
					conn = ds.getConnection();
					
			 		log.info(session.getId() + " - dashboard.Infomart_ModMappatura('"+environment+"','"+in+"','"+out+"')");
					cstmt = conn.prepareCall("{ call dashboard.Infomart_ModMappatura(?,?,?)} ");
					cstmt.setString(1,environment);
			 		cstmt.setString(2, in);
			 		cstmt.setString(3, out);
					cstmt.execute();					
					log.debug(session.getId() + " - executeCall complete");
					// JSON Return OK ==================================		
					JSONObject obj = new JSONObject();
					obj.put("res","OK");
					String re = obj.toString();
					log.info(session.getId() + " - Mod Mappatura Successfully ->" + re);
					response.getOutputStream().print(re);
				} catch (Exception e) {
					log.error(session.getId() + " : " + e.getMessage(), e);
					// JSON Return KO ==================================		
					JSONObject obj = new JSONObject();
					try {
						obj.put("res","KO");
						obj.put("errcode","4");
						obj.put("err",e.getMessage());
					} catch (JSONException e1) {}
					String re = obj.toString();
					log.error(session.getId() + " - Mod Codifica Error ->" + re);
					response.getOutputStream().print(re);
				} finally {
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

	private void readCodificaPartech(HttpSession session) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		try {
			Context ctx = new InitialContext();
			String environment = (String)session.getAttribute("Environment");
			DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
			log.info(session.getId() + " - connection CCC wait...");
			conn = ds.getConnection();
			
	 		log.info(session.getId() + " - dashboard.Infomart_GetCodifiche('"+environment+"')");
			cstmt = conn.prepareCall("{ call dashboard.Infomart_GetCodifiche(?)} ");
			cstmt.setString(1,environment);
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
	 		while (rs.next()) {
				String servizio_partech = rs.getString("servizio_partech");
				String servizio_genesys = rs.getString("servizio_genesys");
				CodificaPartech.put(servizio_partech, servizio_genesys);
	 		}
		} catch (Exception e) {
			log.error(session.getId() + " : " + e.getMessage(), e);
		} finally {
			try {rs.close();} catch (Exception e) {}
			try {cstmt.close();} catch (Exception e) {}
			try {conn.close();} catch (Exception e) {}
		}
	}
	
	private String codificaPartech(String partech) {
		String genesys = CodificaPartech.get(partech);
		if (StringUtils.isEmpty(genesys)) return partech;
		return genesys;
	}

	private void readMappaturaInOut(HttpSession session) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		try {
			Context ctx = new InitialContext();
			String environment = (String)session.getAttribute("Environment");
			DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
			log.info(session.getId() + " - connection CCC wait...");
			conn = ds.getConnection();
			
	 		log.info(session.getId() + " - dashboard.Infomart_GetMappature('"+environment+"')");
			cstmt = conn.prepareCall("{ call dashboard.Infomart_GetMappature(?)} ");
			cstmt.setString(1,environment);
			rs = cstmt.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
	 		while (rs.next()) {
				String servizio_in = rs.getString("servizio_in");
				String servizio_out = rs.getString("servizio_out");
				MappaturaInOut.put(servizio_in, servizio_out);
	 		}
		} catch (Exception e) {
			log.error(session.getId() + " : " + e.getMessage(), e);
		} finally {
			try {rs.close();} catch (Exception e) {}
			try {cstmt.close();} catch (Exception e) {}
			try {conn.close();} catch (Exception e) {}
		}
	}
	
	private String codificaService(String service) {
		String out = MappaturaInOut.get(service);
		if (StringUtils.isEmpty(out)) return service;
		return out;
//		switch (service) {
//			case "KOINE":
//				return "KN";
//			case "LINETECH":
//				return "LT";
//			case "VK_KOINE":
//				return "VK";
//			default:
//				return "";
//		}
	}
}
