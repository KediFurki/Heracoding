package comapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
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

import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

@WebServlet("/UploadDownloadEmergency")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class UploadDownloadEmergency extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public UploadDownloadEmergency() {
		super();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		doPost(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Logger log = Logger.getLogger("comapp." + this.getClass().getName());
		HttpSession session = request.getSession();
		log.info(session.getId() + " - ******* new request ***** ");
		String action = request.getParameter("action");
		String name = request.getParameter("name");
		String browserIE = request.getParameter("browserIE");
		log.info(session.getId() + " action: " + action + " name: " + name + " browserIE: " + browserIE);
		String redirectPage = "";
		OutputStream os = null;
		InputStream is = null;
		try {
			switch (action) {
			case "upload":
				redirectPage = "Emergency.jsp?";
				Connection connCcc = null;
				CallableStatement cstmtCcc = null;
				ResultSet rs = null;
				try {
					ArrayList<String> aEmergencyDestination = new ArrayList<String>();
					aEmergencyDestination = Utility.GetSystemParameters(session, Utility.IT_CCC);
					Context ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
					log.info(session.getId() + " - connection CCC wait...");
					connCcc = ds.getConnection();
					Part filePart = request.getPart("file");
					log.info(session.getId() + " - filePart.getSubmittedFileName(): " + filePart.getSubmittedFileName());
					if (StringUtils.isBlank(name)) {
						name = filePart.getSubmittedFileName();
					} else {
						name = name + "." + FilenameUtils.getExtension(filePart.getSubmittedFileName());
					}
					log.info(session.getId() + " - name:" + name);
					for (String path : aEmergencyDestination) {
						path += "/" + name;
						log.info(session.getId() + " upload path:" + path);
						if (new File(path).exists()) {
							String redirectString = redirectPage+"ErrorCode=" + 1 + "&ErrorMessage=" + URLEncoder.encode("file già esistente in: " + path, StandardCharsets.UTF_8.toString()).replaceAll("'", " ");
							log.info(session.getId() + " - sendRedirect:" + redirectString);
							response.sendRedirect(redirectString);
							return;
						}
					}
					String dest="";
					try {
						for (String path : aEmergencyDestination) {
							dest = path+ "/" + name;						
							is = filePart.getInputStream();
							os = new FileOutputStream(new File(dest));
							IOUtils.copy(is, os);
							is.close();
							os.close();
							log.info(session.getId() + " - copy from: " + filePart.getSubmittedFileName() + " to: " + dest);
						}
					} catch (Exception e) {
						log.error(session.getId() + " - copy: "+dest+" error: " + e.getMessage(), e);
						// rollback
						for (String path : aEmergencyDestination) {
							try {
								path += "/" + name;
								File f = new File(path);
								f.delete();
							} catch (Exception ex) {
								log.debug(session.getId() + " - rollback error:" + ex.getMessage(), ex);
							}
						}
						String redirectString = redirectPage+"ErrorCode=" + 2 + "&ErrorMessage=" + URLEncoder.encode("impossibile copiare il file "+dest+": " +  e.getMessage(), StandardCharsets.UTF_8.toString()).replaceAll("'", " ");
						log.info(session.getId() + " - sendRedirect:" + redirectString);
						response.sendRedirect(redirectString);
						return;
					}
					String CodIvr =(String)session.getAttribute("CodIvr");
					String Status ="OFF";
					log.info(session.getId() + " - dashboard.Emergency_AddMsg('" + CodIvr + "','"	+ Status + "','','" + FilenameUtils.removeExtension(name) + "')");
					cstmtCcc = connCcc.prepareCall("{ call dashboard.Emergency_AddMsg(?,?,?,?)} ");
					cstmtCcc.setString(1, CodIvr);
					cstmtCcc.setString(2, Status);
					cstmtCcc.setString(3, "");
					cstmtCcc.setString(4, FilenameUtils.removeExtension(name));
					cstmtCcc.execute();						
					log.debug(session.getId() + " - executeQuery complete");
				} catch (Exception e) {
					log.error(session.getId() + " - general: " + e.getMessage(), e);
				} finally {
					try {rs.close();} catch (Exception e) {}
					try {cstmtCcc.close();} catch (Exception e) {}
					try {connCcc.close();} catch (Exception e) {}
				}
				break;
			case "play":
				redirectPage = "Emergency.jsp?";
				try {
					ArrayList<String> aEmergencyDestination = new ArrayList<String>();
					aEmergencyDestination = Utility.GetSystemParameters(session, Utility.IT_CCC);
					File directory = new File(aEmergencyDestination.get(0));
					File[] fileList = directory.listFiles(new InternalFilter(name));
					if (fileList.length>0) {
						os = response.getOutputStream();
						response.setContentType("audio/" + FilenameUtils.getExtension(fileList[0].getName()));
						response.setHeader("Content-Disposition", "attachment;filename=" + fileList[0].getName());
						response.setHeader("Pragma", "private");
						response.setHeader("Cache-Control", "private, must-revalidate");
						response.setHeader("Accept-Ranges", "bytes");
						if (browserIE.equals("1")) {
							is = Utility.WavToMp3Converter(fileList[0]);
						} else {
							is = new FileInputStream(fileList[0]);
						}
						IOUtils.copy(is, os);
						is.close();
						os.close();
					} else {
						log.info(session.getId() + " - Messaggio non trovato: " +  name);
						response.setStatus(HttpServletResponse.SC_NOT_FOUND);
					}
					return;
				} catch (ClientAbortException cae) {
					return;
				} catch (Exception e) {
					log.error("download error: "+ e.getMessage(),e);
					String redirectString = redirectPage+"ErrorCode=" + 4 + "&ErrorMessage=" + URLEncoder.encode("Messaggio non trovato: " +  name, StandardCharsets.UTF_8.toString()).replaceAll("'", " ");
					log.info(session.getId() + " - sendRedirect:" + redirectString);
					response.sendRedirect(redirectString);
					return;
				}
				 
			case "download":
				redirectPage = "Emergency.jsp?";
				try {
					ArrayList<String> aEmergencyDestination = new ArrayList<String>();
					aEmergencyDestination = Utility.GetSystemParameters(session, Utility.IT_CCC);
					File directory = new File(aEmergencyDestination.get(0));
					File[] fileList = directory.listFiles(new InternalFilter(name));
					if (fileList.length>0) {
						os = response.getOutputStream();
						response.setContentType("audio/" + FilenameUtils.getExtension(fileList[0].getName()));
						response.setHeader("Content-Disposition", "attachment;filename=" + fileList[0].getName());
						response.setHeader("Pragma", "private");
						response.setHeader("Cache-Control", "private, must-revalidate");
						response.setHeader("Accept-Ranges", "bytes");
						is = new FileInputStream(fileList[0]);
						IOUtils.copy(is, os);
						is.close();
						os.close();
					} else {
						log.info(session.getId() + " - Messaggio non trovato: " +  name);
						String redirectString = redirectPage+"ErrorCode=" + 5 + "&ErrorMessage=" + URLEncoder.encode("Messaggio non trovato: " +  name, StandardCharsets.UTF_8.toString()).replaceAll("'", " ");
						log.info(session.getId() + " - sendRedirect:" + redirectString);
						response.sendRedirect(redirectString);
					}
					return;
				} catch (Exception e) {
					log.error("download error: "+ e.getMessage(),e);
					String redirectString = redirectPage+"ErrorCode=" + 4 + "&ErrorMessage=" + URLEncoder.encode("Messaggio non trovato: " +  name, StandardCharsets.UTF_8.toString()).replaceAll("'", " ");
					log.info(session.getId() + " - sendRedirect:" + redirectString);
					response.sendRedirect(redirectString);
					return;
				}
				 
			default:
				String redirectString = redirectPage+"ErrorCode=" + 3 + "&ErrorMessage=" + URLEncoder.encode("comando non riconosciuto: " +  action, StandardCharsets.UTF_8.toString()).replaceAll("'", " ");
				log.info(session.getId() + " - sendRedirect:" + redirectString);
				response.sendRedirect(redirectString);
				return;
			}

		} catch (Exception e) {
			log.error(session.getId() + " - general: " + e.getMessage(), e);
		} finally {
			try { is.close(); } catch (Exception e) {}
			try { os.close(); } catch (Exception e) {}		
		}
		String redirectString = redirectPage;
		log.info(session.getId() + " - sendRedirect:" + redirectString);
		response.sendRedirect(redirectString);
	}

	class InternalFilter implements FilenameFilter {
		String internal_name;
		InternalFilter(String internal_name) {
			this.internal_name = internal_name;
		}
		@Override
		public boolean accept(File dir, String fname) {
			return (StringUtils.startsWithIgnoreCase(fname, internal_name));
		}
	}
}
