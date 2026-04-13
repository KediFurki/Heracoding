package comapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

@WebServlet("/UploadDownloadFrostEmergency")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150, // 10 MB
	maxFileSize = 1024 * 1024 * 150, // 50 MB
	maxRequestSize = 1024 * 1024 * 300) // 100 MB

public class UploadDownloadFrostEmergency extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public UploadDownloadFrostEmergency() {
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
			case "download":
				redirectPage = "FrostEmergencyGlobalDetails.jsp?";
				try {
					File frostfile = new File(name);
					os = response.getOutputStream();
					response.setContentType("application/octet-stream");
					response.setHeader("Content-Disposition", "attachment;filename=" + frostfile.getName());
					response.setHeader("Pragma", "private");
					response.setHeader("Cache-Control", "private, must-revalidate");
					response.setHeader("Accept-Ranges", "bytes");
					is = new FileInputStream(frostfile);
					IOUtils.copy(is, os);
					is.close();
					os.close();
					return;
				} catch (Exception e) {
					log.error("download error: "+ e.getMessage(),e);
					String redirectString = redirectPage+"action=search&ErrorCode=" + 4 + "&ErrorMessage=" + URLEncoder.encode("Messaggio non trovato: " +  name, StandardCharsets.UTF_8.toString()).replaceAll("'", " ");
					log.info(session.getId() + " - sendRedirect:" + redirectString);
					response.sendRedirect(redirectString);
					return;
				}
				 
			case "play":
				redirectPage = "FrostEmergencyGlobalDetails.jsp?";
				try {
//					ArrayList<String> aMessageDestination = new ArrayList<String>();
//					aMessageDestination = Utility.GetSystemParameters(session, Utility.IT_CCTF);
//					File directory = new File(aMessageDestination.get(0));
//					File[] fileList = directory.listFiles(new InternalFilter(name));
					File fileFrost = new File(name);
					if (fileFrost.exists()) {
						os = response.getOutputStream();
						response.setContentType("audio/" + FilenameUtils.getExtension(fileFrost.getName()));
						response.setHeader("Content-Disposition", "attachment;filename=" + fileFrost.getName());
						response.setHeader("Pragma", "private");
						response.setHeader("Cache-Control", "private, must-revalidate");
						response.setHeader("Accept-Ranges", "bytes");
						if (browserIE.equals("1")) {
							is = Utility.WavToMp3Converter(fileFrost);
						} else {
							is = new FileInputStream(fileFrost);
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
