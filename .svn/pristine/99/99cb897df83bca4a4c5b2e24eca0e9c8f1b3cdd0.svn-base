package comapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

/**
 * Servlet implementation class UploadDownloadEmail
 */
@WebServlet("/UploadDownloadEmail")
public class UploadDownloadEmail extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public UploadDownloadEmail() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		HttpSession session = request.getSession();
		String environment = (String)session.getAttribute("Environment");

		Logger log = Logger.getLogger("comapp." + this.getClass().getName());
		log.info(session.getId() + " - ******* new request ***** ");

		Connection connUcs = null;
		CallableStatement cstmtUcs = null;
		ResultSet rsUcs = null;

		String _Command = request.getParameter("command");
		String _Id = request.getParameter("id");

//		String _Download = "";
//		String _Selected = "";

		String id = "";
//		String affidabilita = "";
//		String startData = "";
//		String endData = "";
//		String user_name = "";
		String MAIL_FROM = "";
		String MAIL_TO = "";
//		String MAIL_CC = "";
		String Subject = "";
//		String documentId = "";
//		String TempoLav = "";
		String structuredText = "";
//		String allegati = "";
		String _Threadid = "";
		String attachId = "";
		String download_location = "";
		File zip = null;
		
		try {
			Context ctx = new InitialContext();
			DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + "UCS");
			Properties prop = Utility.getSystemProperties(environment);
			download_location = prop.getProperty("file-download-location");
			File _Dir_download = new File(download_location);
			if (!_Dir_download.exists()) {
				_Dir_download.mkdirs();
			}
			log.info(session.getId() + " - connection UCS wait...");
			connUcs = ds.getConnection();
			_Threadid = request.getParameter("threadid");
//			_Download = request.getParameter("Download"); 

			switch (_Command) {
			case "DownloadDocument":
				log.info(session.getId() + " - dashboard.MailWeb_getAttachedFile('" + _Id + "')");
				cstmtUcs = connUcs.prepareCall("{call dashboard.MailWeb_getAttachedFile(?)}");
				if (_Id.isEmpty()) {
					cstmtUcs.setNull(1, Types.VARCHAR);
				} else {
					cstmtUcs.setString(1, _Id);
				}
				ResultSet rs_doc = cstmtUcs.executeQuery();
				if (rs_doc.next() == false) {
				} else {
					do {
						String TheName = rs_doc.getString("TheName");
						byte[] TheContent = rs_doc.getBytes("Content");
//						String TheMimeType = rs_doc.getString("MimeType");
						int TheSize = rs_doc.getInt("TheSize");
//						String a = "";
						File TheContentFile = new File(download_location + File.separator + TheName);
						FileOutputStream attach = new FileOutputStream(TheContentFile);
						attach.write(TheContent, 0, TheSize);
						try (BufferedReader br = new BufferedReader(new InputStreamReader(new java.io.FileInputStream(TheContentFile), "UTF-8"))) {
							response.setContentType("application/octet-stream");
							response.setHeader("Content-Disposition", "attachment; filename=\"" + TheContentFile.getName() + "\"");
							StringBuilder sb = new StringBuilder();
							String line;
							while ((line = br.readLine()) != null) {
								sb.append(line);
								sb.append('\n');
							}
							response.getOutputStream().print(sb.toString());
						}
 			           	attach.close();
					} while (rs_doc.next());
				}
				rs_doc.close();
				break;
			case "DownloadConversation":
				log.info(session.getId() + " - dashboard.MailWeb_OpenConversation('" + _Threadid + "')");
				cstmtUcs = connUcs.prepareCall("{call dashboard.MailWeb_OpenConversation(?)}");
				if (_Threadid.isEmpty()) {
					cstmtUcs.setNull(1, Types.VARCHAR);
				} else {
					cstmtUcs.setString(1, _Threadid);
				}
				rsUcs = cstmtUcs.executeQuery();
				log.debug(session.getId() + " - executeQuery complete");
				List<File> files = new ArrayList<File>();
				if (rsUcs.next() == false) {
					response.getOutputStream().println("<script>jQuery('#ErrorCode').html('0x0000000'); jQuery('#ErrorMessage').html('Errore nell'apertura della mail');jQuery('#Error').show()</script>");
				} else {
					do {
						id = rsUcs.getString("id");
						log.info(session.getId() + " - dashboard.MailWeb_OpenMail('" + id + "')");
						CallableStatement cs_email = connUcs.prepareCall("{call dashboard.MailWeb_OpenMail(?)}");
						if (id.isEmpty()) {
							cs_email.setNull(1, Types.VARCHAR);
						} else {
							cs_email.setString(1, id);
						}
						ResultSet rs_email = cs_email.executeQuery();
						log.debug(session.getId() + " - executeQuery complete");
						if (rs_email.next() == false) {
							response.getOutputStream().println("<script>jQuery('#ErrorCode').html('0x0000000'); jQuery('#ErrorMessage').html('Errore nell'apertura della mail');jQuery('#Error').show()</script>");
						} else {
							do {
//								startData = rs_email.getString("StartDate");
//								endData = rs_email.getString("EndDate");
								MAIL_FROM = rs_email.getString("MAIL_FROM");
								MAIL_TO = rs_email.getString("MAIL_TO");
//								MAIL_CC = rs_email.getString("MAIL_CC");
								Subject = rs_email.getString("Subject");
								structuredText = rs_email.getString("structuredText");
								attachId = rs_email.getString("documentId") != null ? rs_email.getString("documentId") : "";

								String[] attach_list = attachId.split(",");

								Message message = new MimeMessage(Session.getInstance(System.getProperties()));
								message.setFrom(new InternetAddress(MAIL_FROM));
								message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(MAIL_TO));
								message.setSubject(Subject);
								MimeBodyPart content = new MimeBodyPart();
								Multipart multipart = new MimeMultipart();
								content.setDataHandler(new DataHandler(new ByteArrayDataSource(structuredText, "text/html")));
								multipart.addBodyPart(content);

								for (int i = 0; i < attach_list.length; i++) {
									log.info(session.getId() + " - dashboard.MailWeb_getAttachedFile('" + attach_list[i] + "')");
									CallableStatement cs_attach = connUcs.prepareCall("{call dashboard.MailWeb_getAttachedFile(?)}");
									if (attach_list[i].isEmpty()) {
										cs_attach.setNull(1, Types.VARCHAR);
									} else {
										cs_attach.setString(1, attach_list[i]);
									}
									ResultSet rs_attach = cs_attach.executeQuery();
									if (rs_attach.next() == false) {
									} else {
										do {
											String TheName = rs_attach.getString("TheName");
											byte[] TheContent = rs_attach.getBytes("Content");
//											String TheMimeType = rs_attach.getString("MimeType");
											int TheSize = rs_attach.getInt("TheSize");
//											String a = "";
											File TheContentFile = new File(download_location + File.separator + TheName);
											FileOutputStream attach = new FileOutputStream(TheContentFile);
											attach.write(TheContent, 0, TheSize);
											content = new MimeBodyPart();
											javax.activation.DataSource source = new FileDataSource(new File(download_location + File.separator + TheName));
											content.setDataHandler(new DataHandler(source));
											content.setFileName(TheName);
											multipart.addBodyPart(content);
					 			           	attach.close();
										} while (rs_attach.next());
									}
									rs_attach.close();
									cs_attach.close();
								}

								message.setContent(multipart, "text/html");

								String email_name = download_location + File.separator + "EMAIL_" + id + ".eml";
								for (File file : files) {
									if (file.getName().contains(id)) {
										email_name = download_location + File.separator + "EMAIL_" + id + "_" + files.indexOf(file) + ".eml";
									} else {
										email_name = download_location + File.separator + "EMAIL_" + id + ".eml";
									}
								}
								File file_email = new File(email_name);
								message.writeTo(new FileOutputStream(file_email));
								files.add(file_email);
							} while (rs_email.next());
						}
						rs_email.close();
						cs_email.close();
					} while (rsUcs.next());
					zip = zip(files, download_location + File.separator + _Threadid + ".zip");
					response.setContentType("application/zip");
					response.setHeader("Content-Disposition", "attachment;filename=\"" + zip.getName() + "\"");
					OutputStream out = response.getOutputStream();
					FileInputStream fis = new FileInputStream(zip);
					int bytes;
					while ((bytes = fis.read()) != -1) {
//						System.out.println(bytes);
						out.write(bytes);
					}
					fis.close();
					out.close();
					response.flushBuffer();
				}
				break;
			case "DownloadMail":
//				_Download = request.getParameter("Download");
				log.info(session.getId() + " - dashboard.MailWeb_OpenMail('" + _Id + "')");
				cstmtUcs = connUcs.prepareCall("{call dashboard.MailWeb_OpenMail(?)}");
				if (_Id.isEmpty()) {
					cstmtUcs.setNull(1, Types.VARCHAR);
				} else {
					cstmtUcs.setString(1, _Id);
				}
				rsUcs = cstmtUcs.executeQuery();

				if (rsUcs.next() == false) {
					response.getOutputStream().println("<script>jQuery('#ErrorCode').html('0x0000000'); jQuery('#ErrorMessage').html('Errore nell'apertura della mail');jQuery('#Error').show()</script>");
				} else {
//					startData = rsUcs.getString("StartDate");
//					endData = rsUcs.getString("EndDate");
					MAIL_FROM = rsUcs.getString("MAIL_FROM");
					MAIL_TO = rsUcs.getString("MAIL_TO");
//					MAIL_CC = rsUcs.getString("MAIL_CC");
					Subject = rsUcs.getString("Subject");
					structuredText = rsUcs.getString("structuredText");
//					allegati = rsUcs.getString("allegati");
					attachId = rsUcs.getString("documentId") != null ? rsUcs.getString("documentId") : "";

					String[] attach_list = attachId.split(",");

					Message message = new MimeMessage(Session.getInstance(System.getProperties()));
					message.setFrom(new InternetAddress(MAIL_FROM));
					message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(MAIL_TO));
					message.setSubject(Subject);
					MimeBodyPart content = new MimeBodyPart();
					Multipart multipart = new MimeMultipart();

					content.setDataHandler(new DataHandler(new ByteArrayDataSource(structuredText, "text/html")));
					multipart.addBodyPart(content);

					for (int i = 0; i < attach_list.length; i++) {

						log.info(session.getId() + " - dashboard.MailWeb_getAttachedFile('" + attach_list[i] + "')");
						CallableStatement cs_attach = connUcs.prepareCall("{call dashboard.MailWeb_getAttachedFile(?)}");
						if (attach_list[i].isEmpty()) {
							cs_attach.setNull(1, Types.VARCHAR);
						} else {
							cs_attach.setString(1, attach_list[i]);
						}
						ResultSet rs_attach = cs_attach.executeQuery();
						if (rs_attach.next() == false) {
						} else {
							do {
								String TheName = rs_attach.getString("TheName");
								byte[] TheContent = rs_attach.getBytes("Content");
//								String TheMimeType = rs_attach.getString("MimeType");
								int TheSize = rs_attach.getInt("TheSize");
//								String a = "";

								File TheContentFile = new File(download_location + File.separator + TheName);
								FileOutputStream attach = new FileOutputStream(TheContentFile);
								attach.write(TheContent, 0, TheSize);

								content = new MimeBodyPart();
								javax.activation.DataSource source = new FileDataSource(new File(download_location + File.separator + TheName));
								content.setDataHandler(new DataHandler(source));
								content.setFileName(TheName);
								multipart.addBodyPart(content);
		 			           	attach.close();
							} while (rs_attach.next());
						}
						rs_attach.close();
						cs_attach.close();
					}
					message.setContent(multipart, "text/html");
					message.writeTo(new FileOutputStream(new File(download_location + File.separator + _Id + ".eml")));
					try (BufferedReader br = new BufferedReader(new InputStreamReader(new java.io.FileInputStream(download_location + File.separator + _Id + ".eml"), "UTF-8"))) {
						response.setContentType("application/octet-stream");
						response.setHeader("Content-Disposition", "attachment; filename=\"EMAIL_" + _Id + ".eml\"");
						StringBuilder sb = new StringBuilder();
						String line;
						while ((line = br.readLine()) != null) {
							sb.append(line);
							sb.append('\n');
						}
						response.getOutputStream().print(sb.toString());
					}
				}
				break;
			}
		} catch (Exception e) {
			response.getOutputStream().print(e.getMessage());
		} finally {
			log.info(session.getId() + " - end page res:");
			try { rsUcs.close(); } catch (Exception e) {}
			try { cstmtUcs.close(); } catch (Exception e) {}
			try { connUcs.close(); } catch (Exception e) {}
		}

	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

	public static File zip(List<File> files, String filename) {
//		System.out.println("zip filename: " + filename);
		File zipfile = new File(filename);
		// Create a buffer for reading the files
		byte[] buf = new byte[4096];
		try {
			// create the ZIP file
			ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipfile));
			// compress the files
			for (int i = 0; i < files.size(); i++) {
//				System.out.println("index: " + i);
//				System.out.println("path filename: " + files.get(i).getPath());
//				System.out.println("name filename: " + files.get(i).getName());
				FileInputStream in = new FileInputStream(files.get(i).getPath());
				// add ZIP entry to output stream
				out.putNextEntry(new ZipEntry(files.get(i).getName()));
				// transfer bytes from the file to the ZIP file
				int len;
				while ((len = in.read(buf)) > 0) {
					out.write(buf, 0, len);
				}
				// complete the entry
				out.closeEntry();
				in.close();
			}
			// complete the ZIP file
			out.finish();
			out.flush();
			out.close();
			return zipfile;
		} catch (IOException ex) {
			System.err.println(ex.getMessage());
		}
		return null;
	}
}