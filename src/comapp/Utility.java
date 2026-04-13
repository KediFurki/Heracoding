package comapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.http.HttpSession;
import javax.sql.DataSource;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import it.sauronsoftware.jave.AudioAttributes;
import it.sauronsoftware.jave.Encoder;
import it.sauronsoftware.jave.EncodingAttributes;


public class Utility {

	static Logger log = Logger.getLogger(Utility.class);
	
	public static String stringJavaToJS(String field) {
		if (field == null) return "";
		return field.replaceAll("'", "''");
	}
	
	public static Properties getSystemProperties(String env) throws Exception {
		return DBUtility.getSystemProperties(" [Utility.getSystemProperties] ", env);
	}

	public static String convertHtmlToText(String text) throws IOException {
		final StringBuilder sb = new StringBuilder();
		HTMLEditorKit.ParserCallback parserCallback = new HTMLEditorKit.ParserCallback() {
			public boolean readyForNewline;

			@Override
			public void handleText(final char[] data, final int pos) {
				String s = new String(data);
				sb.append(s.trim());
				readyForNewline = true;
			}

			@Override
			public void handleStartTag(final HTML.Tag t, final MutableAttributeSet a, final int pos) {
				if (readyForNewline && (t == HTML.Tag.DIV || t == HTML.Tag.BR || t == HTML.Tag.P)) {
					sb.append("\n");
					readyForNewline = false;
				}
			}

			@Override
			public void handleSimpleTag(final HTML.Tag t, final MutableAttributeSet a, final int pos) {
				handleStartTag(t, a, pos);
			}
		};
		new ParserDelegator().parse(new StringReader(text), parserCallback, true);
		return sb.toString();
	}

	public static String getNotNull(String pi_value) {
		return (pi_value==null)?"":pi_value;
	}

	public static final int IT_CCC = 1;
	public static final int IT_CCTF = 2;
	public static final int IT_CCTE = 3;
	public static final int IT_CCTA = 4;
	public static final int IT_STC = 5;
	
	public static ArrayList<String> GetSystemParameters (HttpSession session, int infotype) throws Exception {
//		Logger log = Logger.getLogger("comapp." + this.getClass().getName());
		ArrayList<String> aParameters = new ArrayList<String>();
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		Context ctx = new InitialContext();
		DataSource ds = null;
		switch (infotype) {
		case IT_CCC:
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
			log.info(session.getId() + " - connection CCC wait...");
			break;
		case IT_CCTF:
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTF");
			log.info(session.getId() + " - connection CCTF wait...");
			break;
		case IT_CCTE:
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTE");
			log.info(session.getId() + " - connection CCTE wait...");
			break;
		case IT_CCTA:
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTA");
			log.info(session.getId() + " - connection CCTA wait...");
			break;
		case IT_STC:
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCTF");
			log.info(session.getId() + " - connection CCTF wait...");
			break;
		}
		conn = ds.getConnection();
		log.debug(session.getId() + " - dashboard.System_GetParameters()");
		cstmt = conn.prepareCall("{ call dashboard.System_GetParameters()} ");
		rs = cstmt.executeQuery();
		while (rs.next()) {
			String key = rs.getString("Key");
			log.info(session.getId() + " - key:" + rs.getString("Key") + " Value:" + rs.getString("Value"));
			switch (infotype) {
			case IT_CCC:
				if (StringUtils.startsWithIgnoreCase(key, "EmergencyDestination")) {
					aParameters.add(rs.getString("Value"));
				}
				break;
			case IT_CCTF:
				if (StringUtils.startsWithIgnoreCase(key, "MessageDestination")) {
					aParameters.add(rs.getString("Value"));
				}
				break;
			case IT_CCTE:
				break;
			case IT_CCTA:
				if (StringUtils.startsWithIgnoreCase(key, "MessageDestination")) {
					aParameters.add(rs.getString("Value"));
				}
				break;
			case IT_STC:
				if (StringUtils.startsWithIgnoreCase(key, "MessageDestination")) {
					aParameters.add(rs.getString("Value"));
				}
				break;
			}
		}
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
		return aParameters;
	}

	public static FileInputStream WavToMp3Converter(File source) throws Exception {
		File target = File.createTempFile(source.getName(), "mp3");
		target.deleteOnExit();
		AudioAttributes audio = new AudioAttributes();
		audio.setCodec("libmp3lame");
		audio.setBitRate(new Integer(64000));
		audio.setChannels(new Integer(2));
		audio.setSamplingRate(new Integer(44100));
		EncodingAttributes attrs = new EncodingAttributes();
		attrs.setFormat("mp3");
		attrs.setAudioAttributes(audio);
		Encoder encoder = new Encoder();
		encoder.encode(source, target, attrs);
		return new FileInputStream(target);
	}

	public static Map<String, String> readPersons(HttpSession session, Logger log) {
		Map<String, String> persons = new HashMap<String, String>();
		Connection connCnf = null;
		CallableStatement cstmtCnf = null;
		ResultSet rsCnf = null;
		try {
			Context ctx = new InitialContext();
			DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + "CNF");
			log.info(session.getId() + " - connection CNF wait...");
			connCnf = ds.getConnection();
			log.info(session.getId() + " - dashboard.MailWeb_GetPerson()");
			cstmtCnf = connCnf.prepareCall("{call dashboard.MailWeb_GetPerson()}");
			rsCnf = cstmtCnf.executeQuery();
			while (rsCnf.next()) {
				String dbid = rsCnf.getString("dbid");
				String user_name = rsCnf.getString("user_name"); 
				persons.put(dbid, user_name);
			}
		} catch (Exception e) {
			log.error(session.getId() + " - readPerson: " + e.getMessage(), e);
		} finally {
			try { rsCnf.close(); } catch (Exception e) {}
			try { cstmtCnf.close(); } catch (Exception e) {}
			try { connCnf.close(); } catch (Exception e) {}
		}
		return persons;
	}

	public static String getPerson(Map<String, String> persons, String dbid) {
		String person = "";
		if (persons.get(dbid)!=null)
			return persons.get(dbid);
		return person;
	}
}
