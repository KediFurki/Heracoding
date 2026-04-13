package comapp;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebInitParam;
import javax.servlet.annotation.WebServlet;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

import com.genesyslab.platform.applicationblocks.com.IConfService;

@WebServlet(
		urlPatterns = "/ConfigServlet",
		displayName = "ConfigServlet",
		loadOnStartup = 100,
		asyncSupported = false,
		initParams = {
				@WebInitParam(name = "config-properties-location", value = "C:/Comapp/Config")
		}
	)

public class ConfigServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
//	public static String  version = "2.0.1"; 
	public static String ConfigLocation;
	public static Logger log = Logger.getLogger("comapp");
	public static IConfService confServer = null;
	public static String web_app = "";
	public static String applVersion = "";

	public ConfigServlet() {
		super();
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		
		ConfigLocation = config.getInitParameter("config-properties-location") + "/" + config.getServletContext().getContextPath() + ".properties";
		web_app = config.getServletContext().getContextPath().replaceAll("/", "");

		DBUtility.setSchema("dashboard");
		
		Properties cs = new Properties();
		try {
			cs = getProperties();		
			//dbtype = cs.getProperty("DBType");
			
			String Log4jLocation = cs.getProperty("log4j-properties-location");
//			if (Log4jLocation == null) {
//				BasicConfigurator.configure();
//			} else {
//				File file = new File(Log4jLocation);
//				if (file.exists()) {
//
//					PropertyConfigurator.configure(Log4jLocation);
//				} else
//					BasicConfigurator.configure();
//			}
			LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
			File file = new File(Log4jLocation);
			context.setConfigLocation(file.toURI());
			
			Properties props_manifest = new Properties();
			props_manifest.load(config.getServletContext().getResourceAsStream("/META-INF/MANIFEST.MF"));
			applVersion = props_manifest.getProperty("Implementation-Version");

			log = Logger.getLogger("comapp");
			log.info("\n\n***************************************************************"
					+"\n************** Start: "+web_app+" vers."+applVersion
					+"\n***************************************************************\n");
			DBUtility.DBType = cs.getProperty("DBType","mssql");
			log.info("DBType: "+ DBUtility.DBType);
		} catch (Exception e) {
			e.printStackTrace();
		}
		super.init(config);
	}

	// public static Properties getProperties(HttpSession session) {
	// try {
	// Object tm = null;
	// try {
	// tm = session.getAttribute("Properties");
	// } catch (Exception e) {
	// }
	// if (tm == null) {
	// Properties cs = new Properties();
	// FileInputStream is = new FileInputStream(new
	// File(ConfigServlet.ConfigLocation));
	// cs.load(is);
	// is.close();
	// for (Object s : cs.keySet()) {
	// log.debug((session != null ? session.getId() : "") + " - " + s + " " +
	// cs.getProperty("" + s));
	// }
	// if (session != null)
	// session.setAttribute("Properties", cs);
	// return cs;
	// }
	// return (Properties) tm;
	// } catch (Exception e) {
	// log.log(Level.ERROR, "Config Servlet Exception : ", e);
	// return null;
	// }
	//
	// }

	public static Properties getProperties() {
		try {

			Properties cs = new Properties();
			System.out.println("conf file:"+ConfigServlet.ConfigLocation);
			FileInputStream is = new FileInputStream(new File(ConfigServlet.ConfigLocation));
			cs.load(is);
			is.close();

			return cs;

		} catch (Exception e) {
			log.log(Level.ERROR, "Config Servlet Exception : ", e);
			return null;
		}

	}
}
