package comapp;

import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebInitParam;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;

import org.apache.log4j.Logger;

@WebServlet(
		urlPatterns = "/SystemStartServlet",
		displayName = "SystemStartServlet",
		loadOnStartup = 200,
		asyncSupported = false,
		initParams = {
				@WebInitParam(name = "config-properties-location", value = "C:/Comapp/Config")
		}
	)

public class SystemStartServlet extends HttpServlet {

	private static final long serialVersionUID = 8461902318600070461L;
	Logger log = Logger.getLogger("comappRT." + this.getClass());
	Logger log2 = Logger.getLogger("comappTS." + this.getClass());
	
	@Override
	public void init(ServletConfig config) throws ServletException {			
		super.init(config);
		try {
			Properties props_manifest = new Properties();
			props_manifest.load(getServletContext().getResourceAsStream("/META-INF/MANIFEST.MF"));
			String applVersion = props_manifest.getProperty("Implementation-Version");
			
			String SystemEnvironment = ConfigServlet.getProperties().getProperty("SystemEnvironment");
			if (SystemEnvironment.equalsIgnoreCase("CCC") ||
				SystemEnvironment.equalsIgnoreCase("CCTF")  ) {
				log.info("\n\n***************************************************************"
						+"\n************** Start: RealTime ServLet vers."+applVersion
						+"\n************** SystemEnvironment: "+SystemEnvironment
						+"\n***************************************************************\n");
				log2.info("\n\n***************************************************************"
						+"\n************** Start: TServer ServLet vers."+applVersion
						+"\n************** SystemEnvironment: "+SystemEnvironment
						+"\n***************************************************************\n");
				DBUtility.setSchema("dashboard");
				SystemStart.getInstance(ConfigServlet.getProperties().getProperty("SystemEnvironment","CCC"));
//				Properties cs = Utility.getSystemProperties(ConfigServlet.web_app);
//				String WB_ID = ConfigServlet.getProperties().getProperty("WB_ID", "1");
//				JSONObject jo = ConfigurationUtility.getConfigurationWB(log, cs, WB_ID, WB_ID);
//				log.info("JSONObject: "+jo);	
			} else {
				log.info("\n\n***************************************************************"
						+"\n************** RealTime ServLet vers."+applVersion+" Disabled"
						+"\n************** SystemEnvironment: "+SystemEnvironment
						+"\n***************************************************************\n");
				log2.info("\n\n***************************************************************"
						+"\n************** TServer ServLet vers."+applVersion+" Disabled"
						+"\n************** SystemEnvironment: "+SystemEnvironment
						+"\n***************************************************************\n");
			}
		} catch (Exception e) {
			log.warn("",e);	
		}
	}
	
	
	
}
