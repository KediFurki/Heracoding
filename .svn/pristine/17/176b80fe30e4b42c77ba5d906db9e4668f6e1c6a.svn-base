package comapp;

import javax.servlet.*;
import javax.servlet.annotation.WebInitParam;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;

import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

@WebServlet(
		urlPatterns = "/RealTimeMySQLServlet",
		displayName = "RealTimeMySQLServlet",
		loadOnStartup = 200,
		asyncSupported = false,
		initParams = {
				@WebInitParam(name = "config-properties-location", value = "C:/Comapp/Config")
		}
	)

public class RealTimeMySQLServlet extends HttpServlet implements Servlet {
	private static final long serialVersionUID = 1L;
	private int minute_step;
	private ScheduledExecutorService scheduledExecutor = null;
	private ScheduledFuture<?> scheduleManager;
	private RealTimeMySQL realTimeMySQL = null;
	
	Logger log = Logger.getLogger("comappMySQL." + this.getClass());

	public void init() throws ServletException {
		try {
			Properties props_manifest = new Properties();
			props_manifest.load(getServletContext().getResourceAsStream("/META-INF/MANIFEST.MF"));
			String applVersion = props_manifest.getProperty("Implementation-Version");
			
			String SystemEnvironment = ConfigServlet.getProperties().getProperty("SystemEnvironment");
			if (SystemEnvironment.equalsIgnoreCase("CCTF")) {
				log.info("\n\n***************************************************************"
						+"\n************** Start: RealTime MySQL ServLet vers."+applVersion
						+"\n************** SystemEnvironment: "+SystemEnvironment
						+"\n***************************************************************\n");
				DBUtility.setSchema("dashboard");
				minute_step = 5;
				realTimeMySQL = new RealTimeMySQL(minute_step,this);
				scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
				scheduleManager = scheduledExecutor.scheduleAtFixedRate(realTimeMySQL, millisToNext(), minute_step * 60 * 1000, TimeUnit.MILLISECONDS);
			} else {
				log.info("\n\n***************************************************************"
						+"\n************** RealTime MySQL ServLet vers."+applVersion+" Disabled"
						+"\n************** SystemEnvironment: "+SystemEnvironment
						+"\n***************************************************************\n");
			}
		} catch (Exception e) {
			log.fatal(" init() Exception: "+e.getMessage(), e);
		}
	}

	private long millisToNext() {
		Calendar cal = Calendar.getInstance();
		int minutes = cal.get(Calendar.MINUTE);
		int mod = minutes % minute_step;
		int seconds = cal.get(Calendar.SECOND);
		seconds = (((minute_step - mod) * 60) - seconds + 2);
		log.debug(" next run in minutes:seconds: " + ((int) seconds / 60) + " " + seconds % 60);
		return seconds * 1000;
	}
	
	public void changeExecution() {
	    if (scheduleManager!= null) {
	        scheduleManager.cancel(true);
	    }
		scheduleManager = scheduledExecutor.scheduleAtFixedRate(realTimeMySQL, millisToNext(), minute_step * 60 * 1000, TimeUnit.MILLISECONDS);
	}
	
}
