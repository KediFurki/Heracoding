package comapp;


import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.genesyslab.platform.applicationblocks.com.ConfServiceFactory;
import com.genesyslab.platform.applicationblocks.com.IConfService;
import com.genesyslab.platform.commons.connection.configuration.PropertyConfiguration;
import com.genesyslab.platform.commons.connection.configuration.ClientADDPOptions.AddpTraceMode;
import com.genesyslab.platform.commons.protocol.ChannelState;
import com.genesyslab.platform.commons.protocol.Endpoint;
import com.genesyslab.platform.commons.protocol.Message;
import com.genesyslab.platform.configuration.protocol.ConfServerProtocol;
import com.genesyslab.platform.configuration.protocol.types.CfgAppType;
import com.genesyslab.platform.contacts.protocol.UniversalContactServerProtocol;
import com.genesyslab.platform.openmedia.protocol.InteractionServerProtocol;
import com.genesyslab.platform.openmedia.protocol.interactionserver.InteractionClient;
import com.genesyslab.platform.reporting.protocol.StatServerProtocol;
import com.genesyslab.platform.reporting.protocol.statserver.events.EventServerMode;
import com.genesyslab.platform.voice.protocol.TServerProtocol;

public class ConnectionUtility {
	static Logger log = Logger.getLogger("comapp.ConnectionUtility" );
	private static Object syncConf= new Object();
	public static IConfService getGlobalConfConnection(Properties cs, String id) {

		String confServerPri = cs.getProperty("confServerPri");
		String confPortPri = cs.getProperty("confPortPri");
		String confServerBck = cs.getProperty("confServerBck");
		String confPortBck = cs.getProperty("confPortBck");
		String user = cs.getProperty("user");
		String password = cs.getProperty("password");
	
		String confTimeOut =  cs.getProperty("confTimeOut");
		
		log.info(id + " getGlobalConfConnection - conf_pri:" + confServerPri + ":" + confPortPri + " conf_bck:" + confServerBck + ":" + confPortBck + " user:" + user );
		IConfService confService = null;
		try {
			log.info(id + " - tcp://" + confServerPri + ":" + confPortPri);
			confService = connectConfServer(id, user, password, confServerPri, confPortPri, confServerBck, confPortBck, confTimeOut, ConfigServlet.confServer);
			ConfigServlet.confServer = confService;
		} catch (Exception e) {
			log.warn(id + " - ex", e);
			disconnectConfServer(id, ConfigServlet.confServer);
			return null;
		}

		return confService;
	}

	public static InteractionServerProtocol getConnectIxn(Properties cs, String name) {
		String confServerPri = cs.getProperty("ixnServerPri");
		int confPortPri = Integer.parseInt(cs.getProperty("ixnPortPri"));
		String confServerBck = cs.getProperty("ixnServerBck");
		int confPortBck = Integer.parseInt(cs.getProperty("ixnPortBck"));
		int ixnTimeOut = 3000;
		try {
			ixnTimeOut = Integer.parseInt(cs.getProperty("ixnTimeOut"));
		} catch (Exception e) {}
		return connectIxn(name, confServerPri, confPortPri, confServerBck, confPortBck, ixnTimeOut, InteractionClient.MediaServer, log);
	}
	public static InteractionServerProtocol getConnectReportingIxn(Properties cs, String name) {
		String confServerPri = cs.getProperty("ixnServerPri");
		int confPortPri = Integer.parseInt(cs.getProperty("ixnPortPri"));
		String confServerBck = cs.getProperty("ixnServerBck");
		int confPortBck = Integer.parseInt(cs.getProperty("ixnPortBck"));
		int ixnTimeOut = 3000;
		try {
			ixnTimeOut = Integer.parseInt(cs.getProperty("ixnTimeOut"));
		} catch (Exception e) {}
		
		return connectIxn(name, confServerPri, confPortPri, confServerBck, confPortBck, ixnTimeOut, InteractionClient.ReportingEngine, log);
	}

	
	public static IConfService getConfConnection(Properties cs, String id, String user, String password) {
		String confServerPri = cs.getProperty("confServerPri");
		String confPortPri = cs.getProperty("confPortPri");
		String confServerBck = cs.getProperty("confServerBck");
		String confPortBck = cs.getProperty("confPortBck");
		String transactionName = cs.getProperty("transactionName");
		String defaultWBType = cs.getProperty("defaultWBType");
		String Tenant = cs.getProperty("Tenant");
		String confTimeOut =  cs.getProperty("confTimeOut");
		log.info(id + " - conf_pri:" + confServerPri + ":" + confPortPri + " conf_bck:" + confServerBck + ":" + confPortBck + " user:" + user + " confTimeOut:"+confTimeOut+ " Tenant:" + Tenant + " transactionName:" + transactionName + " defaultWBType:" + defaultWBType);
		IConfService confService = null;
		try {
			log.info(id + " - tcp://" + confServerPri + ":" + confPortPri);
			confService = connectConfServer(id, user, password, confServerPri, confPortPri, confServerBck, confPortBck,confTimeOut, null);

		} catch (Exception e) {
			log.warn(id + " - ex", e);
			disconnectConfServer(id, confService);
			return null;
		}

		return confService;
	}
	

	public static StatServerProtocol getSatServerConnection(Properties cs, String id) {
		String statServerPri = cs.getProperty("statServerPri");
		String statPortPri = cs.getProperty("statPortPri");
		String statServerBck = cs.getProperty("statServerBck");
		String statPortBck = cs.getProperty("statPortBck");
		String sStatTimeOut = cs.getProperty("statTimeOut");
		int statTimeOut = 2500;
		try {
			statTimeOut = Integer.parseInt(sStatTimeOut);
		} catch (Exception e) {}

		log.info(id + " - statServerPri:" + statServerPri + ":" + statPortPri + " statServerBck:" + statServerBck + ":" + statPortBck + " time-out:" + statTimeOut);
		StatServerProtocol statServerProtocol = null;
		try {
			log.info(id + " - tcp://" + statServerPri + ":" + statPortPri);
			statServerProtocol = connectStatServer(log, id, statServerPri, Integer.parseInt(statPortPri), statServerBck, Integer.parseInt(statPortBck), statTimeOut);
		} catch (Exception e) {
			log.warn(id + " - ex", e);
			try {
				statServerProtocol.close();
			} catch (Exception ex) {}
			return null;
		}
		return statServerProtocol;
	}
	public static StatServerProtocol getSatServerConnection(JSONObject cs, String id) throws JSONException {
		String statServerPri = cs.getString("StatServerPri");
		String statPortPri = cs.getString("StatServerPortPri");
		String statServerBck = cs.getString("StatServerBck");
		String statPortBck = cs.getString("StatServerPortBck");
		
		int statTimeOut = 21500;
		try {
			String sStatTimeOut = cs.getString("StatServerTimeOut");
			statTimeOut = Integer.parseInt(sStatTimeOut);
		} catch (Exception e) {}
		int portPri = 0;
		try {
			portPri = Integer.parseInt(statPortPri);
		} catch (Exception e) {}
		int portBck = 0;
		try {
			portBck = Integer.parseInt(statPortBck);
		} catch (Exception e) {}
		log.info(id + " - statServerPri:" + statServerPri + ":" + statPortPri + " statServerBck:" + statServerBck + ":" + statPortBck + " time-out:" + statTimeOut);
		StatServerProtocol statServerProtocol = null;
		try {
			log.info(id + " - tcp://" + statServerPri + ":" + statPortPri);
			statServerProtocol = connectStatServer(log, id, statServerPri, portPri, statServerBck, portBck, statTimeOut);
		} catch (Exception e) {
			log.warn(id + " - ex", e);
			try {
				statServerProtocol.close();
			} catch (Exception ex) {}
			return null;
		}
		return statServerProtocol;
	}
	public static TServerProtocol getTServerConnection(Properties cs, String id) {
		String TServerPri = cs.getProperty("tServerPri");
		String TServerPortPri = cs.getProperty("tPortPri");
		String TServerBck = cs.getProperty("tServerBck");
		String TServerPortBck = cs.getProperty("tPortBck");
		String sTTimeOut = cs.getProperty("tTimeOut");
		
	
		int TTimeOut = 1000;
		try {
			TTimeOut = Integer.parseInt(sTTimeOut);
		} catch (Exception e) {}

		log.info(id + " - conf_pri:" + TServerPri + ":" + TServerPortPri + " conf_bck:" + TServerBck + ":" + TServerPortBck + " time-out:" + TTimeOut);
		TServerProtocol tServerProtocol = null;
		try {
			log.info(id + " - tcp://" + TServerPri + ":" + TServerPortPri);
			tServerProtocol = connectTServer(log, id, TServerPri, Integer.parseInt(TServerPortPri), TServerBck, Integer.parseInt(TServerPortBck), TTimeOut);
		} catch (Exception e) {
			log.warn(id + " - ex", e);
			try {
				tServerProtocol.close();
			} catch (Exception ex) {}
			return null;
		}
		return tServerProtocol;
	}
	public static TServerProtocol getTServerConnection(JSONObject cs, String id) throws JSONException {
		String TServerPri = cs.getString("SwitchTserverPri");
		String TServerPortPri = cs.getString("SwitchTserverPortPri");
		String TServerBck = cs.getString("SwitchTserverBck");
		String TServerPortBck = cs.getString("SwitchTserverPortBck");
		String sTTimeOut = cs.getString("tTimeOut");
		int TTimeOut = 1000;
		try {
			TTimeOut = Integer.parseInt(sTTimeOut);
		} catch (Exception e) {}

		log.info(id + " - conf_pri:" + TServerPri + ":" + TServerPortPri + " conf_bck:" + TServerBck + ":" + TServerPortBck + " time-out:" + TTimeOut);
		TServerProtocol tServerProtocol = null;
		try {
			log.info(id + " - tcp://" + TServerPri + ":" + TServerPortPri);
			tServerProtocol = connectTServer(log, id, TServerPri, Integer.parseInt(TServerPortPri), TServerBck, Integer.parseInt(TServerPortBck), TTimeOut);
		} catch (Exception e) {
			log.warn(id + " - ex", e);
			try {
				tServerProtocol.close();
			} catch (Exception ex) {}
			return null;
		}
		return tServerProtocol;
	}
	

	public static InteractionServerProtocol connectIxn(String text, String server, int port, int TimeOut, InteractionClient ic, Logger log) {
		InteractionServerProtocol Ixn = null;
		try {
			PropertyConfiguration tserverConfig = new PropertyConfiguration();
			tserverConfig.setUseAddp(true);
			tserverConfig.setAddpServerTimeout(30);
			tserverConfig.setAddpClientTimeout(15);
			tserverConfig.setAddpTraceMode(AddpTraceMode.Both);
			Endpoint tserverEndpoint = new Endpoint(text, server, port, tserverConfig);
			Ixn = new InteractionServerProtocol(tserverEndpoint);
			Ixn.setClientName(text);
			Ixn.setClientType(ic);
			Ixn.open(TimeOut);
		} catch (Exception e) {
			log.log(Level.WARN, "", e);
			try {
				if (Ixn != null)
					Ixn.close();
				Ixn = null;
			} catch (Exception eClose) {}
		}
		return Ixn;
	}

	public static InteractionServerProtocol connectIxn(String text, String server, int port, String server_bck, int port_bck, int TimeOut, InteractionClient ic, Logger log) {
		log.info("connectIxn(" + text + "," + server + "," + port + "," + server_bck + "," + port_bck + "," + TimeOut + ")");
		log.debug("try to connect: " + server + ":" + port);
		InteractionServerProtocol Ixn = connectIxn(text, server, port, TimeOut, ic, log);
		if (Ixn == null || Ixn.getState() != ChannelState.Opened) {
			log.warn("primary connection failed " + (Ixn == null ? " Ixn = null" : "Ixn.getState()"));
			try {
				Ixn.close();
			} catch (Exception eClose) {}
			log.debug("try to connect: " + server + ":" + port);
			Ixn = connectIxn(text, server_bck, port_bck, TimeOut, ic, log);
		}
		if (Ixn == null || Ixn.getState() != ChannelState.Opened) {
			log.warn("connection failed " + (Ixn == null ? " Ixn = null" : "Ixn.getState()"));
			try {
				Ixn.close();
			} catch (Exception eClose) {}
			Ixn = null;
		}
		return Ixn;
	}

	public static UniversalContactServerProtocol connectUcs(String name, String server, int port, int TimeOut, Logger log) throws IOException {
		UniversalContactServerProtocol ucs = null;
		try {
			PropertyConfiguration tserverConfig = new PropertyConfiguration();
			tserverConfig.setUseAddp(true);
			tserverConfig.setAddpServerTimeout(30);
			tserverConfig.setAddpClientTimeout(15);
			tserverConfig.setAddpTraceMode(AddpTraceMode.Both);
			Endpoint tserverEndpoint = new Endpoint(name, server, port, tserverConfig);
			ucs = new UniversalContactServerProtocol(tserverEndpoint);
			ucs.setClientName(name);
			ucs.open(TimeOut);
		} catch (Exception e) {
			log.log(Level.WARN, "", e);
			try {
				if (ucs != null)
					ucs.close();
				ucs = null;
			} catch (Exception eClose) {}
		}
		return ucs;
	}

	public static TServerProtocol connectTs(Logger log, String name, String server, int port, int TimeOut) {
		TServerProtocol ts = null;
		try {
			PropertyConfiguration tserverConfig = new PropertyConfiguration();
			tserverConfig.setUseAddp(true);
			tserverConfig.setAddpServerTimeout(30);
			tserverConfig.setAddpClientTimeout(15);
			tserverConfig.setAddpTraceMode(AddpTraceMode.Both);
			Endpoint tserverEndpoint = new Endpoint(name, server, port, tserverConfig);
			ts = new TServerProtocol(tserverEndpoint);
			ts.setClientName(name);
			ts.open(TimeOut);
		} catch (Exception e) {
			log.log(Level.WARN, "", e);
			try {
				if (ts != null)
					ts.close();
				ts = null;
			} catch (Exception eClose) {}
		}
		return ts;
	}

	public static TServerProtocol connectTServer(Logger log, String id, String tServerPri, int tServerPortPri, String tServerBck, int tServerPortBck, int tTimeOut) throws Exception {
		TServerProtocol ssp = null;
		log.info("Main - try to pri connect TServer...");
		if ((ssp = connectTs(log, id, tServerPri, tServerPortPri, tTimeOut)) == null) {
			log.info("Main - try to bck connect TServer...");
			ssp = connectTs(log, id, tServerBck, tServerPortBck, tTimeOut);
		}
		if (ssp == null) {
			log.error("Main - Unabled to connect TServer ..");
			throw new Exception("Main - Unabled to connect TServer");
		}
		return ssp;
	}

	public static StatServerProtocol connectStatServer(Logger log, String name, String serverPri, int portPri, String serverBck, int portBck, int timeOut) throws Exception {
		StatServerProtocol ssp = null;
		log.info("Main - try to pri stat server...");
		if ((ssp = connectStatServer(log, name, serverPri, portPri, timeOut)) == null) {
			log.info("Main - try to bck stat server...");
			ssp = connectStatServer(log, name, serverBck, portBck, timeOut);
		}
		if (ssp == null) {
			log.error("Main - Unabled to connect stat server..");
			throw new Exception("Main - Unabled to connect stat server");
		}
		return ssp;
	}

	private static StatServerProtocol connectStatServer(Logger log, String name, String server, int port, int timeOut) {
		// "s-Server", statServerPri, Integer.parseInt(statPortPri), tserverConfig)
		StatServerProtocol statprotocol = null;
		try {
			log.info("Main - tcp://" + server + ":" + port + "  TimeOut:" + timeOut);
			PropertyConfiguration tserverConfig = new PropertyConfiguration();
			tserverConfig.setUseAddp(true);
			tserverConfig.setAddpServerTimeout(30);
			tserverConfig.setAddpClientTimeout(15);
			tserverConfig.setAddpTraceMode(AddpTraceMode.Both);
			Endpoint ep = new Endpoint(name, server, port, tserverConfig);
			statprotocol = new StatServerProtocol(ep);
			statprotocol.open(timeOut);
			Message receive = statprotocol.receive(700);
			log.info("Main - check if primary: " + receive);
			if (receive != null) {
				EventServerMode esm = (EventServerMode) receive;
				if (esm.getIntValue().intValue() == 0) {
					try {
						statprotocol.close();
					} catch (Exception e) {}
					throw new Exception("Is not primary");
				}
			}
		} catch (Exception e) {
			log.info(e.getMessage(), e);
			return null;
		}
		return statprotocol;
	}

	public static IConfService connectConfServer(String id, String User, String Password, String serverPri, String portPri, String serverBck, String portBck, String confTimeOut, IConfService confService) throws Exception {
		synchronized (syncConf) {
			try {
				try {
					log.info(id + " - tcp://" + serverPri + ":" + portPri);
					confService = connectConfServer(id, User, Password, serverPri, portPri, confTimeOut,confService);
				} catch (Exception e) {
					log.warn(id + "  ", e);
					log.info(id + " - bck tcp://" + serverBck + ":" + portBck);
					confService = connectConfServer(id, User, Password, serverBck, portBck,confTimeOut, confService);
				}
			} catch (Exception e) {
				log.log(Level.WARN, id + "-[Utility] - ex", e);
				try {
					confService.getProtocol().close();
				} catch (Exception exc) {}
				try {
					ConfServiceFactory.releaseConfService(confService);
				} catch (Exception exc) {}
				log.log(Level.WARN, id + "-[Utility] - destroy confService");
			}
		}

		return confService;
	}

	private static IConfService connectConfServer(String id, String User, String Password, String serverPri, String portPri, String confTimeOut, IConfService confService) throws Exception {
		ConfServerProtocol confServerProtocol = null;
		
		try {
			if (confService != null) {
				if (confService.getProtocol() == null) {
					try {
						ConfServiceFactory.releaseConfService(confService);
					} catch (Exception e) {}
				} else {
					log.info(id + "-[Utility] -  IConfService " + confService);
					if (confService.getProtocol().getState() == ChannelState.Opened) {
						log.info(id + "-[Utility] - re-use connection in cache ");
						return confService;
					} else {
						log.info(id + "-[Utility] - re-open connection in cache ");
						confService.getProtocol().open();
						return confService;
					}
				}
			}
		} catch (Exception e) {
			log.info(id + "- exception ", e);
		}
		log.info(id + "- Utility --> new connection");
		if (confTimeOut==null) {
			confTimeOut="2000";
		}
		PropertyConfiguration tserverConfig = new PropertyConfiguration();
		tserverConfig.setUseAddp(true);
		tserverConfig.setAddpServerTimeout(30);
		tserverConfig.setAddpClientTimeout(15);
		tserverConfig.setAddpTraceMode(AddpTraceMode.Both);
		Endpoint ep = new Endpoint(id, serverPri, Integer.parseInt(portPri), tserverConfig);
		confServerProtocol = new ConfServerProtocol(ep);
		confServerProtocol.setClientApplicationType(CfgAppType.CFGSCE.asInteger());
		confServerProtocol.setClientName("default");
		confServerProtocol.setUserName(User);
		confServerProtocol.setUserPassword(Password);
		confService = ConfServiceFactory.createConfService(confServerProtocol);
		confServerProtocol.open(Integer.parseInt(confTimeOut));
		return confService;
	}

	public static void disconnectConfServer(String id, IConfService oConfService) {
		synchronized (syncConf) {

			if (oConfService != null && oConfService instanceof IConfService) {
				try {
					((IConfService) oConfService).getProtocol().close();
				} catch (Exception e) {}
				try {
					ConfServiceFactory.releaseConfService((IConfService) oConfService);
				} catch (Exception e) {}
			}
		}
	}
	public static String getNotNull(String pi_value) {
		return (pi_value==null)?"":pi_value;
	}

}
