package comapp;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.genesyslab.platform.commons.collections.KeyValueCollection;
import com.genesyslab.platform.commons.protocol.ChannelState;
import com.genesyslab.platform.commons.protocol.Message;
import com.genesyslab.platform.commons.protocol.ProtocolException;
import com.genesyslab.platform.json.jackson2.tserver.TServerModule;
import com.genesyslab.platform.voice.protocol.ConnectionId;
import com.genesyslab.platform.voice.protocol.TServerProtocol;
import com.genesyslab.platform.voice.protocol.tserver.AddressType;
import com.genesyslab.platform.voice.protocol.tserver.AgentWorkMode;
import com.genesyslab.platform.voice.protocol.tserver.CommonProperties;
import com.genesyslab.platform.voice.protocol.tserver.ControlMode;
import com.genesyslab.platform.voice.protocol.tserver.RegisterMode;

import com.genesyslab.platform.voice.protocol.tserver.events.EventDiverted;
import com.genesyslab.platform.voice.protocol.tserver.events.EventError;

import com.genesyslab.platform.voice.protocol.tserver.requests.agent.RequestAgentLogout;
import com.genesyslab.platform.voice.protocol.tserver.requests.agent.RequestAgentNotReady;
import com.genesyslab.platform.voice.protocol.tserver.requests.dn.RequestRegisterAddress;
import com.genesyslab.platform.voice.protocol.tserver.requests.dn.RequestUnregisterAddress;
import com.genesyslab.platform.voice.protocol.tserver.requests.party.RequestSingleStepTransfer;
import com.genesyslab.platform.voice.protocol.tserver.requests.special.RequestSendEvent;

public class QueryTServer implements GenesysServer {

	Logger log = Logger.getLogger("comappTS." + this.getClass());
	private volatile TServerProtocol tServerProtocol = null;
	public String version = "1.1.3";
	private Thread TServeListener;
	private Thread ThListener;
	private volatile Vector<Message> vMsg = new Vector<Message>();
	private volatile Hashtable<String, Vector<IListener>> hListener = new Hashtable<>();
	private ObjectMapper mapper = new ObjectMapper();
	private int timeOut = 1000;
	volatile boolean stopThread = false;
	String id = "";
	volatile Object syncView = new Object();
	private volatile Vector<String> registerDn = new Vector<String>();
	JSONObject config = null;
	private String DbConnectionName;
	private String name;

	public QueryTServer(String name, JSONObject config,String DbConnectionName) throws JSONException {
		log.info("******* QueryTServer version: " + version);
		this.name =name;
//		boolean EnabledDbSave = true;
		this.DbConnectionName = DbConnectionName;
//		try {
//			EnabledDbSave = config.getBoolean("EnabledDbSave");
//		} catch (Exception e) {
//			config.put("EnabledDbSave", EnabledDbSave);
//		}
		init(config);
	}

	public QueryTServer(String name, Properties cs, String DbConnectionName) throws JSONException {
		log.info("******* QueryTServer version: " + version);
		this.DbConnectionName = DbConnectionName;
		JSONObject jcs = new JSONObject();
		jcs.put("SwitchTserverPri", cs.getProperty("tServerPri"));
		jcs.put("SwitchTserverPortPri", cs.getProperty("tPortPri"));
		jcs.put("SwitchTserverBck", cs.getProperty("tServerBck"));
		jcs.put("SwitchTserverPortBck", cs.getProperty("tPortBck"));
		jcs.put("tTimeOut", cs.getProperty("tTimeOut"));
	//	jcs.put("EnabledDbSave", Boolean.parseBoolean(cs.getProperty("EnabledDbSave", "true")));
		init(jcs);

	}

	private void init(JSONObject jc) {
		this.config = jc;
		log.info("DbConnectionName: "+ DbConnectionName);
		id = "" + this.hashCode();
		try {
			log.info(id + " - config: " + jc);
			try {
				timeOut = Integer.parseInt(config.getString("tTimeOut"));
			} catch (Exception e) {
				timeOut = 1000;
			}
			mapper.registerModule(new TServerModule());
			mapper.setSerializationInclusion(Include.NON_NULL);
			mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
			tServerProtocol = ConnectionUtility.getTServerConnection(config, "QueryTServer");
			log.info(id + " -   TserverOpenconnection: " + tServerProtocol.getState());
			ThListener = new Thread(new Listener());
			ThListener.start();
			TServeListener = new Thread(new TServeListener()); // event link connected is not propagate to client at the startup
			TServeListener.start();
			log.debug(id + " -  end");
		} catch (Exception e) {
			log.error(id + " -  " + e.getMessage(), e);
		}
	}

	public class TServeListener implements Runnable {
	//	Logger log = Logger.getLogger("comappRT." + this.getClass());

		public void run() {
			while (!stopThread) {
				Message msg = null;
				try {
					while ((tServerProtocol == null || tServerProtocol.getState() != ChannelState.Opened) && !stopThread) {
						try {
							tServerProtocol.close();
						} catch (Exception e) {}
						tServerProtocol = null;
						EventError ee = EventError.create();
						ee.setErrorCode(-1);
						ee.setErrorMessage("Custom: connection to tserver error");
						ee.setThisDN("*");
						log.warn(id + " -  send error message: " + ee);
						sendEvent(ee);
						log.debug(id + " -  suspend for  1000 milliseconds");
						Thread.sleep(1000);
						log.debug(id + " -  retry connection");
						tServerProtocol = ConnectionUtility.getTServerConnection(config, "QueryTServer");
					}
					msg = tServerProtocol.receive(timeOut);
					if (msg == null) {
						continue;
					}
					log.debug(id + " - Genesys <--: " + msg);
					sendEvent(msg);
				} catch (InterruptedException e) {
					log.warn(id + " - " + e.getMessage(), e);
				} catch (JSONException e) {

					log.warn(id + " - " + e.getMessage(), e);
				}
			}
		}

		private void sendEvent(Message msg) {
			vMsg.add(msg);
			synchronized (vMsg) {
				vMsg.notifyAll();
			}
		}
	}

	public class Listener implements Runnable {
	//	Logger log = Logger.getLogger("comappRT." + this.getClass());

		public void run() {
			while (!stopThread) {
				try {
					if (vMsg.size() == 0) {
						try {
							synchronized (vMsg) {
								log.debug(id + " -  wait......");
								vMsg.wait();
								log.debug(id + " -  resume......");
							}
						} catch (InterruptedException e) {
							log.warn(id + " - Listener: ", e);
						}
					}
					Message msg = vMsg.remove(0);
					JSONObject json = null;
					synchronized (syncView) { // prevent send event before to insert in db (it needs to sync view refresh
												// ending)
						json = new JSONObject(mapper.writeValueAsString(msg));
						json.put("MessageName", msg.messageName());
//						try {
//							if (config.getBoolean("EnabledDbSave"))
//								DBUtility.insertToDB(id, log, DbConnectionName, json);
//
//						} catch (Exception e) {
//							log.warn(id + " -  Exception in stored events: " + e.getMessage());
//						}
					}
					String ThisDN = (String) msg.getMessageAttribute("ThisDN");
					log.debug(id + " - Listener ThisDN " + ThisDN);
//					if (ThisDN == null || ThisDN.equalsIgnoreCase("*")) {
//						Enumeration<String> keys = hListener.keys();
//						while (keys.hasMoreElements()) {
//							String key = keys.nextElement();
//							sendMsg(json, hListener.get(key), key);
//						}
//					} else {
						sendMsg(json, hListener.get(ThisDN), ThisDN);
//					}
				} catch (Exception e) {
					log.warn(id + " - " + e);
				}
			}
		}
	}

	private void sendMsg(JSONObject json, Vector<IListener> vListener, String dn) {
		if (vListener == null) {
			log.info(id + " - Listener for " + dn + " is null");
			return;
		}
		try {
			for (IListener listener : vListener) {
				listener.sendEvent(json);
				log.info(id + " - Listener send " + json + " to " + dn);
			}
		} catch (Exception e) {
			log.error(id + " - ", e);
		}
	}

	private Vector<IListener> getRegisterAndListenersDn(String dn) throws Exception {
		Vector<IListener> vListener = hListener.get(dn);
		if (vListener == null) {
			vListener = new Vector<IListener>();
			hListener.put(dn, vListener);
			register(dn);
		} else {
			register(dn);
		}
		return vListener;
	}

	@Override
	public void refresh(IListener l) {
//		Set<String> keys = hListener.keySet();
//		Iterator<String> itr = keys.iterator();
//		while (itr.hasNext()) {
//			String key = itr.next();
//			Vector<IListener> vListener = hListener.get(key);
//			if (vListener == null)
//				return;
//			if (vListener.contains(l)) {
//				synchronized (syncView) { // block db write until view sync is ending
//					JSONArray ja = null;
////					try {
////						if (config.getBoolean("EnabledDbSave"))
////							ja = DBUtility.getLastEvent(id, log, DbConnectionName, key);
////					} catch (Exception e) {
////						log.warn("Exception on getCalls: " + e.getMessage());
////					}
//					if (ja == null)
//						return;
//					for (int i = 0; i < ja.length(); i++) {
//						try {
//							sendMsg(ja.getJSONObject(i), hListener.get(key), key);
//						} catch (JSONException e) {
//							log.warn(l.getSessionId() + "-", e);
//						}
//					}
//				}
//			}
//		}

	}

	public void refreshAll() throws JSONException {
//		Enumeration<String> keys = hListener.keys();
//		while (keys.hasMoreElements()) {
//			String key = keys.nextElement();
//			synchronized (syncView) { // block db write until view sync is ending
//				JSONArray ja = null;
//				if (config.getBoolean("EnabledDbSave"))
//					ja = DBUtility.getLastEvent(id, log, DbConnectionName, key);
//				if (ja == null)
//					return;
//				for (int i = 0; i < ja.length(); i++) {
//					try {
//						sendMsg(ja.getJSONObject(i), hListener.get(key), key);
//					} catch (JSONException e) {
//						log.warn(id + "-", e);
//					}
//				}
//			}
//		}
	}

	// private void unregisterDnListener(IListener l, String dn) {
	// Vector<IListener> vListener = hListener.get(dn);
	// if (vListener == null)
	// return;
	// if (vListener.contains(l)) {
	// vListener.remove(l);
	// }
	// }

	@Override
	public void unregisterListener(IListener l) throws JSONException {
		Set<String> keys = hListener.keySet();
		// Obtaining iterator over set entries
		Iterator<String> itr = keys.iterator();
		// Displaying Key and value pairs
		while (itr.hasNext()) {
			String key = itr.next();
			Vector<IListener> vListener = hListener.get(key);
			if (vListener == null)
				return;
			if (vListener.contains(l)) {
				vListener.remove(l);
			}
		}
	}

	public void transfer(String thisDn, String connID, String destination, String VQs, String CallId) throws Exception {
		getRegisterAndListenersDn(thisDn);
		RequestSingleStepTransfer sst = RequestSingleStepTransfer.create(thisDn, new ConnectionId(connID), destination);
		try {
			log.debug(id + "- [transfer] Genesys--> request" + sst);
			tServerProtocol.send(sst);
		} catch (Exception e) {
			log.error(id + "- [transfer] Error: ", e);
			return;
		}
		try {
			getRegisterAndListenersDn(VQs); // TServer Registered if not present
			CommonProperties cp = CommonProperties.create();
			cp.setConnID(new ConnectionId(connID));
			cp.setCallID(Integer.parseInt(CallId));
			cp.setThisDN(VQs);// "VQ_NC_IT";
			cp.setUserEvent(EventDiverted.ID);
			cp.setCallState(22);
			RequestSendEvent req = RequestSendEvent.create(0, cp);
			log.debug(id + "- [transfer] Genesys-->" + req);
			tServerProtocol.send(req);
		} catch (Exception e) {
			log.error(id + "- [transfer] Error: ", e);
		}

	}

	@Override
	public String getName() {

		return name;
	}

	@Override
	public String getType() {

		return null;
	}

	@Override
	public void registerListener(IListener l, JSONObject jo) throws JSONException, Exception {
		String dn = jo.getString("Number");

		Vector<IListener> vListener = getRegisterAndListenersDn(dn);
		if (!vListener.contains(l)) {
			vListener.add(l);
		}
//		synchronized (syncView) { // block db write until view sync is ending
//
//			JSONArray ja = null;
//			try {
//				if (config.getBoolean("EnabledDbSave"))
//					ja = DBUtility.getLastEvent(id, log, DbConnectionName, dn);
//			} catch (Exception e) {
//				log.warn("Exception on getLastEvent: " + e.getMessage(), e);
//			}
//
//			if (ja == null)
//				return;
//			for (int i = 0; i < ja.length(); i++) {
//				try {
//					l.sendEvent(ja.getJSONObject(i));
//				} catch (JSONException e) {
//					log.warn(id + "-", e);
//				}
//			}
//		}

	}

	@Override
	public boolean register(JSONObject jo) throws Exception {
		String Dn = jo.getString("Number");
		return register(Dn);

	}

	private boolean register(String Dn) throws Exception {

		if (!registerDn.contains(Dn)) {
			registerDn.add(Dn);
		}
		RequestRegisterAddress rqa = RequestRegisterAddress.create(Dn, RegisterMode.ModeShare, ControlMode.RegisterDefault, AddressType.DN);
		try {
			log.debug(id + " - send(" + rqa + ")");
			tServerProtocol.send(rqa);
			return true;
		} catch (ProtocolException | IllegalStateException e) {
			log.error(id + " - Error: ", e);
			return false;
		}

	}

	@Override
	public boolean unregister(JSONObject jo) throws Exception {

		String Dn = jo.getString("Number");
		if (registerDn.contains(Dn)) {
			registerDn.remove(Dn);
		}
		RequestUnregisterAddress rqa = RequestUnregisterAddress.create(Dn, ControlMode.RegisterDefault);
		try {
			log.debug(id + " - send(" + rqa + ")");
			tServerProtocol.send(rqa);
			return true;
		} catch (ProtocolException | IllegalStateException e) {
			log.error(id + " - Error: ", e);
			return false;
		}

	}

 
	public boolean AgentNotReady(JSONObject jo) throws Exception {

		String Dn = jo.getString("Number");
		String Reason = jo.getString("Reason");
		String ReasonCode = jo.getString("ReasonCode");
		RequestAgentNotReady rqa = RequestAgentNotReady.create(Dn, AgentWorkMode.ManualIn);
		KeyValueCollection rea = new KeyValueCollection();
		rea.addString(Reason, ReasonCode);
		rqa.setReasons(rea);
		try {
			log.debug(id + " - send(" + rqa + ")");
			tServerProtocol.send(rqa);
			return true;
		} catch (ProtocolException | IllegalStateException e) {
			log.error(id + " - Error: ", e);
			return false;
		}

	}
 
	public boolean AgentLogoff(JSONObject jo) throws Exception {

		String Dn = jo.getString("Number");
		RequestAgentLogout rqa = RequestAgentLogout.create(Dn);
		try {
			log.debug(id + " - send(" + rqa + ")");
			tServerProtocol.send(rqa);
			return true;
		} catch (ProtocolException | IllegalStateException e) {
			log.error(id + " - Error: ", e);
			return false;
		}

	}

	
	
	public void close() {
		try {
			stopThread = true;
			Thread.yield();
		} catch (Exception e) {

		}
		hListener = null;
		registerDn = null;

		try {
			tServerProtocol.close();
		} catch (Exception e) {

		}

	}

}