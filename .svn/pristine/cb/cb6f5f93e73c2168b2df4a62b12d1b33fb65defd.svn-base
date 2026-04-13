package comapp;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.genesyslab.platform.commons.protocol.ChannelState;
import com.genesyslab.platform.commons.protocol.Message;
import com.genesyslab.platform.commons.protocol.ProtocolException;
import com.genesyslab.platform.configuration.protocol.types.CfgAppType;
import com.genesyslab.platform.json.jackson2.statserver.StatServerModule;
import com.genesyslab.platform.reporting.protocol.StatServerProtocol;
import com.genesyslab.platform.reporting.protocol.statserver.Notification;
import com.genesyslab.platform.reporting.protocol.statserver.NotificationMode;
import com.genesyslab.platform.reporting.protocol.statserver.StatisticMetric;
import com.genesyslab.platform.reporting.protocol.statserver.StatisticObject;
import com.genesyslab.platform.reporting.protocol.statserver.StatisticObjectType;
import com.genesyslab.platform.reporting.protocol.statserver.events.EventError;
import com.genesyslab.platform.reporting.protocol.statserver.events.EventInfo;
import com.genesyslab.platform.reporting.protocol.statserver.events.EventServerMode;
import com.genesyslab.platform.reporting.protocol.statserver.events.EventStatisticOpened;
import com.genesyslab.platform.reporting.protocol.statserver.requests.RequestOpenStatistic;

public class QStatServer implements GenesysServer {

	Logger log = Logger.getLogger("comappRT." + this.getClass());
	private volatile StatServerProtocol statServerProtocol = null;
	private String version = "1.1.3";
	private Thread StatServerListener;
	private Thread ThListener;
	private volatile Vector<Message> vMsg = new Vector<Message>();
	private volatile Hashtable<Integer, Vector<IListener>> hListener = new Hashtable<>();
	private ObjectMapper mapper = new ObjectMapper();
	private int timeOut = 1000;
	volatile boolean stopThread = false;
	private String id = "";
	//volatile Object syncView = new Object();
	private volatile Hashtable<String, JSONObject> hRegisterElement = new Hashtable<>();
	private JSONObject config;
	private String name;
	private String env;

	public QStatServer(String name, JSONObject jo, String env) {
		this.config = jo;
		this.name = name;
		this.env = env;
		id = "" + this.hashCode();
		try {
			log.debug(id + " -  start QStatServer version: " + version);
			try {
				timeOut = Integer.parseInt(jo.getString("tTimeOut"));
			} catch (Exception e) {
				timeOut = 1000;
			}
			mapper.registerModule(new StatServerModule());
			mapper.setSerializationInclusion(Include.NON_NULL);
			mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
			statServerProtocol = ConnectionUtility.getSatServerConnection(jo, "StatServer");
			log.info(id + " -   TserverOpenconnection: " + statServerProtocol.getState());
			ThListener = new Thread(new Listener());
			ThListener.start();
			StatServerListener = new Thread(new StatServeListener()); // event link connected is not propagate to client at the startup
			StatServerListener.start();
			log.debug(id + " -   end");
		} catch (Exception e) {
			log.error(id + " - " + e.getMessage(), e);
		}
	}

	public class StatServeListener implements Runnable {
		Logger log = Logger.getLogger("comappRT." + this.getClass());

		public void run() {
			while (!stopThread) {
				Message msg = null;
				try {
					while ((statServerProtocol == null || statServerProtocol.getState() != ChannelState.Opened) && !stopThread) {
						try {
							statServerProtocol.close();
						} catch (Exception e) {}
						statServerProtocol = null;
						EventError ee = EventError.create();
						ee.setStringValue("Custom: connection to tserver error");
						ee.setReferenceId(-1);
						log.warn(id + " - send error message: " + ee);
						sendEvent(ee);
						log.debug(id + " - suspend for  1000 milliseconds");
						Thread.sleep(1000);
						log.debug(id + " - retry connection");
						statServerProtocol = ConnectionUtility.getSatServerConnection(config, "QueryTServer");

						Enumeration<JSONObject> re = hRegisterElement.elements();
						while (re.hasMoreElements()) {
							registerStats(re.nextElement());
						}
					}
					msg = statServerProtocol.receive(timeOut);
					if (msg == null) {
						continue;
					}

					if (msg.messageId() == EventServerMode.ID) {
						EventServerMode esm = (EventServerMode) msg;
						if (esm.getIntValue().intValue() == 0) {
							try {
								statServerProtocol.close();
							} catch (Exception e) {}
							continue;
						}
					}

					log.debug(id + " -  Genesys <--: " + msg);
					if (msg.messageId() == EventInfo.ID) {
						sendEvent(msg);
					}
				} catch (InterruptedException e) {
					log.warn(id + " -  " + e.getMessage(), e);
				} catch (Exception e) {

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
		Logger log = Logger.getLogger("comappRT." + this.getClass());

		public void run() {
			while (!stopThread) {
				try {
					log.debug(id + " -  start ");
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
					log.info(id + " - Listener  vMsg.size:" + vMsg.size() + "  msg:" + msg);
					JSONObject json = null;
				//	synchronized (syncView) { // prevent send event before to insert in db (it needs to sync view refresh
												// ending)
						json = new JSONObject(mapper.writeValueAsString(msg));
						json.put("MessageName", msg.messageName());
						json.put("ID", json.getInt("referenceId"));
						json.put("ReportName", hRegisterElement.get("" + json.getInt("referenceId")).getString("ReportName"));
						json.put("Cod_IVR", hRegisterElement.get("" + json.getInt("referenceId")).getString("Cod_IVR"));
						switch (msg.messageId()) {
						case EventInfo.ID:
							try {
								json.put("Value", new JSONObject(json.toString()));
							} catch (Exception e) {

							}
							// fix 04/02/2020
							//if (!env.equalsIgnoreCase("CCTF"))
							//	DBUtility.insetStatInfoToDB(id, env, json);
							break;

						default:
							break;
						}

				//	}
					int sid = json.getInt("ID");
					log.debug(id + " - Listener  " + sid);
					if (sid == -1) {
						Enumeration<Integer> keys = hListener.keys();
						while (keys.hasMoreElements()) {
							int key = keys.nextElement();
							sendMsg(json, hListener.get(key));
						}
					} else {
						sendMsg(json, hListener.get(sid));
					}
				} catch (Exception e) {
					log.warn(id + " - " + e.getMessage(), e);
				}
			}
		}
	}

	private void sendMsg(JSONObject json, Vector<IListener> vListener) throws JSONException {
		if (vListener == null) {
			log.info(id + " - Listener for " + json.getInt("ID") + " is null");
			return;
		}
		try {
			for (IListener listener : vListener) {
				listener.sendEvent(json);
				log.info(id + " - Listener send " + json + " to " + json.getInt("ID"));
			}
		} catch (Exception e) {
			log.error(id + " - ", e);
		}
	}

	private Vector<IListener> getRegisterAndListenersDn(JSONObject statistic) throws Exception {
		Vector<IListener> vListener = hListener.get(statistic.getInt("ID"));
		if (vListener == null) {
			vListener = new Vector<IListener>();
			hListener.put(statistic.getInt("ID"), vListener);
			register(statistic);
		}
		return vListener;
	}

	@Override
	public void registerListener(IListener l, JSONObject statistic) throws Exception {
		Vector<IListener> vListener = getRegisterAndListenersDn(statistic);
		if (!vListener.contains(l)) {
			vListener.add(l);
		}
	}

	@Override
	public void refresh(IListener l) {
//		Set<Integer> keys = hListener.keySet();
//		Iterator<Integer> itr = keys.iterator();
//		while (itr.hasNext()) {
//			Integer key = itr.next();
//			Vector<IListener> vListener = hListener.get(key);
//			if (vListener == null)
//				return;
//			if (vListener.contains(l)) {
//			//	synchronized (syncView) { // block db write until view sync is ending
//					JSONArray ja = DBUtility.getStatisticsById(env, "" + key);
//					if (ja == null)
//						return;
//					for (int i = 0; i < ja.length(); i++) {
//						try {
//							sendMsg(ja.getJSONObject(i), hListener.get(key));
//						} catch (JSONException e) {
//							log.warn(l.getSessionId() + "-", e);
//						}
//					}
//				//}
//			}
//		}
		log.warn(l.getSessionId() + " - refresh disabled");
	}

	@Override
	public void unregisterListener(IListener l) throws JSONException {
		Set<Integer> keys = hListener.keySet();
		Iterator<Integer> itr = keys.iterator();
		while (itr.hasNext()) {
			Integer key = itr.next();
			Vector<IListener> vListener = hListener.get(key);
			if (vListener == null)
				return;
			if (vListener.contains(l)) {
				vListener.remove(l);
			}
		}
	}

	@Override
	public boolean register(JSONObject jStatitic) throws Exception {
		log.info(id + " - is " + jStatitic);
		if (!hRegisterElement.containsKey("" + jStatitic.getInt("ID"))) {
			hRegisterElement.put("" + jStatitic.getInt("ID"), jStatitic);

			return registerStats(jStatitic);

			// String StatisticName = jStatitic.getString("StatisticName");
			// String ObjectType = jStatitic.getString("ObjectType");
			// String Object = jStatitic.getString("Object");
			// String Tenant = jStatitic.getString("Tenant");
			// String Filter = jStatitic.getString("Filter");
			// String TimeRange = jStatitic.getString("TimeRange");
			// String TimeProfile = jStatitic.getString("TimeProfile");
			// String TenantPassword = jStatitic.getString("TenantPassword");
			// int statisticId = jStatitic.getInt("ID");
			// StatisticObjectType sot = StatisticObjectType.valueOf(ObjectType);
			// StatisticMetric metric = StatisticMetric.create();
			// metric.setStatisticType(StatisticName);
			// Notification notification = Notification.create();
			// notification.setMode(NotificationMode.Immediate);
			// if (jStatitic.getBoolean("IsTime")) {
			// notification.setInsensitivity(0);
			//
			// if (env.equalsIgnoreCase("CCC"))
			// notification.setFrequency(5);
			// else
			// notification.setFrequency(120);
			//
			// notification.setMode(NotificationMode.Periodical);
			// } else {
			// notification.setInsensitivity(0);
			// notification.setFrequency(1);
			// }
			// StatisticObject object = StatisticObject.create();
			// object.setObjectId(Object);
			// object.setObjectType(sot);
			// object.setTenantName(Tenant);
			// TenantPassword = TenantPassword == null ? "" : TenantPassword;
			// object.setTenantPassword(TenantPassword);
			// RequestOpenStatistic requestOpenStatistic = RequestOpenStatistic.create();
			// requestOpenStatistic.setStatisticObject(object);
			// if (StringUtils.isNotBlank(Filter)) {
			// metric.setFilter(Filter);
			// }
			// if (StringUtils.isNotBlank(TimeRange)) {
			// metric.setTimeRange(TimeRange);
			// }
			// if (StringUtils.isNotBlank(TimeProfile)) {
			// metric.setTimeProfile(TimeProfile);
			// }
			// requestOpenStatistic.setStatisticMetric(metric);
			// requestOpenStatistic.setNotification(notification);
			// requestOpenStatistic.setReferenceId(statisticId);
			// log.info(id + " - send request requestOpenStatistic: " +
			// requestOpenStatistic);
			// Message receive = null;
			// receive = statServerProtocol.request(requestOpenStatistic);
			// log.info(id + " - receve message: " + receive);
			// if (!(receive instanceof EventStatisticOpened)) {
			// log.warn(id + " - statistic error ");
			// return false;
			// }
			// return true;
		}
		return false;
	}

	@Override
	public boolean unregister(JSONObject statitic) throws Exception {
		if (hRegisterElement.containsKey("" + statitic.getInt("ID"))) {
			hRegisterElement.remove("" + statitic.getInt("ID"));
		}
		throw new Exception();
	}

	@Override
	public void transfer(String thisDn, String connID, String destination, String VQs, String CallId) throws Exception {
		throw new Exception();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getType() {
		return CfgAppType.CFGTServer.name();
	}

	public boolean registerStats(JSONObject jStatitic) throws JSONException, ProtocolException, IllegalStateException {
		String StatisticName = jStatitic.getString("StatisticName");
		String ObjectType = jStatitic.getString("ObjectType");
		String Object = jStatitic.getString("Object");
		String Tenant = jStatitic.getString("Tenant");
		String Filter = jStatitic.getString("Filter");
		String TimeRange = jStatitic.getString("TimeRange");
		String TimeProfile = jStatitic.getString("TimeProfile");
		String TenantPassword = jStatitic.getString("TenantPassword");
		int statisticId = jStatitic.getInt("ID");
		StatisticObjectType sot = StatisticObjectType.valueOf(ObjectType);
		StatisticMetric metric = StatisticMetric.create();
		metric.setStatisticType(StatisticName);
		Notification notification = Notification.create();
		notification.setMode(NotificationMode.Immediate);
		if (jStatitic.getBoolean("IsTime")) {
			notification.setInsensitivity(0);

			if (env.equalsIgnoreCase("CCC"))
				notification.setFrequency(5);
			else
				notification.setFrequency(240);

			notification.setMode(NotificationMode.Periodical);
		} else {
			notification.setInsensitivity(0);
			notification.setFrequency(1);
		}
		StatisticObject object = StatisticObject.create();
		object.setObjectId(Object);
		object.setObjectType(sot);
		object.setTenantName(Tenant);
		TenantPassword = TenantPassword == null ? "" : TenantPassword;
		object.setTenantPassword(TenantPassword);
		RequestOpenStatistic requestOpenStatistic = RequestOpenStatistic.create();
		requestOpenStatistic.setStatisticObject(object);
		if (StringUtils.isNotBlank(Filter)) {
			metric.setFilter(Filter);
		}
		if (StringUtils.isNotBlank(TimeRange)) {
			metric.setTimeRange(TimeRange);
		}
		if (StringUtils.isNotBlank(TimeProfile)) {
			metric.setTimeProfile(TimeProfile);
		}
		requestOpenStatistic.setStatisticMetric(metric);
		requestOpenStatistic.setNotification(notification);
		requestOpenStatistic.setReferenceId(statisticId);
		log.info(id + " - send request requestOpenStatistic: " + requestOpenStatistic);
		Message receive = null;
		receive = statServerProtocol.request(requestOpenStatistic);
		log.info(id + " - receve message: " + receive);
		if (!(receive instanceof EventStatisticOpened)) {
			log.warn(id + " - statistic error  ");
			return false;
		}
		return true;
	}
}