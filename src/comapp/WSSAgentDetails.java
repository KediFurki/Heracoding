package comapp;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.servlet.http.HttpSession;
import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.fathzer.soft.javaluator.DoubleEvaluator;

@ServerEndpoint(value = "/WSSAgentDetails", configurator = ServletAwareConfig.class)
public class WSSAgentDetails implements IListener {
	Properties cs = null;
	Logger log = Logger.getLogger("comappRT." + this.getClass());
	private Session session;
	private Future<Void> deliveryTracker;
	private Hashtable<String, JSONObject> hStat = new Hashtable<String, JSONObject>();
	private Hashtable<String, JSONObject> hOperation = new Hashtable<String, JSONObject>();
	private String cod_ivr;
	private HttpSession httpSession = null;
	private String Environment;

	@OnOpen
	public void onOpen(Session session, EndpointConfig config) throws Exception {
		this.session = session;
		httpSession = (HttpSession) config.getUserProperties().get("httpSession");
		Environment = (String) httpSession.getAttribute("Environment");
		log.info(httpSession.getId() + " - Environment: " + Environment);
	}

	@OnMessage
	public void onMessage(String message, Session session) {
		log.info(session.getId() + " - received message:  " + message);
		this.session = session;
		try {
			JSONObject jobj = new JSONObject(message);
			switch (jobj.getString("command")) {
			case "register":
				cod_ivr = jobj.getString("CodIvr");
				log.info(session.getId() + " - openconnection ");
				ArrayList<JSONObject> al = DBUtility.getFlagStatistics(Environment, cod_ivr);
				log.info(session.getId() + " - Operation: " + al);
				for (JSONObject jop : al) {
					hOperation.put(jop.getString("ReportName")+"#"+jop.getString("Cod_IVR"), jop);
				}
				JSONArray ja = DBUtility.getStatistics(Environment, false, cod_ivr);
				log.info(session.getId() + " - Statistic: " + ja);
				for (int i = 0; i < ja.length(); i++) {
					JSONObject jo = ja.getJSONObject(i);
					hStat.put(jo.getString("ReportName")+"#"+jo.getString("Cod_IVR"), jo);
					SystemStart.getInstance(Environment).registerDnListener(this, jo);
				}
				ja = DBUtility.getStatistics(Environment, true, cod_ivr);
				log.info(session.getId() + " - Statistic: " + ja);
				for (int i = 0; i < ja.length(); i++) {
					JSONObject jo = ja.getJSONObject(i);
					hStat.put(jo.getString("ReportName")+"#"+jo.getString("Cod_IVR"), jo);
					SystemStart.getInstance(Environment).registerDnListener(this, jo);
				}
				SystemStart.getInstance(Environment).refresh(this);
				break;
			case "unregister":

				break;
			case "transfer":
				// SystemStart.getInstance().transfer(jobj.getString("thisDn"),
				// jobj.getString("connID"), jobj.getString("destination"),
				// jobj.getString("VQs"), jobj.getString("CallId"));
				break;
			case "error":
				log.error(session.getId() + " - receive error message from client, ivr=" + jobj.getString("CodIvr") + ": \n\t\t*************\n\t\t" + jobj.getString("msg") + "\n\t\t*************");
			default:
				break;
			}
		} catch (Exception ex) {
			log.info(session.getId() + " - Exception:", ex);
		}
	}

	private synchronized void WebSocketSend(Session session, String text) {
		log.debug(session.getId() + " - send init");
		if (deliveryTracker != null) {
			while (!deliveryTracker.isDone())
				try {
					wait(50);
				} catch (InterruptedException e) {}
		}
		deliveryTracker = session.getAsyncRemote().sendText(text);
		log.debug(session.getId() + " - send end");
	}

	public static JSONObject recalc(Logger log, Hashtable<String, JSONObject> hStat, JSONObject op) throws JSONException, Exception {
		Pattern MY_PATTERN = Pattern.compile("\\[(.*?)\\]");
		String operation = op.getString("Operation");
		Matcher m = MY_PATTERN.matcher(operation);
		String codivr = op.getString("Cod_IVR");
		while (m.find()) {
			String s = m.group(1);
			JSONObject jstat = hStat.get(s+"#"+codivr);
			log.debug("hStat.get(" + s+"#"+codivr + "):" + jstat);
			Object obj = "0";
			try {
				obj = jstat.get("Value");
			} catch (Exception e) {
				log.error("hStat.get(" + s+"#"+codivr + ") is " + hStat.get(s+"#"+codivr) + " " + e.getMessage(), e);
			}
			if (obj instanceof JSONObject) {
				obj = ((JSONObject) obj).get("stringValue");
			}
			operation = operation.replace("[" + s + "]", "" + obj);
		}
		Double result = 0.0;
		log.debug("operation: " + operation);
		try {
			result = new DoubleEvaluator().evaluate(operation);
			result =  Math.floor(result * 1000) / 1000;
		} catch (Exception e) {
			log.warn("error on calc (" + operation + "): " + e.getMessage());
		}
		JSONObject jo = new JSONObject();
		jo.put("ID", op.getInt("ID"));
		jo.put("ReportName", op.getString("ReportName"));
		jo.put("Value", "" + result);
		jo.put("Threshold",op.getJSONArray("Threshold"));
		jo.put("ThresholdOp",op.getString("ThresholdOp"));
		JSONArray t = op.getJSONArray("Threshold");
		ScriptEngineManager factory = new ScriptEngineManager();
		ScriptEngine engine = factory.getEngineByName("JavaScript");
		int i = t.length() - 1;
		for (; i >= 0; i--) {
			String ev = "";
			try {
				ev = result + " " + op.getString("ThresholdOp") + " " + t.getString(i);
				log.debug(" - threshold: " + ev);
				Boolean ev_threshold = (Boolean) engine.eval(ev);
				if (ev_threshold)
					break;
			} catch (Exception e) {
				log.warn(" - error on threshold eval (" + ev + "): " + e.getMessage());
			}
		}
		op.put("ThresholdTrigger", i);
		return jo;
	}

	@OnClose
	public void onClose(Session session) {
		try {
			log.info(session.getId() + " - received websoket close  ");
			SystemStart.getInstance(ConfigServlet.web_app + Environment).unregisterDnListener(this);
		} catch (Exception e) {}
	}

	@Override
	public synchronized void sendEvent(JSONObject jo) {
		try {
			log.info(session.getId() + " - Statistic update:" + jo);
			hStat.put(jo.getString("ReportName")+"#"+jo.getString("Cod_IVR"), jo);
			Set<String> keys = hOperation.keySet();
			Iterator<String> itr = keys.iterator();
			while (itr.hasNext()) {
				String str = itr.next();
				JSONObject op = hOperation.get(str);
				log.debug(session.getId() + " - hOperation[" + str + "]->" + op.getString("Operation") + " ?= [" + jo.getString("ReportName") + "]");
				if (op.getString("Operation").contains("[" + jo.getString("ReportName") + "]") && op.getString("Cod_IVR").contains(jo.getString("Cod_IVR"))) {
					log.debug(session.getId() + " recal operatio[" + op.getString("Operation") + "] for statistic:" + jo.getString("ReportName"));
					JSONObject newValue = recalc(log, hStat, op);
					log.info(session.getId() + " - WebSocketSend newValue:" + newValue);
					WebSocketSend(session, newValue.toString());
				}
			}
		} catch (Exception e) {
			log.error(session.getId() + " -Error on send msg: ", e);
			return;
		}
		return;
	}

	@Override
	public String getSessionId() {

		return session.getId();
	}
}