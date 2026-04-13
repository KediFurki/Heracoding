package comapp;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class IntervalListener implements IListener,Runnable {
	Logger log = Logger.getLogger("comappRT." + this.getClass());
	private int minute_step;
	private String environment;
	private Hashtable<String, JSONObject> hStat = new Hashtable<String, JSONObject>();
	private Hashtable<String, JSONObject> hOperation = new Hashtable<String, JSONObject>();

	@Override
	public void run() {
		try {
			Calendar cal = Calendar.getInstance();
			int minutes = cal.get(Calendar.MINUTE);
			minutes = cal.get(Calendar.MINUTE);
			int mod = minutes % minute_step;
			cal.add(Calendar.MINUTE, -mod);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			Date stepDate = cal.getTime();
			if (environment.equalsIgnoreCase("CCTF")) {
				DBUtility.putStatisticalValuesForMySQL("CCTF", hOperation, new java.sql.Timestamp(stepDate.getTime()));
			} else {
				//	SystemStart.getInstance(environment).refresh(this);
				Set<String> keys = hStat.keySet();
				Iterator<String> itr = keys.iterator();
				while (itr.hasNext()) {
					String str = itr.next();
					JSONObject msg = hStat.get(str);
					Object obj = msg.get("Value");
					if (obj instanceof JSONObject) {
						obj = ((JSONObject) obj).get("stringValue");
					}
					DBUtility.putStatisticalValues(environment, msg.getInt("ID"), msg.getString("Cod_IVR"), msg.getString("ReportName"), "" + obj, new java.sql.Timestamp(stepDate.getTime()));
				}
			}
		} catch (Exception e) {
			log.warn("resfesh error: " + e.getMessage(), e);
		}
	}

	public IntervalListener(int minute_step, String environment) {
		this.minute_step = minute_step;
		this.environment = environment;
		try {
			ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
			if (environment.equalsIgnoreCase("CCTF")) {
				scheduledExecutor.scheduleAtFixedRate(this, 6 * 1000, minute_step * 27 * 1000, TimeUnit.MILLISECONDS);
			} else {
				scheduledExecutor.scheduleAtFixedRate(this, millisToNext(1), 1 * 60 * 1000, TimeUnit.MILLISECONDS);
			}
		} catch (Exception e) {
			log.warn(" error: " + e.getMessage(), e);
		}

	}

	private long millisToNext(int step) {
		Calendar cal = Calendar.getInstance();
		int minutes = cal.get(Calendar.MINUTE);
		int mod = minutes % step;
		int seconds = cal.get(Calendar.SECOND);
		seconds = (((step - mod) * 60) - seconds + 2);
		log.debug("next run in minutes:seconds: " + ((int) seconds / 60) + " " + seconds % 60);
		return seconds * 1000;
	}

	@Override
	public void sendEvent(JSONObject msg) {
		Calendar cal = Calendar.getInstance();
		int minutes = cal.get(Calendar.MINUTE);
		minutes = cal.get(Calendar.MINUTE);
		int mod = minutes % minute_step;
		cal.add(Calendar.MINUTE, -mod);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date stepDate = cal.getTime();
		// try {
		// stepDate = new Date(cal.getTime().getTime() -
		// TimeZone.getDefault().getOffset(cal.getTime().getTime()));
		// } catch (Exception ex) {
		// log.info(ex.getMessage(), ex);
		// }
		if (!environment.equalsIgnoreCase("CCTF"))
			try {
				Object obj = msg.get("Value");
				if (obj instanceof JSONObject) {
					obj = ((JSONObject) obj).get("stringValue");
				}
				log.info(" - Statistic update:" + msg);
				hStat.put(msg.getString("ReportName") + "#" + msg.getString("Cod_IVR"), msg);
				
				//DBUtility.putStatisticalValues(environment, msg.getInt("ID"), msg.getString("Cod_IVR"), msg.getString("ReportName"), "" + obj, new java.sql.Timestamp(stepDate.getTime()));
			} catch (Exception ex) {
				log.warn(ex.getMessage(), ex);
			}

		if (environment.equalsIgnoreCase("CCTF"))
			try {
				log.info(" - Statistic update:" + msg);
				hStat.put(msg.getString("ReportName") + "#" + msg.getString("Cod_IVR"), msg);
				Set<String> keys = hOperation.keySet();
				Iterator<String> itr = keys.iterator();
				while (itr.hasNext()) {
					String str = itr.next();
					JSONObject op = hOperation.get(str);
					log.debug(" - hOperation[" + str + "]->" + op.getString("Operation") + " ?= [" + msg.getString("ReportName") + "]");
					if (op.getString("Operation").contains("[" + msg.getString("ReportName") + "]") && op.getString("Cod_IVR").contains(msg.getString("Cod_IVR"))) {
						log.debug(" recal operatio[" + op.getString("Operation") + "] for statistic:" + msg.getString("ReportName"));
						JSONObject newValue = WSSAgentDetails.recalc(log, hStat, op);
						log.info(" - recal newValue:" + newValue);

						String Value = newValue.getString("Value");
						Value = (Value.equals("NaN") || Value.equals("Infinity") ? "0" : Value);

						op.put("Value", Value);
						// op.put("Cod_IVR", msg.getString("Cod_IVR"));
						log.debug(" - hOperation.value:" + op);
						// DBUtility.putStatisticalValuesForMySQL("CCTF", newValue.getInt("ID"),
						// msg.getString("Cod_IVR"), newValue.getString("ReportName"), Value, new
						// java.sql.Timestamp(stepDate.getTime()));

						/// WebSocketSend(session, newValue.toString());
					}
				}
			} catch (Exception e) {
				log.error(" -Error on send msg: ", e);
				return;
			}
	}

	@Override
	public String getSessionId() {
		return "" + this.hashCode();
	}

	public void setStatitic(JSONArray ja) throws JSONException {
		ArrayList<JSONObject> al = DBUtility.getFlagStatistics(environment, null);
		log.info(" - Operation: " + al);
		for (JSONObject jop : al) {
			hOperation.put(jop.getString("ReportName") + "#" + jop.getString("Cod_IVR"), jop);
		}

		for (int i = 0; i < ja.length(); i++) {
			JSONObject jo = ja.getJSONObject(i);
			hStat.put(jo.getString("ReportName") + "#" + jo.getString("Cod_IVR"), jo);

		}

	}

}
