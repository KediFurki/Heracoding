package comapp;


import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SystemStart {
	private Properties _cs;
	private String _environment;
	Logger log = Logger.getLogger("comappRT." + this.getClass());

	private static SystemStart instance = null;
	Hashtable<String, GenesysServer> hGenesysServer = new Hashtable<String, GenesysServer>();
	private QueryTServer instanceTS;

	public Properties getCs() throws Exception {
		if (_cs == null)
			_cs = Utility.getSystemProperties(_environment);
		return _cs;

	}

	public static SystemStart getInstance(String environment) throws Exception {
		if (instance == null)
			instance = new SystemStart(environment);
		return instance;
	}
	public  QueryTServer getTServerInstance( ) throws Exception {
		if (instanceTS == null)
			instanceTS = new  QueryTServer("WebSocketAgentState",getCs(),_environment);
		return instanceTS;
	}

	public SystemStart(String environment) throws Exception {
		_environment = environment;
		JSONArray ja = DBUtility.getStatistics( _environment, true, null );
		log.info("getStatistics[" + ConfigServlet.web_app + _environment +"]: " + ja);
		IntervalListener ql= null;
		if (environment.equalsIgnoreCase("CCC")) {
			ql =  new IntervalListener(15, _environment);
		} else {
			ql =  new IntervalListener(5, _environment);
		}
		ql.setStatitic(ja);
		
		for (int i = 0; i < ja.length(); i++) { 
			JSONObject jo = ja.getJSONObject(i);
		
			GenesysServer gs = getGenesysServerOrCreate(jo);
			gs.registerListener(ql,jo);
		}
	}

	private GenesysServer getGenesysServerOrCreate(JSONObject jo) throws Exception {
		String StatServerName = jo.getString("StatServerPri") + jo.getString("StatServerPortPri");
		try {
			StatServerName = jo.getString("StatServerName");
		} catch (Exception e) {}
		GenesysServer gs = hGenesysServer.get(StatServerName);
		if (gs == null) {
			jo.put("tTimeOut", getCs().getProperty("tTimeOut", "1000"));
			gs = new QStatServer(StatServerName,jo,_environment );
			hGenesysServer.put(StatServerName, gs);
		}
		return gs;
	}
	// public JSONObject getTransactionOption() throws Exception {
	// if (_TransactionOption == null)
	// _TransactionOption = ConfigurationUtility.getTransactionJSON( getCs(),
	// "SystemStart.getVqMonitored", ConfigServlet.web_app);
	// return _TransactionOption;
	// }
	//
	// public JSONArray getVqMonitored() {
	// try {
	// if (_VqMonitored == null)
	// _VqMonitored = ConfigurationUtility.getVQList(getCs(), "SystemStart",
	// ConfigServlet.web_app + "_System");
	// return _VqMonitored;
	// } catch (Exception e) {
	// log.warn("getVqMonitored", e);
	// }
	// return new JSONArray();
	// }

	public static JSONObject findObject(JSONArray ja, JSONObject jo) throws JSONException {
		for (int i = 0; i < ja.length(); i++) {
			JSONObject j = ja.getJSONObject(i);
			if (j.getInt("DBID") == jo.getInt("DBID"))
				return j;
		}
		return null;
	}

	public static JSONObject findObject(JSONArray ja, String Number) throws JSONException {
		for (int i = 0; i < ja.length(); i++) {
			JSONObject j = ja.getJSONObject(i);
			if (StringUtils.equalsAnyIgnoreCase(j.getString("Number"), Number))
				return j;
		}
		return null;
	}

	public void registerDnListener(IListener wSocketServer, JSONObject jo) throws Exception {
		GenesysServer gs = getGenesysServerOrCreate(jo);
		gs.registerListener(wSocketServer, jo);
	}

	public void unregisterDnListener(IListener wSocketServer) throws Exception {
//		GenesysServer gs = getGenesysServerOrCreate(jo);
//		gs.unregisterListener(wSocketServer, jo);
		


		
		
		Set<String> keys = hGenesysServer.keySet();
		 
	    //Obtaining iterator over set entries
	    Iterator<String> itr = keys.iterator();
	 
	    //Displaying Key and value pairs
	    while (itr.hasNext()) { 
	    	String key = itr.next();
	    	GenesysServer gs = hGenesysServer.get(key);
	    	gs.unregisterListener(wSocketServer);
	    }
	
	}

	public void add(JSONObject jo) throws Exception {
		GenesysServer gs = getGenesysServerOrCreate(jo);
		gs.register(jo);

	}

	public void del(JSONObject jo) throws Exception {
		GenesysServer gs = getGenesysServerOrCreate(jo);
		gs.unregister(jo);

	}

	public void refresh(IListener wssAgentDetails) {
		 	Set<String> keys = hGenesysServer.keySet();
		 
		    //Obtaining iterator over set entries
		    Iterator<String> itr = keys.iterator();
		 
		    //Displaying Key and value pairs
		    while (itr.hasNext()) { 
		    	String key = itr.next();
		    	GenesysServer gs = hGenesysServer.get(key);
		    	gs.refresh(wssAgentDetails);
		    }
		
		
		return ;
	}

}
