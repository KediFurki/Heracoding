package comapp;

import org.json.JSONException;
import org.json.JSONObject;

public interface GenesysServer {
	public String getName();
	public String getType();
	public void registerListener(IListener l,JSONObject statitic) throws JSONException, Exception;
	public void transfer(String thisDn, String connID, String destination, String VQs, String CallId) throws Exception;
	
	public boolean register(JSONObject statitic) throws Exception;	
	public boolean unregister(JSONObject statitic) throws Exception;
	public void unregisterListener(IListener l) throws JSONException;
	public void refresh(IListener l);
}
