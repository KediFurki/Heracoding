package comapp;

import org.json.JSONObject;


public interface IListener {


	public void sendEvent(JSONObject msg);
	public String getSessionId();
	
}
