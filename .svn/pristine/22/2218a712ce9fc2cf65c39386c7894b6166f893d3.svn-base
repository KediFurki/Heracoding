package comapp;

import java.util.Properties;
import java.util.concurrent.Future;
import javax.servlet.http.HttpSession;
import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

@ServerEndpoint(value =  "/WebSocketAgentState", configurator = ServletAwareConfig.class)
public class WebSocketAgentState implements IListener {
	Properties cs = null;
	Logger log = Logger.getLogger("comappTS." + this.getClass());
	private Session session;
	private Future<Void> deliveryTracker;
	private QueryTServer tserver;
	private String Environment;
	private HttpSession httpSession;
	
	@OnOpen
	public void onOpen(Session session, EndpointConfig config) throws Exception {
		httpSession = (HttpSession) config.getUserProperties().get("httpSession");
		Environment= (String)httpSession.getAttribute("Environment");
		cs = Utility.getSystemProperties(Environment);
		tserver = SystemStart.getInstance(Environment).getTServerInstance();
	}

	@OnMessage
	public void onMessage(String message, Session session) {
		log.info(session.getId() + " - received message:  " + message);
		this.session = session;
		try {
			JSONObject jobj = new JSONObject(message);
			switch (jobj.getString("command")) {
			case "register": {
				JSONArray ja = ConfigurationUtility.getCfgPlaceGroupsList(cs, getSessionId(), jobj.getString("Group"));
				for (int i = 0; i < ja.length(); i++) {
					log.info(session.getId() + " - register : " +ja.getJSONObject(i));
					tserver.registerListener(this, ja.getJSONObject(i));
				}
				break;
			}
			case "NotReady":
				if(tserver != null) {
					tserver.AgentNotReady(new JSONObject("{ Number : '"+jobj.getString("thisDN")+"', Reason : '"+jobj.getString("reason")+"', ReasonCode : '"+jobj.getString("reasoncode")+"' }"));
				}
				break;
			case "Logoff":
				if(tserver != null) {
					tserver.AgentLogoff(new JSONObject("{ Number : '"+jobj.getString("thisDN")+"'}"));
				}
				break;
			case "unregister": {
				Properties cs = Utility.getSystemProperties(Environment);
				JSONArray ja = ConfigurationUtility.getCfgPlaceGroupsList(cs, getSessionId(), jobj.getString("Group"));
				//tserver = new QueryTServer(cs);
				for (int i = 0; i < ja.length(); i++) {
					tserver.unregisterListener(this);
				}
				break;
			}
			case "transfer":
				break;
			default:
				break;
			}
		} catch (Exception ex) {
			log.info(session.getId() + " - Exception:", ex);
		}
	}

	private synchronized void WebSocketSend(Session session, String text) {
		log.info(session.getId() + " - send init");
		if (deliveryTracker != null) {
			while (!deliveryTracker.isDone())
				try {
					wait(50);
				} catch (InterruptedException e) {}
		}
		deliveryTracker = session.getAsyncRemote().sendText(text);
		log.info(session.getId() + " - send end");
	}

	@OnClose
	public void onClose(Session session) {
		try {
			log.info(session.getId() + " - received websoket close  ");
			tserver.unregisterListener(this);
		} catch (Exception e) {}
	}

	@Override
	public synchronized void sendEvent(JSONObject jo) {
		try {
			log.info(session.getId() + " - jo: " + jo.toString());
//			String MessageName = jo.getString("MessageName");
			WebSocketSend(session, jo.toString());
		} catch (Exception e) {
			log.error(session.getId() + " -Error on send msg: ", e);
			log.info(session.getId() + " -Froce onClose");
			onClose(session);
			return;
		}
		return;
	}

	@Override
	public String getSessionId() {
		return session.getId();
	}
}
