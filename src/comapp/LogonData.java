package comapp;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import com.genesyslab.platform.applicationblocks.com.IConfService;
import com.genesyslab.platform.commons.collections.KeyValueCollection;
import com.genesyslab.platform.commons.protocol.ChannelState;
//import com.genesyslab.platform.configuration.protocol.types.CfgAppType;
//import it.csebo.svcf.sr.srweb.sraccess.bo.LogonOutputData;

public class LogonData {
	Logger log = Logger.getLogger("comapp." + this.getClass());
	public String user;
	private String password;
	private boolean is_logged = false;
	//public LogonOutputData lod = null;
	private String profile = null;
	private IConfService _localConf;
	Properties _cs = null;
	private String _VQGroup;
	private JSONArray _VQList;
	private KeyValueCollection personData;
//	private JSONArray _jStatistic;
	
	private String SystemEnvironment;

	@Override
	public String toString() {
		return "LogonData [user=" + user + ", password=" + password + ", is_logged=" + is_logged + ",  profile=" + profile + "]";
	}

	public String getPersonProfile() throws Exception {
		return getPersonData().getAsString("Profile");
	}

	public String getPersonVQGroup() throws Exception {
		return getPersonData().getAsString("VQGroup");
	}

	public String getPersonPlaceGroup() throws Exception {
		return getPersonData().getAsString("PlaceGroup");
	}

	public String getPersonAgentGroup() throws Exception {
		return getPersonData().getAsString("AgentGroup");
	}

	public boolean getPersonEnabled() throws Exception {
		try {
			return Boolean.parseBoolean(getPersonData().getAsString("Enabled"));
		} catch (Exception e) {

		}
		return false;
	}

	public String getProfile() throws Exception {
		if (StringUtils.isBlank(profile))
			profile = getPersonProfile();

		return profile;
	}

	public KeyValueCollection getPersonData() throws Exception {
		if (personData == null) {
			personData = ConfigurationUtility.getPersonData(getCs(), getUser(), getLocalConfIstance(), getUser(), ConfigServlet.web_app);
			log.info("personData: " + personData);
		}

		return personData;
	}

	public String getVQGroup() throws Exception {
		if (StringUtils.isBlank(_VQGroup)) {
			_VQGroup = getPersonVQGroup();
			if (StringUtils.isBlank(_VQGroup)) {
				_VQGroup = "VoiceDNGroup";
			}
		}
		return _VQGroup;
	}

	public IConfService getLocalConfIstance() throws Exception {
		if (_localConf == null || _localConf.getProtocol().getState() != ChannelState.Opened) {
			_localConf = ConnectionUtility.getConfConnection(getCs(), user, getUser(), getPassword());
		}
		return _localConf;
	}

	public Properties getCs() throws Exception {
		if (_cs == null)
			_cs = Utility.getSystemProperties(SystemEnvironment);
		return _cs;

	}

	public LogonData(String user, String password, String SystemEnvironment) throws Exception {
		this.user = user;
		this.password = password;
		log.info("LogonData: User= " + user);
		this.SystemEnvironment = SystemEnvironment;
	}



	

	public boolean isLogged() {
		if (is_logged)
			return true;
		

		try {
			is_logged = getPersonEnabled();

			
			return is_logged;
		} catch (Exception e) {
			log.info("isLogged false: ", e);
		} finally {

		}
		return false;
	}

	public void logOut() {
		try {
			ConnectionUtility.disconnectConfServer(getUser(), _localConf);
		} catch (Exception e) {}
		log.info("logOut user: " + getUser());
	}

	

	public String getUser() {
		if (StringUtils.isNotBlank(user))
			return user;
		
		return null;
	}

	public String getPassword() {
		if (StringUtils.isNotBlank(password))
			return password;
		return null;
	}

	public JSONArray getVQList() throws Exception {
		if (_VQList == null) {
			_VQList = new JSONArray();
			try {
				_VQList = ConfigurationUtility.getCfgDnGroupsList(getCs(), getUser(), getVQGroup());
				log.info("- [" + getUser() + "] getVQList: " + _VQList);
			} catch (Exception e) {
				log.warn("- [" + getUser() + "] - getVQList[" + getVQGroup() + "]: ", e);
			}
		}
		return _VQList;
	}

	
	public void remove(JSONObject jsonObject) throws Exception {
		log.info("remove jsonObject:" + jsonObject);
		try {
			ConfigurationUtility.delVQList(getCs(), getUser(), getVQGroup(), jsonObject);
		} catch (Exception e) {
			log.warn(e.getMessage(), e);
		}

	}

	public void add(JSONObject jsonObject) {
		log.info("add jsonObject:" + jsonObject);
		try {
			ConfigurationUtility.addVQList(getCs(), getUser(), getVQGroup(), jsonObject);
		} catch (Exception e) {
			log.warn(e.getMessage(), e);
		}
	}


}
