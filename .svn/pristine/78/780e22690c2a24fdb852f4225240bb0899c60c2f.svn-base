package comapp;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.genesyslab.platform.applicationblocks.com.IConfService;
import com.genesyslab.platform.applicationblocks.com.objects.CfgActionCode;
import com.genesyslab.platform.applicationblocks.com.objects.CfgAgentGroup;
import com.genesyslab.platform.applicationblocks.com.objects.CfgApplication;
import com.genesyslab.platform.applicationblocks.com.objects.CfgDN;
import com.genesyslab.platform.applicationblocks.com.objects.CfgDNGroup;
import com.genesyslab.platform.applicationblocks.com.objects.CfgDNInfo;
import com.genesyslab.platform.applicationblocks.com.objects.CfgPerson;
import com.genesyslab.platform.applicationblocks.com.objects.CfgPlace;
import com.genesyslab.platform.applicationblocks.com.objects.CfgPlaceGroup;
import com.genesyslab.platform.applicationblocks.com.objects.CfgSwitch;
import com.genesyslab.platform.applicationblocks.com.objects.CfgTransaction;
import com.genesyslab.platform.applicationblocks.com.queries.CfgActionCodeQuery;
import com.genesyslab.platform.applicationblocks.com.queries.CfgAgentGroupQuery;
import com.genesyslab.platform.applicationblocks.com.queries.CfgApplicationQuery;
import com.genesyslab.platform.applicationblocks.com.queries.CfgDNGroupQuery;
import com.genesyslab.platform.applicationblocks.com.queries.CfgDNQuery;
import com.genesyslab.platform.applicationblocks.com.queries.CfgPersonQuery;
import com.genesyslab.platform.applicationblocks.com.queries.CfgPlaceGroupQuery;
import com.genesyslab.platform.applicationblocks.com.queries.CfgPlaceQuery;
import com.genesyslab.platform.applicationblocks.com.queries.CfgTransactionQuery;
import com.genesyslab.platform.commons.collections.KeyValueCollection;
import com.genesyslab.platform.commons.collections.KeyValuePair;

import com.genesyslab.platform.reporting.protocol.statserver.StatisticObjectType;

public class ConfigurationUtility {

	static Logger log = Logger.getLogger("comapp.ConfigurationUtility");

	public static boolean verifyObj(Properties cs, String id, String type, String name) throws JSONException {
		boolean foundObject = false;
		try {
			IConfService confService = ConnectionUtility.getGlobalConfConnection(cs, id);
			StatisticObjectType sot = StatisticObjectType.valueOf(type);
			if (sot == StatisticObjectType.GroupAgents) {
				CfgAgentGroupQuery cagq = new CfgAgentGroupQuery();
				cagq.setName(name);
				CfgAgentGroup cfgAgentGroup = confService.retrieveObject(CfgAgentGroup.class, cagq);
				foundObject = cfgAgentGroup != null;
			}
			if (sot == StatisticObjectType.RegularDN || sot == StatisticObjectType.RoutePoint || sot == StatisticObjectType.Queue) {
				CfgDNQuery cdq = new CfgDNQuery();
				cdq.setName(name);
				CfgDN cfgDN = confService.retrieveObject(CfgDN.class, cdq);
				foundObject = cfgDN != null;
			}
			if (sot == StatisticObjectType.GroupQueues) {
				CfgDNGroupQuery cdgq = new CfgDNGroupQuery();
				cdgq.setName(name);
				CfgDNGroup cfgDNGroup = confService.retrieveObject(CfgDNGroup.class, cdgq);
				foundObject = cfgDNGroup != null;
			}
			if (sot == StatisticObjectType.GroupPlaces) {
				CfgPlaceGroupQuery cpgq = new CfgPlaceGroupQuery();
				cpgq.setName(name);
				CfgPlaceGroup cfgPlaceGroup = confService.retrieveObject(CfgPlaceGroup.class, cpgq);
				foundObject = cfgPlaceGroup != null;
			}
			if (sot == StatisticObjectType.Agent) {
				CfgPersonQuery cpgq = new CfgPersonQuery();
				cpgq.setEmployeeId(name);
				CfgPerson cfgPerson = confService.retrieveObject(CfgPerson.class, cpgq);
				foundObject = cfgPerson != null;
			}
			if (sot == StatisticObjectType.AgentPlace) {
				CfgPlaceQuery cpgq = new CfgPlaceQuery();
				cpgq.setName(name);
				CfgPlace cfgPlace = confService.retrieveObject(CfgPlace.class, cpgq);
				foundObject = cfgPlace != null;
			}
		} catch (Exception e) {
			log.info(e);
		}
		return foundObject;
	}

	public static boolean verifyStatistic(Properties cs, String id, String name) throws JSONException {

		boolean foundStatistic = false;
		try {
			String StatServerName = cs.getProperty("StatServerName");
			IConfService confService = ConnectionUtility.getGlobalConfConnection(cs, id);
			CfgApplicationQuery cagq = new CfgApplicationQuery();
			cagq.setName(StatServerName);
			CfgApplication cfgApplication = confService.retrieveObject(CfgApplication.class, cagq);
			KeyValueCollection kvc = cfgApplication.getOptions().getList(name);
			foundStatistic = !kvc.isEmpty();

		} catch (Exception e) {
			log.info(e);
		}

		// CfgAgentGroup
		// CfgDN
		// CfgDNGroup
		// confService.
		return foundStatistic;

	}

	public static boolean saveConfigurationWB(Properties cs, String id, JSONObject jo) throws JSONException {
		KeyValueCollection wb_config = new KeyValueCollection();
		String WB_ID = jo.getString("WB_ID");
		String transactionName = cs.getProperty("transactionName");
		IConfService confService = ConnectionUtility.getGlobalConfConnection(cs, id);
		CfgTransactionQuery cfgTransactionQuery = new CfgTransactionQuery();
		cfgTransactionQuery.setName(transactionName.trim());
		try {
			log.info(id + " - cfgTransactionQuery :\n " + cfgTransactionQuery);
			CfgTransaction cfgTransaction = confService.retrieveObject(CfgTransaction.class, cfgTransactionQuery);
			log.info(id + " cfgTransaction:\n" + cfgTransaction);
			cfgTransaction.getUserProperties().remove(WB_ID);
			addKeyValye(jo, wb_config, "");
			cfgTransaction.getUserProperties().addList(WB_ID, wb_config);
			log.info(id + " cfgTransaction:\n" + cfgTransaction);
			cfgTransaction.save();
			return true;
		} catch (Exception e) {
			log.warn(id + " - cfgTransaction", e);
		}
		return false;
	}

	private static void addKeyValye(JSONObject jo, KeyValueCollection wb_config, String pre) throws JSONException {
		@SuppressWarnings("unchecked")
		Iterator<String> it = jo.keys();
		while (it.hasNext()) {
			String key = it.next();
			if (jo.get(key) instanceof JSONArray) {
				JSONArray ja = (JSONArray) jo.get(key);
				for (int i = 0; i < ja.length(); i++) {
					try {
						addKeyValye(ja.getJSONObject(i), wb_config, pre + key + "." + i + ".");
					} catch (Exception e) {
						addKeyValye(new JSONObject(), wb_config, pre + key + "." + i + ".");
					}
				}
				continue;
			}
			if (jo.get(key) instanceof JSONObject) {
				addKeyValye(jo.getJSONObject(key), wb_config, pre + key + ".");
				continue;
			}
			String val = "" + jo.get(key);
			wb_config.addString(pre + key, val);
		}
	}

	// public static boolean getPersonIsEnabled(Properties cs, String id,
	// IConfService confService, String user, String web_app) {
	// if (confService == null || confService.getProtocol().getState() !=
	// ChannelState.Opened) {
	// log.info(id + " - Genesys channel is not opened: " + (confService == null ?
	// null : confService.getProtocol().getState()));
	// return false;
	// }
	// CfgPersonQuery cfgPersonQuery = new CfgPersonQuery();
	// cfgPersonQuery.setUserName(user);
	// try {
	// log.info(id + " - cfgPersonQuery :\n " + cfgPersonQuery);
	// CfgPerson cfgPerson = confService.retrieveObject(CfgPerson.class,
	// cfgPersonQuery);
	// log.info(id + " - cfgPerson:\n" + cfgPerson);
	// log.info(id + " - web_app:" + web_app);
	// KeyValueCollection WallBoard =
	// cfgPerson.getUserProperties().getList(web_app);
	// return Boolean.parseBoolean(WallBoard.getAsString("enabled"));
	// } catch (Exception e) {
	// log.warn(id + " - cfgPerson", e);
	// return false;
	// }
	// }

	public static KeyValueCollection getPersonData(Properties cs, String id, IConfService confService, String user, String web_app) {
		CfgPersonQuery cfgPersonQuery = new CfgPersonQuery();
		cfgPersonQuery.setUserName(user);
		try {
			log.info(id + " - cfgPersonQuery :\n " + cfgPersonQuery);
			CfgPerson cfgPerson = confService.retrieveObject(CfgPerson.class, cfgPersonQuery);
			log.info(id + " - cfgPerson:\n" + cfgPerson);
			log.info(id + " - web_app:" + web_app);
			KeyValueCollection Web_App = cfgPerson.getUserProperties().getList(web_app);
			return Web_App;
		} catch (Exception e) {
			log.warn(id + " - cfgPerson", e);
			return null;
		}
	}

	private static CfgPlaceGroup getCfgPlaceGroups(Properties cs, String id, String Group) throws Exception {
		IConfService confService = ConnectionUtility.getGlobalConfConnection(cs, id);
		CfgPlaceGroupQuery cfgPlaceGroupQuery = new CfgPlaceGroupQuery();
		cfgPlaceGroupQuery.setName(Group.trim());
		CfgPlaceGroup cfgPlaceGroup = null;
		try {
			log.info(id + " - CfgPlaceGroupQuery :\n " + cfgPlaceGroupQuery);
			cfgPlaceGroup = confService.retrieveObject(CfgPlaceGroup.class, cfgPlaceGroupQuery);
			log.info(id + " - cfgDNGroup :\n " + cfgPlaceGroup);
		} catch (Exception e) {
			log.warn(id + " - cfgDNGroupQuery", e);
			return null;
		}
		return cfgPlaceGroup;
	}

	public static JSONArray getCfgPlaceGroupsList(Properties cs, String id, String Group) throws Exception {
		try {
			CfgPlaceGroup cfgPlaceGroup = getCfgPlaceGroups(cs, id, Group);
			Collection<CfgPlace> cfgPlaces = cfgPlaceGroup.getPlaces();
			Iterator<CfgPlace> iterator = cfgPlaces.iterator();
			JSONArray _VQList = new JSONArray();
			JSONObject j = null;
			while (iterator.hasNext()) {
				try {
					Collection<CfgDN> dns = iterator.next().getDNs();
					Iterator<CfgDN> dnsIterator = dns.iterator();
					while (dnsIterator.hasNext()) {
						CfgDN cfgDn = dnsIterator.next();
						j = convertDnToJSONObject(id, cfgDn);
						_VQList.put(j);
					}
				} catch (Exception e) {
					log.info("getVQList erro: " + j);
				}
			}
			log.info("- [" + id + "] getVQList: " + _VQList);
			return _VQList;
		} catch (Exception e) {
			log.warn("- [" + id + "] - " + e.getMessage() + ": ", e);
		}
		return null;
	}

	private static CfgDNGroup getCfgDnGroups(Properties cs, String id, String VqGroup) throws Exception {
		IConfService confService = ConnectionUtility.getGlobalConfConnection(cs, id);
		CfgDNGroupQuery cfgDNGroupQuery = new CfgDNGroupQuery();
		cfgDNGroupQuery.setName(VqGroup.trim());
		CfgDNGroup cfgDNGroup = null;
		try {
			log.info(id + " - cfgDNGroupQuery :\n " + cfgDNGroupQuery);
			cfgDNGroup = confService.retrieveObject(CfgDNGroup.class, cfgDNGroupQuery);
			log.info(id + " - cfgDNGroup :\n " + cfgDNGroup);
		} catch (Exception e) {
			log.warn(id + " - cfgDNGroupQuery", e);
			return null;
		}
		return cfgDNGroup;
	}

	private static CfgAgentGroup getCfAgentGroups(Properties cs, String id, String agGroup) {
		IConfService confService = ConnectionUtility.getGlobalConfConnection(cs, id);
		CfgAgentGroupQuery cfgAgentGroupQuery = new CfgAgentGroupQuery();
		cfgAgentGroupQuery.setName(agGroup.trim());
		CfgAgentGroup cfgAgentGroup = null;
		try {
			log.info(id + " - cfgAgentGroupQuery :\n " + cfgAgentGroupQuery);
			cfgAgentGroup = confService.retrieveObject(CfgAgentGroup.class, cfgAgentGroupQuery);
			log.info(id + " - cfgDNGroup :\n " + cfgAgentGroup);
		} catch (Exception e) {
			log.warn(id + " - cfgDNGroupQuery", e);
			return null;
		}
		return cfgAgentGroup;
	}

	public static JSONArray getCfgDnGroupsList(Properties cs, String id, String VqGroup) throws Exception {
		try {
			CfgDNGroup cfgDNGroup = getCfgDnGroups(cs, id, VqGroup);
			return convertDnGroupsToJArray(id, cfgDNGroup);
		} catch (Exception e) {
			log.warn("- [" + id + "] - " + e.getMessage() + ": ", e);
		}
		return null;
	}

	public static JSONArray getCfgActionCode(Properties cs, String id) throws Exception {
		JSONArray ja = new JSONArray();
		IConfService confService = ConnectionUtility.getGlobalConfConnection(cs, id);
		CfgActionCodeQuery cfgActionCodeQuery = new CfgActionCodeQuery();

//		CfgActionCode cfgActionCode = null;
		Collection<CfgActionCode> colCfgActionCode = null;
		try {
//			log.info(id + " - cfgActionCodeQuery :\n " + cfgActionCode);
			colCfgActionCode = confService.retrieveMultipleObjects(CfgActionCode.class, cfgActionCodeQuery);
//			log.info(id + " - cfgActionCode :\n " + cfgActionCode);
		} catch (Exception e) {
			log.warn(id + " - cfgActionCodeQuery", e);
			return null;
		}
		for (CfgActionCode c : colCfgActionCode) {
			ja.put(new JSONObject().put("Name", c.getName()).put("Code", c.getCode()));
		}
		return ja;
	}

	public static JSONArray getCfgAgentGroupsList(Properties cs, String id, String AgGroup) throws Exception {
		try {
			CfgAgentGroup cfgAgentGroup = getCfAgentGroups(cs, id, AgGroup);
			return convertAgentGroupsToJArray(id, cfgAgentGroup);
		} catch (Exception e) {
			log.warn("- [" + id + "] - " + e.getMessage() + ": ", e);
		}
		return null;
	}

	private static JSONArray convertAgentGroupsToJArray(String id, CfgAgentGroup cfgAgentGroup) {
		// Collection<CfgDNInfo> dns = cfgAgentGroup.getAgents()
		// Iterator<CfgDNInfo> iterator = dns.iterator();
		// JSONArray _VQList = new JSONArray();
		// JSONObject j = null;
		// while (iterator.hasNext()) {
		//
		// CfgDN cfgDn = iterator.next().getDN();
		// j = convertDnToJSONObject(id, cfgDn);
		// _VQList.put(j);
		// }
		// log.info("- [" + id + "] getVQList: " + _VQList);
		// return _VQList;
		return null; // TODO
	}

	private static JSONArray convertDnGroupsToJArray(String id, CfgDNGroup cfgDNGroup) {
		Collection<CfgDNInfo> dns = cfgDNGroup.getDNs();
		Iterator<CfgDNInfo> iterator = dns.iterator();
		JSONArray _DNList = new JSONArray();
		JSONObject j = null;
		while (iterator.hasNext()) {

			CfgDN cfgDn = iterator.next().getDN();
			j = convertDnToJSONObject(id, cfgDn);
			_DNList.put(j);
		}
		log.info("- [" + id + "] DNList: " + _DNList);
		return _DNList;
	}

	private static JSONObject convertDnToJSONObject(String id, CfgDN cfgDn) {
		JSONObject j = new JSONObject();

		try {

			log.info("- [" + id + "] getDn: " + cfgDn);

			j.put("DBID", cfgDn.getDBID());
			j.put("Type", cfgDn.getType());
			j.put("Name", cfgDn.getName());
			j.put("Number", cfgDn.getNumber());
			j.put("State", cfgDn.getState().name());

			CfgSwitch sw = cfgDn.getSwitch();
			log.debug("CfgSwitch: " + sw);
			j.put("SwitchName", sw.getName());
			j.put("SwitchType", sw.getType());
			j.put("SwitchDBID", sw.getDBID());
			// log.debug("tPhysSwitch:"+ sw.getPhysSwitch());

			CfgApplication app = sw.getTServer();
			if (app == null)
				return j;
			// log.debug("CfgApplication: "+ app);
			j.put("SwitchTserverName", app.getName());
			j.put("SwitchTserverType", app.getType().name());
			j.put("SwitchTserverDBID", app.getDBID());
			j.put("SwitchTserverPri", app.getServerInfo().getHost().getName());
			j.put("SwitchTserverPortPri", app.getServerInfo().getPort());
			CfgApplication bck = app.getServerInfo().getBackupServer();
			try {

				j.put("SwitchTserverBck", bck.getServerInfo().getHost().getName());
				j.put("SwitchTserverPortBck", bck.getServerInfo().getPort());
			} catch (Exception e) {
				log.info("backup server not present: " + bck);
				j.put("SwitchTserverBck", "");
				j.put("SwitchTserverPortBck", "0");
			}
		} catch (Exception e) {
			log.warn("getVQList error: " + j, e);
		}
		return j;
	}

	// private static JSONArray convertDnCollectionToJArray_(String id,
	// Collection<CfgDNInfo> dns) {
	// Iterator<CfgDNInfo> iterator = dns.iterator();
	// JSONArray _VQList = new JSONArray();
	// JSONObject j = null;
	// while (iterator.hasNext()) {
	// try {
	// CfgDN cfgDn = iterator.next().getDN();
	// log.info("- [" + id + "] getDn: " + cfgDn);
	// j = new JSONObject();
	// _VQList.put(j);
	// j.put("DBID", cfgDn.getDBID());
	// j.put("Type", cfgDn.getType());
	// j.put("Name", cfgDn.getName());
	// j.put("Number", cfgDn.getNumber());
	// j.put("State", cfgDn.getState().name());
	// CfgSwitch sw = cfgDn.getSwitch();
	// j.put("SwitchName", sw.getName());
	// j.put("SwitchType", sw.getType());
	// j.put("SwitchDBID", sw.getDBID());
	// CfgApplication app = sw.getTServer();
	// j.put("SwitchTserverName", app.getName());
	// j.put("SwitchTserverType", app.getType().name());
	// j.put("SwitchTserverDBID", app.getDBID());
	// j.put("SwitchTserverPri", app.getServerInfo().getHost().getName());
	// j.put("SwitchTserverPortPri", app.getServerInfo().getPort());
	// CfgApplication bck = app.getServerInfo().getBackupServer();
	// try {
	//
	// j.put("SwitchTserverBck", bck.getServerInfo().getHost().getName());
	// j.put("SwitchTserverPortBck", bck.getServerInfo().getPort());
	// } catch (Exception e) {
	// log.info("backup server not present: " + bck);
	// j.put("SwitchTserverBck", "");
	// j.put("SwitchTserverPortBck", "0");
	// }
	// } catch (Exception e) {
	// log.info("getVQList erro: " + j);
	// }
	// }
	// log.info("- [" + id + "] getVQList: " + _VQList);
	// return _VQList;
	// }

	public static JSONObject getTransactionJSON(Properties cs, String id, String transactionName) throws JSONException {
		JSONObject config = new JSONObject();
		IConfService confService = ConnectionUtility.getGlobalConfConnection(cs, id);
		CfgTransactionQuery cfgTransactionQuery = new CfgTransactionQuery();
		cfgTransactionQuery.setName(transactionName.trim());
		try {
			log.info(id + " - cfgTransactionQuery :\n " + cfgTransactionQuery);
			CfgTransaction cfgTransaction = confService.retrieveObject(CfgTransaction.class, cfgTransactionQuery);
			log.info(id + " cfgTransaction:\n" + cfgTransaction);
			KeyValueCollection userP = cfgTransaction.getUserProperties();
			@SuppressWarnings("unchecked")
			Iterator<KeyValuePair> kvp = userP.iterator();
			while (kvp.hasNext()) {
				KeyValuePair k = kvp.next();
				JSONArray ja2 = new JSONArray();
				config.put(k.getStringKey(), ja2);
				KeyValueCollection kvc = k.getTKVValue();
				@SuppressWarnings("unchecked")
				Iterator<KeyValuePair> kvp2 = kvc.iterator();
				while (kvp2.hasNext()) {
					KeyValuePair k2 = kvp2.next();
					JSONObject jo3 = new JSONObject();
					jo3.put(k2.getStringKey(), k2.getStringValue());
					ja2.put(jo3);
				}
			}
		} catch (Exception e) {
			log.warn("getTransactionSection: transactionName ", e);
		}
		return config;
	}

	public static boolean saveTransactionJSON(Properties cs, String id, String transactionName, JSONObject jo) throws JSONException {
		IConfService confService = ConnectionUtility.getGlobalConfConnection(cs, id);
		CfgTransactionQuery cfgTransactionQuery = new CfgTransactionQuery();
		cfgTransactionQuery.setName(transactionName.trim());
		try {
			KeyValueCollection trans = new KeyValueCollection();
			addKeyValyeTrans(jo, trans);
			log.info(id + " - Transaction :\n " + trans);
			CfgTransaction cfgTransaction = confService.retrieveObject(CfgTransaction.class, cfgTransactionQuery);
			log.info(id + " cfgTransaction:\n" + cfgTransaction);
			cfgTransaction.setUserProperties(trans);
			log.info(id + " cfgTransaction:\n" + cfgTransaction);
			cfgTransaction.save();
			return true;
		} catch (Exception e) {
			log.warn(id + " - cfgTransaction", e);
		}
		return false;
	}

	private static void addKeyValyeTrans(JSONObject jo, KeyValueCollection config) throws JSONException {
		@SuppressWarnings("unchecked")
		Iterator<String> it = jo.keys();
		while (it.hasNext()) {
			String key = it.next();
			if (jo.get(key) instanceof JSONArray) {
				KeyValueCollection arr =  new KeyValueCollection();
				JSONArray ja = (JSONArray) jo.get(key);
				for (int i = 0; i < ja.length(); i++) {
					try {
						addKeyValyeTrans(ja.getJSONObject(i), arr, key);
					} catch (Exception e) {
						addKeyValyeTrans(new JSONObject(), arr, key);
					}
				}
				config.addObject(key, arr);
				continue;
			}
			if (jo.get(key) instanceof JSONObject) {
				addKeyValyeTrans(jo.getJSONObject(key), config, key);
				continue;
			}
			String val = "" + jo.get(key);
			config.addString(key, val);
		}
	}

	private static void addKeyValyeTrans(JSONObject jo, KeyValueCollection wb_config, String pre) throws JSONException {
		@SuppressWarnings("unchecked")
		Iterator<String> it = jo.keys();
		while (it.hasNext()) {
			String key = it.next();
			if (jo.get(key) instanceof JSONArray) {
				JSONArray ja = (JSONArray) jo.get(key);
				for (int i = 0; i < ja.length(); i++) {
					try {
						addKeyValye(ja.getJSONObject(i), wb_config, key);
					} catch (Exception e) {
						addKeyValye(new JSONObject(), wb_config, key);
					}
				}
				continue;
			}
			if (jo.get(key) instanceof JSONObject) {
				addKeyValye(jo.getJSONObject(key), wb_config, key);
				continue;
			}
			String val = "" + jo.get(key);
			wb_config.addString(key, val);
		}
	}
	
	public static JSONObject getConfigurationWB(Properties cs, String id, String WB_ID) throws JSONException {
		KeyValueCollection wb_config = null;
		String transactionName = cs.getProperty("TransactionName");
		JSONObject config = new JSONObject();
		IConfService confService = ConnectionUtility.getGlobalConfConnection(cs, id);
		CfgTransactionQuery cfgTransactionQuery = new CfgTransactionQuery();
		cfgTransactionQuery.setName(transactionName.trim());
		try {
			log.info(id + " - cfgTransactionQuery :\n " + cfgTransactionQuery);
			CfgTransaction cfgTransaction = confService.retrieveObject(CfgTransaction.class, cfgTransactionQuery);
			log.info(id + " cfgTransaction:\n" + cfgTransaction);
			if (WB_ID == null) {
				JSONArray ja = new JSONArray();
				config.put("config", ja);
				@SuppressWarnings("unchecked")
				Enumeration<KeyValuePair> enu = cfgTransaction.getUserProperties().getEnumeration();
				while (enu.hasMoreElements()) {
					KeyValuePair kvp = enu.nextElement();
					JSONObject jo = new JSONObject();
					JSONObject jo2 = new JSONObject();
					jo.put("name", kvp.getStringKey());
					jo.put("obj", jo2);
					@SuppressWarnings("unchecked")
					Iterator<KeyValuePair> kvp2 = kvp.getTKVValue().iterator();
					while (kvp2.hasNext()) {
						KeyValuePair k = kvp2.next();
						jo2.put(k.getStringKey(), k.getValue());
					}
					ja.put(jo);
					log.info(kvp.getStringKey() + " " + kvp.getTKVValue().getString("WBType") + " " + kvp.getTKVValue().getString("Description"));
				}
				return config;
			}
			wb_config = cfgTransaction.getUserProperties().getList(WB_ID);
		} catch (Exception e) {
			log.warn(id + " - cfgTransaction", e);
			return null;
		}
		JSONObject js = new JSONObject();
		config.put("config", js);
		@SuppressWarnings("unchecked")
		Iterator<KeyValuePair> itr = wb_config.iterator();
		int k = 0;
		while (itr.hasNext()) {
			k++;
			KeyValuePair element = itr.next();
			log.info(id + " start to parse key:" + element.getStringKey() + " Value:" + element.getStringValue());
			k = insertKey(js, element.getStringKey(), element.getStringValue(), k);
			log.info(id + " result parse js:" + js);
		}
		log.info(id + " All config statistic:" + config);
		return config;
	}

	private static int insertKey(JSONObject jac, String key, String value, int id) throws JSONException {
		try {
			log.debug("key:" + key + " value:" + value);
			int pos = key.indexOf(".");
			pos = pos < 0 ? 0 : pos;
			String temp_key = key;
			if (pos <= 0) {
				value = value == null ? " " : value;
				jac.put(key, value);
				log.debug("no indent return: " + jac);
				return id;
			}
			temp_key = key.substring(0, pos);
			String next_step_key = key.substring(pos + 1);
			Object obj = null;
			try {
				obj = jac.get(temp_key);
			} catch (Exception e) {}

			JSONArray ja = null;
			JSONObject jo = null;
			if (obj == null) {
				if (Character.isDigit(next_step_key.charAt(0))) { // started a name with number is not allowed, only arrays can start with a
																	// number
					int end_dot = next_step_key.indexOf(".");
					String array_index = next_step_key.substring(0, end_dot);
					ja = new JSONArray();
					jac.put(temp_key, ja);
					jo = new JSONObject();
					ja.put(Integer.parseInt(array_index), jo);
					id++;
					jo.put("id", id);

					return insertKey(jo, next_step_key.substring(end_dot + 1), value, id);
				} else {
					jo = new JSONObject();
					jac.put(temp_key, jo);
					id++;
					jo.put("id", id);

					return insertKey(jo, next_step_key, value, id);
				}
			}
			if (Character.isDigit(next_step_key.charAt(0))) { // started a name with number is not allowed, only arrays can start with a
																// number
				int end_dot = next_step_key.indexOf(".");
				String array_index = next_step_key.substring(0, end_dot);

				try {
					jo = ((JSONArray) obj).getJSONObject(Integer.parseInt(array_index));
				} catch (Exception e) {
					jo = new JSONObject();
					id++;
					jo.put("id", id);
				}

				((JSONArray) obj).put(Integer.parseInt(array_index), jo);

				return insertKey(jo, next_step_key.substring(end_dot + 1), value, id);
			}
			// log.debug("call for obj keynext_step_key:"+next_step_key+" jo:"+jo);
			return insertKey((JSONObject) obj, next_step_key, value, id);
		} catch (Exception e) {
			log.warn("", e);
		}
		return id;
	}

	// public static JSONObject saveTransactionJSON( Properties cs, String id,
	// String transactionName, JSONArray jo) throws JSONException {
	// log.info("JSONObject: " + jo);
	// JSONObject config = new JSONObject();
	// IConfService confService = ConnectionUtility.getGlobalConfConnection( cs,
	// id);
	// CfgTransactionQuery cfgTransactionQuery = new CfgTransactionQuery();
	// cfgTransactionQuery.setName(transactionName.trim());
	// try {
	// log.info(id + " - cfgTransactionQuery :\n " + cfgTransactionQuery);
	// CfgTransaction cfgTransaction =
	// confService.retrieveObject(CfgTransaction.class, cfgTransactionQuery);
	// log.info(id + " cfgTransaction:\n" + cfgTransaction);
	// KeyValueCollection userP = cfgTransaction.getUserProperties();
	//
	// //
	// // Iterator<KeyValuePair> kvp = userP.iterator();
	// // while (kvp.hasNext()) {
	// // KeyValuePair k = kvp.next();
	// // JSONArray ja2 = new JSONArray();
	// // config.put( k.getStringKey() , ja2);
	// // KeyValueCollection kvc = k.getTKVValue();
	// // Iterator<KeyValuePair> kvp2 = kvc.iterator();
	// // while (kvp2.hasNext()) {
	// // KeyValuePair k2 = kvp2.next();
	// // JSONObject jo3 = new JSONObject();
	// // jo3.put( k2.getStringKey(), k2.getStringValue());
	// // ja2.put(jo3);
	// // }
	// // }
	// } catch (Exception e) {
	// log.warn("getTransactionSection: transactionName ", e);
	// }
	// return config;
	// }

	public static JSONArray addVQList(Properties cs, String id, String VQGroup, JSONObject jo) throws Exception {
		log.info("add JSONObject: " + jo);
		CfgDNGroup cfgDNGroup = getCfgDnGroups(cs, id, VQGroup);
		Collection<CfgDNInfo> dns = cfgDNGroup.getDNs();
		CfgDNInfo cfgDNInfo = new CfgDNInfo(cfgDNGroup.getConfigurationService(), cfgDNGroup);
		cfgDNInfo.setDNDBID(jo.getInt("DBID"));

		dns.add(cfgDNInfo);
		cfgDNGroup.setDNs(dns);
		cfgDNGroup.save();
		return convertDnGroupsToJArray(id, cfgDNGroup);
	}

	public static JSONArray delVQList(Properties cs, String id, String VQGroup, JSONObject jo) throws Exception {
		log.info("remove JSONObject: " + jo);
		CfgDNGroup cfgDNGroup = getCfgDnGroups(cs, id, VQGroup);
		Collection<CfgDNInfo> dns = cfgDNGroup.getDNs();

		for (Iterator<CfgDNInfo> iterator = dns.iterator(); iterator.hasNext();) {
			CfgDNInfo cf = iterator.next();
			if (cf.getDNDBID() == jo.getInt("DBID"))
				iterator.remove();
		}

		cfgDNGroup.setDNs(dns);
		log.info("cfgDNGroup after remove: " + cfgDNGroup);
		cfgDNGroup.save();
		return convertDnGroupsToJArray(id, cfgDNGroup);
	}
}
