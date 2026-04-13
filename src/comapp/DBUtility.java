package comapp;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class DBUtility {
	public static String DBType = "mssql";
	private static String Schema = "";
	public static String version = "1.1.1";

	public static String getSchema() {
		return Schema;
	}

	public static void setSchema(String schema) {
		if (StringUtils.isNotBlank(schema))
			Schema = schema + ".";
	}

	static Logger log = Logger.getLogger("comapp.DBUtility " + version);
	static Logger logRT = Logger.getLogger("comappRT.DBUtility " + version);
	static Logger logMySQL = Logger.getLogger("comappMySQL.DBUtility " + version);

	public static String verifyNull(String t) {
		if (t == null)
			t = "";
		return t.trim();
		
	}

	public static ResultSet execute(String id, CallableStatement cs) throws Exception {
		switch (DBType.toLowerCase()) {
		case "postgresql": {

			return null;
		}
		case "oracle": {
			return null;
		}
		}
		return null;
	}

	public static Properties getSystemProperties(String id, String env) throws Exception {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		Properties prop = new Properties();
		try {
			Context ctx = new InitialContext();
			DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + env);
			log.info("- connection wait...");
			conn = ds.getConnection();
			log.info("-  call " + Schema + "System_GetParameters() ");
			cstmt = conn.prepareCall("{ call " + Schema + "System_GetParameters() } ");
			rs = cstmt.executeQuery();
			log.info("DB properties:");
			while (rs.next()) {
				prop.setProperty(rs.getString("key"), rs.getString("value"));
				log.info(rs.getString("key") + "-" + rs.getString("value"));
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return null;
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}

			try {
				cstmt.close();
			} catch (Exception e) {}

			try {
				conn.close();
			} catch (Exception e) {}
		}
		return prop;
	}

	public static Connection getConnection(String id, Logger log, String env) throws Exception {
		Connection con = null;
		Context ctx = new InitialContext();
		log.debug(id + " get connection: " + "java:/comp/env/jdbc/" + ConfigServlet.web_app + env);
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + env);
		con = ds.getConnection();
		con.setAutoCommit(false);

		return con;
	}

	public static Connection getDBConnection(String id, Logger log, String env, boolean pri) throws Exception {
		if (pri) {
			log.debug(id + "- try to primary connection: java:/comp/env/jdbc/" + ConfigServlet.web_app + env);
			try {
				return getConnection(id, log, env);
			} catch (Exception e) {
				log.warn(id + " -  primary connection is down", e);
				throw e;
			}
		} else {
			log.info(id + " -  try to bck connection sar: java:/comp/env/jdbc/" + ConfigServlet.web_app + env + "_bck");
			try {
				return getConnection(id, log, env + "_bck");
			} catch (Exception e) {
				log.error(id + " - backup connections are down", e);
				throw e;
			}
		}
	}

	public static Connection getDBConnection(String id, Logger log, String env) throws Exception {
		Connection con = null;
		try {
			con = getDBConnection(id, log, env, true);
		} catch (Exception e) {}
		return con == null ? getDBConnection(id, log, env, false) : con;
	}

	// private static JSONArray getCalls(String id, Logger log, String web_app,
	// String dn) {
	// Connection connection = null;
	// CallableStatement cstmtSQL = null;
	// ResultSet rs = null;
	// try {
	// connection = DBUtility.getDBConnection(id, log, web_app);
	// switch (DBType.toLowerCase()) {
	// case "mssql": {
	// cstmtSQL = connection.prepareCall("{ call " + Schema + "getCall(?) }");
	// cstmtSQL.setString(1, dn);
	// rs = cstmtSQL.executeQuery();
	// JSONArray ja = new JSONArray();
	// while (rs.next()) {
	// JSONObject jo = new JSONObject(rs.getString("JSONObject"));
	// ja.put(jo);
	// }
	// return ja;
	// }
	// }
	// } catch (Exception e) {
	// log.warn(id + "- ", e);
	// } finally {
	// try {
	// cstmtSQL.close();
	// } catch (Exception e) {}
	// try {
	// connection.close();
	// } catch (Exception e) {}
	// }
	// return null;
	// }

	public static JSONArray getLastEvent(String id, Logger log, String web_app, String dn) {
		Connection connection = null;
		CallableStatement cstmtSQL = null;
		ResultSet rs = null;
		try {
			connection = DBUtility.getDBConnection(id, log, web_app);
			switch (DBType.toLowerCase()) {
			case "mssql": {
				cstmtSQL = connection.prepareCall("{  call " + Schema + "LastEvent(?) }");
				cstmtSQL.setString(1, dn);

				rs = cstmtSQL.executeQuery();
				JSONArray ja = new JSONArray();
				while (rs.next()) {
					JSONObject jo = new JSONObject(rs.getString("JSONObject"));
					ja.put(jo);
				}
				try {
					rs.close();
				} catch (Exception e) {}
				try {
					cstmtSQL.close();
				} catch (Exception e) {}

				return ja;
			}
			}
		} catch (Exception e) {
			log.warn(id + "- ", e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}
			try {
				cstmtSQL.close();
			} catch (Exception e) {}
			try {
				connection.close();
			} catch (Exception e) {}
		}
		return null;
	}

	public static void insertToDB(String id, Logger log, String web_app, JSONObject json) {
		Connection connection = null;
		CallableStatement cstmtSQL = null;
		String thisDn = null;
		try {
			thisDn = json.getString("thisDN");
		} catch (Exception e) {

		}
		if (thisDn == null) {
			log.info("ThisDn is null: " + json);
			return;
		}
		try {
			connection = DBUtility.getDBConnection(id, log, web_app);
			switch (DBType.toLowerCase()) {
			case "mssql": {
				cstmtSQL = connection.prepareCall("{  call " + Schema + "insertEvent(?,?,?,?) }");
				String connid = null;
				try {
					connid = json.getString("connID");
				} catch (Exception e) {}
				cstmtSQL.setString(1, connid);
				cstmtSQL.setString(2, json.getString("MessageName"));
				cstmtSQL.setString(3, json.getString("thisDN"));
				cstmtSQL.setString(4, json.toString());
				cstmtSQL.execute();
			}
			}
			connection.commit();
		} catch (Exception e) {
			log.warn(id + " -[" + json + "] - ", e);
		} finally {
			try {
				cstmtSQL.close();
			} catch (Exception e) {}
			try {
				connection.close();
			} catch (Exception e) {}
		}

	}

	public static void removeFromDB(String id, Logger log, String web_app, JSONObject json) {
		Connection connection = null;
		CallableStatement cstmtSQL = null;
		try {
			connection = DBUtility.getDBConnection(id, log, web_app);

			switch (DBType.toLowerCase()) {
			case "mssql": {
				cstmtSQL = connection.prepareCall("{  call " + Schema + "removeCall(?) }");
				cstmtSQL.setString(1, json.getString("connID"));
				cstmtSQL.execute();
				break;
			}
			}
		} catch (Exception e) {
			log.warn(id + " -[Utility] - ", e);
		} finally {
			try {
				cstmtSQL.close();
			} catch (Exception e) {}
			try {
				connection.close();
			} catch (Exception e) {}
		}

	}

	public static ArrayList<JSONObject> getFlagStatistics(String env, String cod_ivr) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		ArrayList<JSONObject> al = new ArrayList<>();
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + env);
			logRT.debug("  - connection "+env+" wait...");
			conn = ds.getConnection();
			logRT.info("  - "+Schema + "getFlagStatistics(" + cod_ivr + ")");
			cstmt = conn.prepareCall("{ call "+Schema + "getFlagStatistics(?)}");
			cstmt.setString(1, cod_ivr);
			rs = cstmt.executeQuery();
			logRT.debug(" - executeCall complete");
			while (rs.next()) {
				JSONObject jo = new JSONObject();
				jo.put("ID", rs.getInt("ID"));
				jo.put("Operation", DBUtility.verifyNull(rs.getString("Operation")));
				JSONArray t = new JSONArray();
				for (int i = 1; i <= 5; i++) {
					t.put(rs.getString("threshold" + i));
				}
				jo.put("Threshold", t);
				jo.put("ThresholdOp", DBUtility.verifyNull(rs.getString("ThresholdOp")));
				jo.put("ReportName", rs.getString("ReportName"));
				try {
					jo.put("Value", rs.getString("Value"));
				} catch (Exception e) {
					jo.put("Value", "");
				}
				jo.put("Cod_IVR", rs.getString("Cod_IVR"));

				al.add(jo);
			}
		} catch (Exception e) {
			logRT.error(" - general: " + e.getMessage(), e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {}
		}
		return al;
	}

	
	public static ArrayList<JSONObject> getFlagStatisticsIvr(String env, String cod_ivr,ArrayList<String> reportNames) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		ArrayList<JSONObject> al = new ArrayList<>();
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + env);
			logRT.debug("  - connection CCC wait...");
			conn = ds.getConnection();
			logRT.info("  - "+Schema + "getFlagStatistics(" + cod_ivr + ")");
			cstmt = conn.prepareCall("{ call "+Schema + "getFlagStatistics(?)}");
			cstmt.setString(1, cod_ivr);
			rs = cstmt.executeQuery();
			logRT.debug(" - executeCall complete");
			while (rs.next()) {
				
				 if (!reportNames.contains(rs.getString("ReportName"))) {
					 continue;
				 }
				
				JSONObject jo = new JSONObject();
				
				jo.put("ID", rs.getInt("ID"));
				jo.put("Operation", DBUtility.verifyNull(rs.getString("Operation")));
				JSONArray t = new JSONArray();
				for (int i = 1; i <= 5; i++) {
					t.put(rs.getString("threshold" + i));
				}
				jo.put("Threshold", t);
				jo.put("ThresholdOp", DBUtility.verifyNull(rs.getString("ThresholdOp")));
				jo.put("ReportName", rs.getString("ReportName"));
				try {
					jo.put("Value", rs.getString("Value"));
				} catch (Exception e) {
					jo.put("Value", "");
				}
				jo.put("Cod_IVR", rs.getString("Cod_IVR"));
				al.add(jo);
			}
		} catch (Exception e) {
			logRT.error(" - general: " + e.getMessage(), e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {}
		}
		return al;
	}

	public static JSONArray getStatisticsById(String _environment, String cod) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		JSONArray ja = new JSONArray();
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + _environment);
			logRT.debug("  - connection CCC wait...");
			conn = ds.getConnection();
			logRT.info("  - " + Schema + "getStatisticsById(" + cod + ")");
			cstmt = conn.prepareCall("{ call " + Schema + "getStatisticsById(?)}");
			cstmt.setString(1, cod);
			rs = cstmt.executeQuery();
			logRT.debug(" - executeCall complete");
			ResultSetMetaData mta = rs.getMetaData();
			while (rs.next()) {
				JSONObject jo = new JSONObject();
				for (int i = 1; i <= mta.getColumnCount(); i++) {
					logRT.debug(" type: " + mta.getColumnType(i) + " name: " + mta.getColumnName(i) + " value: " + rs.getString(mta.getColumnName(i)));
					switch (mta.getColumnType(i)) {
					case 4: // integer
						jo.put(mta.getColumnName(i), rs.getInt(mta.getColumnName(i)));
						break;
					case -7:// bit
						jo.put(mta.getColumnName(i), rs.getBoolean(mta.getColumnName(i)));
						break;
					default:
						jo.put(mta.getColumnName(i), DBUtility.verifyNull(rs.getString(mta.getColumnName(i))));
					}

				} ;
				ja.put(jo);
			}
		} catch (Exception e) {
			logRT.error(" - general: " + e.getMessage(), e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {}
		}
		return ja;
	}

	public static String getForecastDaily(String _environment, String CodIVR, String graph, Date dt) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;		
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + _environment);
			logRT.debug("  - connection CCC wait...");
			conn = ds.getConnection();
			logRT.info("  - " + Schema + "Graph_GetTotalForecastValues(" + CodIVR + "," + graph + "," + new Timestamp(dt.getTime()) + ")");
			cstmt = conn.prepareCall("{ call " + Schema + "Graph_GetTotalForecastValues(?,?,?)}");
			cstmt.setString(1, CodIVR);
			cstmt.setString(2, graph);
			cstmt.setTimestamp(3, new Timestamp(dt.getTime()));
			rs = cstmt.executeQuery();
			logRT.debug(" - executeCall complete");			
			while (rs.next()) {
				return rs.getString("Value");
			}
		} catch (Exception e) {
			logRT.error(" - general: " + e.getMessage(), e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {}
		}
		return "0";
	}
	
	public static String getForecastQuarter(String _environment, String CodIVR, String graph, Date dt) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;		
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + _environment);
			logRT.debug("  - connection CCC wait...");
			conn = ds.getConnection();
			logRT.info("  - " + Schema + "Graph_GetSingleForecastValues(" + CodIVR + "," + graph + "," + new Timestamp(dt.getTime()) + ")");
			cstmt = conn.prepareCall("{ call " + Schema + "Graph_GetSingleForecastValues(?,?,?)}");
			cstmt.setString(1, CodIVR);
			cstmt.setString(2, graph);
			cstmt.setTimestamp(3, new Timestamp(dt.getTime()));
			rs = cstmt.executeQuery();
			logRT.debug(" - executeCall complete");			
			while (rs.next()) {
				return rs.getString("Value");
			}
		} catch (Exception e) {
			logRT.error(" - general: " + e.getMessage(), e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {}
		}
		return "0";
	}
	public static JSONArray getStatisticsIvr(String environment, boolean b, String cod_ivr, ArrayList<String> reportNames) {
		///bug per questo tipo di report non � possibile effettuare calcoli ma il flags deve essere uguale alla stataistica
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		JSONArray ja = new JSONArray();
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + environment);
			logRT.debug("  - connection CCC wait...");
			conn = ds.getConnection();
			logRT.info("  - "+Schema + "getStatisitc(" + cod_ivr + ")");
			cstmt = conn.prepareCall("{ call "+Schema + "getStatisitc(?,?)}");
			cstmt.setString(1, cod_ivr);
			cstmt.setBoolean(2, b);
			rs = cstmt.executeQuery();
			logRT.debug(" - executeCall complete");
			ResultSetMetaData mta = rs.getMetaData();
			while (rs.next()) {
				 if (!reportNames.contains(rs.getString("ReportName"))) {
					 continue;
				 }
				JSONObject jo = new JSONObject();
				for (int i = 1; i <= mta.getColumnCount(); i++) {
					logRT.debug(" type: " + mta.getColumnType(i) + " name: " + mta.getColumnName(i) + " value: " + rs.getString(mta.getColumnName(i)));
					switch (mta.getColumnType(i)) {
					case 4: // integer

						jo.put(mta.getColumnName(i), rs.getInt(mta.getColumnName(i)));
						break;
					case -7:// bit
						jo.put(mta.getColumnName(i), rs.getBoolean(mta.getColumnName(i)));
						break;
					default:
						jo.put(mta.getColumnName(i), DBUtility.verifyNull(rs.getString(mta.getColumnName(i))));
					}
				}
				ja.put(jo);
			}
		} catch (Exception e) {
			logRT.error(" - general: " + e.getMessage(), e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {}
		}
		return ja;
	}
	public static JSONArray getStatistics(String _environment, boolean onstart, String cod_ivr) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		JSONArray ja = new JSONArray();
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + _environment);
			logRT.debug("  - connection CCC wait...");
			conn = ds.getConnection();
			logRT.info("  - "+Schema + ".getStatisitc(" + cod_ivr + ")");
			cstmt = conn.prepareCall("{ call "+Schema + "getStatisitc(?,?)}");
			cstmt.setString(1, cod_ivr);
			cstmt.setBoolean(2, onstart);
			rs = cstmt.executeQuery();
			logRT.debug(" - executeCall complete");
			ResultSetMetaData mta = rs.getMetaData();
			while (rs.next()) {
				JSONObject jo = new JSONObject();
				for (int i = 1; i <= mta.getColumnCount(); i++) {
					logRT.debug(" type: " + mta.getColumnType(i) + " name: " + mta.getColumnName(i) + " value: " + rs.getString(mta.getColumnName(i)));
					switch (mta.getColumnType(i)) {
					case 4: // integer

						jo.put(mta.getColumnName(i), rs.getInt(mta.getColumnName(i)));
						break;
					case -7:// bit
						jo.put(mta.getColumnName(i), rs.getBoolean(mta.getColumnName(i)));
						break;
					default:
						jo.put(mta.getColumnName(i), DBUtility.verifyNull(rs.getString(mta.getColumnName(i)) ));
					}
				}
				ja.put(jo);
			}
		} catch (Exception e) {
			logRT.error(" - general: " + e.getMessage(), e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {}
		}
		return ja;
	}

	public static JSONArray getStatisticsValue(String _environment, String cod_ivr, Date date) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		JSONArray ja = new JSONArray();
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + _environment);
			logRT.debug("  - connection CCC wait...");
			conn = ds.getConnection();
			logRT.info("  - "+Schema + "AgentDetailsHistorical(" + new Timestamp(date.getTime()) + "," + cod_ivr + ")");
			cstmt = conn.prepareCall("{ call "+Schema + "AgentDetailsHistorical(?,?)}");
			cstmt.setTimestamp(1, new Timestamp(date.getTime()));
			cstmt.setString(2, cod_ivr);
			rs = cstmt.executeQuery();
			logRT.debug(" - executeCall complete");
			ResultSetMetaData mta = rs.getMetaData();
			while (rs.next()) {
				JSONObject jo = new JSONObject();
				for (int i = 1; i <= mta.getColumnCount(); i++) {
					logRT.debug(" type: " + mta.getColumnType(i) + " name: " + mta.getColumnName(i) + " value: " + rs.getString(mta.getColumnName(i)));
					switch (mta.getColumnType(i)) {
					case 4: // integer

						jo.put(mta.getColumnName(i), rs.getInt(mta.getColumnName(i)));
						break;
					case -7:// bit
						jo.put(mta.getColumnName(i), rs.getBoolean(mta.getColumnName(i)));
						break;
					default:
						jo.put(mta.getColumnName(i), DBUtility.verifyNull(rs.getString(mta.getColumnName(i))));
					}
				}
				ja.put(jo);
			}
		} catch (Exception e) {
			logRT.error(" - general: " + e.getMessage(), e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {}
		}
		return ja;
	}

	public static JSONArray getStatisticsValueLast(String _environment, String cod_ivr, Date date, ArrayList<String> stats) {
		Connection conn = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		JSONArray ja = new JSONArray();
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + _environment);
			logRT.debug("  - connection CCC wait...");
			conn = ds.getConnection();
			logRT.info("  - "+Schema + "AgentDetailsHistorical(" + new Timestamp(date.getTime()) + "," + cod_ivr + ")");
			cstmt = conn.prepareCall("{ call "+Schema + "AgentDetailsLastHistorical(?,?)}");
			cstmt.setTimestamp(1, new Timestamp(date.getTime()));
			cstmt.setString(2, cod_ivr);
			rs = cstmt.executeQuery();
			logRT.debug(" - executeCall complete");
			ResultSetMetaData mta = rs.getMetaData();
			while (rs.next()) {
				 if (!stats.contains(rs.getString("ReportName"))) {
					 continue;
				 }
				JSONObject jo = new JSONObject();
				for (int i = 1; i <= mta.getColumnCount(); i++) {
					logRT.debug(" type: " + mta.getColumnType(i) + " name: " + mta.getColumnName(i) + " value: " + rs.getString(mta.getColumnName(i)));
					switch (mta.getColumnType(i)) {
					case 4: // integer

						jo.put(mta.getColumnName(i), rs.getInt(mta.getColumnName(i)));
						break;
					case -7:// bit
						jo.put(mta.getColumnName(i), rs.getBoolean(mta.getColumnName(i)));
						break;
					default:
						jo.put(mta.getColumnName(i), DBUtility.verifyNull(rs.getString(mta.getColumnName(i))));
					}
				}
				ja.put(jo);
			}
		} catch (Exception e) {
			logRT.error(" - general: " + e.getMessage(), e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {}
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {}
		}
		return ja;
	}

	public static void insetStatInfoToDB(String id, String env, JSONObject json) {
		logRT.debug(" -[" + env + "] - " + json);
		Connection connection = null;
		CallableStatement cstmtSQL = null;
		try {
			connection = DBUtility.getDBConnection(id, logRT, env);

			switch (DBType.toLowerCase()) {
			case "mssql": {
				
				cstmtSQL = connection.prepareCall("{  call " + Schema + "insertStatisticToDb(?,?) }");
				
				// referenceId
				cstmtSQL.setInt(1, json.getInt("referenceId"));
				cstmtSQL.setString(2, json.getString("stringValue"));
				cstmtSQL.execute();
				connection.commit();
				break;
			}
			}
		} catch (Exception e) {
			logRT.warn(id + " - ", e);
		} finally {
			try {
				cstmtSQL.close();
			} catch (Exception e) {}
			try {
				connection.close();
			} catch (Exception e) {}
		}

	}

	public static void putStatisticalValues(String _environment, int id, String CodIvr, String ReportName, String Value, Timestamp timestamp) {

		Connection conn = null;
		CallableStatement cstmt = null;
		try {

			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + _environment);
			logRT.debug("  - connection CCC wait...");
			conn = ds.getConnection();
			logRT.info("  - "+Schema + ".putStatisticalValues(" + id + "," + CodIvr + "," + ReportName + "," + Value + "," + timestamp + " )");
			cstmt = conn.prepareCall("{ call "+Schema + "putStatisticalValues(?,?,?,?,?)}");
			cstmt.setInt(1, id);
			cstmt.setString(2, CodIvr);
			cstmt.setString(3, ReportName);
			cstmt.setString(4, Value);
			cstmt.setTimestamp(5, timestamp);
			cstmt.execute();
			conn.commit();
		} catch (Exception e) {
			logRT.warn(id + " - ", e);
		} finally {
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {}
		}

	}

	
	public static void putThresholds(String _environment,  String CodIvr, String ReportName, String ThresholdOp, String Threshold1, String Threshold2) {
		Connection conn = null;
		CallableStatement cstmt = null;
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + _environment);
			logRT.debug("  - connection CCC wait...");
			conn = ds.getConnection();
			logRT.debug("  - "+Schema + "putThresholds(" + CodIvr + "," + ReportName + "," + ThresholdOp + "," + Threshold1 + "," + Threshold2 + " )");
			cstmt = conn.prepareCall("{ call "+Schema + "putThresholds(?,?,?,?,?)}");
			cstmt.setString(1, CodIvr);
			cstmt.setString(2, ReportName);
			cstmt.setString(3, ThresholdOp);
			cstmt.setString(4, Threshold1);
			cstmt.setString(5, Threshold2);
			cstmt.execute();
			conn.commit();
		} catch (Exception e) {
			logRT.warn(" - ", e);
		} finally {
			try {
				cstmt.close();
			} catch (Exception e) {}
			try {
				conn.close();
			} catch (Exception e) {} 
		}
	}

	public static void putStatisticalValuesForMySQL(String _environment, Hashtable<String, JSONObject> hOperation, Timestamp timestamp) throws Exception {
		Connection conn = null;
		CallableStatement cstmt = null;
		try {
//			CREATE TABLE `REPORT_REALTIME` (
//					  `DATA_AGGIORNAMENTO` datetime DEFAULT NULL,
//					  `SERVIZI` varchar(100) DEFAULT NULL,
//					  `TOTALE_CHIAMATE_ENTRATE` decimal(10,0) DEFAULT NULL,
//					  `TOTALE_CHIAMATE_RISPOSTE` decimal(10,0) DEFAULT NULL,
//					  `TOTALE_CHIAMATE_ABBANDONATE` decimal(10,0) DEFAULT NULL,
//					  `TOTALE_RISPOSTE_ENTRO_120s` decimal(10,0) DEFAULT NULL,
//					  `TOTALE_RISPOSTE_DOPO_120s` decimal(10,0) DEFAULT NULL,
//					  `TOTALE_ABBANDONATE_ENTRO_120s` decimal(10,0) DEFAULT NULL,
//					  `TOTALE_ABBANDONATE_DOPO_120s` decimal(10,0) DEFAULT NULL,
//					  `TEMPO_MEDIO_ATTESA` decimal(10,0) DEFAULT NULL,
//					  `TEMPO_MEDIO_RISPOSTA` decimal(10,0) DEFAULT NULL,
//					  `CHIAMATE_TRABOCCATE` decimal(10,0) DEFAULT NULL,
//					  `TOTALE_LOGGATI_SU_CODE` decimal(10,0) DEFAULT NULL,
//					  `TEMPI_TOTALI_ATTESA` decimal(10,0) DEFAULT NULL,
//					  `TEMPO_TOTALE_ABBANDONATE` decimal(10,0) DEFAULT NULL,
//					  `TEMPO_TOTALE_RISPOSTE` decimal(10,0) DEFAULT NULL,
//					  `TEMPI_TOTALI_ATTESA_RISPOSTA` decimal(10,0) DEFAULT NULL,
//					  `TEMPI_TOTALI_ATTESA_ABBANDONATE` decimal(10,0) DEFAULT NULL,
//					  `TEMPO_TOTALI_CONVERSAZIONE` decimal(10,0) DEFAULT NULL,
//					  `TEMPO_MEDIO_CONVERSAZIONE` decimal(10,0) DEFAULT NULL
//					) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + _environment);
			logMySQL.debug("  - connection "+_environment+" wait...");
			conn = ds.getConnection();

			Hashtable<String, JSONObject> hValue = new Hashtable<String, JSONObject>();
//			JSONObject jo = new JSONObject();

			Enumeration<String> op = hOperation.keys();
			while (op.hasMoreElements()) {
				String key = op.nextElement();
				JSONObject jo = hOperation.get(key);
//				int id = jo.getInt("ID");
				String CodIvr = jo.getString("Cod_IVR");
				String ReportName = jo.getString("ReportName");
				String Value = jo.getString("Value");
				
				JSONObject jv = hValue.get(CodIvr);
				if (jv==null) jv = new JSONObject();
				jv.put(ReportName, Value);
				hValue.put(CodIvr,jv);
				logMySQL.debug(" - hValue - CodIvr:" +CodIvr+ " - " + jv);
			}

			Enumeration<String> va = hValue.keys();
			while (va.hasMoreElements()) {
				String CodIvr = va.nextElement();
				JSONObject jv = hValue.get(CodIvr);
				logMySQL.info(" - "+Schema+"ReportRT_PutRecord('" + sdf.format(new Date(timestamp.getTime())) + "','"+CodIvr+"',"+"'"+
																	","+jv.getString("TOTALE_CHIAMATE_ENTRATE")+","+jv.getString("TOTALE_CHIAMATE_RISPOSTE")+","+jv.getString("TOTALE_CHIAMATE_ABBANDONATE")+
																	","+jv.getString("TOTALE_RISPOSTE_ENTRO_120s")+","+jv.getString("TOTALE_RISPOSTE_DOPO_120s")+","+jv.getString("TOTALE_ABBANDONATE_ENTRO_120s")+
																	","+jv.getString("TOTALE_ABBANDONATE_DOPO_120s")+","+jv.getString("TEMPO_MEDIO_ATTESA")+","+jv.getString("TEMPO_MEDIO_RISPOSTA")+
																	","+jv.getString("CHIAMATE_TRABOCCATE")+","+jv.getString("TOTALE_LOGGATI_SU_CODE")+","+jv.getString("TEMPI_TOTALI_ATTESA")+
																	","+jv.getString("TEMPO_TOTALE_ABBANDONATE")+","+jv.getString("TEMPO_TOTALE_RISPOSTE")+","+jv.getString("TEMPI_TOTALI_ATTESA_RISPOSTA")+
																	","+jv.getString("TEMPI_TOTALI_ATTESA_ABBANDONATE")+","+jv.getString("TEMPO_TOTALI_CONVERSAZIONE")+","+jv.getString("TEMPO_MEDIO_CONVERSAZIONE")+")");
				
				cstmt = conn.prepareCall("{ call "+Schema+"ReportRT_PutRecord(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
				cstmt.setTimestamp(1, timestamp);

				cstmt.setString(2, CodIvr);
				cstmt.setDouble(3, Double.parseDouble(jv.getString("TOTALE_CHIAMATE_ENTRATE")));
				cstmt.setDouble(3, Double.parseDouble(jv.getString("TOTALE_CHIAMATE_ENTRATE")));
				cstmt.setDouble(4, Double.parseDouble(jv.getString("TOTALE_CHIAMATE_RISPOSTE")));
				cstmt.setDouble(5, Double.parseDouble(jv.getString("TOTALE_CHIAMATE_ABBANDONATE")));
				cstmt.setDouble(6, Double.parseDouble(jv.getString("TOTALE_RISPOSTE_ENTRO_120s")));
				cstmt.setDouble(7, Double.parseDouble(jv.getString("TOTALE_RISPOSTE_DOPO_120s")));
				cstmt.setDouble(8, Double.parseDouble(jv.getString("TOTALE_ABBANDONATE_ENTRO_120s")));
				cstmt.setDouble(9, Double.parseDouble(jv.getString("TOTALE_ABBANDONATE_DOPO_120s")));
				cstmt.setDouble(10, Double.parseDouble(jv.getString("TEMPO_MEDIO_ATTESA")));
				cstmt.setDouble(11, Double.parseDouble(jv.getString("TEMPO_MEDIO_RISPOSTA")));
				cstmt.setDouble(12, Double.parseDouble(jv.getString("CHIAMATE_TRABOCCATE")));
				cstmt.setDouble(13, Double.parseDouble(jv.getString("TOTALE_LOGGATI_SU_CODE")));
				cstmt.setDouble(14, Double.parseDouble(jv.getString("TEMPI_TOTALI_ATTESA")));
				cstmt.setDouble(15, Double.parseDouble(jv.getString("TEMPO_TOTALE_ABBANDONATE")));
				cstmt.setDouble(16, Double.parseDouble(jv.getString("TEMPO_TOTALE_RISPOSTE")));
				cstmt.setDouble(17, Double.parseDouble(jv.getString("TEMPI_TOTALI_ATTESA_RISPOSTA")));
				cstmt.setDouble(18, Double.parseDouble(jv.getString("TEMPI_TOTALI_ATTESA_ABBANDONATE")));
				cstmt.setDouble(19, Double.parseDouble(jv.getString("TEMPO_TOTALI_CONVERSAZIONE")));
				cstmt.setDouble(20, Double.parseDouble(jv.getString("TEMPO_MEDIO_CONVERSAZIONE")));
				cstmt.execute();
				logMySQL.debug(" - executeCall complete");
				try { cstmt.close(); } catch (Exception e) {}
//				stmt = conn.createStatement();
//				String sqlUpdate = "UPDATE dashboard.REPORT_REALTIME SET "+ReportName+"="+Value+" WHERE DATA_AGGIORNAMENTO=CONVERT(DATETIME,'"+sdf.format(new Date(timestamp.getTime()))+"',120) AND SERVIZI='"+CodIvr+"'";
//				String sqlInsert = "INSERT INTO dashboard.REPORT_REALTIME (DATA_AGGIORNAMENTO, SERVIZI, "+ReportName+") VALUES (CONVERT(DATETIME,'"+sdf.format(new Date(timestamp.getTime()))+"',120),'"+CodIvr+"', "+Value+")";
//				logMySQL.info("  - SQL: "+sqlUpdate);
//				int res = stmt.executeUpdate(sqlUpdate);
//				if (res == 0) {
//					logMySQL.info("  - SQL: "+sqlInsert);
//					stmt.executeUpdate(sqlInsert);
//				}
			}		
		} catch (Exception e) {
			throw e;
		} finally {
			try { cstmt.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}
	}
}
