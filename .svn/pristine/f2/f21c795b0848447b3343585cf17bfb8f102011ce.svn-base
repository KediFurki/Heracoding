<%@page import="org.json.JSONArray"%>
<%@page import="org.json.JSONObject"%>
<%@page import="java.sql.Timestamp"%>
<%@page import="java.util.Date"%>
<%@page import="java.util.concurrent.TimeUnit"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="java.util.ArrayList"%>
<%@page import="javax.naming.Context"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="java.util.Properties"%>
<%@page import="org.apache.log4j.Logger"%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%
	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");
	//==	REQUEST PARAMETERS	====
	String CodIvr = request.getParameter("CodIvr");
	if (StringUtils.isBlank(CodIvr)) {
		CodIvr = (String) session.getAttribute("CodIvr");
	} else {
		session.setAttribute("CodIvr", CodIvr);
	}
	//==============================
	Connection conn = null;
	CallableStatement cstmt_R = null;
	CallableStatement cstmt_B = null;
	CallableStatement cstmt_L = null;
	ResultSet rs_R = null;
	ResultSet rs_B = null;
	ResultSet rs_L = null;
	SimpleDateFormat sdf_full_day_of_year = new SimpleDateFormat("yyyy-MM-dd");
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================
	//==	FILTER PARAMETERS	====
	Date filterDay = new Date();
	String sFilterDay = sdf_full_day_of_year.format(filterDay);
	//==============================

	JSONArray jaDetails = new JSONArray();
			
	int _nBranch = 0;
	boolean _fBranch[] = null;
// 	int _nLeave[] = null;
// 	boolean _fLeave[][] = null;
// 	String Root = null;
	int numBranches = 0;
	int numLeaves = 0;
	if (StringUtils.isBlank(CodIvr)) {
	} else {
		try {
			Context ctx = new InitialContext();
			DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
			log.info(session.getId() + " - connection CCC wait...");
			conn = ds.getConnection();

			log.info(session.getId() + " - dashboard.Detail_GetInfoRoot('" + CodIvr + "','"+sFilterDay+"')");
			cstmt_R = conn.prepareCall("{ call dashboard.Detail_GetInfoRoot(?,?)} ");
			cstmt_R.setString(1, CodIvr);
			cstmt_R.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
			rs_R = cstmt_R.executeQuery();					
			log.debug(session.getId() + " - executeCall complete");
			if (rs_R.next()) {
// 				Root = rs_R.getString("COD_NODO");
				numBranches = rs_R.getInt("BRANCHES");
			}
			if (numBranches>0) {
				_fBranch = new boolean[numBranches];
// 				_nLeave = new int[numBranches];
// 				_fLeave = new boolean[numBranches][];
				String BranchesCodNodo;

				log.info(session.getId() + " - dashboard.Detail_GetInfoBranches('" + CodIvr + "','"+sFilterDay+"')");
				cstmt_B = conn.prepareCall("{ call dashboard.Detail_GetInfoBranches(?,?)} ");
				cstmt_B.setString(1, CodIvr);
				cstmt_B.setTimestamp(2, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
				rs_B = cstmt_B.executeQuery();					
				log.debug(session.getId() + " - executeCall complete");
				while (rs_B.next()) {
					BranchesCodNodo = rs_B.getString("COD_NODO");
					int nLeaves = rs_B.getInt("BRANCHES");
					int nAllLeaves = rs_B.getInt("ALLBRANCHES");
					_fBranch[_nBranch] = (rs_B.getInt("ENTERED") > 0);
// 					_nLeave[_nBranch] = rs_B.getInt("BRANCHES");
// 					_fLeave[_nBranch] = new boolean[nAllLeaves];
					numLeaves = 0;
					String idBranches = "branches_"+BranchesCodNodo;
					

					JSONObject jaDetail = new JSONObject();
					jaDetail.put("id",idBranches+"_CARD");
					jaDetail.put("value","");
					jaDetail.put("action",((_fBranch[_nBranch])?"show":"hide"));
					jaDetails.put(jaDetail);

					jaDetail = new JSONObject();
					jaDetail.put("id",idBranches+"_ENTERED");
					jaDetail.put("value",rs_B.getString("ENTERED"));
					jaDetail.put("action","value");
					jaDetails.put(jaDetail);
					
					jaDetail = new JSONObject();
					jaDetail.put("id",idBranches+"_ACTIVECALLS");
					jaDetail.put("value",rs_B.getString("ACTIVECALLS"));
					jaDetail.put("action","value");
					jaDetails.put(jaDetail);

					String arrScelte[] = new String[nLeaves];
					String arrSceltaMenu[] = new String[nAllLeaves];
					String arrCodNodo[] = new String[nAllLeaves];
// 					String arrEtichetta[] = new String[nAllLeaves];
					int arrEntered[] = new int[nAllLeaves];
					int arrActiveCalls[] = new int[nAllLeaves];
					log.info(session.getId() + " - dashboard.Detail_GetInfoLeaves('" + CodIvr + "','" + BranchesCodNodo + "','"+sFilterDay+"')");
					cstmt_L = conn.prepareCall("{ call dashboard.Detail_GetInfoLeaves(?,?,?)} ");
					cstmt_L.setString(1, CodIvr);
					cstmt_L.setString(2, BranchesCodNodo);
					cstmt_L.setTimestamp(3, new Timestamp(sdf_full_day_of_year.parse(sFilterDay).getTime()));
					rs_L = cstmt_L.executeQuery();					
					log.debug(session.getId() + " - executeCall complete");
					while (rs_L.next()) {
// 						_fLeave[_nBranch][numLeaves] = true;
						arrSceltaMenu[numLeaves] = rs_L.getString("SCELTA_MENU");
						arrCodNodo[numLeaves] = rs_L.getString("COD_NODO");
// 						arrEtichetta[numLeaves] = rs_L.getString("ETICHETTA");
						arrEntered[numLeaves] = rs_L.getInt("ENTERED");
						arrActiveCalls[numLeaves] = rs_L.getInt("ACTIVECALLS");
						numLeaves++;
					}
					String CurrScelta = "";
					int iScelte = 0;
					for (int iLoopLeaves=0; iLoopLeaves<numLeaves; iLoopLeaves++) {
						if (!arrSceltaMenu[iLoopLeaves].equals(CurrScelta)) {
							arrScelte[iScelte] = arrSceltaMenu[iLoopLeaves];
							CurrScelta = arrSceltaMenu[iLoopLeaves];
							iScelte++;
						}
					}

					for (int iLoopLeaves=0; iLoopLeaves<nLeaves; iLoopLeaves++) {
						String currScelta = arrScelte[iLoopLeaves];
						String idLeaves = "lives_"+BranchesCodNodo+"_";

						for (int iLoopAllLeaves=0; iLoopAllLeaves<numLeaves; iLoopAllLeaves++) {
							if (arrSceltaMenu[iLoopAllLeaves].equals(currScelta)) {
								
								jaDetail = new JSONObject();
								jaDetail.put("id",idLeaves+arrCodNodo[iLoopAllLeaves]+"_ENTERED");
 								jaDetail.put("value",arrEntered[iLoopAllLeaves]);
								jaDetail.put("action","value");
								jaDetails.put(jaDetail);

								jaDetail = new JSONObject();
								jaDetail.put("id",idLeaves+arrCodNodo[iLoopAllLeaves]+"_ACTIVECALLS");
								jaDetail.put("value",arrActiveCalls[iLoopAllLeaves]);
								jaDetail.put("action","value");
								jaDetails.put(jaDetail);
								
							}
						}
					}			
					rs_L.close();
					cstmt_L.close();
					_nBranch++;
				}
				rs_B.close();
				cstmt_B.close();
			}
			out.println(jaDetails);
		} catch (Exception e) {
			log.error(session.getId() + " - general: " + e.getMessage(), e);
		} finally {
			try { rs_R.close(); } catch (Exception e) {}
			try { rs_B.close(); } catch (Exception e) {}
			try { rs_L.close(); } catch (Exception e) {}
			try { cstmt_R.close(); } catch (Exception e) {}
			try { cstmt_B.close(); } catch (Exception e) {}
			try { cstmt_L.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}
	}
	log.info(session.getId() + " - ******* end page res ***** ");
%>