<%@page import="comapp.ConfigServlet"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Date"%>
<%@page import="javax.sql.DataSource"%>
<%@page import="javax.naming.InitialContext"%>
<%@page import="javax.naming.Context"%>
<%@page import="org.json.JSONObject"%>
<%@page import="org.json.JSONArray"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.CallableStatement"%>
<%@page import="java.sql.Connection"%>
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
	String sFilterDay = request.getParameter("Data");

	//==============================
	Connection conn = null;
	CallableStatement cstmt = null;
	ResultSet rs = null;
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); 
	SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"); 
	//==	ACTION PARAMETERS	====
	//		NONE
	//==============================
	if (StringUtils.isBlank(sFilterDay)){		
		java.util.Date filterDay = new java.util.Date();
		sFilterDay = dateFormat.format(filterDay);
	}
	java.util.Date Day = dateFormat.parse(sFilterDay);
	String strDay = dateFormat.format(Day);  		

	JSONArray jaGraphs = new JSONArray();
	
	int i = 0;
	int errorCode = 0;

	int numConfOS = 0;
	String confIdObject[] = new String[5];
	String confSigla[] = new String[5];
	String confNome[] = new String[5];
	
	try {
		Context ctx = new InitialContext();
		DataSource ds = null;
		String environment = (String)session.getAttribute("Environment");
		ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/"+ConfigServlet.web_app+"CCC");
		log.info(session.getId() + " - connection CCC wait...");
		conn = ds.getConnection();
		log.info(session.getId() + " - dashboard.Graph_ReadConfig('"+environment+"','"+CodIvr+"')");
		cstmt = conn.prepareCall("{ call dashboard.Graph_ReadConfig(?,?)}");
		cstmt.setString(1,environment);
		cstmt.setString(2,CodIvr);
		rs = cstmt.executeQuery();
		while (rs.next()) {
			confIdObject[numConfOS] = rs.getString("IDOBJECT");
			confSigla[numConfOS] = rs.getString("SIGLA");
			confNome[numConfOS] = rs.getString("NOME");
			numConfOS++;
		}

		for (int iGraph=1; iGraph<=(numConfOS+1); iGraph++) {
// 		for (int iGraph=1; iGraph<=4; iGraph++) {
			JSONObject jaGraph = new JSONObject();
			JSONArray jaPrev = new JSONArray();
			JSONArray jaMaxP = new JSONArray();
			JSONArray jaCurr = new JSONArray();
			
			int prevValue[] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
							   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
							   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
							   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
			int maxPrevQuarto = 0;

			log.info(session.getId() + " - dashboard.Graph_GetForecastValues('" + CodIvr + "'," + iGraph + ",'"+strDay+"')");
			cstmt = conn.prepareCall("{ call dashboard.Graph_GetForecastValues(?,?,?)} ");
		
			cstmt.setString(1, CodIvr);
			cstmt.setInt(2, iGraph);
			cstmt.setDate(3, new Date(Day.getTime()));

			rs = cstmt.executeQuery();
			log.debug(session.getId() + " - executeCall complete");
			while (rs.next()) {
				java.util.Date timeStamp = rs.getTimestamp("TimeStamp");
				
				int Value = rs.getInt("Value");
// 				log.info(session.getId() + " - "+datetimeFormat.format(timeStamp)+" - "+Value);
				maxPrevQuarto = (timeStamp.getHours()*4)+(timeStamp.getMinutes()/15);
				prevValue[maxPrevQuarto] = Value;
			}
			
			for (int iloop=0; iloop<prevValue.length; iloop++) {
				int ipValue = prevValue[iloop];
				jaPrev.put(ipValue);
				int imValue = (int)Math.round(ipValue*1.1);
				jaMaxP.put(imValue);
			}
			jaGraph.put("prev",jaPrev);
			jaGraph.put("maxp",jaMaxP);

			try { rs.close(); } catch (Exception e) {}
			try { cstmt.close(); } catch (Exception e) {}
			
			int currValue[] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
							   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
							   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
							   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
			int maxCurrQuarto = 0;
			int initValue = 0;

			log.info(session.getId() + " - dashboard.Graph_GetStatisticalValues('" + CodIvr + "'," + iGraph + ",'"+strDay+"')");
			cstmt = conn.prepareCall("{ call dashboard.Graph_GetStatisticalValues(?,?,?)} ");
		
			cstmt.setString(1, CodIvr);
			cstmt.setInt(2, iGraph);
			cstmt.setDate(3, new Date(Day.getTime()));

			rs = cstmt.executeQuery();
			log.debug(session.getId() + " - executeCall complete");
			while (rs.next()) {
				java.util.Date timeStamp = rs.getTimestamp("TimeStamp");
				
				int Value = rs.getInt("Value");
// 				log.info(session.getId() + " - "+datetimeFormat.format(timeStamp)+" - "+Value);
				maxCurrQuarto = (timeStamp.getHours()*4)+(timeStamp.getMinutes()/15);
				currValue[maxCurrQuarto] = Value-initValue;
				initValue = Value;
			}
			for (int iloop=0; iloop<=maxCurrQuarto; iloop++) {
				jaCurr.put(currValue[iloop]);
			}
			jaGraph.put("curr",jaCurr);

			jaGraphs.put(jaGraph);
		}
		out.println(jaGraphs);
	} catch (Exception e) {
		log.error(session.getId() + " - general:" + e.getMessage(), e);
	} finally {
		try { rs.close(); } catch (Exception e) {}
		try { cstmt.close(); } catch (Exception e) {}
		try { conn.close(); } catch (Exception e) {}
	}
	log.info(session.getId() + " - ******* end page res ***** ");
%>