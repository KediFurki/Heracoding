<%@page import="comapp.LogonData"%>
<%@page import="comapp.Utility"%>
<%@page import="java.util.Properties"%>
<%@page import="comapp.ConfigServlet"%>
<%@page import="comapp.ConfigurationUtility"%>
<%@page import="com.genesyslab.platform.applicationblocks.com.IConfService"%>
<%@page import="comapp.ConnectionUtility"%>
<%@page import="java.sql.Connection"%>
<%@page import="org.apache.log4j.Logger"%>
<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1"%>
<%
	Logger log = Logger.getLogger("comapp." + this.getClass().getName());
	log.info(session.getId() + " - ******* new request ***** ");
	String user = request.getParameter("user");
	String password = request.getParameter("password");
	if (StringUtils.isBlank(user))
		user = "-";
	//user = Character.toUpperCase(user.charAt(0)) + user.substring(1);
	session.setAttribute("UserName", user);

	log.info(session.getId() + " - **** Login Info ****");
	log.info(session.getId() + " - **** User: " + user);

	String SystemEnvironment = ConfigServlet.getProperties().getProperty("SystemEnvironment","CCC");
	Properties cs = Utility.getSystemProperties(SystemEnvironment);
	IConfService conf = null;
	String environment = "";
	String placeGroup = "";
	try {
		LogonData  ld = new LogonData(user,password,SystemEnvironment);
		if (!ld.isLogged()){
			log.info(session.getId() + " - autentication failed User: " + user);
			session.setAttribute("DBSystemProperties", "");
			session.setAttribute("Environment", "");
			response.sendRedirect("index.jsp?ERROR=Errore Login");
			return;
		}
// 		conf = ConnectionUtility.getConfConnection( cs, session.getId(), user, password);
// 		environment = ConfigurationUtility.getPersonProfile(cs, ConfigServlet.web_app+"CCC", conf, user, ConfigServlet.web_app);
// 		placeGroup = ConfigurationUtility.getPersonPlaceGroup(cs, ConfigServlet.web_app+"CCC", conf, user, ConfigServlet.web_app);
		session.setAttribute("LogonData", ld);
		
		session.setAttribute("Environment", ld.getPersonProfile());
		session.setAttribute("PlaceGroup", ld.getPersonPlaceGroup());
		environment = ld.getPersonProfile();
		log.info(session.getId() + " - Environment: " + ld.getPersonProfile());
		log.info(session.getId() + " - PlaceGroup: " + ld.getPersonPlaceGroup());
	} catch (Exception e) {
		log.warn(e.getMessage(),e); 
	} finally {
	//	ConnectionUtility.disconnectConfServer(session.getId(), conf);
	}

	log.info(session.getId() + " - **** Environment: " + environment);

	session.setAttribute("CodIvr", null);

	if (StringUtils.isBlank(environment)) {
		session.setAttribute("DBSystemProperties", "");
		session.setAttribute("Environment", "");
		response.sendRedirect("index.jsp?ERROR=Errore Profilo Utente");
	} else {

		switch (environment) {
		//	****	CCC		**************************** //
		case "CCC":
			session.setAttribute("DBSystemProperties", "CCC");
			session.setAttribute("Environment", "CCC");
			session.setAttribute("CCC-MAIL", false);
			session.setAttribute("CCC-MAIL-RT", false);
			session.setAttribute("CCC-INFOMART", false);
			response.sendRedirect("MainCCC.jsp");
			break;
		case "CCC-MAIL":
			session.setAttribute("DBSystemProperties", "CCC");
			session.setAttribute("Environment", "CCC");
			session.setAttribute("CCC-MAIL", true);
			session.setAttribute("CCC-MAIL-RT", false);
			session.setAttribute("CCC-INFOMART", false);
			response.sendRedirect("MainCCC.jsp");
			break;
		case "CCC-MAIL-RT":
			session.setAttribute("DBSystemProperties", "CCC");
			session.setAttribute("Environment", "CCC");
			session.setAttribute("CCC-MAIL", true);
			session.setAttribute("CCC-MAIL-RT", true);
			session.setAttribute("CCC-INFOMART", true);
			response.sendRedirect("MainCCC.jsp");
			break;
		case "WEB-MAIL-CCC":
			session.setAttribute("DBSystemProperties", "CCC");
			session.setAttribute("Environment", "CCC");
			response.sendRedirect("MailWeb.jsp");
			break;
		case "STATO-AGENTI":
			session.setAttribute("DBSystemProperties", "CCC");
			session.setAttribute("Environment", "CCC");
			response.sendRedirect("MainSA.jsp");
			break;
		case "OUTBOUNDLEAD":
			session.setAttribute("DBSystemProperties", "CCC");
			session.setAttribute("Environment", "CCC");
			response.sendRedirect("MainOBLEAD.jsp");
			break;
		case "OUTBOUNDLEAD-BCK":
			session.setAttribute("DBSystemProperties", "CCC");
			session.setAttribute("Environment", "CCC");
			session.setAttribute("OBLEAD-BCK", true);
			response.sendRedirect("MainOBLEAD.jsp");
			break;

		//	****	CCT-F	**************************** //
		case "CCT-F":
			session.setAttribute("DBSystemProperties", "CCTF");
			session.setAttribute("Environment", "CCT-F");
			response.sendRedirect("MainCCTF.jsp");
			break;
		case "CCT-F-RT":
			session.setAttribute("DBSystemProperties", "CCTF");
			session.setAttribute("Environment", "CCT-F");
			session.setAttribute("CCT-F-RT", true);
			response.sendRedirect("MainCCTF.jsp");
			break;
		
		//	****	CCT-E	**************************** //
		case "CCT-E":
			session.setAttribute("DBSystemProperties", "CCTF");
			session.setAttribute("Environment", "CCT-E");
			response.sendRedirect("MainCCTE.jsp");
			break;
		case "CCT-E-RT":
			session.setAttribute("DBSystemProperties", "CCTF");
			session.setAttribute("Environment", "CCT-E");
			session.setAttribute("CCT-E-RT", true);
			response.sendRedirect("MainCCTE.jsp");
			break;
		
		//	****	CCT-A	**************************** //
		case "CCT-A":
			session.setAttribute("DBSystemProperties", "CCTF");
			session.setAttribute("Environment", "CCT-A");
			response.sendRedirect("MainCCTA.jsp");
			break;
		
		//	****	STC		**************************** //
		case "STC":
			session.setAttribute("DBSystemProperties", "CCTF");
			session.setAttribute("Environment", "STC");
			response.sendRedirect("MainSTC.jsp");
			break;

		//	****	AP		**************************** //
		case "AP":
			session.setAttribute("DBSystemProperties", "CCC");
			session.setAttribute("Environment", "AP");
			session.setAttribute("AP-MAIL", false);
			session.setAttribute("AP-MAIL-RT", true);
			session.setAttribute("AP-INFOMART", true);
			response.sendRedirect("MainAP.jsp");
			break;

		//	****	ADMIN	**************************** //
		case "ADMIN-CCC":
			session.setAttribute("DBSystemProperties", "CCC");
			session.setAttribute("Environment", "CCC");
			response.sendRedirect("MainAdminCCC.jsp");
			break;
		case "ADMIN-CCT-F":
			session.setAttribute("DBSystemProperties", "CCTF");
			session.setAttribute("Environment", "CCT-F");
			response.sendRedirect("MainAdminCCTF.jsp");
			break;
		case "ADMIN-CCT-E":
			session.setAttribute("DBSystemProperties", "CCTF");
			session.setAttribute("Environment", "CCT-E");
			response.sendRedirect("MainAdminCCTE.jsp");
			break;
			
		//	****	Unknown		************************ //
		default:
			session.setAttribute("DBSystemProperties", "");
			session.setAttribute("Environment", "");
			response.sendRedirect("index.jsp?ERROR=Errore Profilo Utente");
			break;
		}

	}
	log.info(session.getId() + " - ******* end page res ***** ");
/*	======================================================================================================================================================================	*
 *	ORGANIZZAZIONE JSP	                                                                                     
 *	------------------------+-----------------------------------------------------------+---+---+---+---+---+---+---+---+---+---+-----------------------------------------	*
 *	Ccc CcteCctfCctaStc Sa  |															|	Cnf Ccc CcteCctfCctaStc Ocs Ucs Inf	| ServLet / WebSocket
 *	+---+---+---+---+---+---+-----------------------------------------------------------+---+---+---+---+---+---+---+---+---+---+-----------------------------------------	*
 *	#	.	.	.	.	.	|	MainCCC.jsp												|	.	#	.	.	.	.	.	.	.	|
 *	G1	.	.	.	.	.	|		#->	ServiceGlobal.jsp								|	.	#	.	.	.	.	.	.	.	|
 *	G2	.	.	.	.	.	|		#->	AutorityGlobal.jsp								|	.	#	.	.	.	.	.	.	.	|
 *	 g	.	.	.	.	.	|		|		+->	AutorityGlobalDate.jsp					|	.	#	.	.	.	.	.	.	.	|
 *	G3	.	.	.	.	.	|		#->	CalendarGlobal.jsp								|	.	#	.	.	.	.	.	.	.	|
 *	 g	.	.	.	.	.	|		|		+->	CalendarGlobalDate.jsp					|	.	#	.	.	.	.	.	.	.	|
 *	G4	.	.	.	.	.	|		#->	SwitchGlobal.jsp								|	.	#	.	.	.	.	.	.	.	|
 *	 g	.	.	.	.	.	|		|		+->	SwitchGlobalDetail.jsp					|	.	#	.	.	.	.	.	.	.	|
 *	G5	.	.	.	.	.	|		#->	FlagGlobal.jsp									|	.	#	.	.	.	.	.	.	.	|
 *	 g	.	.	.	.	.	|		|		+->	FlagGlobalDetails.jsp					|	.	#	.	.	.	.	.	.	.	|
 *	G6	.	.	.	.	.	|		#->	ListGlobal.jsp									|	.	#	.	.	.	.	.	.	.	|
 *	G7	.	.	.	.	.	|		#->	AgentStateGlobal.jsp							|	.	.	.	.	.	.	.	.	.	|	ws:\WebSocketAgentState
 *	G8	.	.	.	.	.	|		#->	PerturbedTrafficGlobal.jsp						|	.	#	.	.	.	.	.	.	.	|
 *	G9	.	.	.	.	.	|		#->	SelfReadingGlobal.jsp							|	.	#	.	.	.	.	.	.	.	|
 *	G10	.	.	.	.	.	|		#->	MailWeb.jsp										|	.	.	.	.	.	.	.	.	.	|
 *	 g	.	.	.	.	.	|		|		+->	MailWebConversation.jsp					|	.	.	.	.	.	.	.	.	.	|
 *	 g	.	.	.	.	.	|		|		+->	MailWebEmail.jsp						|	.	.	.	.	.	.	.	.	.	|
 *	 g	.	.	.	.	.	|		|		+->	MailWebQuery.jsp						|	.	.	.	.	.	.	.	#	.	|	UploadDownloadEmail
 *	 g	.	.	.	.	.	|		|		+->	MailWebSearchList.jsp					|	.	.	.	.	.	.	.	.	.	|
 *	G11	.	.	.	.	.	|		#->	InfomartGlobal.jsp								|	.	.	.	.	.	.	.	.	#	|	UploadInfomart
 *	G12	.	.	.	.	.	|		#->	WhiteListGlobal.jsp								|	.	#	.	.	.	.	.	.	.	|	UploadWhiteList
 *	G13	.	.	.	.	.	|		#->	AbandonedGlobal.jsp								|	#	#	.	.	.	.	.	.	.	|
 *	G14	.	.	.	.	.	|		#->	PriorityServiceGlobal.jsp						|	#	#	.	.	.	.	.	.	.	|
 *	G15	.	.	.	.	.	|		#->	AmbienteSpecialGlobal.jsp						|	.	#	.	.	.	.	.	.	.	|	UploadAmbienteSpecial - DownloadAmbienteSpecial
 *	G16	.	.	.	.	.	|		#->	TopGlobal.jsp									|	.	#	.	.	.	.	.	.	.	|	UploadTop - DownloadTop
 *	G17	.	.	.	.	.	|		#->	OperatorAvailableGlobal.jsp						|	#	#	.	.	.	.	.	.	.	|
 *	G18	.	.	.	.	.	|		#->	OutboundListLeadGlobal.jsp						|	.	#	.	.	.	.	.	.	.	|
 *	G19	.	.	.	.	.	|		#->	TcpGlobal.jsp									|	.	#	.	.	.	.	.	.	.	|	UploadTcp - DownloadTcp
 *	S1	.	.	.	.	.	|		#->	Privacy.jsp										|	.	#	.	.	.	.	.	.	.	|
 *	S2	.	.	.	.	.	|		#->	Emergency.jsp									|	.	#	.	.	.	.	.	.	.	|	UploadDownloadEmergency
 *	S3	.	.	.	.	.	|		#->	Calendar.jsp									|	.	#	.	.	.	.	.	.	.	|
 *	S4	.	.	.	.	.	|		#->	HighTraffic.jsp									|	.	#	.	.	.	.	.	.	.	|
 *	S5	.	.	.	.	.	|		#->	Switch.jsp										|	.	#	.	.	.	.	.	.	.	|
 *	S6	.	.	.	.	.	|		#->	Autority.jsp									|	.	#	.	.	.	.	.	.	.	|
 *	S7	.	.	.	.	.	|		#->	PerturbedTraffic.jsp							|	.	#	.	.	.	.	.	.	.	|
 *	S8	.	.	.	.	.	|		#->	SelfReading.jsp									|	.	#	.	.	.	.	.	.	.	|
 *	S9	.	.	.	.	.	|		#->	Recall.jsp										|	.	#	.	.	.	.	.	.	.	|
 *	 s	.	.	.	.	.	|				+->	RecallBlackList.jsp						|	.	#	.	.	.	.	.	.	.	|
 *	S10	.	.	.	.	.	|		#->	DissuasionCCC.jsp								|	#	#	.	.	.	.	.	.	.	|
 *	S11	.	.	.	.	.	|		#->	Abandoned.jsp									|	#	#	.	.	.	.	.	.	.	|
 *	S12	.	.	.	.	.	|		#->	PriorityService.jsp								|	#	.	.	.	.	.	.	.	.	|
 *	S13	.	.	.	.	.	|		#->	OperatorAvailable.jsp							|	#	#	.	.	.	.	.	.	.	|
 *	S14	.	.	.	.	.	|		#->	RoutingParameters.jsp							|	#	#	.	.	.	.	.	.	.	|
 *	S15	.	.	.	.	.	|		#->	TimeoutOVSec.jsp								|	#	#	.	.	.	.	.	.	.	|
 *	+---+---+---+---+---+---+-----------------------------------------------------------+---+---+---+---+---+---+---+---+---+---+-----------------------------------------	*
 *	.	#	.	.	.	.	|	MainCCTE.jsp											|	.	.	#	.	.	.	.	.	.	|
 *	.	G1	.	.	.	.	|		#->	ServiceGlobalCCTE.jsp							|	#	.	#	.	.	.	.	.	.	|	UploadDownloadSetEmergency
 *	.	G2	.	.	.	.	|		#->	NoRecListGlobal.jsp								|	.	.	#	.	.	.	.	.	.	|
 *	.	G3	.	.	.	.	|		#->	PesseGlobal.jsp									|	.	.	.	.	.	.	.	.	.	|	UploadPesse
 *	.	 g	.	.	.	.	|		|		+->	PesseQuery.jsp							|	.	.	#	.	.	.	#	.	.	|
 *	.	G4	.	.	o	.	|		#->	SurveyWeb.jsp									|	.	.	.	.	.	.	.	.	.	|
 *	.	 g	.	.	 o	.	|		|		+->	SurveyWebMCallList.jsp					|	.	.	#	.	.	.	.	.	.	|	UploadCallList
 *	.	 g	.	.	 o	.	|		|		+->	SurveyWebMDayTime.jsp					|	.	.	#	.	.	.	.	.	.	|
 *	.	 g	.	.	 o	.	|		|		+->	SurveyWebMSurvey.jsp					|	.	.	#	.	.	.	.	.	.	|
 *	.	 g	.	.	 o	.	|		|		+->	SurveyWebQuery.jsp						|	.	.	#	.	.	.	.	#	.	|
 *	.	 g	.	.	 o	.	|		|		+->	SurveyWebToolbar.jsp					|	.	.	#	.	.	.	.	.	.	|
 *	.	S1	o	o	o	.	|		#->	SetEmergency.jsp								|	.	.	#	o	o	o	.	.	.	|	UploadDownloadSetEmergency
 *	.	S2	o	.	o	.	|		#->	Survey.jsp										|	.	.	#	.	.	.	.	.	.	|
 *	.	S3	.	.	.	.	|		#->	LivelloPesse.jsp								|	.	.	#	.	.	.	.	.	.	|
 *	.	S4	.	o	.	.	|		#->	SwitchCCT.jsp									|	.	.	#	.	o	.	.	.	.	|
 *	.	S5	o	o	.	.	|		#->	RealTimeTraffic.jsp								|	.	.	#	o	o	.	.	.	.	|
 *	+---+---+---+---+---+---+-----------------------------------------------------------+---+---+---+---+---+---+---+---+---+---+-----------------------------------------	*
 *	.	.	#	.	.	.	|	MainCCTF.jsp											|	.	.	.	#	.	.	.	.	.	|
 *	.	.	G1	.	.	.	|		#->	ServiceGlobalCCTF.jsp							|	.	.	.	#	.	.	.	.	.	|	UploadDownloadSetEmergency
 *	.	.	G2	.	.	.	|		#->	FrostEmergencyGlobal.jsp						|	.	.	.	.	.	.	.	.	.	|
 *	.	.	 g	.	.	.	|		|		+->	FrostEmergencyGlobalDetails.jsp			|	.	.	.	#	.	.	.	.	.	|
 *	.	.	S1	o	o	.	|		#->	Priority.jsp									|	.	.	.	#	.	o	.	.	.	|
 *	.	o	S2	o	o	.	|		#->	SetEmergency.jsp								|	.	.	o	#	o	o	.	.	.	|	UploadDownloadSetEmergency
 *	.	o	S3	.	o	.	|		#->	Survey.jsp										|	.	.	.	#	.	.	.	.	.	|
 *	.	.	S4	.	.	.	|		#->	TechniciansList.jsp								|	.	.	.	#	.	.	.	.	.	|	UploadTechniciansList - DownloadTechniciansList
 *	.	o	S5	o	.	.	|		#->	RealTimeTraffic.jsp								|	.	.	o	#	o	.	.	.	.	|
 *	+---+---+---+---+---+---+-----------------------------------------------------------+---+---+---+---+---+---+---+---+---+---+-----------------------------------------	*
 *	.	.	.	#	.	.	|	MainCCTA.jsp											|	.	.	.	.	#	.	.	.	.	|
 *	.	.	.	G1	.	.	|		#->	ServiceGlobalCCTA.jsp							|	.	.	.	.	#	.	.	.	.	|	UploadDownloadSetEmergency
 *	.	.	o	S1	o	.	|		#->	Priority.jsp									|	.	.	.	o	#	o	.	.	.	|
 *	.	o	o	S2	o	.	|		#->	SetEmergency.jsp								|	.	.	o	o	#	o	.	.	.	|	UploadDownloadSetEmergency
 *	.	o	.	S3	.	.	|		#->	SwitchCCT.jsp									|	.	.	o	.	#	.	.	.	.	|
 *	.	o	o	S4	.	.	|		#->	RealTimeTraffic.jsp								|	.	.	o	o	#	.	.	.	.	|
 *	+---+---+---+---+---+---+-----------------------------------------------------------+---+---+---+---+---+---+---+---+---+---+-----------------------------------------	*
 *	.	.	.	.	#	.	|	MainSTC.jsp												|	.	.	.	.	.	#	.	.	.	|
 *	.	.	.	.	G1	.	|		#->	ServiceGlobalSTC.jsp							|	.	.	.	.	.	#	.	.	.	|	UploadDownloadSetEmergency
 *	.	o	.	.	G2	.	|		#->	SurveyWeb.jsp									|	.	.	.	.	.	.	.	.	.	|
 *	.	 o	.	.	 g	.	|		|		+->	SurveyWebMCallList.jsp					|	.	.	o	.	.	.	.	.	.	|	UploadCallList
 *	.	 o	.	.	 g	.	|		|		+->	SurveyWebMDayTime.jsp					|	.	.	o	.	.	.	.	.	.	|
 *	.	 o	.	.	 g	.	|		|		+->	SurveyWebMSurvey.jsp					|	.	.	o	.	.	.	.	.	.	|
 *	.	 o	.	.	 g	.	|		|		+->	SurveyWebQuery.jsp						|	.	.	o	.	.	.	.	o	.	|
 *	.	 o	.	.	 g	.	|		|		+->	SurveyWebToolbar.jsp					|	.	.	o	.	.	.	.	.	.	|
 *	.	.	.	.	G3	.	|		#->	CalendarCCTGlobal.jsp							|	.	.	.	.	.	#	.	.	.	|
 *	.	.	.	.	 g	.	|		|		+->	CalendarCCTGlobalDate.jsp				|	.	.	.	.	.	#	.	.	.	|
 *	.	.	o	o	S1	.	|		#->	Priority.jsp									|	.	.	.	o	o	#	.	.	.	|
 *	.	o	o	o	S2	.	|		#->	SetEmergency.jsp								|	.	.	o	o	o	#	.	.	.	|	UploadDownloadSetEmergency
 *	.	o	o	.	S3	.	|		#->	Survey.jsp										|	.	.	o	o	.	#	.	.	.	|
 *	.	.	.	.	S4	.	|		#->	CalendarCCT.jsp									|	.	.	.	.	.	#	.	.	.	|
 *	.	.	.	.	S5	.	|		#->	DissuasionSTC.jsp								|	#	.	.	.	.	#	.	.	.	|
 *	.	.	.	.	 s	.	|				+->	DissuasionSTCDetail.jsp					|	#	.	.	.	.	.	.	.	.	|
 *	.	.	.	.	S6	.	|		#->	DistributionSTC.jsp								|	#	.	.	.	.	.	.	.	.	|
 *	.	.	.	.	 s	.	|				+->	DistributionSTCDetail.jsp				|	#	.	.	.	.	.	.	.	.	|
 *	+---+---+---+---+---+---+-----------------------------------------------------------+---+---+---+---+---+---+---+---+---+---+-----------------------------------------	*
 *	.	.	.	.	.	#	|	MainSA.jsp                                               	.	.	.	.	.	.	.	.	.	                    
 *	.	.	.	.	.	.	|	MainAP.jsp                                               	.	.	.	.	.	.	.	.	.	                    
 *	+---+---+---+---+---+---+-----------------------------------------------------------+---+---+---+---+---+---+---+---+---+---+-----------------------------------------	*
 *	.	.	.	.	.	.	|	MainGraph.jsp                                            	.	.	.	.	.	.	.	.	.	                    
 *	.	.	.	.	.	.	|	Parameters.jsp                                           	.	.	.	.	.	.	.	.	.	                    
 *	.	.	.	.	.	.	|	StartCollection.jsp                                      	.	.	.	.	.	.	.	.	.	                    
 *	.	.	.	.	.	.	|	SwitchService.jsp                                        	.	.	.	.	.	.	.	.	.	                    
 *	.	.	.	.	.	.	|	WizardGlobal.jsp                                         	.	.	.	.	.	.	.	.	.	                    
 *	.	.	.	.	S5	.	|		+->	DissuasionCCT.jsp								|	#	.	.	.	.	.	.	.	.	|
 *	.	.	.	.	 s	.	|				+->	DissuasionCCTDetail.jsp					|	#	.	.	.	.	.	.	.	.	|
 *	+---+---+---+---+---+---+-----------------------------------------------------------+---+---+---+---+---+---+---+---+---+---+-----------------------------------------	*
 *																						 	.	.	.	.	.	.	.	.	.	 	UploadForecast
 *	+---+---+---+---+---+---+-----------------------------------------------------------+---+---+---+---+---+---+---+---+---+---+-----------------------------------------	*
 *	Miglioramenti:                                                                                           
 *	[_]	Configurazione ASSOCIAZIONE NODI-ESIGENZE                                        
 *	[_]	Aggiunta guidata NODI                                                            
 *	=======================================================================================================================================================================	*/
%>
