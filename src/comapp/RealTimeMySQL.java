package comapp;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

public class RealTimeMySQL implements Runnable {
	Logger log = Logger.getLogger("comappMySQL." + this.getClass());
	private int minute_step;
	private RealTimeMySQLServlet realTimeMySQLServlet = null;

	public RealTimeMySQL(int minute, RealTimeMySQLServlet rtMySQLServlet) {
		minute_step = minute;
		realTimeMySQLServlet = rtMySQLServlet;
	}
	
	public void run() {
		log.info("START  - RealTime MySQL");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, -minute_step);
		int minutes = cal.get(Calendar.MINUTE);
		minutes = cal.get(Calendar.MINUTE);
		int mod = minutes % minute_step;
		cal.add(Calendar.MINUTE, -mod);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date stepDate = cal.getTime();
		cal.add(Calendar.DAY_OF_YEAR, -7);
		Date purgDate = cal.getTime();

		Connection connSql = null;
		CallableStatement cstmtSql = null;
		ResultSet rs = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + "CCTF");
			log.debug("       - connection CCTF wait...");
			connSql = ds.getConnection();
			
			// =================================================================
			log.info("FASE 1 - SET DATE TO EXPORT - START");
			log.info("       - "+DBUtility.getSchema() + ".ReportRT_DateBegin('" + sdf.format(stepDate) + "')");
			cstmtSql = connSql.prepareCall("{ call "+DBUtility.getSchema() + "ReportRT_DateBegin(?)}");
			cstmtSql.setTimestamp(1, new java.sql.Timestamp(stepDate.getTime()));
			cstmtSql.execute();
			log.debug("       - execute complete");
			try { cstmtSql.close(); } catch (Exception e) {}
			
			// =================================================================
			log.info("FASE 2 - EXPORT REALTIME - Data:" + sdf.format(stepDate));
			insertReport(connSql,stepDate,purgDate,true);

			// =================================================================
			log.info("FASE 3 - SET DATE TO EXPORT - END");
			log.info("       - "+DBUtility.getSchema() + ".ReportRT_DateEnd('" + sdf.format(stepDate) + "')");
			cstmtSql = connSql.prepareCall("{ call "+DBUtility.getSchema() + "ReportRT_DateEnd(?)}");
			cstmtSql.setTimestamp(1, new java.sql.Timestamp(stepDate.getTime()));
			cstmtSql.execute();
			log.debug("       - execute complete");
			try { cstmtSql.close(); } catch (Exception e) {}
			
			// =================================================================
			log.info("FASE 4 - CHECK UNEXPORTED REALTIME");
			log.info("       - "+DBUtility.getSchema() + ".ReportRT_GetNotExecuteDate()");
			cstmtSql = connSql.prepareCall("{ call "+DBUtility.getSchema() + "ReportRT_GetNotExecuteDate()}");
			rs = cstmtSql.executeQuery();
			log.debug("       - executeQuery complete");
			while (rs.next()) {
				Timestamp ts = rs.getTimestamp("DATA_AGGIORNAMENTO");;
				Date dtne = new Date(ts.getTime());
				
				// =================================================================
				log.info("FASE 5 - EXPORT REALTIME - Data:" + sdf.format(dtne));
				insertReport(connSql,dtne,null,false);

				// =================================================================
				log.info("FASE 6 - SET DATE TO EXPORT - END");
				log.info("       - "+DBUtility.getSchema() + ".ReportRT_DateEnd('" + sdf.format(dtne) + "')");
				cstmtSql = connSql.prepareCall("{ call "+DBUtility.getSchema() + "ReportRT_DateEnd(?)}");
				cstmtSql.setTimestamp(1, new java.sql.Timestamp(dtne.getTime()));
				cstmtSql.execute();
				log.debug("       - execute complete");
				try { cstmtSql.close(); } catch (Exception e) {}
			}
		} catch (Exception e) {
			log.fatal("       - run() Exception: "+e.getMessage(), e);
		} finally {
			try { rs.close(); } catch (Exception e) {}
			try { cstmtSql.close(); } catch (Exception e) {}
			try { connSql.close(); } catch (Exception e) {}
		}
		log.info("STOP   - RealTime MySQL");
		realTimeMySQLServlet.changeExecution();
	}
	
	private void insertReport(Connection connSql, Date stepDate, Date purgDate, boolean purge) throws Exception {
		Connection connMyS = null;
		Statement stmtMyS = null;
		CallableStatement cstmtSql = null;
		ResultSet rs = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		SimpleDateFormat sdfD = new SimpleDateFormat("dd/MM/yyyy");
		SimpleDateFormat sdfT = new SimpleDateFormat("HH:mm:ss");
		SimpleDateFormat sdfP = new SimpleDateFormat("yyyy/MM/dd");
		int nReportIns = 0;
		
		try {
			Context ctx = new InitialContext();
			DataSource ds = null;
			ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + "MYSQL");
			log.debug("       - connection MYSQL wait...");
			connMyS = ds.getConnection();
			connMyS.setAutoCommit(false);
	
			// =================================================================
			log.info("       - Step 1 - INSERT INTO LOG_ESPORTAZIONE_CCT");
			stmtMyS = connMyS.createStatement();
			String sqlInsIni = "INSERT INTO LOG_ESPORTAZIONE_CCT (DATA, ORA, ATTIVITA) VALUES ('"+sdfD.format(new Date())+"','"+sdfT.format(new Date())+"', 'INIZIO ESTRAZIONE REALTIME')";
			log.info("                - SQL: "+sqlInsIni);
			stmtMyS.executeUpdate(sqlInsIni);
			log.debug("               - executeUpdate complete");
	
			// =================================================================
			log.info("       - Step 2 - INSERT INTO REPORT_REALTIME");
			log.info("                - "+DBUtility.getSchema() + ".MySQL_GetReportRealtime('" + sdf.format(stepDate) + "')");
			cstmtSql = connSql.prepareCall("{ call "+DBUtility.getSchema() + "MySQL_GetReportRealtime(?)}");
			cstmtSql.setTimestamp(1, new java.sql.Timestamp(stepDate.getTime()));
			rs = cstmtSql.executeQuery();
			log.debug("                - executeQuery complete");
			while (rs.next()) {
				Timestamp DATA_AGGIORNAMENTO = rs.getTimestamp("DATA_AGGIORNAMENTO");;
				String SERVIZI = rs.getString("SERVIZI");;
				int TOTALE_CHIAMATE_ENTRATE = rs.getInt("TOTALE_CHIAMATE_ENTRATE");
				int TOTALE_CHIAMATE_RISPOSTE = rs.getInt("TOTALE_CHIAMATE_RISPOSTE");
				int TOTALE_CHIAMATE_ABBANDONATE = rs.getInt("TOTALE_CHIAMATE_ABBANDONATE");
				int TOTALE_RISPOSTE_ENTRO_120s = rs.getInt("TOTALE_RISPOSTE_ENTRO_120s");
				int TOTALE_RISPOSTE_DOPO_120s = rs.getInt("TOTALE_RISPOSTE_DOPO_120s");
				int TOTALE_ABBANDONATE_ENTRO_120s = rs.getInt("TOTALE_ABBANDONATE_ENTRO_120s");
				int TOTALE_ABBANDONATE_DOPO_120s = rs.getInt("TOTALE_ABBANDONATE_DOPO_120s");
				int TEMPO_MEDIO_ATTESA = rs.getInt("TEMPO_MEDIO_ATTESA");
				int TEMPO_MEDIO_RISPOSTA = rs.getInt("TEMPO_MEDIO_RISPOSTA");
				int CHIAMATE_TRABOCCATE = rs.getInt("CHIAMATE_TRABOCCATE");
				int TOTALE_LOGGATI_SU_CODE = rs.getInt("TOTALE_LOGGATI_SU_CODE");
				int TEMPI_TOTALI_ATTESA = rs.getInt("TEMPI_TOTALI_ATTESA");
				int TEMPO_TOTALE_ABBANDONATE = rs.getInt("TEMPO_TOTALE_ABBANDONATE");
				int TEMPO_TOTALE_RISPOSTE = rs.getInt("TEMPO_TOTALE_RISPOSTE");
				int TEMPI_TOTALI_ATTESA_RISPOSTA = rs.getInt("TEMPI_TOTALI_ATTESA_RISPOSTA");
				int TEMPI_TOTALI_ATTESA_ABBANDONATE = rs.getInt("TEMPI_TOTALI_ATTESA_ABBANDONATE");
				int TEMPO_TOTALI_CONVERSAZIONE = rs.getInt("TEMPO_TOTALI_CONVERSAZIONE");
				int TEMPO_MEDIO_CONVERSAZIONE = rs.getInt("TEMPO_MEDIO_CONVERSAZIONE");
				String sqlInsRec = "INSERT INTO REPORT_REALTIME ("
									+" DATA_AGGIORNAMENTO"
									+",SERVIZI"
									+",TOTALE_CHIAMATE_ENTRATE"
									+",TOTALE_CHIAMATE_RISPOSTE"
									+",TOTALE_CHIAMATE_ABBANDONATE"
									+",TOTALE_RISPOSTE_ENTRO_120s"
									+",TOTALE_RISPOSTE_DOPO_120s"
									+",TOTALE_ABBANDONATE_ENTRO_120s"
									+",TOTALE_ABBANDONATE_DOPO_120s"
									+",TEMPO_MEDIO_ATTESA"
									+",TEMPO_MEDIO_RISPOSTA"
									+",CHIAMATE_TRABOCCATE"
									+",TOTALE_LOGGATI_SU_CODE"
									+",TEMPI_TOTALI_ATTESA"
									+",TEMPO_TOTALE_ABBANDONATE"
									+",TEMPO_TOTALE_RISPOSTE"
									+",TEMPI_TOTALI_ATTESA_RISPOSTA"
									+",TEMPI_TOTALI_ATTESA_ABBANDONATE"
									+",TEMPO_TOTALI_CONVERSAZIONE"
									+",TEMPO_MEDIO_CONVERSAZIONE"
									+") VALUES ("
									+"'"+sdf.format(new Date(DATA_AGGIORNAMENTO.getTime()))+"'"
									+",'"+SERVIZI+"'"
									+","+TOTALE_CHIAMATE_ENTRATE
									+","+TOTALE_CHIAMATE_RISPOSTE
									+","+TOTALE_CHIAMATE_ABBANDONATE
									+","+TOTALE_RISPOSTE_ENTRO_120s
									+","+TOTALE_RISPOSTE_DOPO_120s
									+","+TOTALE_ABBANDONATE_ENTRO_120s
									+","+TOTALE_ABBANDONATE_DOPO_120s
									+","+TEMPO_MEDIO_ATTESA
									+","+TEMPO_MEDIO_RISPOSTA
									+","+CHIAMATE_TRABOCCATE
									+","+TOTALE_LOGGATI_SU_CODE
									+","+TEMPI_TOTALI_ATTESA
									+","+TEMPO_TOTALE_ABBANDONATE
									+","+TEMPO_TOTALE_RISPOSTE
									+","+TEMPI_TOTALI_ATTESA_RISPOSTA
									+","+TEMPI_TOTALI_ATTESA_ABBANDONATE
									+","+TEMPO_TOTALI_CONVERSAZIONE
									+","+TEMPO_MEDIO_CONVERSAZIONE
									+")";
				log.info("       -  - SQL: "+sqlInsRec);
				stmtMyS.executeUpdate(sqlInsRec);
				log.debug("               - executeUpdate complete");
				nReportIns++;
			}
			log.info("       - Step 3 - INSERT INTO LOG_ESPORTAZIONE_CCT");

			// =================================================================
			log.info("       - Step 3 - INSERT INTO LOG_ESPORTAZIONE_CCT");
			String sqlInsFin = "INSERT INTO LOG_ESPORTAZIONE_CCT (DATA, ORA, ATTIVITA) VALUES ('"+sdfD.format(new Date())+"','"+sdfT.format(new Date())+"', 'FINE ESTRAZIONE REALTIME')";
			log.info("                - SQL: "+sqlInsFin);
			stmtMyS.executeUpdate(sqlInsFin);
			log.debug("               - executeUpdate complete");

			if (purge) {
				// =================================================================
				log.info("       - Step 4 - DELETE FROM REPORT_REALTIME");
				String sqlPurRec = "DELETE FROM REPORT_REALTIME WHERE DATA_AGGIORNAMENTO < '"+sdfP.format(purgDate)+" 00:00:00'";
				log.info("                - SQL: "+sqlPurRec);
				stmtMyS.executeUpdate(sqlPurRec);
				log.debug("               - executeUpdate complete");
			}
			
			connMyS.commit();
		} catch (Exception e) {
			try { connMyS.rollback(); } catch (Exception erb) {}
			throw e;
		} finally {
			try { stmtMyS.close(); } catch (Exception e) {}
			try { connMyS.close(); } catch (Exception e) {}
			try { cstmtSql.close(); } catch (Exception e) {}
			try { rs.close(); } catch (Exception e) {}
		}
	}
}
