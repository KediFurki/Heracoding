package comapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.Part;
import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

@WebServlet("/UploadOutboundListBck")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 150,
    maxFileSize = 1024 * 1024 * 150,
    maxRequestSize = 1024 * 1024 * 300)

public class UploadOutboundListBck extends HttpServlet {

    private static final long serialVersionUID = 1L;
    Logger log = Logger.getLogger("comapp." + this.getClass().getName());

    public UploadOutboundListBck() {
        super();
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request, response);
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        HttpSession session = request.getSession();
        log.info(session.getId() + " - ******* new request ***** ");
        String action = request.getParameter("action");
        String environment = (String) session.getAttribute("Environment");
        Connection conn = null;
        CallableStatement cstmt = null;
        ResultSet rs = null;
        log.info(session.getId() + " action: " + action);
        try {
            switch (action) {
            case "upload":
                Part filePart = request.getPart("uploadfile");
                String username = (String) session.getAttribute("UserName");
                Properties cs = ConfigServlet.getProperties();
                String upload_location = cs.getProperty("file-outbound-location");
                upload_location += File.separator + username;
                File _Dir_upload = new File(upload_location);
                if (!_Dir_upload.exists()) {
                    _Dir_upload.mkdirs();
                }
                log.info(session.getId() + " - username : " + username + " - file-outbound-location : " + upload_location);
                SimpleDateFormat sdf_dest = new SimpleDateFormat("yyyyMMddhhmmss");
                int nRecordLetti = 0;
                try {
                    Context ctx = new InitialContext();
                    DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/" + ConfigServlet.web_app + "OCS");
                    log.info(session.getId() + " - connection OCS wait...");
                    conn = ds.getConnection();

                    log.info(session.getId() + " - Upload : inizio upload");
                    if (filePart != null) {
                        String file_name = filePart.getSubmittedFileName();
                        upload_location += File.separator + sdf_dest.format(new Date()) + "_" + file_name;
                        InputStream is = filePart.getInputStream();
                        FileOutputStream os = new FileOutputStream(new File(upload_location));
                        IOUtils.copy(is, os);
                        is.close();
                        os.close();
                    }
                    log.info(session.getId() + " - Upload : File salvato in : " + upload_location);
                    log.info(session.getId() + " - Upload : fine upload");

                    long startTime = System.currentTimeMillis();
                    log.info(session.getId() + " - File : inizio caricamento csv in memoria");

                    List<String[]> records = new ArrayList<String[]>();
                    Map<String, Integer> actCounter = new HashMap<String, Integer>();

                    try (BufferedReader br = new BufferedReader(new FileReader(upload_location))) {
                        String line;
                        boolean isHeader = true;
                        while ((line = br.readLine()) != null) {
                            if (isHeader) {
                                isHeader = false;
                                continue;
                            }
                            if (!StringUtils.isBlank(line)) {
                                String[] cols = line.split(";", -1);
                                if (cols.length >= 27) {
                                    String numeroAct = cols[24].trim();
                                    int chainN = actCounter.getOrDefault(numeroAct, 0);
                                    cols[15] = String.valueOf(chainN);
                                    actCounter.put(numeroAct, chainN + 1);
                                    records.add(cols);
                                } else {
                                    log.warn(session.getId() + " - Row skipped, insufficient column count: " + cols.length);
                                }
                            }
                        }
                    } catch (Exception ex) {
                        log.error(session.getId() + " - CSV read error: " + ex.getMessage(), ex);
                    }
                    nRecordLetti = records.size();
                    log.info(session.getId() + " - File : numero record caricati: " + nRecordLetti);
                    long endTime = System.currentTimeMillis();
                    log.info(session.getId() + " - File : Caricato in " + (endTime - startTime) + " ms");

                    startTime = System.currentTimeMillis();
                    log.info(session.getId() + " - DB : inizio inserimento record");

                    for (String[] cols : records) {
                        log.info(session.getId() + " - dashboard.OutboundListBck_InsertCallingList - record_id:" + cols[0] + " contact_info:" + cols[1] + " Numero_ACT:" + cols[24]);

                        cstmt = conn.prepareCall("{ call dashboard.OutboundListBck_InsertCallingList(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");

                        cstmt.setInt(1, parseIntSafe(cols[0]));
                        cstmt.setString(2, nullIfNull(cols[1]));
                        cstmt.setInt(3, parseIntSafe(cols[2]));
                        cstmt.setInt(4, parseIntSafe(cols[3]));
                        cstmt.setInt(5, parseIntSafe(cols[4]));
                        cstmt.setInt(6, parseIntSafe(cols[5]));
                        cstmt.setInt(7, parseIntSafe(cols[6]));
                        cstmt.setString(8, nullIfNull(cols[7]));
                        cstmt.setString(9, nullIfNull(cols[8]));
                        cstmt.setInt(10, parseIntSafe(cols[9]));
                        cstmt.setInt(11, parseIntSafe(cols[10]));
                        cstmt.setInt(12, parseIntSafe(cols[11]));
                        cstmt.setInt(13, parseIntSafe(cols[12]));
                        cstmt.setString(14, nullIfNull(cols[13]));
                        cstmt.setInt(15, parseIntSafe(cols[14]));
                        cstmt.setInt(16, parseIntSafe(cols[15]));
                        cstmt.setString(17, nullIfNull(cols[16]));
                        cstmt.setString(18, nullIfNull(cols[17]));
                        cstmt.setString(19, nullIfNull(cols[18]));
                        cstmt.setString(20, nullIfNull(cols[19]));
                        cstmt.setString(21, nullIfNull(cols[20]));
                        cstmt.setString(22, nullIfNull(cols[21]));
                        cstmt.setString(23, nullIfNull(cols[22]));
                        cstmt.setString(24, nullIfNull(cols[23]));
                        cstmt.setString(25, nullIfNull(cols[24]));
                        cstmt.setString(26, nullIfNull(cols[25]));
                        cstmt.setString(27, nullIfNull(cols[26]));

                        cstmt.execute();
                        log.debug(session.getId() + " - executeCall complete");
                        try { cstmt.close(); } catch (Exception e) {}
                    }

                    endTime = System.currentTimeMillis();
                    log.info(session.getId() + " - DB : Inserimento in " + (endTime - startTime) + " ms");
                    log.info(session.getId() + " - DB : fine inserimento");

                    JSONObject obj = new JSONObject();
                    obj.put("res", "OK");
                    obj.put("read", nRecordLetti);
                    String re = obj.toString();
                    log.info(session.getId() + " - File Uploaded Successfully ->" + re);
                    response.getOutputStream().print(re);

                } catch (Exception e) {
                    log.error(session.getId() + " : " + e.getMessage(), e);
                    JSONObject obj = new JSONObject();
                    try {
                        obj.put("res", "KO");
                        obj.put("errcode", "1");
                        obj.put("err", e.getMessage());
                    } catch (JSONException e1) {}
                    String re = obj.toString();
                    log.error(session.getId() + " - File Uploaded Error ->" + re);
                    response.getOutputStream().print(re);
                } finally {
                    try { rs.close(); } catch (Exception e) {}
                    try { cstmt.close(); } catch (Exception e) {}
                    try { conn.close(); } catch (Exception e) {}
                }
                break;
            }
        } catch (Exception e) {
            log.error(session.getId() + " : " + e.getMessage(), e);
            JSONObject obj = new JSONObject();
            try {
                obj.put("res", "KO");
                obj.put("errcode", "1");
                obj.put("err", e.getMessage());
            } catch (JSONException e1) {}
            String re = obj.toString();
            log.error(session.getId() + " - Servlet Error ->" + re);
            response.getOutputStream().print(re);
        }
    }

    private String nullIfNull(String val) {
        if (val == null) return null;
        val = val.trim();
        if (val.equalsIgnoreCase("NULL") || val.isEmpty()) return null;
        return val;
    }

    private int parseIntSafe(String val) {
        if (val == null) return 0;
        val = val.trim();
        if (val.equalsIgnoreCase("NULL") || val.isEmpty()) return 0;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
