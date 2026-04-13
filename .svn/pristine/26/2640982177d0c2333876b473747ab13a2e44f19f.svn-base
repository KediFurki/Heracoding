package comapp;

import java.io.IOException;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;


/**
 * Servlet Filter implementation class RequestLoggingFilter
 */
@WebFilter(
		filterName = "SecurityFilter",
		urlPatterns = {
				"/*",
				"/SecurityFilter"
		}
	)

public class SecurityFilter implements Filter {

	private ServletContext context;

	public void init(FilterConfig fConfig) throws ServletException {
		this.context = fConfig.getServletContext();
		this.context.log("RequestLoggingFilter initialized"); 
	}

	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		Logger log = Logger.getLogger("comappSF." + this.getClass());
		HttpServletRequest req = (HttpServletRequest) request;
		HttpServletResponse res = (HttpServletResponse) response;
		HttpSession session = req.getSession(false);
		String uri = req.getRequestURI();
		double id = Math.random();
		log.debug("["+id+"] Uri: " + uri);
		if (uri.toLowerCase().contains("error.jsp")) {
			log.debug("["+id+"] Uri: " + uri + " req.getContextPath():" + req.getContextPath() + " send to error.jsp");
			chain.doFilter(request, response);
			return;
		}
		String r = req.getContextPath().toLowerCase();
		if (uri.toLowerCase().startsWith(r + "/comapp/") || 
			uri.toLowerCase().startsWith(r + "/dojo/") || 
			uri.toLowerCase().startsWith(r + "/dijit/") || 
			uri.toLowerCase().startsWith(r + "/images/") || 
			uri.toLowerCase().startsWith(r + "/css/") || 
			uri.toLowerCase().startsWith(r + "/js/")) {
			chain.doFilter(request, response);
			return;
		}
		if (uri.toLowerCase().endsWith("index.jsp") || 
			uri.toLowerCase().endsWith("login.jsp") || 
			uri.toLowerCase().endsWith("environment.jsp") ) {
			chain.doFilter(request, response);
			return;
		}
		try {
		
				log.debug("*************** start request, session is NOT null and login.jsp****************  ");
				
				LogonData ld = (LogonData) session.getAttribute("LogonData");				
				if (ld != null && ld.isLogged()) {					
					chain.doFilter(request, response);
					return;
					
				} else {
					log.info(session.getId()+ " - Logon data is null  ");
					throw new Exception("["+id+"] Logon data is null");
				}
			
		} catch (Exception e) {
			log.info( " - Exception ", e);
			String error = e.getMessage() == null ? "Generic Error" : e.getMessage();
			String page = req.getContextPath() + "/index.jsp?Error=" + URLEncoder.encode(error, StandardCharsets.UTF_8.toString());
			log.info( " -  redirect to: " + page);
			res.sendRedirect(page);
			return;
		}
		
	}

	public void destroy() {
		// we can close resources here
	}

}
