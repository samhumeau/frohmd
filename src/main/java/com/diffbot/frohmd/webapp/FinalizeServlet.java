package com.diffbot.frohmd.webapp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

/** Servlet to call to create a new collection */
public class FinalizeServlet extends HttpServlet{
	private static final long serialVersionUID = -31780459585887L;


	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setContentType("application/json;charset=UTF-8");
        resp.setCharacterEncoding("UTF-8");
        
		String nameCollection = req.getParameter("name");
		if (nameCollection == null || nameCollection.length() ==0){
			sendError("No name of collection specified", resp);
			return;
		}
		
		List<Exception> exceptionRaised = new ArrayList<>();
		Server.shards.parallelStream().forEach(s -> {
			try{
				s.finalize(nameCollection);
			}catch(Exception ioe){
				exceptionRaised.add(ioe);
				return;
			}
		});
			
		sendSuccess("success", resp);
		if (exceptionRaised.size() ==0 )
			AddCollectionServlet.sendSuccess("success", resp);
		else
			AddCollectionServlet.sendError(exceptionRaised.get(0).getMessage(), resp);
	}
	
	public static void sendError(String message, HttpServletResponse resp) throws IOException{
		JSONObject jo = new JSONObject();
		jo.put("success", false);
		jo.put("message", message);
		resp.getWriter().write(jo.toString());
	}
	
	
	public static void sendSuccess(String message, HttpServletResponse resp) throws IOException{
		JSONObject jo = new JSONObject();
		jo.put("success", true);
		jo.put("message", message);
		resp.getWriter().write(jo.toString());
	}

}

