package com.diffbot.frohmd.webapp;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

/** Servlet to call to create a new collection */
public class AddCollectionServlet extends HttpServlet{
	private static final long serialVersionUID = -3178045958538400887L;


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
		
		String compression_s = req.getParameter("name");
		boolean compression = "true".equals(compression_s);
		
		for (Shard s : Server.shards){
			try{
				s.openCollection(nameCollection, compression);
			}catch(Exception ioe){
				sendError(ioe.getMessage(), resp);
				return;
			}
		}
		sendSuccess("success", resp);
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
