package com.diffbot.frohmd.webapp;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

public class StatusServlet extends HttpServlet{
	private static final long serialVersionUID = -31780459585887L;


	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setContentType("application/json;charset=UTF-8");
        resp.setCharacterEncoding("UTF-8");
        
        JSONObject status = new JSONObject();
        JSONArray shards = new JSONArray();
        for (Shard shard : Server.shards){
        	shards.put(shard.status());
        }
        status.put("shards", shards);
        resp.getWriter().write(status.toString());
	}

}
