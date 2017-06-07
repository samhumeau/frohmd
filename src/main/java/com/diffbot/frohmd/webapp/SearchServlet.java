package com.diffbot.frohmd.webapp;

import java.io.IOException;
import java.util.Arrays;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import com.diffbot.frohmd.FrohmdMap;
import com.diffbot.frohmd.FrohmdMapBuilder;

public class SearchServlet extends HttpServlet{
	private static final long serialVersionUID = 833733090761169017L;
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setContentType("application/json;charset=UTF-8");
        resp.setCharacterEncoding("UTF-8");
        long start = System.nanoTime();
        
		String nameCollection = req.getParameter("name");
		if (nameCollection == null || nameCollection.length() ==0){
			AddCollectionServlet.sendError("No name of collection specified", resp);
			return;
		}
		
		String keys = req.getParameter("keys");
		if (keys == null || keys.length() ==0){
			AddCollectionServlet.sendError("No keys of collection specified", resp);
			return;
		}
		String[] allKeys = keys.split(",");
		
		JSONObject result = new JSONObject();
		try{
			for (String key : allKeys){
				System.out.println(key);
				byte[] key_b = FrohmdMapBuilder.stringToBytes(key);
				System.out.println(Arrays.toString(key_b));
				int idShard = Server.keyToShard(key_b);
				byte[] val = Server.shards.get(idShard).getData(nameCollection, key_b);
				if (val!=null){
					result.put(key, FrohmdMap.bytesToString(val));
				}
			}
		}catch(Exception e){
			AddCollectionServlet.sendError(e.getMessage(), resp);
			e.printStackTrace();
		}
		
		JSONObject container = new  JSONObject();
		container.put("success", true);
		container.put("timeSpentMs", (long)((System.nanoTime()-start)/1e6));
		container.put("results", result);
		resp.getWriter().write(container.toString());
	}

}
