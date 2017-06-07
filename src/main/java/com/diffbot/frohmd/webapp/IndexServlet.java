package com.diffbot.frohmd.webapp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.diffbot.frohmd.webapp.Shard.CollectionStatus;

public class IndexServlet extends HttpServlet{
	private static final long serialVersionUID = 868260901897746788L;
	
	public static class CollectionDetail{
		String name;
		boolean compressed;
		String status;
		long sizeOnDisk;
		

	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setContentType("text/html;charset=UTF-8");
        resp.setCharacterEncoding("UTF-8");
        
        StringBuilder html = new StringBuilder();
        html.append("<html><head><title>Welcome to Frohmd</title></head><body>");
        html.append("<h1>FROHMD</h1>");
        html.append("<h2>Collections</h2>");
        Map<String, CollectionStatus> collections = new HashMap<>();
        for (Shard shard : Server.shards){
        	Shard.Status status = shard.status();
        	for (CollectionStatus cd : status.collections){
        		CollectionStatus detail = collections.get(cd.name);
        		if (detail == null){
        			collections.put(cd.name, cd);
        		}
        		else{
        			detail.merge(cd);
        		}
        	}
        }
        html.append("<table>");
        html.append(CollectionStatus.getHeader());
        for (CollectionStatus cs : collections.values())
        	html.append(cs.toString());
        html.append("</table>");
        
        html.append("<h2>Shards</h2>");
        html.append("<table>");
        html.append("<tr><th>Shard id</th><th>Location on Disk</th><th>status</th></tr>");
        for (int i=0; i<Server.shards.size(); i++){
        	html.append("<tr><td>"+i+"</td><td>"+Server.shards.get(i).status().folderOnDisk+"</td><td>"+Server.shards.get(i).status().status+"</td></tr>");
        }
        html.append("</table>");
        html.append("</body></html>");
        resp.getWriter().write(html.toString());
        
        
	}
}
