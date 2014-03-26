package Controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import HbaseDAO.HbaseDAO;



public class Controller extends HttpServlet{
	/**
	 * 
	 */
	private static Controller handler = null;
			
	private static final long serialVersionUID = 1L;
	
	private final static String familyName = "info";
	private final static String colName = "tweetId";
	private final static String TABLENAME = "tweetData";
	Configuration conf = HBaseConfiguration.create();
	HTable table = null;
	
	public void init() throws ServletException{
		try {
			table = new HTable(this.conf, TABLENAME);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException{
		try {
				performTheAction(request, response);
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ServletException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private void performTheAction(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException, NumberFormatException{
		HttpSession session = request.getSession(true);
		String servletPath = request.getServletPath();
		//User user = (User)session.getAttribute("user");
		String action = getActionName(servletPath);

		PrintWriter out = response.getWriter();
		if (action.equals("q1")){
			//return Action.perform(action, request);
			
			out.println("lmao, " + "0992-3171-4790");
			Date date = new Date();
			out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
		}

		if (action.equals("q2")){
			//return Action.perform("list.do", request);
			String userId = request.getParameter("userid");
			String tweet_time = request.getParameter("tweet_time");
			HbaseDAO hbaseDAO = new HbaseDAO();
			//System.out.println(userId + " " + tweet_time);
			String tweetId = hbaseDAO.get(table, userId+tweet_time, familyName, colName);
			//System.out.println(tweet);
//			Arrays.sort(tweet, new Comparator<TweetBean>() {
//				@Override
//				public int compare(TweetBean arg0, TweetBean arg1) {
//					// TODO Auto-generated method stub
//					if (arg0.getTweetId() < arg1.getTweetId()) return -1;
//					else if (arg0.getTweetId() == arg1.getTweetId()) return 0;
//					else return 1;
//				}	
			//});
			out.println("lmao, " + "0992-3171-4790");
			//for (int i = 0; i < tweet.length; i++) {
				out.println(tweetId);
			//}
		}

		//return Action.perform(action, request);
	}

	private String getActionName(String path){
		int slash = path.lastIndexOf('/');
		return path.substring(slash + 1, slash+3);
	}

}
