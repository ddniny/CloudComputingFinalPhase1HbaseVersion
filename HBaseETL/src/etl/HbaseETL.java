package etl;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParsingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import HbaseBean.HbaseBean;
import HbaseDAO.HbaseDAO;

public class HbaseETL {
	JsonParser parser = null;	
	HbaseDAO dao = null;
	private final static String familyName = "info";
	private final static String colName = "tweetId";
	private final static String TABLENAME = "tweetData";
	Configuration conf = HBaseConfiguration.create();
	HTable table = null;

	public HbaseETL() throws IOException{
		parser = Json.createParser(new FileReader("/home/ec2-user/jsonTest"));
		table = new HTable(this.conf, TABLENAME);
		try {
			parseJson(parser);
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void parseJson(JsonParser parser) throws IOException{
		String key = null;
		String userId = null;
		String createdAt = null;
		String tweetId = null;
		String objType = null;

		HbaseBean bean = null;
		dao = new HbaseDAO();

		while (parser.hasNext()) {
			JsonParser.Event event;
			try {
				event = parser.next();
			} catch(JsonParsingException e) { continue; }
			catch(JsonException e) {continue;}

			switch(event) {
			case START_ARRAY:
			case END_ARRAY:
			case START_OBJECT:		    	 
			case END_OBJECT:
				if (key != null && key.equals("lang")) {
					//TODO:insert into database
					bean = new HbaseBean();
					bean.setTweetTime(createdAt);
					bean.setTweetId(tweetId);
					bean.setUserId(userId);
					dao.insert(table, bean, familyName, colName);

					key = null;
					createdAt = null;
					tweetId = null;
					userId = null;
					objType = null;
					System.out.println("---------Object END!---------");
					break;
				}
			case VALUE_FALSE:
			case VALUE_NULL:
			case VALUE_TRUE:
				//System.out.println(event.toString());
				break;
			case KEY_NAME:
				key = parser.getString();
				if (key.equals("user")) {
					userId = parseUser(parser);
					objType = "user";
				} else if (key.equals("retweeted_status")) {
					objType = "retweet";
					parseRetweet(parser);
				} 
				break;
			case VALUE_STRING:
				if (objType == null && key.equals("created_at")) {
					createdAt = parseTime(parser.getString());
				} else if (objType == null && key.equals("id_str")) {
					if (tweetId == null) {
						tweetId = parser.getString();
					}
				}

				break;
			case VALUE_NUMBER:
				break;
			default:
				break;
			}
		}
	}
	
	public void parseRetweet(JsonParser parser) {
		String key = null;
		
		while (parser.hasNext()) {
			JsonParser.Event event;
			try {
			event = parser.next();
			} catch(JsonParsingException e) { continue; }
			catch(JsonException e) {continue;}
			switch(event) {
			case START_ARRAY:
			case END_ARRAY:
			case START_OBJECT:		    	 
			case END_OBJECT:
				if (key != null && key.equals("lang")) {
					return;
				}
				break;
			case VALUE_FALSE:
			case VALUE_NULL:
			case VALUE_TRUE:
				//System.out.println(event.toString());
				break;
			case KEY_NAME:
				key = parser.getString();
				break;
			case VALUE_STRING:
				break;
			case VALUE_NUMBER:

				break;
			}
		}
		return;
	}


	public String parseTime (String oldForm) {
		String[] createdTime = oldForm.split(" ");
		String month = createdTime[1];
		if (createdTime[1].equalsIgnoreCase("Jan")) {
			return createdTime[5] + "-01-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("Feb")) {
			return createdTime[5] + "-02-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("Mar")) {
			return createdTime[5] + "-03-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("Apr")) {
			return createdTime[5] + "-04-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("May")) {
			return createdTime[5] + "-05-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("Jun")) {
			return createdTime[5] + "-06-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("Jul")) {
			return createdTime[5] + "-07-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("Aug")) {
			return createdTime[5] + "-08-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("Sept")) {
			return createdTime[5] + "-09-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("Oct")) {
			return createdTime[5] + "-10-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("Nov")) {
			return createdTime[5] + "-11-" + createdTime[2] + " " + createdTime[3];
		} else if (month.equalsIgnoreCase("Dec")) {
			return createdTime[5] + "-12-" + createdTime[2] + " " + createdTime[3];
		}
		return null;

	}


	public String parseUser(JsonParser parser) {
		String key = null;
		String uidStr = null;

		while (parser.hasNext()) {
			JsonParser.Event event;
			try {
				event = parser.next();
			} catch(JsonParsingException e) { continue; }
			catch(JsonException e) {continue;}
			//System.out.println(event.toString() + "From parseUser!!!");
			switch(event) {
			case START_ARRAY:
			case END_ARRAY:
			case START_OBJECT:		    	 
			case END_OBJECT:
				if (key != null && key.equals("notifications")) {
					return uidStr;
				}
				break;
			case VALUE_FALSE:
			case VALUE_NULL:
			case VALUE_TRUE:
				break;
			case KEY_NAME:
				key = parser.getString();
				break;
			case VALUE_STRING:
				if (key.equals("id_str")) {
					uidStr = parser.getString();
				}
				break;
			case VALUE_NUMBER:
				break;
			}
		}
		return null;
	}


}
