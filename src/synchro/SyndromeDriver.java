package synchro;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.*;
import java.io.*;

public class SyndromeDriver {
	Connection dbConn = null ;
	
	String dbURL = "jdbc:mysql://localhost/syndrome_dev";
	String dbUser = "root";
	String dbPasswd = "hotsu6986";
	
	void connectDB() throws SQLException{
		dbConn = DriverManager.getConnection
				(dbURL, dbUser, dbPasswd);
		System.out.println("Connected to DB");
    }

	String getAddRow(ResultSet rs, String succCode) throws SQLException {
		String uid = rs.getString("uid") ;
		
		String addRow = "";
		addRow += "|add|"+uid+"|1|"+succCode+"|"
				+Long.toString(rs.getTimestamp("logtime").getTime()/1000)+"|";
		// Add the attributes
		addRow += "+01:"+rs.getString("nodename")+" "
				+ "+02:"+rs.getString("folder")+" "
				+ "+03:"+rs.getString("application")+" "
				+ "+04:"+rs.getString("ipaddr")+" ";
		String eventText_0 = rs.getString("eventtext").trim().
				replace('+',  '_');
		String eventText_1 = eventText_0.substring(0, eventText_0.indexOf('\"'));
		String eventText_2 = "+05:"+eventText_1.replaceAll("\\s+", " +05:");
		
		// TODO: Put more fancy processing
		addRow += eventText_2;
		return addRow;
	} 
	

	void templateLearnerDriver() throws SQLException {
		
		System.out.println("Going to try and get data!");

		Statement dbStmt = dbConn.createStatement();
		Statement failStmt = dbConn.createStatement();
		ResultSet failEvTypes = failStmt.executeQuery("SELECT DISTINCT msgtype FROM prediction");
		
		while (failEvTypes.next()) {
			int evType = failEvTypes.getInt("msgtype");
			PrintStream addStream = null;
			try {
				addStream = new PrintStream(new File(evType+".tmpl_addfile"));
			} catch (FileNotFoundException ex) {
				System.out.println("Couldn't write to file."+ex.toString());
				System.exit(-1);
			}

			ResultSet predResults = 
					dbStmt.executeQuery("SELECT uuid,timerec FROM prediction where " +
							"msgtype="+Integer.toString(evType));

			
			int predRow = 1;
			while (predResults.next()) {
				System.out.println("Reading row " + Integer.toString(predRow++) + " from prediction");
				String uuid = predResults.getString("uuid");					
				Date predDate = predResults.getDate("timerec") ;
				Time predTime = predResults.getTime("timerec") ;
				String datetime = predDate.toString()+" "+predTime.toString();
				System.out.println("Prediction time is: " + datetime);

				Statement msgStmt = dbConn.createStatement();
				ResultSet msgResults = 
					msgStmt.executeQuery("SELECT * from message where logtime <= \'"
							+ datetime + "\' and logtime >= TIMESTAMPADD(MINUTE, -5, \'" 
							+ datetime + "\')");
				
				int msgRow = 1;
				while (msgResults.next()) {
					System.out.println("Reading msg row " + Integer.toString(msgRow++));
					String uid = msgResults.getString("uid") ;
					String succCode = "1";
					Statement succStmt = dbConn.createStatement();
					ResultSet patternResults = 
							succStmt.executeQuery("select uuid,uid from pattern where uuid=\'"+uuid
									+"\' and uid=\'"+uid+"\'");
					if (patternResults.next()) {
						succCode = "0";
					}
					patternResults.close();
					succStmt.close();
					String addRow = getAddRow(msgResults, succCode);
					
					System.out.println("Add row: "+addRow);
					addStream.println(addRow);
				}
				System.out.println("Finished failure event");
				msgResults.close();
				msgStmt.close();
			}
			addStream.close();
			predResults.close();
			dbStmt.close();
			
			
			
		}
		failEvTypes.close();
		failStmt.close();
		
		ResultSet predResults = 
				dbStmt.executeQuery("SELECT uuid,msgtype,timerec FROM prediction;");

	}

	void joinData() throws SQLException {
		Statement dbStmt = dbConn.createStatement();
		
		System.out.println("Going to try and get data!");
		ResultSet rs = dbStmt.executeQuery("SELECT VERSION()");

        if (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
        
		ResultSet predResults = 
				dbStmt.executeQuery("SELECT uuid,msgtype,timerec FROM prediction;");

		int predRow = 1;
		while (predResults.next()) {
			System.out.println("Reading row " + Integer.toString(predRow++) + " from prediction");

			String uuid = predResults.getString("uuid");
			PrintStream addStream = null;
			try {
				addStream = new PrintStream(new File(uuid+".addfile"));
			} catch (FileNotFoundException ex) {
				System.out.println("Couldn't write to file."+ex.toString());
				System.exit(-1);
			}
				
			Date predDate = predResults.getDate("timerec") ;
			Time predTime = predResults.getTime("timerec") ;
			String datetime = predDate.toString()+" "+predTime.toString();
			System.out.println("Prediction time is: " + datetime);

			Statement msgStmt = dbConn.createStatement();
			ResultSet msgResults = 
				msgStmt.executeQuery("SELECT * from message where logtime <= \'"
						+ datetime + "\' and logtime >= TIMESTAMPADD(MINUTE, -5, \'" 
						+ datetime + "\')");
			
			int msgRow = 1;
			while (msgResults.next()) {
				System.out.println("Reading msg row " + Integer.toString(msgRow++));
				String uid = msgResults.getString("uid") ;
				String succCode = "1";
				Statement succStmt = dbConn.createStatement();
				ResultSet patternResults = 
						succStmt.executeQuery("select uuid,uid from pattern where uuid=\'"+uuid
								+"\' and uid=\'"+uid+"\'");
				if (patternResults.next()) {
					succCode = "0";
				}
				patternResults.close();
				succStmt.close();
				String addRow = getAddRow(msgResults, succCode);
				
				System.out.println("Add row: "+addRow);
				addStream.println(addRow);
			}
			System.out.println("Finished failure event");
			msgResults.close();
			msgStmt.close();
			addStream.close();
		}
		predResults.close();
		dbStmt.close();
	}
	
	void joinDataTime() throws SQLException {
		Statement dbStmt = dbConn.createStatement();
		
		System.out.println("Going to try and get data!");
        
		ResultSet predResults = 
				dbStmt.executeQuery("SELECT uuid,msgtype,timerec FROM prediction;");

		int predRow = 1;
		while (predResults.next()) {
			System.out.println("Reading failure row " + Integer.toString(predRow++) + " from prediction");

			String uuid = predResults.getString("uuid");
			PrintStream addStream = null;
			try {
				addStream = new PrintStream(new File(uuid+".addfile_time"));
			} catch (FileNotFoundException ex) {
				System.out.println("Couldn't write to file."+ex.toString());
				System.exit(-1);
			}
				
			Date predDate = predResults.getDate("timerec") ;
			Time predTime = predResults.getTime("timerec") ;
			String datetime = predDate.toString()+" "+predTime.toString();
			System.out.println("Prediction time is: " + datetime);

			Statement msgStmt = dbConn.createStatement();
			ResultSet msgResults = 
				msgStmt.executeQuery("SELECT * from message where logtime <= \'"
						+ datetime + "\' and logtime >= TIMESTAMPADD(MINUTE, -5, \'" 
						+ datetime + "\')");
			
			int msgRow = 1;
			while (msgResults.next()) {
				System.out.println("Reading failure msg " + Integer.toString(msgRow++));

				String addRow = getAddRow(msgResults, "0");
				System.out.println("Add row: "+addRow);
				addStream.println(addRow);
			}

			msgResults = 
				msgStmt.executeQuery("SELECT * from message where logtime <= " +
						"TIMESTAMPADD(MINUTE, -5, \'"+ datetime + "\') and " +
						"logtime >= TIMESTAMPADD(MINUTE, -60, \'" 
						+ datetime + "\')");
			
			msgRow = 1;
			while (msgResults.next()) {
				System.out.println("Reading success msg " + Integer.toString(msgRow++));

				String addRow = getAddRow(msgResults, "1");
				System.out.println("Add row: "+addRow);
				addStream.println(addRow);
			}
			
			System.out.println("Finished failure event");
			msgResults.close();
			msgStmt.close();
			addStream.close();
		}
		predResults.close();
		dbStmt.close();
	}


	void closeDB() throws SQLException {
		if (dbConn != null) {
			dbConn.close();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SyndromeDriver driver = new SyndromeDriver();

		try {
			driver.connectDB();
			if (args.length > 0 && "time".startsWith(args[0].trim())) {
				driver.joinDataTime();
			} else {
				driver.joinData();
			}
			driver.closeDB();
		} catch (SQLException ex) {
			System.out.println("SQLException: " +
					ex.getMessage());
		}			
	}
}
