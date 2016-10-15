import java.sql.*;
import java.util.Scanner;
import org.json.simple.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;


public class JSONProcessing 
{
	public static void processJSON(String json) {
		/************* 
		 * Add your code to insert appropriate tuples into the database.
		 ************/
		
		System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");
       	 	try {
          		Class.forName("org.postgresql.Driver");
       		 } catch (ClassNotFoundException e) {
           		 System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
          		  e.printStackTrace();
           		 return;
       		 }

       		 System.out.println("PostgreSQL JDBC Driver Registered!");
       		 Connection connection = null;
	      	 try {
            		connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/flightsskewed","vagrant", "vagrant");
      		 } catch (SQLException e) {
           		System.out.println("Connection Failed! Check output console");
            		e.printStackTrace();
            		return;
        	 }

        	if (connection != null) {
         	   System.out.println("You made it, take control your database now!");
       		 } else {
         	   System.out.println("Failed to make connection!");
            	   return;
    		 }
		
		
		
		
		Object obj = parser.parse(json);
		JSONObject jsonObject = (JSONObject) obj;
		Statement stmt = null;
		
		
		if (json.contains("newcustomer") == true) {
			
			JSONObject newcustomer = (JSONObject) jsonObject.get("newcustomer");
			String customerid = (String) newcustomer.get("customerid");
			String name = (String) newcustomer.get("name");
			String birthdate = (String) newcustomer.get("birthdate");
			String frequentflieron = (String) newcustomer.get("frequentflieron");
			
			try {
				String query = "select * from customers where customerid =" + customerid + ";";
				stmt = connection.createStatement();
            			ResultSet rs = stmt.executeQuery(query);
				
				if (rs != null) {
					System.out.println("This customer already exists!");
            	   			return;		
				}
						
				else { 
				
					String query1 = "select hub from airlines where name =" + frequentflieron + ";";
					
            				rs = stmt.executeQuery(query1);
            			
                			String hub_name = rs.getString("hub");
				
               				String query2 = "INSERT into customers VALUES(" + customerid + "," + name + ", to_date(" + birthdate + ", 'yyyy-mm-dd')," + hub_name + ");";
            	
            				rs = stmt.executeQuery(query2);
				}
						
            		stmt.close();
				
        		} catch (SQLException e) {
          			  System.out.println(e);
        		}
		}
		
		else if (json.contains("flightinfo") == true) {
			
			JSONObject flightinfo = (JSONObject) jsonObject.get("flightinfo");
			String flightid = (String) flightinfo.get("flightid");
			String flightdate = (String) flightinfo.get("flightdate");
			
			JSONArray customers = (JSONArray) flightinfo.get("customers");
			
			for (int i = 0; i < customers.length(); i++) {
				
				JSONObject customer_info = (JSONObject) customers.get(i);
				String customerid = (String) customer_info.get("customerid");
				
			   try {
				   			
				String query = "select * from customers where customerid =" + customerid + ";";
				stmt = connection.createStatement();
            			ResultSet rs = stmt.executeQuery(query);
				
				if (rs != null) {
					
					String query1 = "INSERT into flewon VALUES(" + flightid + "," + customerid + ", to_date(" + flightdate + ", 'yyyy-mm-dd'));";
            				rs = stmt.executeQuery(query1);
					rs = null;
				}
				
				else {

					String name = (String) customer_info.get("name");
					String birthdate = (String) customer_info.get("birthdate");
					String frequentflieron = (String) customer_info.get("frequentflieron");
					
					String q = "select * from airlines where airlineid =" + frequentflieron + ";"; 
            				rs = stmt.executeQuery(query);
				
					if (rs == null) {
						System.out.println("The frequentflieron code does not exists!");
            	   				return;		
					}
					
					String query2 = "select hub from airlines where name = '" + frequentflieron + ";";
					
            				rs = stmt.executeQuery(query2);
                			String hub_name = rs.getString("hub");
					
				
               				String query3 = "INSERT into customers VALUES(" + customerid + "," + name + ", to_date(" + birthdate + ", 'yyyy-mm-dd')," + hub_name + ");";
            	
            				rs = stmt.executeQuery(query3);
					
					
					String query4 = "INSERT into flewon VALUES(" + flightid + "," + customerid + ", to_date(" + flightdate + ", 'yyyy-mm-dd'));";
            				rs = stmt.executeQuery(query4);					
					
				}
			
			    stmt.close();
					
			    } catch (SQLException e ) {
          			  System.out.println(e);
        		    }
					
			}
			
			
		}
		
		else {
			System.out.println("The update cannot be supported");
		}
		
		
		System.out.println("Adding data from " + json + " into the database");
	}

		
	
	public static void main(String[] argv) {
		Scanner in_scanner = new Scanner(System.in);

		while(in_scanner.hasNext()) {
			String json = in_scanner.nextLine();
			processJSON(json);
		}
	}
}
