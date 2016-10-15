import java.sql.*;
import java.util.Scanner;
import org.json.simple.*;
import org.json.simpler.parser.JSONParser

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
			
			JSONObject jsonObject2 = (JSONObject) jsonObject.get("newcustomer");
			String customerid = (String) jsonObject2.get("customerid");
			String name = (String) jsonObject2.get("name");
			String birthdate = (String) jsonObject2.get("birthdate");
			String frequentflieron = (String) jsonObject2.get("frequentflieron");
			
			try {
				String check_exists = null;
				String query = "select * from customers where customerid =" + customerid + ";"
				stmt = connection.createStatement();
            			ResultSet rs = stmt.executeQuery(query);
				
				if (check_exists != null) {
					System.out.println("This customer already exists!");
            	   			return;		
				}
				
					
				else { 
				
					String query1 = "select hub from airlines where name = '" + frequentflieron + "';";
					
           				stmt = connection.createStatement();
            				ResultSet rs = stmt.executeQuery(query1);
            			
                			String hub_name = rs.getString("hub");
				
                		
					String query2 = "INSERT into customers VALUES(" + customerid + "," + name + ", to_date(" + birthdate + ", 'yyyy-mm-dd')," + hub + ");";
            	
					stmt = connection.createStatement();
            				rs = stmt.executeQuery(query2);
				}
						
            		stmt.close();
				
        		} catch (SQLException e ) {
           			 System.out.println("Failed to add tuple");
        		}	
			
		}
		
		else if (json.contains("flightinfo") == true) {
			
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
