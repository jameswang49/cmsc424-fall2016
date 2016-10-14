import java.sql.*;
import java.util.Scanner;
import org.json.simple.*;

public class JSONProcessing 
{
	public static void processJSON(String json) {
		/************* 
		 * Add your code to insert appropriate tuples into the database.
		 ************/
		Object obj = parser.parse(json);
		JSONObject jsonObject = (JSONObject) obj;
		
		if (json.contains("newcustomer") == true) {
			String customerid = (String) jsonObject.get("customerid");
			String name = (String) jsonObject.get("name");
			String birthdate = (String) jsonObject.get("birthdate");
			String frequentflieron = (String) jsonObject.get("frequentflieron");
			
			String query = "INSERT into customers VALUES (" + customerid "," + name "," + birthdate "," + frequentflieron ");"
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
