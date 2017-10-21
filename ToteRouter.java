import org.apache.http.client.methods.HttpPost;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.entity.StringEntity;
import java.io.*;
import java.util.ResourceBundle;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.io.PrintWriter;
import java.io.FileWriter;

// TODO Plug in a Version and Date
// TODO Refine the PUTAWAY Tote Logic
// TODO Plug in a Nagios Trigger / Lock?
// TODO Log file?
// TODO 15-minute timing on re-routing
// TODO Please change the savant.moto_toterouter to not point at the production instance when we add the putaway logic! ;)
// TODO Expired Routes

public class ToteRouter {
	Connection conn=null;
	Statement stmt=null;
	PreparedStatement pstmt = null;
	PreparedStatement pistmt = null;
	PreparedStatement pdstmt = null;
	PreparedStatement psstmt = null;
	PreparedStatement ppstmt = null;
	
	PrintWriter out = null;
	String dbname = null;
	String dbuser = null;
	String dbpass = null;
	String dbhost = null;
	String endpoint =null;
	int dbport = 0;
	String filename = null;
	
	public ToteRouter () 
	{
		ResourceBundle bundle = ResourceBundle.getBundle("ToteRouter");
		
		dbname = bundle.getString("DatabaseName");
		dbuser = bundle.getString("DatabaseUser");
		dbpass = bundle.getString("DatabasePass");
		dbhost = bundle.getString("DatabaseHost");
		dbport = Integer.parseInt(bundle.getString("DatabasePort"));
		endpoint = bundle.getString("EndPoint");
		
		filename = bundle.getString("LogFile");
	}
	
	public void connect()
	{
		try
		{
			
			Class.forName("com.microsoft.jdbc.sqlserver.SQLServerDriver"); 
			conn = DriverManager.getConnection ("jdbc:microsoft:sqlserver://"+dbhost+":"+dbport+";User="+dbuser+";Password="+dbpass+";databasename="+dbname);
			stmt = conn.createStatement();
			
			
			pstmt = conn.prepareStatement("UPDATE Savant.moto_ToteRouter SET routed_date=GETDATE() WHERE id=? AND tote_id=?");
			pistmt = conn.prepareStatement("INSERT INTO Savant.moto_ToteRouter (tote_id,route,chute,noncon) VALUES (?,?,?,?)");
			pdstmt = conn.prepareStatement("DELETE FROM Savant.moto_ToteRouter WHERE tote_id=?");
			
			psstmt = conn.prepareStatement("SELECT id, tote_id, noncon, route FROM Savant.moto_ToteRouter WITH (NOLOCK) WHERE tote_id=?");
			ppstmt = conn.prepareStatement("SELECT tote_id FROM Savant.moto_ToteRouter WHERE GETDATE() > DATEADD(hh, 1, routed_date)");
			
		}
		catch (Exception e)
		{
			System.out.println("Error Connecting to Database!");
			e.printStackTrace();
			System.exit(1);
		}
		
		try
		{
			out = new PrintWriter(new FileWriter(filename, true));
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}
	
	public void close()
	{
		try
		{
			stmt.close();
			conn.close();
			out.close();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

	
	public void run () {
		String sql = "";
		ResultSet rs = null;
		int totecount = 0;
		
		System.out.println("Smart Tote Router Starting up....");
		System.out.println("Endpoint: "+endpoint);
		
		System.out.println("Connecting to Database!");
		connect();
		
		System.out.println("About to purge Expired Routes...");
		purgeExpiredRoutes();
		System.out.println("Done purging Expired Routes...");

		
		//
		// Run the query to generate the tote routings and put them in the
		// savant.moto_ToteRouter table for routing.
		//
		getToteRoutes();
		
		sql = 
				"SELECT " +
					"tr.id " +
					",tr.tote_id " +
					",tr.route " +
					",tr.chute " +
				"FROM " +
					"Savant.moto_ToteRouter tr WITH (NOLOCK)" +
				"WHERE " +
					"tr.routed_date IS NULL " +
				"ORDER BY " +
					"tr.id";
		
		try 
		{
			rs = stmt.executeQuery(sql);
			
			// For each of the tote routings out there, route 'em.

			while (rs.next())
			{	
				totecount++;
				int route_id = rs.getInt("id");
				String tote_id = rs.getString("tote_id");
				String route = rs.getString("route");
				int chute = rs.getInt("chute");
				
				System.out.println("Tote "+totecount+": " + tote_id + "  Route: "+route+" Chute: "+chute);

				//boolean validtote = false;
				//String[] validtotes = { "12477", "12285", "13247" };
				
				//for (int i=0; i<validtotes.length; ++i)
				//{
				//	if (validtotes[i].equals(tote_id)) {
				//		validtote = true;
				//		break;
				//	}
				//}
				
				//if (validtote) {
				
					//
					// Get the Tote Routing info and then Route the tote.
					//				
					if (routeTote(tote_id, route, chute))
					{
						//System.out.println("Successfully Routed Tote!");
						pstmt.setInt(1, route_id);
						pstmt.setString(2, tote_id);
						pstmt.execute();
					}
					else
					{
						System.out.println("Failed to Route Tote");
					}			
			
				//}  // if (validtote)...
				
			}

			rs.close();	
				
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		System.out.println("Disconnecting from Database!");
		close();
		
		System.out.println("Smart Tote Router Spinning Down!");
		
	}
	
	
	boolean purgeExpiredRoutes()
	{
		ResultSet rs = null;
		String tote_id = "";
		
		try {
			rs = ppstmt.executeQuery();
			
			while (rs.next())
			{
				tote_id = rs.getString("tote_id");
				
				System.out.println("Purging Expired Tote Route for: "+tote_id+" from ToteRoute table...");
				pdstmt.setString(1,tote_id);
				pdstmt.execute();
				System.out.println("Purged Expired Tote Route for: "+tote_id);
			}
			
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	
	
	//
	// This method runs the query against the database to get the open totes in Savant,
	// identify what is in the tote (for example, an single order or a tote), and then based
	// on where that tote needs to go, it generates the routing string of locations.  Once
	// that routing string is generated, we preroute, or apply our business logic, and then
	// actually route it.  Read on.
	//
	public void getToteRoutes()
	{
		ResultSet rs = null;
		
		//
		// So this gigantic query below takes the LocationID (destination)
		// of the Tote and based on the mapping below of location to zone
		// in the SQL case statement, it generates the route to send the 
		// tote to the appropriate zone.
		//
		
		String sql =
			"SELECT DISTINCT " +
				"x.ToteID [tote_id] " +
				",x.Type [tote_state] " +
				",[chute] = 3 " +
				",[zone] = ISNULL(CASE   WHEN x.LocationID = 'Packing' THEN 'Packing' "+ 
				"  WHEN LEFT(x.LocationID,3) BETWEEN '1UA' AND '1VA' THEN 'FB' "+
				"  WHEN LEFT(x.LocationID,3) BETWEEN '1VB' AND '1VF' THEN 'FD' "+
				"  WHEN LEFT(x.LocationID,3) BETWEEN '1VG' AND '1WB' THEN 'HB' "+
				"  WHEN LEFT(x.LocationID,3) BETWEEN '1WC' AND '1WG' THEN 'HD' "+
				"  WHEN LEFT(x.LocationID,3) BETWEEN '1WH' AND '1XB' THEN 'JB' "+
				"  WHEN LEFT(x.LocationID,3) BETWEEN '1XC' AND '1XG' THEN 'JD' "+
				"  WHEN x.LocationID = 'RANDOM' THEN (CASE  "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '0' AND '3' THEN 'AD' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '4' AND '7' THEN 'AF' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '8' AND '11' THEN 'AH' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '12' AND '15' THEN 'BA' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '16' AND '19' THEN 'BB' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '20' AND '23' THEN 'BC' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '24' AND '27' THEN 'BD' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '28' AND '31' THEN 'CB' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '32' AND '35' THEN 'CD' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '36' AND '39' THEN 'CF' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '40' AND '43' THEN 'CH' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '44' AND '47' THEN 'DA' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '48' AND '51' THEN 'DB' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '52' AND '55' THEN 'DC' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '56' AND '59' THEN 'DD' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '60' AND '63' THEN 'EF' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '64' AND '67' THEN 'EH' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '68' AND '71' THEN 'FA' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '72' AND '75' THEN 'FC' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '75' AND '78' THEN 'GF' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '78' AND '81' THEN 'GH' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '81' AND '84' THEN 'HA' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '84' AND '87' THEN 'HC' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '87' AND '90' THEN 'IF' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '90' AND '93' THEN 'IH' "+
				"              WHEN LEFT(RAND()*10,2) BETWEEN '93' AND '96' THEN 'JA' "+
				"              ELSE 'JC' END ) "+                           
				"ELSE y.ZoneID END,'NC') "+
                ",tote=tr.tote_id " +
                ",tr.noncon " +
			"FROM " +
				"( " +
					"SELECT " +
						"td.ToteID "+
						",[LocationID] = COALESCE(tdpick.LocationID,trepl.ToLocationID,tputaway1.LocationID,tputaway2.LocationID,mh.HomeLocID,CASE WHEN o.Status IS NULL THEN NULL WHEN o.Status = 'picked' THEN 'Packing' ELSE 'NC' END,'Hospital') " +
						",[Type] = CASE WHEN tdpick.LocationID IS NULL THEN 'PUTAWAY' ELSE 'SINGLE' END " +
					"FROM " +
						"savant.Totedetail td WITH(NOLOCK) " +
				        "LEFT JOIN ( " +
				        "           SELECT DISTINCT " +
				        "                 td.orderid " +
				        "                 ,[LocationID] = CASE    WHEN LEFT(td.LocationID,2) IN ('ww','xx','yy','zz') THEN 'NC' " +
				        "                                                     WHEN LEFT(td.locationID,3) BETWEEN '1ua' AND '1xg' THEN td.LocationID " +
				        "                                                     WHEN LEFT(td.LocationID,2) BETWEEN '00' AND '99' THEN 'NC' " +
				        "                                                     ELSE td.LocationID END " +
				        "           FROM " +
				        "                 savant.TransDetail td WITH(NOLOCK) " +
				        "                 JOIN savant.Trans t WITH(NOLOCK) ON td.TranID = t.TranID AND t.TranType = 'pick' " +
				        "           WHERE 1=1 " +
				        "                 AND td.OrderID > '' " +
				        "                 ) tdpick ON td.inventoryid = tdpick.orderid " +
				        "LEFT JOIN savant.trans trepl WITH(NOLOCK) ON td.toteid = trepl.InventoryID AND trepl.TranType = 'repl' AND trepl.InventoryID > '' " +
						"LEFT JOIN savant.trans tputaway1 WITH(NOLOCK) ON td.toteid = tputaway1.InventoryID AND tputaway1.TranType = 'putaway' AND tputaway1.InventoryID > '' " +
						"LEFT JOIN savant.trans tputaway2 WITH(NOLOCK) ON td.inventoryid = tputaway2.InventoryID AND tputaway2.TranType = 'putaway' AND tputaway2.InventoryID > '' " +
						"LEFT JOIN savant.orders o WITH(NOLOCK) ON td.inventoryid = o.OrderID " +
						"LEFT JOIN savant.MultiHomeLoc mh WITH(NOLOCK) ON td.inventoryid = mh.ProductID " +
					"WHERE 1=1 " +
						"AND (CASE WHEN tdpick.LocationID IS NULL THEN 'PUTAWAY' ELSE 'SINGLE' END)='SINGLE' " +
				")x " +
				"LEFT JOIN " +
					"( " +
						"SELECT " +
							"zl.LocationID "+
							",MAX(zl.ZoneID) [ZoneID] "+
						"FROM "+
							"savant.ZoneLocations zl WITH(NOLOCK) " +
							"JOIN savant.zone z WITH(NOLOCK) ON zl.ZoneID = z.ZoneID AND z.Picking = 1 " +
						"WHERE 1=1 " +
						"GROUP BY " +
							"zl.LocationID " +
					") y ON x.LocationID = y.LocationID " +
					"FULL OUTER JOIN Savant.moto_ToteRouter tr WITH (NOLOCK) ON tr.tote_id=x.ToteID " +
				"ORDER BY "+
					"x.ToteID";
		
		try
		{
			rs = stmt.executeQuery(sql);

			String tote = "";
			String route = "";
			
			String ttote = "t";
			String tbin = "t";
			String delim = "-";
			
			String rtote="";
			int rchute=0;
			
			//
			// OK, here's where it gets a little weird.  Because there can be multiple entries
			// for each tote, we need to read each row, concatenate the zone into a route string
			// and look for when the tote id's change.  When we see a change (tote != ttote), we 
			// know that it's time to preroute.
			//
			// ...and if we hit a null, we need to route immediately, as that's a completed pick.
			//
			
			while (rs.next())
			{
				delim = "-";
				try {
					ttote = rs.getString("tote_id").trim();
					tbin = rs.getString("zone").trim();
					
				}
				catch (NullPointerException npe)
				{
					//
					// If Tote_id is null (since we're doing a full outer join), we'll have
					// a tote routing entry in the moto_ToteRouter table with no lines open
					// to pick.  In this case, we'll just send it to the packing lines
					// out on one of the chutes.
					//
					
					System.out.println("Encountered a Completed Order Needing a Route!");
					
					rtote = rs.getString("tote").trim();
					rchute = rs.getInt("noncon");
					
					//
					// If our chute is 4 (noncon), we send it to chute 4, otherwise
					// it's the default value 0, or the recirculate 3, or maybe even 1
					// already--whatever, if it's not 4 already, we route it to 1.
					//
					if (rchute != 4) 
					{
						rchute = 1;
					}
					
					System.out.println("Routing Tote "+rtote+" to Chute: "+rchute);
					
					//
					// We're sending the tote to the 'rchute' chute with no zones along the way ("-")
					//
					routeTote(rtote, "-", rchute);
					
					System.out.println("Deleting Tote "+rtote+" from ToteRoute table...");
					pdstmt.setString(1,rtote);
					pdstmt.execute();
					System.out.println("Deleted Tote: "+rtote);
					
					continue;
				}
					
				//
				// This is the change in tote compared to ttote.  We preroute the tote
				// via the method below, and then reset the variables.
				//
				if (!ttote.equalsIgnoreCase(tote))
				{
					
					if (tote.trim().length()>1)
					{
						preRouteTote(tote, route);
					}
					
					route = "";
					delim = "";
					tote = ttote;
				}
	
				//
				// Concatenate the route with the next bin.
				//
				route = route + delim + tbin;
				
			}  // end of the while loop.

			//
			// Route the last tote (since there are no more changes to trigger it).
			//			
			if (tote.trim().length()>1)
			{
				preRouteTote(tote, route);
			}
			
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
	}
	
	//
	// preRouteTote takes the Routing String and then applies the necessary
	// business logic to sort out the chutes.
	//
	public boolean preRouteTote(String tote, String route)
	{
		int noncon = 0;
		int chute = 3;
		String origroute = route;
		ResultSet rs2 = null;
		
		//System.out.println("Start of Preroute Tote");
		//System.out.println("tote "+tote+": "+route);
		
		if (route.contains("NC")){
			noncon = 4;
			route = route.replaceAll("NC","-");
		}
		
		if (route.contains("Hospital"))
		{
			noncon = 4;
			route = "-";
		}
		
		if (route.contains("Packing"))
		{
			route = "-";
		}
		
		//
		// If the route is "-", then there's just open Non-Con picks, so send it to 4
		// Otherwise, recirculate if we're out of pick locations.
		//		
		if (route.equalsIgnoreCase("-"))
		{
			chute = 4;
		}
		else
		{
			chute = 3;
		}

		System.out.println("----------------------------");
		System.out.println("  Tote: "+tote);
		System.out.println("  Original Route: "+origroute);
		System.out.println("  Final Route: "+route);
		System.out.println("  Routing to Chute: "+chute);
		System.out.println("  Non-Con: "+noncon);
		System.out.println("----------------------------");
					
		try {
			
			psstmt.setString(1, tote);
			rs2 = psstmt.executeQuery();
			
			String droute = null;
			while (rs2.next())
			{
				droute = rs2.getString("route");
			}
			
			if (droute == null) droute = "";
			
			if (!droute.equalsIgnoreCase(route))
			{
				System.out.println("Routings for Tote "+tote+" Don't match: "+route+" vs. "+droute);

				//
				// Let's Delete the Route in the Routing Database Already.
				//
				if (!droute.equalsIgnoreCase(""))
				{
					pdstmt.setString(1, tote);
					pdstmt.execute();
				}
				
				//
				// Let's Rebuild the Route in the Routing Database.
				//

				pistmt.setString(1, tote);
				pistmt.setString(2, route);
				pistmt.setInt(3, chute);		// Default to Recirculate
				pistmt.setInt(4, noncon);
				pistmt.execute();
				
			}
			else 
			{
				System.out.println("Routings for Tote "+tote+" match, skipping...");
			}
			
			rs2.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}

		//System.out.println("End of Preroute Tote");
		
		return true;
	}
	
	//
	// SMART - Stevebrain Memorial Automatic Router for Totes
	// This sends the REST call to Interchange to route the totes
	//
	public boolean routeTote(String tote, String route, int chute)
	{
		StringEntity input = null;
		HttpResponse response = null;
		DefaultHttpClient httpClient = null;
		HttpPost postRequest = null;
				
		httpClient = new DefaultHttpClient();
		
		try
		{		
			//
			// Set up the data to be input (JSON, headers, etc), then dump it to show that it's good..
			//			
			input = new StringEntity("{ \"tote\": \""+tote+"\", \"path\": \""+route+"\", \"chute\": "+chute+" }");
			input.setContentType("'application/json");
			input.setContentEncoding("UTF-8");
			
			//System.out.println("Dump of String Entity being added to postRequet: ");
			//input.writeTo(System.out);
			//System.out.println();
		
			//
			// Set up the post request to talk to the web service, add the input and then send it.
			//			
			postRequest = new HttpPost(endpoint);
			postRequest.setEntity(input);
			response = httpClient.execute(postRequest);
			
			//
			// Not sure if we should see anything here.
			//
			//System.out.println("Dump of HttpEntity:");
			//HttpEntity hpe = response.getEntity();
			//hpe.writeTo(System.out);
			//System.out.println("End of HttpEntity.");
			
			//
			// Print Request and response strings.  You can see the
			//
			//System.out.println("Request: "+ postRequest.toString());
			//System.out.println("Response: "+response.toString());

			if (response.getStatusLine().getStatusCode() != 201) {
				throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatusLine().getStatusCode());
			}
			else
			{
				//System.out.println("Successful Response Code!");
			}
	 
			//
			// Close open connection.
			//
			httpClient.getConnectionManager().shutdown();
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
		
		
		return true;
	}
	
	
	//
	// Test of the REST service to route the totes.
	// Basically, this routes Tote 15306 to AE and then AF and ends it down chute 3.
	// hostname and hostnamedev replaced the hard-coded hostnames of the original endpoints. 
	//
	public void TestRoute()
	{
		StringEntity input = null;
		HttpResponse response = null;
		DefaultHttpClient httpClient = null;
		HttpPost postRequest = null;
				
		httpClient = new DefaultHttpClient();
		
		try
		{		
			//
			// Set up the data to be input (JSON, headers, etc), then dump it to show that it's good..
			//			
			input = new StringEntity("{ \"tote\": \"15306\", \"path\": \"AE-AF\", \"chute\": 3 }");
			input.setContentType("'application/json");
			input.setContentEncoding("UTF-8");
			
			System.out.println("Dump of String Entity being added to postRequet: ");
			input.writeTo(System.out);
			System.out.println();
			
			//
			// Set up the post request to talk to the web service, add the input and then send it.
			//			
			postRequest = new HttpPost("http://hostnamedev/manager/conveyor_service/routes");
			//postRequest = new HttpPost("https://hostname/manager/conveyor_service/routes");
			postRequest.setEntity(input);
			response = httpClient.execute(postRequest);
			
			//
			// Not sure if we should see anything here.
			//
			//System.out.println("Dump of HttpEntity:");
			//HttpEntity hpe = response.getEntity();
			//hpe.writeTo(System.out);
			//System.out.println("End of HttpEntity.");
			
			//
			// Print Request and response strings.  You can see the
			//
			System.out.println("Request: "+ postRequest.toString());
			System.out.println("Response: "+response.toString());

			if (response.getStatusLine().getStatusCode() != 201) {
				throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatusLine().getStatusCode());
			}
			else
			{
				System.out.println("Successful Response Code!");
			}
	 
			
			//
			// Try to read some input, but Stream should be closed at this point.
			//
			BufferedReader br = new BufferedReader(
	                        new InputStreamReader((response.getEntity().getContent())));
	 
			String output;
			System.out.println("Output from Server ....");
			if (br.ready()) {
				while ((output = br.readLine()) != null) {
					System.out.println(output);
				}
			}
			else {
				System.out.println("Stream is closed (not ready).");
			}
				
			System.out.println("Done Reading Input!");

			//
			// Close open connection.
			//
			httpClient.getConnectionManager().shutdown();
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	

	public static void main(String[] args) {

		ToteRouter tr = new ToteRouter();
		
		//
		// Test Program to simulate sending routings to IC
		//
		//tr.TestRoute();
		
		//
		// Run the ToteRouting program
		//
		tr.run();
				
	}

}
