package apps;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;

import drivers.SQLError;

/**
 * Implements a user console for
 * interacting with a database.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public class Console {
	/**
	 * The entry point for execution
	 * with user input/output.
	 *
	 * @param args unused.
	 */
	public static void main(String[] args) {
			try (
				final Database db = new Database(true);
				final Scanner in = new Scanner(System.in);
				final PrintStream out = System.out;
			) {
				while(true) {
					out.print(">> ");
		
					String query = in.nextLine();
					String[] tokens = query.split(";");
					out.println(); 
					
					
					for (int i = 0 ; i < tokens.length ; i++) {
						
						if(tokens[i].strip().startsWith("--")) {
							out.println("COMMENT: " + tokens[i].strip().toString().substring(2).strip()); 
							out.println(); 
							continue; 
						}
							try {
								if(tokens[i].strip().toLowerCase().equals("exit")) {
									return; 
								}
								out.println("Query: " + tokens[i].toString()); 
								
								Object retVal = db.interpret(tokens[i]); 
								if(retVal.getClass() == Integer.class) {
									out.println("Number of Rows Affected: " + retVal);
								}else {
									out.println("Result: " + retVal);
								}
							}
							catch (SQLError e) {
								out.println("Error: " + e);
							}
					out.println(); 
					}
					
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		
		
		 
	}
}

//try (
//		final Database db = new Database(true);
//		final Scanner in = new Scanner(System.in);
//		final PrintStream out = System.out;
//	) {
//		out.print(">> ");
//
//		String query = in.nextLine();
//		try {
//			out.println("Result: " + db.interpret(query));
//		}
//		catch (SQLError e) {
//			out.println("Error: " + e);
//		}
//	}
//	catch (IOException e) {
//		e.printStackTrace();
//	}
