package drivers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import tables.HashArrayTable;
import tables.Table;

/*
 * Example:
 *   SHOW TABLE example_table
 *
 * Result:
 * 	 result set: the example_table in the database
 */
public class CreateTable implements Driver {
	static final Pattern pattern = Pattern.compile(
			"CREATE\\s+TABLE\\s+([a-z][a-z0-9_]*)\\s*\\(\\s*([a-z][a-z0-9_]*\\s+(?:INTEGER|BOOLEAN|STRING)\\s*(?:PRIMARY)*(?:\\s*,\\s*[a-z][a-z0-9_]*\\s+(?:INTEGER|BOOLEAN|STRING)\\s*(?:PRIMARY)*)*)\\s*\\)",
			Pattern.CASE_INSENSITIVE
		);

		@SuppressWarnings("null")
		@Override
		public Object execute(String query, Database db) throws SQLError {
			Matcher matcher = pattern.matcher(query.strip());
			if (!matcher.matches()) return null;
			
			// Set tablename to string, and determine if table with name already exists. 
			String tablename = matcher.group(1).toString();
			
			if (tablename.length() < 1) {
				throw new SQLError("table name is too short"); 
			}else if (tablename.length() > 15) {
				throw new SQLError("Tablename is too long"); 
			}
			if(db.find(tablename) != null) {
				throw new SQLError("Table <%s> already exsists".formatted(tablename)); 
			}
			
			
			// Split the column definitions into individual columns by delimiter ","
			String column_defs = matcher.group(2); 
			String[] cols = column_defs.split(","); 
			
			
			//Determine if there are a valid number of columns (i.e. more than 0, and less than 16)
			if(cols.length < 1) {
				throw new SQLError("Too few columns"); 
			}else if (cols.length > 15) {
				throw new SQLError("Too many columns"); 
			}
			
			//declare lists for column values 
			List<String> ColumnNames = new ArrayList<String>(); 
			List<String> ColumnTypes = new ArrayList<String>(); 
			int PrimaryIndex = -1; 
			
			// For each column declared, parse its unique values and store in respective list
			for (int i = 0; i < cols.length ; i++) {
				
				// Split cols into values based on whitespace; 
				String[] col_vals = cols[i].strip().split(" "); 
				
				
				// if there are three values in the column, we can assume this to be the primary index 
				if(col_vals.length == 3 && col_vals[2].equalsIgnoreCase("PRIMARY")) {
					 if(PrimaryIndex != -1) {
						 throw new SQLError("Multiple primary indexes in table"); 
					 }else {
						PrimaryIndex = i; 
					 }
				}
				
				// determine if column name already exists
				if(ColumnNames.contains(col_vals[0])) {
					throw new SQLError("Column Name <%s> already exsists".formatted(col_vals[0])); 
				}else {
					ColumnNames.add(col_vals[0]); 
				}
				
				// Determine if the type is valid
				if(col_vals[1].strip().equalsIgnoreCase("string") || col_vals[1].strip().equalsIgnoreCase("integer") || col_vals[1].strip().equalsIgnoreCase("boolean")) {
					ColumnTypes.add(col_vals[1].toLowerCase());
				}else {
					throw new SQLError("Invalid column type"); 
				}
			
				
			}
			
			if(PrimaryIndex == -1) {
				throw new SQLError("No Primary Index assigned"); 
			}
			
			Table newTable = new HashArrayTable(tablename, ColumnNames, ColumnTypes, PrimaryIndex); 			
			db.create(newTable); 			
			Table table = db.find(tablename); 
			return table; 
		}
}