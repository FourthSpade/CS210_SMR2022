package drivers;

import java.util.ArrayList;
import java.util.Arrays;
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
public class InsertRow implements Driver {
	static final Pattern pattern = Pattern.compile(
			"(INSERT|REPLACE)\\s+INTO\\s+([a-z][a-z0-9_]*)\\s*(?:\\(\\s*([a-z][a-z0-9_-]*\\s*(?:,\\s*(?:[a-z][a-z0-9_-]*))*)\\s*\\))?\\s+VALUES\\s+\\(([^()]*)\\)",
			Pattern.CASE_INSENSITIVE
	);

	@SuppressWarnings({ "null", "unused" })
	@Override
	public Object execute(String query, Database db) throws SQLError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches()) return null;
		
		String tablename = matcher.group(2).toString();
		
		if(db.find(tablename) == null) {
			throw new SQLError("Table <%s> does not exist".formatted(tablename)); 
		}
		Table table = db.find(tablename); 
		List<String> tableCols = table.getColumnNames();
		List<String> colTypes = table.getColumnTypes(); 
		

		List<Object> ptrList = new ArrayList<Object>(); 
		List<Object> cols = new ArrayList<Object>(); 
		String column_defs = matcher.group(3);
		if(column_defs != null) { 
			String[] colsArr = column_defs.split(","); 
			// Clean up values
			for(int i = 0; i < colsArr.length; i++) {
				cols.add(colsArr[i].strip());
			}
		}
		
		if (column_defs != null) {
			for (int i = 0; i < cols.size() ; i++) {
				int j = tableCols.indexOf(cols.get(i)); 
				if (j == -1) {
					throw new SQLError("Column <%s> does not exist".formatted(cols.get(i))); 
				}
				
				if (ptrList.contains(j)) {
					throw new SQLError("Column <%s> has been listed twice in the schema".formatted(cols.get(i))); 
				}
				
				ptrList.add(j); 
			}
			
			if(!cols.contains(tableCols.get(table.getPrimaryIndex()))) {
				throw new SQLError("Primary column value not given"); 

			}
		}else {
			for (int j = 0; j < tableCols.size(); j++) {
				ptrList.add(j); 
			}
		}
		
		int counter = 0; 
		List<Object> vals = new ArrayList<Object>(); 
		String column_vals = matcher.group(4); 
		String[] valsArr = column_vals.split(","); 
		for(int i = 0; i < valsArr.length; i++) {
			vals.add(valsArr[i].strip());
		}
		
		if(vals.size() != ptrList.size()) {
			throw new SQLError("Number of values passed is not equal to number of columns"); 
		}
		
		Object[] newRow = new Object[tableCols.size()]; 
		
		for(int i = 0; i < ptrList.size(); i++) {

//			System.out.println("Loop Iteration: " + i + " | Row Value: " + Arrays.deepToString(newRow)); 
			int j = (int) ptrList.get(i); 
			String value = (String) vals.get(i); 
			String name = tableCols.get(j); 
			String type = colTypes.get(j); 
//			System.out.println("Value: " + value); 
			
//			System.out.println(value); // Debug code
			
 
			if(value.matches("\"[^\"]*\"")) {
				if(!type.equalsIgnoreCase("string")) {
					throw new SQLError("Type mismatch"); 
				}else if(value.length() > 129) {
					throw new SQLError("value for column is too long"); 
				}else if(value.length() < 1) {
					throw new SQLError("value <s%> is too short".formatted(value)); 
				}else {
					value = value.replace("\"", "");  
					newRow[j] = value; 
				}
			}else if(value.matches("[+-]*\\d+")) {
				if(!type.equalsIgnoreCase("integer")) {
					throw new SQLError("Type mismatch"); 
				}else if(value.matches("[+|-]*^0\\d+")) {
					throw new SQLError("Integer cannot start with 0"); 
				}
				try {
					int intValue = Integer.parseInt(value); 
					newRow[j] = intValue;
				}catch(Exception e) {
					throw new SQLError("Error parsing integer upon insert"); 
				} 
			}else if(value.toLowerCase().matches("true|false")) {
				if(!type.equalsIgnoreCase("boolean")) {
					throw new SQLError("Type mismatch"); 
				}
				
				try {
					boolean booleanVal = Boolean.parseBoolean(value); 
					newRow[j] = booleanVal; 
				}catch(Exception e) {
					throw new SQLError("Error parsing boolean upon insert"); 
				}
			}else if(!value.toLowerCase().matches("null")){
				throw new SQLError("Type for value not determinable"); 
			}
			
			Object key = null;
			if (j == table.getPrimaryIndex()) {
				key = newRow[j];
				
				if(key == null) {
					throw new SQLError("Primary index key value cannot be a null. Key Value: ".formatted(key)); 
				}
				
				if(matcher.group(1).equalsIgnoreCase("insert") && table.get(key) != null) {
					throw new SQLError("Primary index key value already exists in the table on insert"); 
				}	
			}
			
			
			
		}
		List<Object> validRow = Arrays.asList(newRow); 
//		System.out.println("Final Value: " + validRow.toString());
		table.put(validRow); 
		return 1; 
	}
}