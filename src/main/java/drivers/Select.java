package drivers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
	public class Select implements Driver {
		static final Pattern pattern = Pattern.compile(
				"SELECT\\s+\\(?(\\*|(?:[a-z][a-z0-9_]*(?:\\s+AS\\s+(?:[a-z][a-z0-9_]*))?)\\s*(?:,\\s*(?:\\*|[a-z][a-z0-9_]*(?:\\s+AS\\s+(?:[a-z][a-z0-9_]*))?)\\s*)*)\\)?\\s+FROM\\s+([a-z][a-z0-9_]*)(\\s+WHERE\\s+(?:[a-z][a-z0-9_]*)\\s*(?:\\>|\\<|\\=|\\<\\>|\\<\\=|\\>\\=)\\s*\\\"?(?:[a-z0-9][a-z0-9_]*)*\\\"?)?",
				Pattern.CASE_INSENSITIVE
		);
	
		@SuppressWarnings({ "null", "unused" })
		@Override
		public Object execute(String query, Database db) throws SQLError {
			Matcher matcher = pattern.matcher(query.strip());
			if (!matcher.matches()) return null;
			
System.out.println("Group 1: " + matcher.group(1).strip()); 
System.out.println("Group 2: " + matcher.group(2).strip()); 
System.out.println("Group 3: " + matcher.group(3)); 
			
			String tablename = matcher.group(2).toString().strip();
			
			if(db.find(tablename) == null) {
				throw new SQLError("Table <%s> does not exist".formatted(tablename)); 
			}
			Table table = db.find(tablename); 
			List<String> tableCols = table.getColumnNames();
			List<String> colTypes = table.getColumnTypes(); 
			
			List<Object> ptrList = new ArrayList<Object>(); 
			List<Object> cols = new ArrayList<Object>(); 
			List<String> selectSchema = new ArrayList<String>(); 
			List<String> referenceSchema = new ArrayList<String>(); 
			int primaryIndex = 0; 
			List<String> selectType = new ArrayList<String>();
			
			String lhsName = null; 
			String lhsType = null; 
			String operator = null; 
			String rhsValue = null;
			String rhsType = null; 
			
			
			String column_defs = matcher.group(1);
			if(column_defs != null) { 
				String[] colsArr = column_defs.split(","); 
				// Clean up values
				for(int i = 0; i < colsArr.length; i++) {
					String[] temp = colsArr[i].strip().split("(?i)\\s+AS\\s+"); 
					cols.add(temp[0]);
					if (temp.length > 1) {
						if(selectSchema.contains(temp[1])) {
							throw new SQLError("Duplicate/Unambiguous Column Names");
						}else {
							selectSchema.add(temp[1]);
							referenceSchema.add(temp[0]); 
						}
					}else {
						if(selectSchema.contains(temp[0])) {
							throw new SQLError("Duplicate/Unambiguous Column Names"); 
						}else {
							selectSchema.add(temp[0]);
							referenceSchema.add(temp[0]);
						}
					}
				}
			}
			
			if (column_defs != null) {
				if(cols.get(0).equals("*")) {
					selectSchema = tableCols;
					selectType = colTypes; 
					primaryIndex = table.getPrimaryIndex(); 
					for (int i = 0; i < selectSchema.size(); i++) {
						ptrList.add(i); 
					}
				}else {
					for (int i = 0; i < cols.size() ; i++) {
						int j = tableCols.indexOf(cols.get(i)); 
						if (j == -1) {
							throw new SQLError("Column <%s> does not exist".formatted(cols.get(i))); 
						}
						
						ptrList.add(j); 
					}
					for (int i = 0; i < ptrList.size(); i++) {
						selectType.add(colTypes.get((int) ptrList.get(i))); 
					}
					if(!referenceSchema.contains(tableCols.get(table.getPrimaryIndex()))){
						throw new SQLError("Primary Index not Included"); 
					}
					primaryIndex = referenceSchema.indexOf(tableCols.get(table.getPrimaryIndex())); 
				}
			}
			
				if(primaryIndex == -1) {
				primaryIndex = 0; 
			}

System.out.println(ptrList); 
System.out.println(selectSchema); 
System.out.println(selectType);		
System.out.println(primaryIndex);	


			Table resultSet = new HashArrayTable("_select", selectSchema, selectType, primaryIndex); 

			if (matcher.group(3) != null) {
				String whereClause = matcher.group(3).strip();
				String[] whereArr = whereClause.split("\\s+"); 
System.out.println("whereClause: " + Arrays.toString(whereArr));
				
				if(whereArr.length < 4) {
					throw new SQLError("Invalid number of operators in WHERE statement"); 
				}
				
				lhsName = whereArr[1].strip(); 
System.out.println("lhsName: " + lhsName); 
				if(!tableCols.contains(lhsName)) {
					throw new SQLError("Left Hand Side must be defined"); 
				}
				lhsType = colTypes.get(tableCols.indexOf(lhsName)); 
				operator = whereArr[2].strip(); 
				rhsValue = whereArr[3].strip(); 
				rhsType = null; 
//System.out.println(lhsName);
//System.out.println(lhsType); 
//System.out.println(operator);
//System.out.println(rhsValue);	
				if(rhsValue.matches("\"[^\"]*\"")) {
					if(!lhsType.equalsIgnoreCase("string")) {
				return resultSet; 
//						throw new SQLError("Type mismatch"); 
					}else if(rhsValue.length() > 129) {
						throw new SQLError("value for column is too long"); 
					}else if(rhsValue.length() < 1) {
						throw new SQLError("value <s%> is too short".formatted(rhsValue)); 
					}else {
						rhsValue = rhsValue.replace("\"", "");  
						rhsType = "string"; 
					}
				}else if(rhsValue.matches("[+-]*\\d+")) {
					if(!lhsType.equalsIgnoreCase("integer")) {
				return resultSet;
//						throw new SQLError("Type mismatch"); 
					}else if(rhsValue.matches("[+|-]*^0\\d+")) {
						throw new SQLError("Integer cannot start with 0"); 
					}
					try {
						int intValue = Integer.parseInt(rhsValue); 
						rhsType = "integer";
					}catch(Exception e) {
						throw new SQLError("Error parsing integer upon insert"); 
					} 
				}else if(rhsValue.toLowerCase().matches("true|false")) {
					if(!lhsType.equalsIgnoreCase("boolean")) {
				return resultSet;
//						throw new SQLError("Type mismatch"); 
					}
					
					rhsType = "boolean"; 
//					try {
//						boolean booleanVal = Boolean.parseBoolean(rhsValue); 
//						
//					}catch(Exception e) {
//						throw new SQLError("Error parsing boolean upon insert"); 
//					}
				}else if(!rhsValue.toLowerCase().matches("null")){
					throw new SQLError("Type for value not determinable"); 
				}		
			}
			
			Set<List<Object>> rows = table.rows();

			for (List<Object> row : rows) {
				boolean flag = true; 
				if(matcher.group(3) != null) {
					var lhsValue = row.get(tableCols.indexOf(lhsName)); 
System.out.println(row); 
					
					if(rhsValue == null || lhsValue == null) {
						flag = false; 
					}else {
						var compResult = -1; 
						if(!rhsValue.equals("null")) {
							if (lhsType.equalsIgnoreCase("string") && rhsType.equalsIgnoreCase("string")) {
System.out.println("lhsval: " + lhsValue.toString());
System.out.println("rhsval: " + rhsValue);
								compResult =  lhsValue.toString().compareTo(rhsValue); 
							}else if(lhsType.equalsIgnoreCase("integer") && rhsType.equalsIgnoreCase("integer")) {
								Integer lhsInt = (Integer) lhsValue; 
								compResult =  lhsInt.compareTo(Integer.parseInt(rhsValue)); 
							}else if(lhsType.equalsIgnoreCase("boolean") && rhsType.equalsIgnoreCase("boolean")) {
								Boolean lhsBool = (Boolean) lhsValue; 
								compResult =  lhsBool.compareTo(Boolean.parseBoolean(rhsValue)); 
							}else {
								String lhsString = lhsValue.toString(); 
								compResult = lhsString.compareTo(rhsValue); 
							}
						}
System.out.println(compResult); 

						if(operator.strip().equals("=")){
							flag = (compResult == 0); 
						}else if (operator.strip().equals("<>")) {
							flag = (compResult != 0); 
						}else if (operator.strip().equals("<")) {
							flag = (compResult < 0); 
						}else if (operator.strip().equals(">")) {
							flag = (compResult > 0); 
						}else if (operator.strip().equals("<=")) {
							flag = (compResult <= 0); 
						}else if (operator.strip().equals(">=")) {
							flag = (compResult >= 0); 
						}	
						
						if(rhsValue == null || rhsValue.equals("null")) {
							flag = false; 
						}
					}
				}
				

				if(flag == true) {
					List<Object> newRow = new ArrayList<Object>(); 
					for(int j = 0 ; j < ptrList.size(); j++) {
						newRow.add(row.get((int) ptrList.get(j))); 
					}
System.out.println(newRow); 
					resultSet.put(newRow); 
				}
			}
			
			return resultSet; 
		}
}