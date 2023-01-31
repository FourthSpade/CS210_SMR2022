package drivers;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import tables.SearchTable;
import tables.Table;

/*
 * Examples:
 * 	 RANGE 5
 * 	 RANGE 3 AS x
 *
 * 1st Result:
 *   result set:
 * 	   primary integer column "number"
 *	   rows [0]; [1]; [2]; [3]; [4]
 *
 * 2nd Result:
 *   result set:
 * 	   primary integer column "x"
 *	   rows [0]; [1]; [2]
 */
public class ShowTables implements Driver {
	static final Pattern pattern = Pattern.compile(
		"SHOW\\s+TABLES",
		Pattern.CASE_INSENSITIVE
	);

	@Override
	public Object execute(String query, Database db) throws SQLError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches()) return null;

		Table result_set = new SearchTable(
			"_tables",
			List.of("table_name", "column_count", "row_count"),
			List.of("string", "integer", "integer"),
			0
		);

		// for each table t in the database
		List<Table> t = db.tables(); 
		for (int i = 0; i < t.size(); i++) {
			List<Object> row = new LinkedList<>(); 
			// add each of the corresponding fields to the row 
			row.add(t.get(i).getTableName()); // table name from t
			row.add(t.get(i).getColumnTypes().size());	// column count from t's schema 
			row.add(t.get(i).size());	// row count from t's size
			result_set.put(row);
		}

		return result_set;
	}
}
