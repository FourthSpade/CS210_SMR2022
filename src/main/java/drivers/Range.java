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
public class Range implements Driver {
	static final Pattern pattern = Pattern.compile(
		"RANGE\\s+([0-9]+)(?:\\s+AS\\s+([a-z][a-z0-9_]*))?",
		Pattern.CASE_INSENSITIVE
	);

	@Override
	public Object execute(String query, Database db) throws SQLError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches()) return null;

		int upper = Integer.parseInt(matcher.group(1));
		String name = matcher.group(2) != null ? matcher.group(2) : "number";

		Table result_set = new SearchTable(
			"_range",
			List.of(name),
			List.of("integer"),
			0
		);

		for (int i = 0; i < upper; i++) {
			List<Object> row = new LinkedList<>();
			row.add(i);
			result_set.put(row);
		}

		return result_set;
	}
}
