package drivers;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import tables.SearchTable;
import tables.Table;

/**
 * Driver for execution of named macros
 * for testing or grading purposes.
 * <p>
 * Modify the macros for your use case.
 */
@Deprecated
public class Macros implements Driver {
	static final Pattern pattern = Pattern.compile(
		"MACRO\\s+(\\w+)(?:\\s+(.+))?",
		Pattern.CASE_INSENSITIVE
	);

	@Override
	public Object execute(String query, Database db) throws SQLError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches()) return null;

		String macro = matcher.group(1).strip();
		String option = matcher.group(2) != null ? matcher.group(2).strip() : null;

		/*
		 * Example:
		 *   MACRO 1
		 *
		 * Result:
		 *   table: macro_1 with 3 columns by 7 rows
		 *   [table also injected into database]
		 */
		if (macro.equals("1")) {
			Table table = new SearchTable(
				"macro_1",
				List.of("letter", "order", "vowel"),
				List.of("string", "integer", "boolean"),
				0
			);
			table.put(List.of("alpha", 1, true));
			table.put(List.of("beta", 2, false));
			table.put(List.of("gamma", 3, false));
			table.put(List.of("delta", 4, false));
			table.put(List.of("tau", 19, false));
			table.put(List.of("pi", 16, false));
			table.put(Arrays.asList("omega", null, null));

			db.create(table);

			return table;
		}
		
		if (macro.equals("3")) {
			Table table = new SearchTable(
				"macro_3",
				List.of("FirstName", "LastName", "ID", "City"),
				List.of("string", "string", "integer", "string"),
				2
			);
			table.put(List.of("Ron", "Reaser", 2, "Morgantown")); 
			table.put(List.of("Tom", "Devine", 3, "Fairmont")); 
			table.put(List.of("Brian", "Powell", 4, "Parkersburg")); 
			table.put(List.of("Don", "Knotts", 5, "Morgantown")); 
			table.put(Arrays.asList("Mountaineer", null, 1867, "Morgantown")); 
			table.put(Arrays.asList("Philip", "Wilson", 1, "Parkersburg")); 

			db.create(table);

			return table;
		}
		
		if (macro.equals("4")) {
			Table table = new SearchTable(
				"AnimalTable",
				List.of("name", "mammal", "legs", "extinct"),
				List.of("string", "boolean", "integer", "boolean"),
				0
			);
			table.put(List.of("dog", true, 4, false)); 
			table.put(List.of("Iquana", false, 4, false)); 
			table.put(List.of("Cow", true, 4, false)); 
			table.put(List.of("Centipede", false, 100, false)); 
			table.put(List.of("Archaeornithomimus", false, 2, true)); 
			table.put(List.of("Komodo Dragon", false, 4, false)); 
			table.put(List.of("dolphin", true, 0, false)); 
			table.put(List.of("Giraffe", true, 4, false)); 
			table.put(List.of("Dodo", false, 2, true)); 

		db.create(table);

		return table;
		}

		/*
		 * Example:
		 *   MACRO 2
		 *
		 * Result:
		 *   result set: macro_2 renamed from nested query
		 *   [table not injected into database]
		 */
		if (macro.equalsIgnoreCase("2")) {
			Table result_set = (Table) db.interpret("RANGE 10 AS x");
			result_set.setTableName("macro_2");

			return result_set;
		}

		/*
		 * Example:
		 *   MACRO SIZEOF macro_1
		 *
		 * Result:
		 *   integer (affected rows): 7
		 *   [number of rows in given table]
		 */
		if (macro.equalsIgnoreCase("SIZEOF")) {
			Table table = (Table) db.interpret("SHOW TABLE %s".formatted(option));

			return table.size();
		}

		/*
		 * Example:
		 *   MACRO RESET
		 *
		 * Result:
		 *   integer (affected rows): varies
		 *   [all tables dropped from database]
		 */
		if (macro.equalsIgnoreCase("RESET")) {
			int sum = 0;

			for (Table table: db.tables()) {
				sum += table.size();
				db.drop(table.getTableName());
			}

			return sum;
		}

		throw new SQLError("Macro <%s> is undefined".formatted(macro));
	}
}
