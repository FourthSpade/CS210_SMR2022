package grade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import apps.Database;
import drivers.SQLError;
import tables.Table;

public abstract class SQLModule {
	protected static int graded, passed;

	protected static String module_tag;
	protected static Object[][] query_data;
	protected static Object[][] table_data;

	protected static Database DB;

	protected static PrintStream LOG_FILE;

	protected static Arguments[] data() {
		var arguments = new Arguments[query_data.length];

		for (var a = 0; a < arguments.length; a++) {
			Table table = null;
			if (a < table_data.length && table_data[a] != null) {
				var i = 0;

				var tableName = (String) table_data[a][i++];
				var columnCount = (int) table_data[a][i++];
				var primaryIndex = (int) table_data[a][i++];

				var columnNames = new LinkedList<String>();
				for (var j = 1; j <= columnCount; j++)
					columnNames.add((String) table_data[a][i++]);

				var columnTypes = new LinkedList<String>();
				for (var j = 1; j <= columnCount; j++)
					columnTypes.add((String) table_data[a][i++]);

				List<List<Object>> rows = new LinkedList<>();
				for (var j = i; j < table_data[a].length; j += columnCount) {
					var row = new LinkedList<>();
					for (var k = 0; k < columnCount; k++)
						row.add(table_data[a][j+k]);
					rows.add(row);
				}

				table = new ReadOnlyTable(
					tableName,
					columnNames,
					columnTypes,
					primaryIndex,
					rows
				);
			}

			arguments[a] = Arguments.of(
				query_data[a][1],
				query_data[a][2] != null ? query_data[a][2] : "none provided",
				query_data[a][0],
				table
			);
		}

		return arguments;
	}

	@BeforeAll
	protected static final void initialize() throws IOException {
		try {
			DB = new Database(false);
		}
		catch (Exception e) {
			fail("Database constructor must not throw exceptions", e);
		}

		graded = 0;
		passed = 0;
	}

	@DisplayName("Queries")
	@ParameterizedTest(name = "[{index}] {0}")
	@MethodSource("data")
	protected void testQuery(String query, String reason, Object expectedResult, Table expectedTable) {
		if (!reason.contains("prerequisite"))
			graded++;

		Object actualResult = null;
		try {
			logQuery(query);
			actualResult = DB.interpret(query);
		}
		catch (SQLError error) {
			actualResult = error;
		}
		catch (Exception thrown) {
			fail(
				"Query must not throw <%s> exception, reason: <%s>".formatted(
					thrown.getClass(),
					reason
				),
				thrown
			);
		}

		Table actualTable = null;
		if (expectedResult == Table.class) {
			if (!(actualResult instanceof Table))
				assertEquals(
					Table.class,
					actualResult,
					"Query must return %s, reason: <%s>".formatted(
						expectedTable != null && expectedTable.getTableName().startsWith("_") ? "result set" : "table",
						reason
					)
				);

			actualTable = (Table) actualResult;
		}
		else if (expectedResult instanceof Integer) {
			assertEquals(
				expectedResult,
				actualResult,
				"Query must return integer (number of affected rows), reason: <%s>".formatted(reason)
			);

			var embeddedName = query.strip().split("\\s+")[2];
			for (var table: DB.tables()) {
				if (table.getTableName().equals(embeddedName)) {
					actualTable = table;
					break;
				}
			}
		}
		else if (expectedResult instanceof String) {
			assertEquals(
				expectedResult,
				actualResult,
				"Query must return string, reason: <%s>".formatted(reason)
			);
		}
		else if (expectedResult instanceof Boolean) {
			assertEquals(
				expectedResult,
				actualResult,
				"Query must return boolean, reason: <%s>".formatted(reason)
			);
		}
		else if (expectedResult == SQLError.class) {
			if (!(actualResult instanceof SQLError))
				assertEquals(
					SQLError.class,
					actualResult,
					"Query must throw SQLError, reason: <%s>".formatted(reason)
				);
		}

		if (expectedTable != null) {
			var friendlyName = friendly(expectedTable.getTableName());

			assertEquals(
				expectedTable.getTableName(),
				actualTable.getTableName(),
				"%s has incorrect table name in schema".formatted(friendlyName)
			);

			assertEquals(
				expectedTable.getColumnNames(),
				actualTable.getColumnNames(),
				"%s has incorrect column names in schema".formatted(friendlyName)
			);

			assertEquals(
				expectedTable.getColumnTypes(),
				actualTable.getColumnTypes(),
				"%s has incorrect column types in schema".formatted(friendlyName)
			);

			assertEquals(
				expectedTable.getPrimaryIndex(),
				actualTable.getPrimaryIndex(),
				"%s has incorrect primary index in schema".formatted(friendlyName)
			);

			for (var e_row: expectedTable) {
				var e_key = e_row.get(expectedTable.getPrimaryIndex());

				if (!actualTable.contains(e_key))
					fail(
						"%s doesn't contain expected key <%s> with type <%s> in state".formatted(
							friendlyName,
							e_key,
							typeOf(e_key, true)
						)
					);

				var a_row = actualTable.get(e_key);

				assertEquals(
					typesOf(e_row),
					typesOf(a_row),
					"%s has unexpected types of row <%s> in state".formatted(
						friendlyName,
						a_row
					)
				);

				assertEquals(
					stringsOf(e_row),
					stringsOf(a_row),
					"%s has unexpected field values of row with key <%s> in state".formatted(
						friendlyName,
						e_key
					)
				);
			}

			for (var a_key: actualTable.keys()) {
				if (!expectedTable.contains(a_key))
					fail(
						"%s contains unexpected key <%s> with type <%s> in state".formatted(
							friendlyName,
							a_key,
							typeOf(a_key, true)
						)
					);
			}
		}

		if (!reason.contains("prerequisite"))
			passed++;

		serialTable = actualTable;
	}

	protected static Table serialTable;

	@AfterAll
	protected static void report(TestReporter reporter) throws IOException {
		System.out.println();

		var earned = (int) Math.ceil(passed / (double) graded * 100);
		var ungraded = query_data.length - graded;
		if (ungraded > 0)
			System.out.println("Prerequisites (Ungraded): %s".formatted(ungraded));
		System.out.println("Graded Tests: %s".formatted(graded));
		System.out.println("Passed Tests: %s".formatted(passed));
		System.out.println("Module Grade: %s / %s = %s%%".formatted(passed, graded, earned));

		System.out.println();

		System.out.printf(
			"[%s PASSED %d%% OF UNIT TESTS]\n",
			module_tag,
			earned
		);

		System.out.println();

		reporter.publishEntry(module_tag, String.valueOf(earned));

		try {
			DB.close();
		}
		catch (Exception e) {
			fail("Database close should not throw exceptions", e);
		}
	}

	private static final String friendly(String tableName) {
		return tableName.startsWith("_")
			? "result set <%s>".formatted(tableName)
			: "table <%s> in the database".formatted(tableName);
	}

	private static final String typeOf(Object obj) {
		return typeOf(obj, false);
	}

	private static final String typeOf(Object obj, boolean length) {
		if (obj == null)
			return "null";

		var type = obj.getClass().getSimpleName().toLowerCase();

		if (length && obj instanceof String str)
			return "%s | length %d".formatted(type, str.length());
		else
			return type;
	}

	private static final List<String> typesOf(List<Object> list) {
		if (list == null)
			return null;

		return list.stream().map(v -> typeOf(v)).collect(Collectors.toList());
	}

	private static final List<String> stringsOf(List<Object> list) {
		if (list == null)
			return null;

		return list.stream().map(v -> String.valueOf(v)).collect(Collectors.toList());
	}

	protected static final void startLog() {
		try {
			var path = Paths.get("data", "logging", "%s.sql".formatted(module_tag.toLowerCase()));

			System.out.println("Logging Script to: %s".formatted(path));

			Files.createDirectories(path.getParent());
			LOG_FILE = new PrintStream(path.toFile());
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected static final void logQuery(String line) {
		if (LOG_FILE == null)
			startLog();

		if (LOG_FILE != null)
			LOG_FILE.println("%s;".formatted(line));
	}

	protected static class ReadOnlyTable extends Table {
		private Map<Object, List<Object>> map;

		public ReadOnlyTable(String tableName, List<String> columnNames, List<String> columnTypes, int primaryIndex, List<List<Object>> rows) {
			setTableName(tableName);
			setColumnNames(columnNames);
			setColumnTypes(columnTypes);
			setPrimaryIndex(primaryIndex);

			map = new HashMap<>();
			for (var row: rows) {
				var key = row.get(primaryIndex);
				map.put(key, row);
			}
		}

		@Override
		public void clear() {
			map.clear();
		}

		@Override
		public boolean put(List<Object> row) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean remove(Object key) {
			throw new UnsupportedOperationException();
		}

		@Override
		public List<Object> get(Object key) {
			return map.get(key);
		}

		@Override
		public boolean contains(Object key) {
			return map.containsKey(key);
		}

		@Override
		public int size() {
			return map.size();
		}

		@Override
		public int capacity() {
			return size();
		}

		@Override
		public Iterator<List<Object>> iterator() {
			return new Iterator<>() {
				Iterator<List<Object>> iter = map.values().iterator();

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public List<Object> next() {
					return iter.next();
				}
			};
		}
	}
}