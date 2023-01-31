package grade;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestMethodOrder;

import tables.HashFileTable;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Module2 extends DFSModule {
	@BeforeAll
	public static void setup() {
		module_tag = "M2";
		calls_per_table = 2500;
	}

	@TestFactory
    @DisplayName("Prerequisites")
    @Order(0)
    public final Stream<DynamicTest> audits() throws IllegalAccessException {
		return Stream.of(
			dynamicTest("Constructor (4-ary)", () -> {
				ungraded++;

				try {
					actualTable = firstTestConstructor(() -> {
						return new HashFileTable(
							"m2_table00",
							List.of("a", "b", "c"),
							List.of("string", "integer", "boolean"),
							0
						);
			        });
				}
				catch (Exception e) {
					fail("Unexpected exception with 4-ary constructor", e);
				}
    		}),
			dynamicTest("Constructor (1-ary)", () -> {
				ungraded++;
				try {
					actualTable = firstTestConstructor(() -> {
						return new HashFileTable("m2_table00");
			        });
				}
				catch (Exception e) {
					fail("Unexpected exception with 1-ary constructor", e);
				}
    		}),
			dynamicTest("Forbidden Classes", () -> {
				ungraded++;
				if (actualTable == null)
					fail("Depends on constructor prerequisite");

				testForbiddenClasses(
					actualTable,
					HashFileTable.class,
					List.of(
						"tables",
						"java.lang",
						"java.util.ImmutableCollections",
						"java.util.LinkedList",
						"java.nio",
						"sun.nio.ch",
						"sun.nio.cs",
						"sun.nio.fs"
					)
				);
    		})
    	);
    }

	private static final List<String> tableNames01 = List.of(n(), n(), n());
	private final Stream<DynamicTest> wrapTable01(boolean readOnly) {
		return testTable(
			"m2_table01",
			tableNames01,
			List.of("string", "integer", "boolean"),
			0,
			readOnly
		);
	}

	@TestFactory
	@DisplayName("Create m2_table01 [s*, i, b]")
	@Order(1)
	public final Stream<DynamicTest> createTable01() {
		return wrapTable01(false);
	}

	@TestFactory
	@DisplayName("Reopen m2_table01 [s*, i, b]")
	@Order(2)
	public final Stream<DynamicTest> reopenTable01() {
		return wrapTable01(true);
	}

	private static final List<String> tableNames02 = List.of(n(), n(), n(), n(), n(), n());
	private final Stream<DynamicTest> wrapTable02(boolean readOnly) {
		return testTable(
			"m2_table02",
			tableNames02,
			List.of("integer", "boolean", "boolean", "integer", "integer", "boolean"),
			3,
			readOnly
		);
	}

	@TestFactory
	@DisplayName("Create m2_table02 [i, b, b, i*, i, b]")
	@Order(3)
	public final Stream<DynamicTest> createTable02() {
		return wrapTable02(false);
	}

	@TestFactory
	@DisplayName("Reopen m2_table02 [i, b, b, i*, i, b]")
	@Order(4)
	public final Stream<DynamicTest> reopenTable02() {
		return wrapTable02(true);
	}

	private static final List<String> tableNames03 = List.of(n(), n(), n(), n(), n(), n(), n(), n(), n(), n(), n(), n(), n(), n(), n());
	private final Stream<DynamicTest> wrapTable03(boolean readOnly) {
		return testTable(
			"m2_table03",
			tableNames03,
			List.of("string", "string", "string", "integer", "integer", "integer", "boolean", "boolean", "boolean", "string", "string", "integer", "integer", "boolean", "boolean"),
			9,
			readOnly
		);
	}

	@TestFactory
	@DisplayName("Create m2_table03 [s, s, s, i, i, i, b, b, b, s*, s, i, i, b, b]")
	@Order(5)
	public final Stream<DynamicTest> createTable03() {
		return wrapTable03(false);
	}

	@TestFactory
	@DisplayName("Reopen m2_table03 [s, s, s, i, i, i, b, b, b, s*, s, i, i, b, b]")
	@Order(6)
	public final Stream<DynamicTest> reopenTable03() {
		return wrapTable03(true);
	}

	public final Stream<DynamicTest> testTable(String tableName, List<String> columnNames, List<String> columnTypes, int primaryIndex, boolean reopen) {
		if (!reopen)
			startLog(tableName);

		if (!reopen) {
			actualTable = firstTestConstructor(() -> {
				return new HashFileTable(tableName, columnNames, columnTypes, primaryIndex);
	        });
		}
		else {
			actualTable = firstTestConstructor(() -> {
				return new HashFileTable(tableName);
			});
		}

		logRandomSeed();
		if (!reopen) {
			logConstructor("HashFileTable", tableName, columnNames, columnTypes, primaryIndex);
		}
		else {
			logLine("%s = null;".formatted(tableName));
			logLine("");
			logConstructor("HashFileTable", tableName);
		}

		testTableName(tableName);
		testColumnNames(tableName, columnNames);
		testColumnTypes(tableName, columnTypes);
		testPrimaryIndex(tableName, primaryIndex);

		if (!reopen)
			expectedTable = new ReadWriteTable(tableName, columnNames, columnTypes, primaryIndex);

		return IntStream.range(0, calls_per_table/2).mapToObj(i -> {
			if (i == 0)
				return testTableName(tableName);
			else if (i == 1)
				return testColumnNames(tableName, columnNames);
			else if (i == 2)
				return testColumnTypes(tableName, columnTypes);
			else if (i == 3)
				return testPrimaryIndex(tableName, primaryIndex);

			if ((i == 4 && !reopen) || (i == calls_per_table/2-1 && reopen))
				return testClear(tableName, columnNames, columnTypes, primaryIndex);
			else if ((i == 4 && reopen) || (i == calls_per_table/2-1 && !reopen))
				return testGet(tableName, columnTypes, primaryIndex);

			if (i % 10 == 0)
				if (RNG.nextBoolean())
					return testIterator();
				else
					return testFingerprint();

			var p = RNG.nextDouble();
			if (!reopen && p < 0.70)
				return testPut(tableName, columnTypes, primaryIndex);
			else if (!reopen && p < 0.90)
				return testRemove(tableName, columnTypes, primaryIndex);
			else
				return testGet(tableName, columnTypes, primaryIndex);
		});
	}
}