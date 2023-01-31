package apps;

import java.util.Arrays;
import java.util.List;

import tables.SearchTable;
import tables.Table;

/**
 * Sandbox for execution of arbitrary code
 * for testing or grading purposes.
 * <p>
 * Modify the code for your use case.
 */
@Deprecated
public class Sandbox {
	public static void main(String[] args) {
		Table table = new SearchTable(
			"sandbox_1",
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
		table.put(List.of("omega", 24, true));
		table.put(Arrays.asList("N/A", null, null));

		System.out.println(table);
	}
}
