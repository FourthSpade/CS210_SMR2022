package grade;

import org.junit.jupiter.api.BeforeAll;

import drivers.SQLError;
import tables.Table;

@Deprecated
public class Lab2 extends SQLModule {
	@BeforeAll
	public static void setup() {
		module_tag = "L2";

		query_data = new Object[][]{
			// DEFAULT COLUMNS
			{ Table.class, "SQUARES BELOW 20", null },
			{ Table.class, "SQUARES BELOW 16", "strict upper bound required" },
			{ Table.class, "SQUARES BELOW 0", "empty result set allowed" },
			{ Table.class, "SQUARES BELOW 1000", "unbounded integer allowed" },

			// BASE NAME WITH DEFAULT POWER NAME
			{ Table.class, "SQUARES BELOW 500 AS a", "partial aliasing allowed" },
			{ Table.class, "SQUARES BELOW 100 AS a", "partial aliasing allowed" },
			{ Table.class, "SQUARES BELOW 100 AS x", "default aliasing allowed" },

			// BASE AND POWER NAMES
			{ Table.class, "SQUARES BELOW 1000 AS a, b", "total aliasing allowed" },
			{ Table.class, "SQUARES BELOW 50 AS a, b", "total aliasing allowed" },
			{ SQLError.class, "SQUARES BELOW 50 AS a, a", "duplicate aliasing forbidden" },

			// SYNTAX
			{ Table.class, "squares below 20 as A, b", "lower case keyword and upper case name allowed" },
			{ Table.class, "SqUaReS bElOw 20 As a, B", "mixed case keyword and upper case name allowed" },
			{ Table.class, " SQUARES BELOW 20 AS a, b ", "unstripped whitespace allowed" },
			{ Table.class, "SQUARES  BELOW  20  AS  a  ,  b", "excess internal whitespace allowed" },
			{ Table.class, "SQUARES BELOW 20 AS a,b", "omitting whitespace around comma allowed" },
			{ SQLError.class, "SQUARESBELOW20ASa,b", "whitespace between keywords and data required" },
			{ SQLError.class, "SQUARES 20", "BELOW keyword required" },
			{ SQLError.class, "BELOW 20", "SQUARES keyword required" },
			{ SQLError.class, "SQUARES BELOW AS a, b", "integer literal required" },
			{ SQLError.class, "SQUARES BELOW 20 a, b", "keyword when aliasing required" },
			{ SQLError.class, "SQUARES BELOW 20 AS a b", "comma when total aliasing required" },
			{ SQLError.class, "SQUARES BELOW 20 AS a,", "incomplete total aliasing forbidden" },
			{ SQLError.class, "SQUARES BELOW 20 AS , b", "incomplete total aliasing forbidden" },

			// ROBUSTNESS
			{ String.class, "ECHO \"Hello, world!\"", null },
			{ SQLError.class, "AN UNRECOGNIZABLE QUERY", null }
		};

		table_data = new Object[][]{
			{ "_squares", 2, 0, "x", "x_squared", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16 },
			{ "_squares", 2, 0, "x", "x_squared", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9 },
			{ "_squares", 2, 0, "x", "x_squared", "integer", "integer" },
			{ "_squares", 2, 0, "x", "x_squared", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16, 5, 25, 6, 36, 7, 49, 8, 64, 9, 81, 10, 100, 11, 121, 12, 144, 13, 169, 14, 196, 15, 225, 16, 256, 17, 289, 18, 324, 19, 361, 20, 400, 21, 441, 22, 484, 23, 529, 24, 576, 25, 625, 26, 676, 27, 729, 28, 784, 29, 841, 30, 900, 31, 961 },
			{ "_squares", 2, 0, "a", "a_squared", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16, 5, 25, 6, 36, 7, 49, 8, 64, 9, 81, 10, 100, 11, 121, 12, 144, 13, 169, 14, 196, 15, 225, 16, 256, 17, 289, 18, 324, 19, 361, 20, 400, 21, 441, 22, 484 },
			{ "_squares", 2, 0, "a", "a_squared", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16, 5, 25, 6, 36, 7, 49, 8, 64, 9, 81 },
			{ "_squares", 2, 0, "x", "x_squared", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16, 5, 25, 6, 36, 7, 49, 8, 64, 9, 81 },
			{ "_squares", 2, 0, "a", "b", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16, 5, 25, 6, 36, 7, 49, 8, 64, 9, 81, 10, 100, 11, 121, 12, 144, 13, 169, 14, 196, 15, 225, 16, 256, 17, 289, 18, 324, 19, 361, 20, 400, 21, 441, 22, 484, 23, 529, 24, 576, 25, 625, 26, 676, 27, 729, 28, 784, 29, 841, 30, 900, 31, 961 },
			{ "_squares", 2, 0, "a", "b", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16, 5, 25, 6, 36, 7, 49 },
			null,
			{ "_squares", 2, 0, "A", "b", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16 },
			{ "_squares", 2, 0, "a", "B", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16 },
			{ "_squares", 2, 0, "a", "b", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16 },
			{ "_squares", 2, 0, "a", "b", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16 },
			{ "_squares", 2, 0, "a", "b", "integer", "integer", 0, 0, 1, 1, 2, 4, 3, 9, 4, 16 },
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null
		};
	}
}