package grade;

import org.junit.jupiter.api.BeforeAll;

import drivers.SQLError;
import tables.Table;

public class Module4 extends SQLModule {
	@BeforeAll
	public static void setup() {
		module_tag = "M4";

		query_data = new Object[][]{
			// BASICS
			{ Table.class, "CREATE TABLE m4_table01 (id INTEGER PRIMARY, name STRING, flag BOOLEAN)", null },
			{ Table.class, "SHOW TABLES", null },
			{ SQLError.class, "CREATE TABLE m4_table01 (id INTEGER PRIMARY, name STRING, flag BOOLEAN)", null },
			{ Table.class, "SHOW TABLES", null },

			// CASE, WHITESPACE
			{ Table.class, "create table M4_TABLE02 (ID integer primary, NAME string, flag BOOLEAN)", "lower case keyword and upper case table name allowed" },
			{ Table.class, " CREATE TABLE m4_table03 (id INTEGER PRIMARY, name STRING, flag BOOLEAN) ", "unstripped whitespace allowed" },
			{ Table.class, "CREATE  TABLE  m4_table04  (id INTEGER PRIMARY, name STRING, flag BOOLEAN)", "excess internal whitespace allowed" },
			{ Table.class, "CREATE TABLE m4_table05 ( id INTEGER PRIMARY , name STRING , flag BOOLEAN )", "excess internal whitespace allowed" },
			{ Table.class, "CREATE TABLE m4_table06 (id INTEGER PRIMARY,name STRING,flag BOOLEAN)", "whitespace around punctuation not required" },
			{ Table.class, "SHOW TABLES", null },
			{ SQLError.class, "CREATETABLE m4_table07 (id INTEGERPRIMARY, name STRING, flag BOOLEAN)", "whitespace between keywords required " },
			{ SQLError.class, "CREATE TABLEm4_table08 (idINTEGER PRIMARY, nameSTRING, flag BOOLEAN)", "whitespace between keywords and names required" },
			{ Table.class, "SHOW TABLES", null },

			// NAMES, KEYWORDS, PUNCTUATION
			{ Table.class, "CREATE TABLE t (i INTEGER PRIMARY, n STRING, f BOOLEAN)", "1-character name allowed" },
			{ Table.class, "CREATE TABLE m4_table10_____ (n23456789012345 INTEGER PRIMARY)", "15-character name allowed" },
			{ Table.class, "SHOW TABLES", null },
			{ SQLError.class, "CREATE TABLE m4_table11______ (n234567890123456 INTEGER PRIMARY)", "at most 15-character names allowed" },
			{ SQLError.class, "CREATE TABLE 1m_table12 (2id INTEGER PRIMARY, 3name STRING, 4flag BOOLEAN)", "name starting with number forbidden" },
			{ SQLError.class, "CREATE TABLE _m1table13 (_id INTEGER PRIMARY, _name STRING, _flag BOOLEAN)", "name starting with underscore forbidden" },
			{ SQLError.class, "CREATE TABLE (id INTEGER PRIMARY, name STRING, flag BOOLEAN)", "table name required" },
			{ SQLError.class, "CREATE m4_table15 (id INTEGER PRIMARY, name STRING, flag BOOLEAN)", "TABLE keyword required" },
			{ SQLError.class, "CREATE TABLE m4_table16 (id INTEGER PRIMARY name STRING flag BOOLEAN)", "commas between definitions required" },
			{ SQLError.class, "CREATE TABLE m4_table17 id INTEGER PRIMARY, name STRING, flag BOOLEAN", "parentheses required" },
			{ Table.class, "SHOW TABLES", null },

			// COLUMNS, PRIMARY
			{ Table.class, "CREATE TABLE m4_table18 (c1 INTEGER PRIMARY)", "1 column allowed" },
			{ Table.class, "CREATE TABLE m4_table19 (c1 INTEGER PRIMARY, c2 STRING)", "2 columns allowed" },
			{ Table.class, "CREATE TABLE m4_table20 (c1 INTEGER PRIMARY, c2 INTEGER, c3 INTEGER, c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER, c8 INTEGER, c9 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER)", "15 columns allowed" },
			{ Table.class, "CREATE TABLE m4_table21 (id INTEGER, name STRING PRIMARY, flag BOOLEAN)", "non-0 primary column index allowed" },
			{ Table.class, "SHOW TABLES", null },
			{ SQLError.class, "CREATE TABLE m4_table22 (id INTEGER PRIMARY, other STRING, other BOOLEAN)", "duplicate column name forbidden" },
			{ SQLError.class, "CREATE TABLE m4_table23 (id INTEGER, name STRING, flag BOOLEAN)", "primary column required" },
			{ SQLError.class, "CREATE TABLE m4_table24 (id INTEGER PRIMARY, name STRING PRIMARY, flag BOOLEAN PRIMARY)", "duplicate primary column forbidden" },
			{ SQLError.class, "CREATE TABLE m4_table25 ()", "at least 1 column required" },
			{ SQLError.class, "CREATE TABLE m4_table26 (c1 INTEGER PRIMARY, c2 INTEGER, c3 INTEGER, c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER, c8 INTEGER, c9 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER, c16 INTEGER)", "at most 15 columns allowed" },
			{ Table.class, "SHOW TABLES", null },

			// DROP TABLE
			{ Integer.class, "DROP TABLE m4_table01", null },
			{ SQLError.class, "DROP TABLE m4_table01", null },
			{ Table.class, "SHOW TABLES", null },
			{ Table.class, "CREATE TABLE m4_table01 (ps STRING PRIMARY)", null },
			{ Table.class, "SHOW TABLES", null },
			{ Integer.class, "drop table M4_TABLE02", "lower case keyword and upper case table name allowed" },
			{ Integer.class, " DROP TABLE m4_table03 ", "unstripped whitespace allowed" },
			{ Integer.class, "DROP  TABLE  m4_table04", "excess internal whitespace allowed" },
			{ Integer.class, "DROP TABLE t", "1-letter name allowed" },
			{ Table.class, "SHOW TABLES", null },
			{ SQLError.class, "DROPTABLE m4_table05", "whitespace between keywords required " },
			{ SQLError.class, "DROP TABLEm4_table06", "whitespace between keywords and names required" },
			{ SQLError.class, "DROP m4_table17", "TABLE keyword required" },
			{ SQLError.class, "DROP TABLE", "table name required" },
			{ Table.class, "SHOW TABLES", null },

			// SHOW TABLES
			{ Table.class, "SHOW TABLES", "upper case keyword allowed" },
			{ Table.class, "show tables", "lower case keyword allowed" },
			{ Table.class, "ShOw tAbLeS", "mixed case keyword allowed" },
			{ Table.class, "  SHOW  TABLES  ", "excess internal whitespace and unstripped whitespace allowed" },
			{ Table.class, "SHOW TABLES", null },
			{ SQLError.class, "SHOWTABLES", "whitespace between keywords required " },
			{ SQLError.class, "SHOW", "TABLES keyword required" },
			{ SQLError.class, "TABLES", "SHOW keyword required" },
			{ Table.class, "SHOW TABLES", null },

			// DATABASE INTERPRETER
			{ String.class, "ECHO \"Hello, world!\"", null },
			{ Table.class, "RANGE 5", null },
			{ Table.class, "SHOW TABLE m4_table01", null },
			{ SQLError.class, "SHOW TABLE m4_table00", null },
			{ SQLError.class, "AN UNRECOGNIZABLE QUERY", null },
			{ Table.class, "SHOW TABLES", null },
		};

		table_data = new Object[][]{
			{ "m4_table01", 3, 0, "id", "name", "flag", "integer", "string", "boolean" },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 3, 0 },
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 3, 0 },
			{ "M4_TABLE02", 3, 0, "ID", "NAME", "flag", "integer", "string", "boolean" },
			{ "m4_table03", 3, 0, "id", "name", "flag", "integer", "string", "boolean" },
			{ "m4_table04", 3, 0, "id", "name", "flag", "integer", "string", "boolean" },
			{ "m4_table05", 3, 0, "id", "name", "flag", "integer", "string", "boolean" },
			{ "m4_table06", 3, 0, "id", "name", "flag", "integer", "string", "boolean" },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0 },
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0 },
			{ "t", 3, 0, "i", "n", "f", "integer", "string", "boolean" },
			{ "m4_table10_____", 1, 0, "n23456789012345", "integer" },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "t", 3, 0 },
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "t", 3, 0 },
			{ "m4_table18", 1, 0, "c1", "integer" },
			{ "m4_table19", 2, 0, "c1", "c2", "integer", "string" },
			{ "m4_table20", 15, 0, "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer" },
			{ "m4_table21", 3, 1, "id", "name", "flag", "integer", "string", "boolean" },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0, "t", 3, 0 },
			null,
			null,
			null,
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0, "t", 3, 0 },
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "M4_TABLE02", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0, "t", 3, 0 },
			{ "m4_table01", 1, 0, "ps", "string" },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "M4_TABLE02", 3, 0, "m4_table01", 1, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0, "t", 3, 0 },
			null,
			null,
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			null,
			null,
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			null,
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			null,
			{ "_range", 1, 0, "number", "integer", 0, 1, 2, 3, 4 },
			{ "m4_table01", 1, 0, "ps", "string" },
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", "string", "integer", "integer", "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 }
		};
	}
}