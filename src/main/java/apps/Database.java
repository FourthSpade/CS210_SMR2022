package apps;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import drivers.CreateTable;
import drivers.Driver;
import drivers.DropTable;
import drivers.Echo;
import drivers.Export;
import drivers.Import;
import drivers.InsertRow;
import drivers.Range;
import drivers.Macros; 
import drivers.SquaresBelow; 
import drivers.ShowTable;
import drivers.ShowTables;
import drivers.SQLError;
import drivers.Select;
import tables.Table;

/**
 * This class implements a
 * database management system.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public class Database implements Closeable {
	private final List<Table> tables;
	private final List<Driver> drivers;
	private final boolean persistent;

	/**
	 * Initializes the drivers and the tables.
	 *
	 * @param persistent whether the database is persistent.
	 */
	public Database(boolean persistent) {
		this.persistent = persistent;

		tables = new LinkedList<>();

		drivers = List.of(
			new Echo(),
			new Range(),
			new ShowTable(),
			new ShowTables(), 
			new DropTable(),
			new CreateTable(), 
			new InsertRow(),
			new Select(),
			new Export(), 
			new Import(), 
			new Macros()
			
		);
	}

	/**
	 * Returns whether the database is persistent.
	 *
	 * @return whether the database is persistent.
	 */
	public boolean isPersistent() {
		return persistent;
	}

	/**
	 * Returns an unmodifiable list
	 * of the tables in the database.
	 *
	 * @return the list of tables.
	 */
	public List<Table> tables() {
		return List.copyOf(tables);
	}

	/**
	 * Returns the table with the given name,
	 * or <code>null</code> if there is none.
	 *
	 * @param tableName a table name.
	 * @return the corresponding table, if any.
	 */
	public Table find(String tableName) {
		for (var table:tables) {
			if (table.getTableName().equals(tableName)) {
				return table; 
			}
		} 
		return null; 
	}

	/**
	 * Returns <code>true</code> if a table with the
	 * given name exists or <code>false</code> otherwise.
	 *
	 * @param tableName a table name.
	 * @return whether the corresponding table exists.
	 */
	public boolean exists(String tableName) {
		for (int i = 0 ; i < tables.size(); i++) {
			if (tables.get(i).getTableName().equals(tableName)) {
				return true; 
			}
		}
		return false; 
	}

	/**
	 * Creates the given table, unless
	 * a table with the corresponding name exists.
	 * <p>
	 * Returns <code>true</code> if created
	 * or <code>false</code> otherwise.
	 *
	 * @return whether the table was created.
	 */
	public boolean create(Table table) {
		if (exists(table.getTableName())) 
			return false; 
			
		tables.add(table); 
		return true; 
	}

	/**
	 * Drops the table with the given name, unless
	 * no table with the given name exists.
	 * <p>
	 * Returns <code>true</code> if dropped
	 * or <code>false</code> otherwise.
	 *
	 * @return whether the table was dropped.
	 */
	public boolean drop(String tableName) {		
		for (int i = 0 ; i < tables.size(); i++) {
			if (tables.get(i).getTableName().equals(tableName)) {
				tables.remove(i); 
				return true; 
			}
		}
		return false; 
	}

	/**
	 * Interprets a list of queries and returns
	 * a list of results to each in sequence.
	 *
	 * @param queries the list of queries.
	 * @return the list of results.
	 * @throws SQLError
	 */
	public Object interpret(String query) throws SQLError {
		for (var driver: drivers) {
			var res = driver.execute(query,  this); 
			if (res != null)
				return res;
		}
		throw new SQLError("Unrecognized query");
	}

	/**
	 * Executes any required tasks when
	 * the database is closed.
	 */
	@Override
	public void close() throws IOException {

	}
}
