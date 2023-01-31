package drivers;

import apps.Database;

/**
 * Defines the protocols for a driver.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public interface Driver {
	/**
	 * Executes the given query against the given database
	 * and returns the result of the query.
	 *
	 * @param query the query to execute.
	 * @param db the database to execute against.
	 * @return the result of the query.
	 *
	 * @throws SQLError if the query fails.
	 **/
	Object execute(String query, Database db) throws SQLError;
}