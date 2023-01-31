package tables;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.lang.StringBuilder; 

/**
 * Defines the protocols for a table
 * with a schema and a state.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public abstract class Table implements Iterable<List<Object>> {
	protected String tableName;
	protected List<String> columnNames;
	protected List<String> columnTypes;
	protected int primaryIndex;

	/**
	 * Sets the table name in the schema.
	 *
	 * @param tableName the table name.
	 */
	public void setTableName(String tableName) {
		if (!tableName.matches("[a-zA-Z0-9_]+"))
			throw new IllegalArgumentException("Table name <%s> must be valid".formatted(tableName));

		this.tableName = tableName;
	}

	/**
	 * Gets the table name from the schema.
	 *
	 * @return the table name.
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Sets an unmodifiable list of
	 * the column names in the schema.
	 *
	 * @param columnNames the column names.
	 */
	public void setColumnNames(List<String> columnNames) {
		if (columnNames.size() < 1 || columnNames.size() > 15)
			throw new IllegalArgumentException("Number of column types <%d> must be from 1 to 15".formatted(columnNames.size()));
		for (int i = 0; i < columnNames.size(); i++) {
			String name = columnNames.get(i);
			if (!name.matches("[a-zA-Z0-9_]+"))
				throw new IllegalArgumentException("Column name <%s> at index <%d> must be valid".formatted(name, i));
			if (columnNames.indexOf(name) != i)
				throw new IllegalArgumentException("Duplicate column name <%s>".formatted(name));
		}

		this.columnNames = List.copyOf(columnNames);
	}

	/**
	 * Gets an unmodifiable list of
	 * the column names from the schema.
	 *
	 * @return the column names.
	 */
	public List<String> getColumnNames() {
		return columnNames;
	}

	/**
	 * Sets an unmodifiable list of
	 * the column types in the schema.
	 *
	 * @param columnTypes the column types.
	 */
	public void setColumnTypes(List<String> columnTypes) {
		if (columnTypes.size() != columnNames.size())
			throw new IllegalArgumentException("Number of column types <%d> must match number of column names <%d>".formatted(columnTypes.size(), columnNames.size()));
		for (int i = 0; i < columnTypes.size(); i++) {
			String type = columnTypes.get(i);
			if (!type.equals("string") && !type.equals("integer") && !type.equals("boolean"))
				throw new IllegalArgumentException("Column type <%s> at index <%d> must be string, integer, or boolean".formatted(type, i));
		}

		this.columnTypes = List.copyOf(columnTypes);
	}

	/**
	 * Gets an unmodifiable list of
	 * the column types from the schema.
	 *
	 * @return the column types.
	 */
	public List<String> getColumnTypes() {
		return columnTypes;
	}

	/**
	 * Sets the primary index in the schema.
	 *
	 * @param primaryIndex the primary index.
	 */
	public void setPrimaryIndex(int primaryIndex) {
		if (primaryIndex < 0 || primaryIndex >= columnNames.size())
			throw new IllegalArgumentException("Primary index <%d> must be from 0 to %d".formatted(primaryIndex, columnNames.size() - 1));

		this.primaryIndex = primaryIndex;
	}

	/**
	 * Gets the primary index from the schema.
	 *
	 * @return the primary index.
	 */
	public int getPrimaryIndex() {
		return primaryIndex;
	}

	/**
	 * Removes all rows from the state.
	 */
	public abstract void clear();

	/**
	 * On a hit, updates the corresponding row in the state,
	 * then returns <code>true</code>.
	 * <p>
	 * On a miss, creates the given row in the state,
	 * then returns <code>false</code>.
	 *
	 * @param row a row.
	 * @return whether the operation was a hit.
	 *
	 * @throws IllegalArgumentException
	 * if the row violates the schema.
	 *
	 * @throws NullPointerException
	 * if the row contains a null key.
	 */
	public abstract boolean put(List<Object> row);

	/**
	 * Tries to {@link #put(List)} each row
	 * from the given iterable of rows.
	 *
	 * @param rows an iterable of rows.
	 */
	public void putAll(Iterable<List<Object>> rows) {
		for (List<Object> row: rows)
			put(row);
	}

	/**
	 * On a hit, removes the corresponding row
	 * from the state, then returns <code>true</code>.
	 * <p>
	 * On a miss, returns <code>false</code>.
	 *
	 * @param key a key.
	 * @return whether the operation was a hit.
	 */
	public abstract boolean remove(Object key);

	/**
	 * On a hit, returns the corresponding row
	 * from the state.
	 * <p>
	 * On a miss, returns <code>null</code>.
	 *
	 * @param key a key.
	 * @return whether the operation was a hit.
	 */
	public abstract List<Object> get(Object key);

	/**
	 * On a hit, returns <code>true</code>.
	 * <p>
	 * On a miss, returns <code>false</code>.
	 *
	 * @param key a key.
	 * @return whether the operation was a hit.
	 */
	public boolean contains(Object key) {
		return get(key) != null;
	}

	/**
	 * Returns the size of the table, which is
	 * the number of rows in the state.
	 *
	 * @return the size of the table.
	 */
	public abstract int size();

	/**
	 * Returns whether the {@link #size()} is zero.
	 *
	 * @return <code>true</code> if there are no rows
	 * 		or <code>false</code> if there are rows.
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Returns the capacity of the table, which is
	 * the length of the data/file structure or
	 * the maximum size before resizing it.
	 *
	 * @return the capacity of the table.
	 */
	public abstract int capacity();

	/**
	 * Returns the load factor of the table,
	 * which is the {@link #size()}
	 * divided by the {@link #capacity()}.
	 *
	 * @return the load factor.
	 */
	public double loadFactor() {
		return (double) size() / (double) capacity();
	}

	/**
	 * Returns a string representation of this table,
	 * including its schema and state.
	 *
	 * @return a string representation of this table.
	 */
	@Override
	public String toString() {
	// print header 
		
		Set<List<Object>> rows = rows(); 
		
		StringBuilder SB = new StringBuilder("\n"); 
		String horBar = "~".repeat(18*columnNames.size()+1) + "\n"; 
		// Print header
		 
		SB.append("   +" + "~".repeat(this.getTableName().length()+2) + "+\n"); 
		SB.append("   | %s |\n".formatted(this.getTableName()));
	
		// print columns headers
		SB.append(horBar);
		for (int i = 0; i < columnNames.size(); i++) {
			if (columnTypes.get(i).equals("integer")) {
				if (i == this.getPrimaryIndex()) {
					SB.append("| %15s ".formatted(columnNames.get(i) + "*")); 
				}else {
					SB.append("| %15s ".formatted(columnNames.get(i))); 
				}
			}else {
				if (i == this.getPrimaryIndex()) {
					SB.append("| %-15s ".formatted(columnNames.get(i) + "*")); 
				}else {
					SB.append("| %-15s ".formatted(columnNames.get(i))); 
				}
			}
		}
		SB.append("|" + "\n"); 
		SB.append(horBar);  
// -------------------------------------------------------------------------------------------------------------------------------------------
		// Print Body 
		for (var row : rows) {
			for (int i = 0; i < row.size(); i++) {
				if (row.get(i) == null) {
						SB.append("|" + " ".repeat(17)); 
				}else if (columnTypes.get(i).equals("integer")) {
						SB.append("| %15s ".formatted(row.get(i))); 
				}else if (columnTypes.get(i).equals("string")) {
					if (row.get(i).toString().length() > 13) {
						String str = row.get(i).toString(); 
						String subStr = str.substring(0, 11); 
						SB.append("| \"" + "%-10s".formatted(subStr) + "... "); 
					}else {
						SB.append("| \"" + "%-15s".formatted(row.get(i) + "\" ")); 
					}
				}else {
						SB.append("| %-15s ".formatted(row.get(i))); 
				}
			}
			SB.append("|\n"); 
		}
		SB.append(horBar);  
		
		return SB.toString(); 
	}

	/**
	 * Returns whether the given object is also a table
	 * and has the same fingerprint as this table.
	 * <p>
	 * A <code>true</code> result indicates with near
	 * certainty that the given object is equal.
	 *
	 * @param an object.
	 * @return whether the given object equals this table.
	 */
	@Override
	public boolean equals(Object o) {
		if (o instanceof Table t)
			return this.hashCode() == t.hashCode();
		else return false;
	}

	/**
	 * Returns the fingerprint of this table,
	 * which is the sum of the hash codes of
	 * each property value in the schema and
	 * each field value in each row in the state.
	 *
	 * @return this table's fingerprint.
	 */
	@Override
	public int hashCode() {
		int sum = tableName.hashCode()
			+ Integer.valueOf(primaryIndex).hashCode();

		for (String name: columnNames)
			sum += name.hashCode();

		for (String type: columnTypes)
			sum += type.hashCode();

		for (List<Object> row: rows())
			for (Object field: row)
				sum += field != null ? field.hashCode() : 0;

		return sum;
	}

	/**
	 * Returns an iterator over each row in the state.
	 * <p>
	 * This method is an alias of {@link #rows()}.
	 *
	 * @return an iterator of rows.
	 */
	@Override
	public abstract Iterator<List<Object>> iterator();

	/**
	 * Returns an unmodifiable set of
	 * the rows in the state.
	 *
	 * @return the set of rows.
	 */
	public Set<List<Object>> rows() {
		Set<List<Object>> rows = new HashSet<>();
		for (List<Object> row: this)
			rows.add(row);
		return Set.copyOf(rows);
	}

	/**
	 * Returns an unmodifiable set of
	 * the keys of the rows in the state.
	 *
	 * @return the set of keys.
	 */
	public Set<Object> keys() {
		Set<Object> keys = new HashSet<>();
		for (List<Object> row: this)
			keys.add(row.get(getPrimaryIndex()));
		return Set.copyOf(keys);
	}
}