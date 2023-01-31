package tables;

import java.util.Iterator;
import java.util.List;
import java.io.*; 

/**
 * Implements a hash-based table
 * using an array data structure.
 */
public class HashArrayTable extends Table {
	Object[] table; 
	private int size;
	@SuppressWarnings("unused")
	private int contaminations;
	private static final List<Object> TOMBSTONE = List.of(); 

	/* Creates a table and initializes
	 * the data structure.
	 *
	 * @param tableName the table name
	 * @param columnNames the column names
	 * @param columnTypes the column types
	 * @param primaryIndex the primary index
	 */
	public HashArrayTable(String tableName, List<String> columnNames, List<String> columnTypes, int primaryIndex) {
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);
		
		clear(); 
	}

	@Override
	public void clear() {
		table = new Object[19]; 	// Instantiate new table 
		size = 0; 					// keeps track of the current size of the table. 
		contaminations = 0 ; 
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean put(List<Object> row) {
		Object key = (Object) row.get(this.getPrimaryIndex()); 
		int RIndex = -1; 
		int index = hash(key); 
		int initIndex = index; 
		for (int i = 0 ; i < this.capacity() ; i++) {
			
			if (table[index] == TOMBSTONE && RIndex == -1) {
				RIndex = index;
			}
			
			if(table[index] == null) {// if the table location is null, then the value is input and the size is incremented. 
				if (RIndex == -1) {
					table[index] = row; 
					size++; 
				}else {
					table[RIndex] = row;
					size++; 
					contaminations--; 
				}
				if (size >= this.capacity() * .8) {
					rehash(); 
				}
				return false; 
			}
			
			if (table[index] != null && table[index] != TOMBSTONE && (((List<Object>) table[index]).get(this.getPrimaryIndex()).equals(row.get(this.getPrimaryIndex())))) {
				if(RIndex == -1) {
					table[index] = row; 
				}else {
					table[RIndex] = row; 
					table[index] = TOMBSTONE;
				}
				return true;
			}
		
			if (i%2 == 1) {
				index = initIndex + (int) Math.pow((i + 1), 2); 
			}else {
				index = initIndex + (int) Math.pow((i + 1), 2); 
				index = index * -1; 
			}
			index = Math.floorMod(index, this.capacity()); 		
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object key) {
		int index = hash(key); 
		int initIndex = index; 
		for (int i = 0 ; i < this.capacity() - 1 ; i++) {
			
			if(table[index] == null) {
				return false; 
			}
			else if (table[index] != null && table[index] != TOMBSTONE && ((List<Object>) table[index]).get(this.getPrimaryIndex()).equals(key)) {
				table[index] = TOMBSTONE; 
				size--; 
				contaminations++; 
				return true; 
			}
						
			if (i%2 == 1) {
				index = initIndex + (int) Math.pow((i + 1), 2); 
			}else {
				index = initIndex + (int) Math.pow((i + 1), 2); 
				index = index * -1; 
			}
			index = Math.floorMod(index, this.capacity()); 
		}
		return false; 
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Object> get(Object key) {
		int index = hash(key); 
		int initIndex = index; 
		for (int i = 0 ; i < this.capacity() - 1 ; i++) {
					
			if (table[index] == null){
				return null; 
			}else if (table[index] != null && table[index] != TOMBSTONE && ((List<Object>) table[index]).get(this.getPrimaryIndex()).equals(key)) { 
					return (List<Object>) table[index]; 
			}
									
			if (i%2 == 1) {
				index = initIndex + (int) Math.pow((i + 1), 2); 
			}else {
				index = initIndex + (int) Math.pow((i + 1), 2); 
				index = index * -1; 
			}
			index = Math.floorMod(index, this.capacity()); 		}
		return null; 
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public int capacity() {
		return table.length;
	}

	@Override
	public Iterator<List<Object>> iterator() {
		return new Iterator<>() {
			int index = 0; //starts at 0

			@Override
			public boolean hasNext() {				
				
				// Iterate until value is found
				while (index < capacity()){
					if(table[index] != null && table[index] != TOMBSTONE) { // if not null or tombstone, return true
						return true; 
					}
					index++; 
				}
				return false; 
			}

			@SuppressWarnings("unchecked")
			@Override
			public List<Object> next() {
				if (!hasNext()) return null; 
				
				List<Object> temp = (List<Object>) table[index]; 
				index++;
				return temp; // returns the row
			}	
		};
	}

	public int validRow(int index) {
		int val = index; 
		while((table[val] == null || table[val] == TOMBSTONE) && val < this.capacity()) {
			val = val + 1; 
		}
		return val; 
	}
	
	@SuppressWarnings("rawtypes")
	public int hash(Object key) { //
		int hash = 0; 
		Class keyType = key.getClass(); 
		if (keyType.getName() == "java.lang.String") { // Determines if the key is a String based on the java naming convention. 
			String strKey = (String) key; // cast to a string. 
			for (int i = 0; i < strKey.length(); i++) { // run a primitive hash function upon key string. 
				hash = hash + strKey.charAt(i) * 67; //This hash takes each character from the string, squares it to its current position in the string, then multiplies it by 67
			}
			return Math.floorMod(hash, this.capacity()); // Returns the hash modulo array capacity. 
		}else {
			return Math.floorMod(key.hashCode(), this.capacity()); // Returns the hashCode, modulo the arrays current capacity. 
		}
	}
	
	@SuppressWarnings("unchecked")
	public void rehash() {
		int arraySize = primeFinder(this.capacity() * 2); 
		Object[] oldTable = table; 
		Object[] newTable = new Object[arraySize];
		table = newTable; 
		size = 0; 
		contaminations = 0; 
		for (int i = 0 ;  i < oldTable.length ; i++) {
			if ((List<Object>) oldTable[i] != null && (List<Object>) oldTable[i] != TOMBSTONE && ((List<Object>) oldTable[i]).get(this.getPrimaryIndex()) != null && ((List<Object>) oldTable[i]).get(this.getPrimaryIndex()) != TOMBSTONE){ 
			put((List<Object>) oldTable[i]); 
			}
		}
		return; 
	}
	
	@SuppressWarnings("unchecked")
	public int colRes(int hash) {
		int newIndex = hash; 
		int counter = 0; 
		while((List<Object>) table[newIndex] != null || (List<Object>)  table[newIndex] != TOMBSTONE ) {
			newIndex = Math.floorMod(((int) Math.pow((hash + counter + 1), 2) * -1), this.capacity()); 
			counter++; 
		}
		return newIndex; 
	}
	
	public static int primeFinder(int initialInt) {
		int val = initialInt + 1 ;
		if(val % 2 != 0 && val % 4 == 3 && val % 3 != 0 && val % 4 != 0 && val % 5 != 0) {
			return val; 
		}else {
			val = primeFinder(val); 
		}
		return val; 
	}
}
