package tables;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Implements a search-based table
 * using a list data structure.
 */
public class SearchTable extends Table {
	private List<List<Object>> list;

	/**
	 * Creates a table and initializes
	 * the data structure.
	 *
	 * @param tableName the table name
	 * @param columnNames the column names
	 * @param columnTypes the column types
	 * @param primaryIndex the primary index
	 */
	public SearchTable(String tableName, List<String> columnNames, List<String> columnTypes, int primaryIndex) {
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);

		list = new LinkedList<>();
	}

	
	private class RowKeyComparator implements Comparator<Object> {

		@Override
		public int compare(Object o1, Object o2) {
			return keyOf(o1).compareTo(keyOf(o2)); 
		}
		
		@SuppressWarnings("unchecked")
		private Comparable<Object> keyOf(Object obj){
			if (obj instanceof List<?> row) {
				return (Comparable<Object>) row.get(getPrimaryIndex()); 
			}
			else {
				return (Comparable<Object>) obj; 
			}
		}
	}

	@Override
	public void clear() {
		list.clear();
	}

	@Override
	public boolean put(List<Object> row) {
		int index = Collections.binarySearch(list, row, new RowKeyComparator()); 
		
		if(index >= 0) {
			list.set(index, row);
			return true;
		}
		else {
			list.add(-index - 1, row);
			return false;
		}
//		Object key = row.get(getPrimaryIndex());
//
//		if (key == null)
//			throw new NullPointerException("Key field %d must not be null".formatted(getPrimaryIndex()));
//
//		for (var i = 0; i < row.size(); i++) {
//			var field = row.get(i);
//			var type = getColumnTypes().get(i);
//
//			if (field == null) {
//				continue;
//			}
//			else if (type.equals("string")) {
//				if (!(field instanceof String))
//					throw new IllegalArgumentException("Field <%d> must be a string".formatted(i));
//			}
//			else if (type.equals("integer")) {
//				if (!(field instanceof Integer))
//					throw new IllegalArgumentException("Field <%d> must be an integer".formatted(i));
//			}
//			else if (type.equals("boolean")) {
//				if (!(field instanceof Boolean))
//					throw new IllegalArgumentException("Field <%d> must be a boolean".formatted(i));
//			}
//		}
//
//		for (int i = 0; i < list.size(); i++) {
//			List<Object> old = list.get(i);
//			if (old.get(getPrimaryIndex()).equals(key)) {
//				list.set(i, row);
//				return true;
//			}
//		}
//		list.add(row);
//		return false;
	}

	@Override
	public boolean remove(Object key) {
		int index = Collections.binarySearch(list, key, new RowKeyComparator()); 
		
		if(index >= 0) {
			list.remove(index);
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public List<Object> get(Object key) {
		int index = Collections.binarySearch(list, key, new RowKeyComparator()); 
		
		if(index >= 0) {
			return list.get(index);
		}
		else {
			return null;
		}
	
//		for (int i = 0; i < list.size(); i++) {
//			List<Object> row = list.get(i);
//			if (row.get(getPrimaryIndex()).equals(key))
//				return row;
//		}
//		return null;
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public int capacity() {
		return size();
	}
	
	public static int hash(String key) { //
		if (key instanceof String) {
			float hash = 0;  
			for (int i = 0; i < key.length(); i++) { // run a primitive hash function upon key string. 
				hash = hash + key.charAt(i) * 67; //This hash takes each character from the string, squares it to its current position in the string, then multiplies it by 67
			}
			return (int)hash;
		}else {
			return key.hashCode(); 
		}
	}

	@Override
	public Iterator<List<Object>> iterator() {
		return new Iterator<>() {
			int index = 0;

			@Override
			public boolean hasNext() {
				return index < list.size();
			}

			@Override
			public List<Object> next() {
				return list.get(index++);
			}
		};
	}
}