package tables;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List; 

/**
 * Implements a hash-based table
 * using a random access file structure.
 */
public class HashFileTable extends Table {
	private Path path; 
	private FileChannel channel; 
	private MappedByteBuffer header, records; 

	private static final short TOMBSTONE = (short) 0xFFFF;
	
	private String tableName; 
	private int size; 
	private int contaminations; 
	private int record_width; 
	private int capacity; 
	private List<String> columnNames; 
	private List<String> columnTypes; 
	private Integer primaryIndex; 
	
	public HashFileTable(String tableName) {
		this.tableName = tableName; 
		this.path = Paths.get("data", "Persistent", "%s.bin".formatted(tableName)); 
		
		open();
		
		reopenTable(); 
		
		setTableName(tableName);
		setColumnNames(this.columnNames);
		setColumnTypes(this.columnTypes);
		setPrimaryIndex(this.primaryIndex);
	}
	
	public HashFileTable(String tableName, List<String> columnNames, List<String> columnTypes, int primaryIndex) {
		this.tableName = tableName; 
		this.path = Paths.get("data", "Persistent", "%s.bin".formatted(tableName)); 
		
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);
		
		open(); 
		
		createTable(tableName, columnNames, columnTypes, primaryIndex); 
	}
	
	private final Charset
	STRING_ENCODING = StandardCharsets.UTF_8;

	private final int
		MAX_COLUMNS = 15,
		MAX_COL_NAME = 15, 
		MAX_NAME = 127;
		
	private final int
		LENGTH_BYTES = 1,
		CHAR_BYTES = 1,
		NAME_BYTES = LENGTH_BYTES + CHAR_BYTES * MAX_NAME,
		COL_BYTES = LENGTH_BYTES + CHAR_BYTES * MAX_COL_NAME, 
		TYPE_BYTES = 1,
		INTEGER_BYTES = 4,
		SHORT_BYTES = 2,
		BOOLEAN_BYTES = 1,
		STRING_BYTES = NAME_BYTES, 
		MASK_BYTES = SHORT_BYTES;
		
	private final int
		COLUMN_WIDTH = COL_BYTES + TYPE_BYTES,
		HEADER_WIDTH = NAME_BYTES + INTEGER_BYTES * 5 + COLUMN_WIDTH * MAX_COLUMNS;
	
	private void open() {
		try {
			Files.createDirectories(path.getParent());
			channel = FileChannel.open(path, CREATE, READ, WRITE);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void reopenTable() {
		System.out.printf("Reopen tableName: %s\n\n", tableName);

		bufferHeader();

		readHeaderSchema();

		System.out.printf("Read columnCount:   %s\n", columnNames.size());
		System.out.printf("Read primaryIndex:  %s\n", primaryIndex);
		System.out.printf("Read columnNames:   %s\n", columnNames);
		System.out.printf("Read columnTypes:   %s\n\n", columnTypes);
		

		measureRecord();
		bufferRecords();
	}
	
	private void createTable(String tableName, List<String> columnNames, List<String> columnTypes, int primaryIndex) {
		System.out.printf("Create tableName: %s\n\n", tableName);

		bufferHeader(); 
		
		this.capacity = 911; 
		
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.primaryIndex = primaryIndex;

		writeHeaderSchema();
		measureRecord();
		bufferRecords();

		System.out.printf("Write columnCount:  %s\n", columnNames.size());
		System.out.printf("Write primaryIndex: %s\n", primaryIndex);
		System.out.printf("Write columnNames:  %s\n", columnNames);
		System.out.printf("Write columnTypes:  %s\n\n", columnTypes);
	}
	
	private void bufferRecords() {
		try {
			records = channel.map(READ_WRITE, HEADER_WIDTH, capacity * record_width);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void measureRecord() {
		record_width = 0 ; 
		record_width += MASK_BYTES; 
		
		for (int i = 0 ; i < columnTypes.size(); i ++) {
			if (columnTypes.get(i).equalsIgnoreCase("string")) {
				record_width += NAME_BYTES;
			} else if (columnTypes.get(i).equalsIgnoreCase("integer")) {
				record_width += 4;
			} else if (columnTypes.get(i).equalsIgnoreCase("boolean")) {
				record_width += 1;
			}
		}
	}
	
	private void bufferHeader() {
		try {
			header = channel.map(READ_WRITE, 0, HEADER_WIDTH);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void writeHeaderSchema() {
		header.position(0);
	
		writeHeaderStr(tableName); 
		header.putInt(this.primaryIndex);
		header.putInt(this.capacity);
		header.putInt(this.size); 
		header.putInt(this.contaminations); 

		int columnCount = columnNames.size();
		header.putInt(columnCount);

	    for (int i = 0; i < columnCount; i++) {
	    	writeHeaderStr(columnNames.get(i));

	    	String type = columnTypes.get(i);
	    	header.put(switch (type) {
		    	case "string" -> (byte) 1;
		    	case "integer" -> (byte) 2;
		    	case "boolean" -> (byte) 3;
		    	default -> throw new IllegalArgumentException();
	    	});
	    }
	}

	public void readHeaderSchema() {
		header.position(0);
		
		tableName = readHeaderStr(); 
		this.primaryIndex = header.getInt(); 
		this.capacity = header.getInt(); 
		this.size = header.getInt(); 
		this.contaminations = header.getInt(); 

		int columnCount = header.getInt();

		this.columnNames = new LinkedList<>();
		this.columnTypes = new LinkedList<>();
		for (int i = 0; i < columnCount; i++) {
			columnNames.add(readHeaderStr());

			columnTypes.add(switch (header.get()) {
		    	case 1 -> "string";
		    	case 2 -> "integer";
		    	case 3 -> "boolean";
		    	default -> throw new RuntimeException();
	    	});
	    }
	}
	
	public void writeHeaderStr(String str) {
		final byte[] chars = str.getBytes(STRING_ENCODING);
		header.put((byte) chars.length);
		header.put(chars);
		header.position(header.position() + MAX_COL_NAME - chars.length);
	}
	
	public String readHeaderStr() {
		final byte[] chars = new byte[header.get()];
		header.get(chars);
		header.position(header.position() + MAX_COL_NAME - chars.length);
		final String str = new String(chars, STRING_ENCODING);

		return str;
	}

	@Override
	public void clear() {
		for(int i = 0 ; i < capacity() ; i++) {
			 writeNull(i); 
		}
		size = 0; 					// keeps track of the current size of the table. 
		contaminations = 0 ; 
	}

	@Override
	public boolean put(List<Object> row) {
		Object key = (Object) row.get(this.getPrimaryIndex()); 
		int RIndex = -1; 
		int index = hash(key); 
		int initIndex = index; 
		for (int i = 0 ; i < this.capacity() ; i++) {
			
			if (isTombstone(index) && RIndex == -1) {
				RIndex = index;
			}
			
			if(isNull(index)) {// if the table location is null, then the value is input and the size is incremented. 
				if (RIndex == -1) {
					write(index, row); 
					size++; 
				}else {
					write(RIndex, row); 
					size++; 
					contaminations--; 
				}

				updateVals(); 
				return false; 
			}
			
			if (isNull(index) && !isTombstone(index) && (((List<Object>) read(index)).get(this.getPrimaryIndex()).equals(row.get(this.getPrimaryIndex())))) {
				if(RIndex == -1) {
					write(index, row); 
				}else {
					write(RIndex, row); 
					writeTombstone(index); 
				}

				updateVals(); 
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
		updateVals(); 
		return false;
	}
	
	public void updateVals() {
		header.position(0);
		
		writeHeaderStr(this.tableName); 
		header.putInt(this.primaryIndex);
		header.putInt(this.capacity);
		header.putInt(this.size); 
		header.putInt(this.contaminations); 
		return; 
	}
	
	@Override
	public boolean remove(Object key) {
		int index = hash(key); 
		int initIndex = index; 
		for (int i = 0 ; i < this.capacity() - 1 ; i++) {
			
			if(isNull(index)) {
				return false; 
			}
			else if (isNull(index) && !isTombstone(index) && ((List<Object>) read(index)).get(this.getPrimaryIndex()).equals(key)) {
				writeTombstone(index); 
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
//
//	@SuppressWarnings("unchecked")
//	@Override
//	public boolean remove(Object key) {
//		int index = hash(key); 
//		int initIndex = index; 
//		for (int i = 0 ; i < this.capacity() - 1 ; i++) {
//			
//			if(table[index] == null) {
//				return false; 
//			}
//			else if (table[index] != null && table[index] != TOMBSTONE && ((List<Object>) table[index]).get(this.getPrimaryIndex()).equals(key)) {
//				table[index] = TOMBSTONE; 
//				size--; 
//				contaminations++; 
//				return true; 
//			}
//						
//			if (i%2 == 1) {
//				index = initIndex + (int) Math.pow((i + 1), 2); 
//			}else {
//				index = initIndex + (int) Math.pow((i + 1), 2); 
//				index = index * -1; 
//			}
//			index = Math.floorMod(index, this.capacity()); 
//		}
//		return false; 
//	}
//@Override
	public List<Object> get(Object key) {
		int index = hash(key); 
		int initIndex = index; 
		for (int i = 0 ; i < this.capacity() - 1 ; i++) {
					
				if (isNull(index)){
					return null; 
				}else if (isNull(index) && !isTombstone(index) && ((List<Object>) read(index)).get(this.getPrimaryIndex()).equals(key)) { 
						return (List<Object>) read(index); 
				}
										
				if (i%2 == 1) {
					index = initIndex + (int) Math.pow((i + 1), 2); 
				}else {
					index = initIndex + (int) Math.pow((i + 1), 2); 
					index = index * -1; 
				}
			index = Math.floorMod(index, this.capacity());
			}
		return null;
	}
	
	
	@Override
	public Iterator<List<Object>> iterator() {
		return new Iterator<>() {
			int index = 0; //starts at 0

			@Override
			public boolean hasNext() {				
				
				// Iterate until value is found
				while (index < capacity()){
					if(!isNull(index) && !isTombstone(index)) { // if not null or tombstone, return true
						return true; 
					}
					index++; 
				}
				return false; 
			}

			@Override
			public List<Object> next() {
				if (!hasNext()) return null; 
				
				List<Object> temp = read(index); 
				index++;
				return temp; // returns the row
			}	
		};
	}
//
//	public int validRow(int index) {
//		int val = index; 
//		while((table[val] == null || table[val] == TOMBSTONE) && val < this.capacity()) {
//			val = val + 1; 
//		}
//		return val; 
//	}
//	
	@SuppressWarnings("rawtypes")
	public int hash(Object key) { 
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

@Override
public int size() {
	return this.size; 
}

@Override
public int capacity() {
	return this.capacity; 
}

//	
//	@SuppressWarnings("unchecked")
//	public void rehash() {
//		int arraySize = primeFinder(this.capacity() * 2); 
//		Object[] oldTable = table; 
//		Object[] newTable = new Object[arraySize];
//		table = newTable; 
//		size = 0; 
//		contaminations = 0; 
//		for (int i = 0 ;  i < oldTable.length ; i++) {
//			if ((List<Object>) oldTable[i] != null && (List<Object>) oldTable[i] != TOMBSTONE && ((List<Object>) oldTable[i]).get(this.getPrimaryIndex()) != null && ((List<Object>) oldTable[i]).get(this.getPrimaryIndex()) != TOMBSTONE){ 
//			put((List<Object>) oldTable[i]); 
//			}
//		}
//		return; 
//	}
//	
//	@SuppressWarnings("unchecked")
//	public int colRes(int hash) {
//		int newIndex = hash; 
//		int counter = 0; 
//		while((List<Object>) table[newIndex] != null || (List<Object>)  table[newIndex] != TOMBSTONE ) {
//			newIndex = Math.floorMod(((int) Math.pow((hash + counter + 1), 2) * -1), this.capacity()); 
//			counter++; 
//		}
//		return newIndex; 
//	}
//	
//	public static int primeFinder(int initialInt) {
//		int val = initialInt + 1 ;
//		if(val % 2 != 0 && val % 4 == 3 && val % 3 != 0 && val % 4 != 0 && val % 5 != 0) {
//			return val; 
//		}else {
//			val = primeFinder(val); 
//		}
//		return val; 
//	}
//}

	@SuppressWarnings("null")
	public void write(int index, List<Object> row) {
		MappedByteBuffer record = records.slice(index * record_width, record_width);
	
		
		short mask = 0;
		for (int i = 0; i < row.size(); i++) {
			if (row.get(i) != null) {
			mask = (short) (mask | (1 << i));
			}
		}
		record.putShort(mask);
			
		for(int i = 0 ; i < columnTypes.size(); i++) {
			if(columnTypes.get(i).equalsIgnoreCase("string")) {
				try { 
					String letter = (String) row.get(0);
					byte[] chars = letter.getBytes(STRING_ENCODING);
					record.put((byte) chars.length);
					record.put(chars);
					record.put(new byte[STRING_BYTES - chars.length - LENGTH_BYTES]);
				} catch (Exception e) {
					record.put(new byte[STRING_BYTES]);  
				}
			}else if (columnTypes.get(i).equalsIgnoreCase("integer")) {
				try {
					int order = (int) row.get(1);
					record.putInt(order);
				}catch (Exception e) {
					record.put(new byte[4]);  
				}
			}else if (columnTypes.get(i).equalsIgnoreCase("integer")) {	
				try {
					boolean vowel = (boolean) row.get(2);
					record.put(vowel ? (byte) 1 : 0);
				}catch(Exception e) {
					record.put(new byte[1]);  
				}
			}
		}
		return; 
	}
	
	public void writeNull(int index) {
		records.position(index * record_width);
		records.putShort((short) 0);
		return; 
	}
	
	public void writeTombstone(int index) {
		records.position(index * record_width);
		records.putShort(TOMBSTONE);
		return; 
	}
	
	public List<Object> read(int index) {
		MappedByteBuffer record = records.slice(index * record_width, record_width);
	
		short mask = record.getShort();
		if (mask == 0)
			throw new IllegalStateException();
	
		byte[] chars = new byte[record.get()];
		record.get(chars);
		record.position(record.position() + STRING_BYTES - chars.length - LENGTH_BYTES);
		String letter = new String(chars, STRING_ENCODING);
	
		int order = record.getInt();
	
		boolean vowel = record.get() == 1;
	
		List<Object> row = List.of(letter, order, vowel);
	
		return row;
	}
	
	public boolean isNull(int index) {

		// Putting the position
		records.position(index * record_width);

		// Getting the mask
		short mask = records.getShort();

		return mask == 0;
	}

	public boolean isTombstone(int index) {

		// Putting the position
		records.position(index * record_width);

		// Getting the mask
		short mask = records.getShort();

		return mask == TOMBSTONE;
	}
	
//	public boolean isNull(int index) {
//		MappedByteBuffer record = records.slice(index * record_width, record_width);
//		short mask = record.getShort(); 
//		if (mask == 0) {
//			return true; 
//		}
//		return false; 
//	}
//	
//	public boolean isTombstone(int index) {
//		MappedByteBuffer record = records.slice(index * record_width, record_width);
//		short mask = record.getShort(); 
//		if (mask == -1) {
//			return true; 
//		}
//		return false; 
//	}
}

