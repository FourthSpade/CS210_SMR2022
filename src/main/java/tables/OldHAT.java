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
public class OldHAT extends Table {
	private Path path; 
	private FileChannel channel; 
	private MappedByteBuffer header, records; 
	
	private String tableName; 
	private int size; 
	private int contaminations; 
	private int record_width; 
	private int capacity; 
	private List<String> columnNames; 
	private List<String> columnTypes; 
	private Integer primaryIndex; 
	
	public OldHAT(String tableName, List<String> columnNames, List<String> columnTypes, int primaryIndex) {
		this.tableName = tableName; 
		this.path = Paths.get("data", "Persistent", "%s.bin".formatted(tableName)); 
		
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);
		
		open(); 
		
		createTable(tableName, columnNames, columnTypes, primaryIndex); 	
				
	}

	/**
	 * Reopens a table from an
	 * existing file structure.
	 *
	 * @param tableName a table name
	 */
	public OldHAT(String tableName) {
		this.tableName = tableName; 
		this.path = Paths.get("data", "Persistent", "%s.bin".formatted(tableName)); 
		
		open();
		
		reopenTable(); 
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
	
	private void reopenTable() {
		System.out.printf("Reopen tableName: %s\n\n", tableName);

		bufferHeader();

		readHeaderSchema();

		System.out.printf("Read columnCount:   %s\n", columnNames.size());
		System.out.printf("Read primaryIndex:  %s\n", primaryIndex);
		System.out.printf("Read columnNames:   %s\n", columnNames);
		System.out.printf("Read columnTypes:   %s\n\n", columnTypes);
	}
	
	private final Charset
		STRING_ENCODING = StandardCharsets.UTF_8;

	private final int
		MAX_COLUMNS = 15,
		MAX_NAME = 15;
	
	private final int
		LENGTH_BYTES = 1,
		CHAR_BYTES = 1,
		NAME_BYTES = LENGTH_BYTES + CHAR_BYTES * MAX_NAME,
		TYPE_BYTES = 1,
		INTEGER_BYTES = 4,
		SHORT_BYTES = 2,
		BOOLEAN_BYTES = 1,
		STRING_BYTES = NAME_BYTES, 
		MASK_BYTES = SHORT_BYTES;

	private final int
		COLUMN_WIDTH = NAME_BYTES + TYPE_BYTES,
		HEADER_WIDTH = INTEGER_BYTES * 2 + COLUMN_WIDTH * MAX_COLUMNS;
	
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
				return false; 
			}
			
			if (isNull(index) && !isTombstone(index) && (((List<Object>) read(index)).get(this.getPrimaryIndex()).equals(row.get(this.getPrimaryIndex())))) {
				if(RIndex == -1) {
					write(index, row); 
				}else {
					write(RIndex, row); 
					writeTombstone(index); 
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
	
	public void write(int index, List<Object> row) {
		MappedByteBuffer record = records.slice(index * record_width, record_width);

		record.putShort((short) 7);

    	String letter = (String) row.get(0);
    	byte[] chars = letter.getBytes(STRING_ENCODING);
    	record.put((byte) chars.length);
    	record.put(chars);
    	record.put(new byte[STRING_BYTES - chars.length - LENGTH_BYTES]);

    	int order = (int) row.get(1);
    	record.putInt(order);

    	boolean vowel = (boolean) row.get(2);
    	record.put(vowel ? (byte) 1 : 0);
	}

	public void writeNull(int index) {
		records.position(index * record_width);
		records.putShort((short) 0);
		records.put(new byte[record_width - MASK_BYTES]);
	}
	
	public void writeTombstone(int index) {
		records.position(index * record_width);
		records.putShort((short) -1);
		records.put(new byte[record_width - MASK_BYTES]);
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
		MappedByteBuffer record = records.slice(index * record_width, record_width);
		short mask = record.getShort(); 
		if (mask == 0) {
			return true; 
		}
		return false; 
	}
	
	public boolean isTombstone(int index) {
		MappedByteBuffer record = records.slice(index * record_width, record_width);
		short mask = record.getShort(); 
		if (mask == -1) {
			return true; 
		}
		return false; 
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

	@Override
	public List<Object> get(Object key) {
		int index = hash(key); 
		int initIndex = index; 
		for (int i = 0 ; i < this.capacity() - 1 ; i++) {
					
			if (isNull(index)){
				return null; 
			}else if (!isNull(index) && !isTombstone(index) && ((List<Object>) read(index)).get(this.getPrimaryIndex()).equals(key)) { 
					return (List<Object>) read(index); 
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
		return capacity;
	}
	
	private void bufferHeader() {
		try {
			header = channel.map(READ_WRITE, 0, HEADER_WIDTH);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
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
		record_width = MASK_BYTES + NAME_BYTES + INTEGER_BYTES + BOOLEAN_BYTES;
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
	
	private void open() {
		try {
			Files.createDirectories(path.getParent());
			channel = FileChannel.open(path, CREATE, READ, WRITE);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
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
	
	public static int primeFinder(int initialInt) {
		int val = initialInt + 1 ;
		if(val % 2 != 0 && val % 4 == 3 && val % 3 != 0 && val % 4 != 0 && val % 5 != 0) {
			return val; 
		}else {
			val = primeFinder(val); 
		}
		return val; 
	}
	
	public void readHeaderSchema() {
		header.position(0);

		int columnCount = header.getInt();
		primaryIndex = header.getInt();

		columnNames = new LinkedList<>();
		columnTypes = new LinkedList<>();
		for (int i = 0; i < columnCount; i++) {
			byte[] chars = new byte[header.get()];
			header.get(chars);
			header.position(header.position() + NAME_BYTES - chars.length - LENGTH_BYTES);
			columnNames.add(new String(chars, STRING_ENCODING));

			columnTypes.add(switch (header.get()) {
		    	case 1 -> "string";
		    	case 2 -> "integer";
		    	case 3 -> "boolean";
		    	default -> throw new RuntimeException();
	    	});
	    }
	}
	
	public void writeHeaderSchema() {
		header.position(0);

		int columnCount = columnNames.size();
		header.putInt(columnCount);
		header.putInt(primaryIndex);

	    for (int i = 0; i < columnCount; i++) {
	    	String name = columnNames.get(i);
	    	byte[] chars = name.getBytes(STRING_ENCODING);
	    	header.put((byte) chars.length);
	    	header.put(chars);
	    	header.put(new byte[NAME_BYTES - chars.length - LENGTH_BYTES]);

	    	String type = columnTypes.get(i);
	    	header.put(switch (type) {
		    	case "string" -> (byte) 1;
		    	case "integer" -> (byte) 2;
		    	case "boolean" -> (byte) 3;
		    	default -> throw new IllegalArgumentException();
	    	});
	    }
	}
}
