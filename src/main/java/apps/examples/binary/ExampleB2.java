package apps.examples.binary;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Demonstrates de/serialization in binary
 * for the state of a table, assuming:
 * <p>
 * Each record encodes a row or null,
 * disregarding tombstones.
 * <p>
 * Rows only contain non-null fields.
 */
public class ExampleB2 {
	private static Path path;
	private static FileChannel channel;
	private static MappedByteBuffer header, records;

	private static String tableName;

	private static int size, capacity, record_width;

	private static final int FIXED_CAPACITY = 10;

	public static void main(String[] args) {
		tableName = "example_b2";
		path = Paths.get("data", "persistent", "%s.bin".formatted(tableName));

		open();

		createDemo();

		reopenDemo();
	}

	private static void createDemo() {
		System.out.printf("Create tableName: %s\n\n", tableName);

		bufferHeader();

		capacity = FIXED_CAPACITY;
		size = 0;
		writeHeaderDimensions();
		System.out.printf("Write capacity: %s\n", capacity);
		System.out.printf("Write size:     %s\n\n", size);

		measureRecord();
		bufferRecords();

		List<Object> row;
		row = List.of("alpha", 1, true); 	write(0, row); 	System.out.printf("Write %d: %s\n", 0, row);
		row = List.of("beta", 2, false); 	write(1, row); 	System.out.printf("Write %d: %s\n", 1, row);
		row = null;							writeNull(2); 	System.out.printf("Write %d: %s\n", 2, "Null");
		row = List.of("gamma", 3, false); 	write(3, row); 	System.out.printf("Write %d: %s\n", 3, row);
		row = null;							writeNull(4); 	System.out.printf("Write %d: %s\n", 4, "Null");
		row = List.of("delta", 4, false); 	write(5, row); 	System.out.printf("Write %d: %s\n", 5, row);
		row = null;							writeNull(6); 	System.out.printf("Write %d: %s\n", 6, "Null");
		row = List.of("tau", 19, false); 	write(7, row); 	System.out.printf("Write %d: %s\n", 7, row);
		row = List.of("pi", 16, false); 	write(8, row); 	System.out.printf("Write %d: %s\n", 8, row);
		row = List.of("omega", 24, true); 	write(9, row); 	System.out.printf("Write %d: %s\n\n", 9, row);

		size = 7;
		writeHeaderDimensions();
		System.out.printf("Write size:     %s\n\n", size);
	}

	private static void reopenDemo() {
		System.out.printf("Reopen tableName: %s\n\n", tableName);

		bufferHeader();

		capacity = -1;
		size = -1;
		readHeaderDimensions();
		System.out.printf("Read capacity:  %s\n", capacity);
		System.out.printf("Read size:      %s\n\n", size);

		measureRecord();
		bufferRecords();

		System.out.printf("Read %d:  %s\n", 0, isRow(0) ? read(0) : "Non-Row");
		System.out.printf("Read %d:  %s\n", 1, isRow(1) ? read(1) : "Non-Row");
		System.out.printf("Read %d:  %s\n", 2, isNull(2) ? "Null" : "Non-Null");
		System.out.printf("Read %d:  %s\n", 3, isRow(3) ? read(3) : "Non-Row");
		System.out.printf("Read %d:  %s\n", 4, isNull(4) ? "Null" : "Non-Null");
		System.out.printf("Read %d:  %s\n", 5, isRow(5) ? read(5) : "Non-Row");
		System.out.printf("Read %d:  %s\n", 6, isNull(6) ? "Null" : "Non-Null");
		System.out.printf("Read %d:  %s\n", 7, isRow(7) ? read(7) : "Non-Row");
		System.out.printf("Read %d:  %s\n", 8, isRow(8) ? read(8) : "Non-Row");
		System.out.printf("Read %d:  %s\n\n", 9, isRow(9) ? read(9) : "Non-Row");
	}

	private static void open() {
		try {
			Files.createDirectories(path.getParent());
			channel = FileChannel.open(path, CREATE, READ, WRITE);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static final Charset
		STRING_ENCODING = StandardCharsets.UTF_8;

	private static final int
		MAX_STRING = 7;

	private static final int
		LENGTH_BYTES = 1,
		CHAR_BYTES = 1,
		STRING_BYTES = LENGTH_BYTES + CHAR_BYTES * MAX_STRING,
		SHORT_BYTES = 2,
		INTEGER_BYTES = 4,
		BOOLEAN_BYTES = 1,
		MASK_BYTES = SHORT_BYTES;

	private static final int
		HEADER_WIDTH = INTEGER_BYTES * 2;

	public static void measureRecord() {
		record_width = MASK_BYTES + STRING_BYTES + INTEGER_BYTES + BOOLEAN_BYTES;
	}

	private static void bufferHeader() {
		try {
			header = channel.map(READ_WRITE, 0, HEADER_WIDTH);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void bufferRecords() {
		try {
			records = channel.map(READ_WRITE, HEADER_WIDTH, capacity * record_width);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void writeHeaderDimensions() {
		header.position(0);

		header.putInt(capacity);
		header.putInt(size);
	}

	public static void readHeaderDimensions() {
		header.position(0);

		capacity = header.getInt();
		size = header.getInt();
	}

	public static void write(int index, List<Object> row) {
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

	public static void writeNull(int index) {
		records.position(index * record_width);
		records.putShort((short) 0);
		records.put(new byte[record_width - MASK_BYTES]);
	}

	public static List<Object> read(int index) {
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

	private static boolean isRow(int index) {
		records.position(index * record_width);
		return records.getShort() > 0;
	}

	private static boolean isNull(int index) {
		records.position(index * record_width);
		return records.getShort() == 0;
	}
}
