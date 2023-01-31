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
import java.util.LinkedList;
import java.util.List;

/**
 * Demonstrates de/serialization in binary
 * for the schema of a table, assuming:
 * <p>
 * The table name is stored as the file name,
 * not encoded within the file.
 */
public class ExampleB1 {
	private static Path path;
	private static FileChannel channel;
	private static MappedByteBuffer header;

	private static String tableName;
	private static List<String> columnNames;
	private static List<String> columnTypes;
	private static int primaryIndex;

	public static void main(String[] args) {
		tableName = "example_b1";
		path = Paths.get("data", "persistent", "%s.bin".formatted(tableName));

		open();

		createDemo();

		reopenDemo();
	}

	private static void createDemo() {
		System.out.printf("Create tableName: %s\n\n", tableName);

		bufferHeader();

		columnNames = List.of("letter", "order", "vowel");
		columnTypes = List.of("string", "integer", "boolean");
		primaryIndex = 0;

		writeHeaderSchema();

		System.out.printf("Write columnCount:  %s\n", columnNames.size());
		System.out.printf("Write primaryIndex: %s\n", primaryIndex);
		System.out.printf("Write columnNames:  %s\n", columnNames);
		System.out.printf("Write columnTypes:  %s\n\n", columnTypes);
	}

	private static void reopenDemo() {
		System.out.printf("Reopen tableName: %s\n\n", tableName);

		bufferHeader();

		columnNames = null;
		columnTypes = null;
		primaryIndex = -1;

		readHeaderSchema();

		System.out.printf("Read columnCount:   %s\n", columnNames.size());
		System.out.printf("Read primaryIndex:  %s\n", primaryIndex);
		System.out.printf("Read columnNames:   %s\n", columnNames);
		System.out.printf("Read columnTypes:   %s\n\n", columnTypes);
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
		MAX_COLUMNS = 3,
		MAX_NAME = 7;

	private static final int
		LENGTH_BYTES = 1,
		CHAR_BYTES = 1,
		NAME_BYTES = LENGTH_BYTES + CHAR_BYTES * MAX_NAME,
		TYPE_BYTES = 1,
		INTEGER_BYTES = 4;

	private static final int
		COLUMN_WIDTH = NAME_BYTES + TYPE_BYTES,
		HEADER_WIDTH = INTEGER_BYTES * 2 + COLUMN_WIDTH * MAX_COLUMNS;

	private static void bufferHeader() {
		try {
			header = channel.map(READ_WRITE, 0, HEADER_WIDTH);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void writeHeaderSchema() {
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

	public static void readHeaderSchema() {
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
}
