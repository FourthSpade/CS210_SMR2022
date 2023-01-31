package apps.examples.json;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;

import tables.SearchTable;
import tables.Table;

/**
 * Demonstrates de/serialization in JSON
 * for the schema of a table, assuming:
 * <p>
 * The table name is already known.
 * <p>
 * The properties are hard-coded
 * when exporting only.
 */
public class ExampleJ1 {
	public static void main(String[] args) {
		Path path = Paths.get("data", "exported", "example_j1.json");

		write(path);

		Table table = read(path);
		System.out.println(table);
	}

	// Using JSON-P (JSON Processing API)

	public static void write(Path path) {
		try {
			JsonObjectBuilder root_object_builder = Json.createObjectBuilder();

			root_object_builder.add("table_name", "example_j1");

			JsonArrayBuilder column_names_builder = Json.createArrayBuilder();
			column_names_builder.add("letter");
			column_names_builder.add("order");
			column_names_builder.add("vowel");
			root_object_builder.add("column_names", column_names_builder.build());

			JsonArrayBuilder column_types_builder = Json.createArrayBuilder();
			column_types_builder.add("string");
			column_types_builder.add("integer");
			column_types_builder.add("boolean");
			root_object_builder.add("column_types", column_types_builder.build());

			root_object_builder.add("primary_index", 0);

			JsonObject root_object = root_object_builder.build();

			Files.createDirectories(path.getParent());
			JsonWriterFactory factory = Json.createWriterFactory(Map.of(JsonGenerator.PRETTY_PRINTING, true));
			JsonWriter writer = factory.createWriter(new FileOutputStream(path.toFile()));
			writer.writeObject(root_object);
			writer.close();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static Table read(Path path) {
		try {
			JsonReader reader = Json.createReader(new FileInputStream(path.toFile()));
			JsonObject root_object = reader.readObject();
			reader.close();

			String table_name = root_object.getString("table_name");

			JsonArray column_names_array = root_object.getJsonArray("column_names");
			List<String> column_names = new LinkedList<>();
			for (int i = 0; i < column_names_array.size(); i++) {
				column_names.add(column_names_array.getString(i));
			}

			JsonArray column_types_array = root_object.getJsonArray("column_types");
			List<String> column_types = new LinkedList<>();
			for (int i = 0; i < column_types_array.size(); i++) {
				column_types.add(column_types_array.getString(i));
			}

			int primary_index = root_object.getInt("primary_index");

			Table table = new SearchTable(
				table_name,
				column_names,
				column_types,
				primary_index
			);

			return table;
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
