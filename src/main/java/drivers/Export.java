package drivers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import apps.Database;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;
import tables.HashArrayTable;
import tables.Table;

/*
 * Example:
 *   SHOW TABLE example_table
 *
 * Result:
 * 	 result set: the example_table in the database
 */
public class Export implements Driver {
	static final Pattern pattern = Pattern.compile(
			"EXPORT\\s+([a-z][a-z0-9_]*)\\s+((?:TO\\s+([a-z0-9_][a-z0-9_]*.(?:xml|json)))|(?:AS\\s+(XML|JSON)))",
			Pattern.CASE_INSENSITIVE
		);
	
	public Object execute(String query, Database db) throws SQLError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches()) return null;

		String tablename = matcher.group(1);
		String identifier = matcher.group(2);
		String file_name = null; 
		String file_type = null; 
		String[] arr = identifier.split("\s+"); 
		
		if(arr.length != 2) {
			throw new SQLError("invalid number of parameters"); 
		}
		
		if(arr[0].equalsIgnoreCase("TO")) {
			file_name = arr[1]; 
			String[] fileParts = file_name.split("\\.");
			if(fileParts[1].equalsIgnoreCase("json")) {
				file_type = "JSON"; 
			}else if (fileParts[1].equalsIgnoreCase("xml")) {
				file_type = "XML"; 
			}else {
				throw new SQLError("EXPORT type not recognized");
			}
		}else if(arr[0].equalsIgnoreCase("AS")) {
			if(arr[1].equalsIgnoreCase("JSON")) {
				file_name = (tablename + ".json"); 
				file_type = "JSON"; 
			}else if(arr[1].equalsIgnoreCase("XML")) {
				file_name = (tablename + ".xml"); 
				file_type = "XML";	
			}else {
				throw new SQLError("EXPORT type not recognized");
			}
		}else {
			throw new SQLError("EXPORT phrase not recognized");
		}

		Path path = Paths.get("data", "exported", file_name);
		File f = new File(path.toString());
		if(f.exists() && !f.isDirectory()) { 
			int i = 1; 
			String[] fileParts = file_name.split("\\.");
			String prepend = fileParts[0] + "_"; 
			int length = prepend.length();
			prepend = prepend + i; 
			file_name = prepend + "." + fileParts[1]; 
			path = Paths.get("data", "exported", file_name);
			f = new File(path.toString());
			while(f.exists()) {
				prepend = prepend.substring(0, length); 
				i++; 
				prepend = prepend + i;
				file_name = prepend + "." + fileParts[1]; 
				path = Paths.get("data", "exported", file_name);
				f = new File(path.toString());
			}
		}
		
		if(file_type.equalsIgnoreCase("JSON")) {
			writeJSON(path, tablename, db); 
		}else if(file_type.equalsIgnoreCase("XML")) {
			writeXML(path, tablename, db); 
		}
		
		return true; 
	}
	
	public static void writeJSON(Path path, String table_name, Database db) throws SQLError {
		try {
			
			if(db.find(table_name) == null) {
				throw new SQLError("Table <%s> does not exist".formatted(table_name)); 
			}
			Table table = db.find(table_name); 
			List<String> tableCols = table.getColumnNames();
			List<String> colTypes = table.getColumnTypes();
			int primaryIndex = table.getPrimaryIndex(); 			
			
			// Create Object builder
			JsonObjectBuilder root_object_builder = Json.createObjectBuilder();
			
			// Create the Schema
			JsonArrayBuilder column_names_builder = Json.createArrayBuilder();
			JsonArrayBuilder column_types_builder = Json.createArrayBuilder();
			for(int i = 0; i < tableCols.size(); i++) {
				column_names_builder.add(tableCols.get(i));
				column_types_builder.add(colTypes.get(i));
			}
			
			// Build the Schema 
			JsonObjectBuilder schema_builder = Json.createObjectBuilder();
			schema_builder.add("table_name", table_name);
			schema_builder.add("column_names", column_names_builder);
			schema_builder.add("column_types", column_types_builder);
			schema_builder.add("primary_index", primaryIndex);
			root_object_builder.add("schema", schema_builder.build());
			
			// Build the State
			JsonArrayBuilder row_builder = Json.createArrayBuilder();
			JsonArrayBuilder root_array_builder = Json.createArrayBuilder();	
			Set<List<Object>> rows = table.rows(); 
			for ( List<Object> row : rows) {
				for (int i = 0; i < row.size(); i++) {
					if(row.get(i) == null || row.get(i).equals("null")) {
						row_builder.addNull(); 
					}else if(colTypes.get(i).equalsIgnoreCase("string")) {
						row_builder.add((String) row.get(i)); 
					}else if(colTypes.get(i).equalsIgnoreCase("integer")) {
						row_builder.add((Integer) row.get(i)); 
					}else if(colTypes.get(i).equalsIgnoreCase("boolean")) {
						row_builder.add((Boolean) row.get(i)); 
					}
				}
				root_array_builder.add(row_builder.build());
			}
			root_object_builder.add("state", root_array_builder.build());
			

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
		return; 
	}
	
	public static void writeXML(Path path, String table_name, Database db) throws SQLError {
		try {
			// Setup table data 
			if(db.find(table_name) == null) {
				throw new SQLError("Table <%s> does not exist".formatted(table_name)); 
			}
			Table table = db.find(table_name); 
			List<String> colNames = table.getColumnNames();
			List<String> colTypes = table.getColumnTypes();
			Integer primaryIndex = table.getPrimaryIndex();
			String priInd = primaryIndex.toString(); 
			
			// Create DOM
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

			Element root = doc.createElement("table");
			root.setAttribute("name", table_name);
			doc.appendChild(root);

			Element schema = doc.createElement("schema");
			root.appendChild(schema);

				Element columns = doc.createElement("columns");
				columns.setAttribute("primary", priInd);
				schema.appendChild(columns);

				for(int i = 0 ; i < colTypes.size(); i++) {	
					Element column = doc.createElement("column");				
					column.setAttribute("name", colNames.get(i));
					column.setAttribute("type", colTypes.get(i));
					columns.appendChild(column);
				}
				
			Element state = doc.createElement("state");
			root.appendChild(state);
				
			Set<List<Object>> rows = table.rows(); 
			for ( List<Object> row1 : rows) {
				Element row = doc.createElement("row");
				for (int i = 0; i < row1.size(); i++) {
					Element field = doc.createElement("field");
					if(row1.get(i) == null || row1.get(i).equals("null")) {
						field.setAttribute("null", "yes");
					}else{
						String val = row1.get(i).toString(); 
						field.setTextContent(val); 
					}
					row.appendChild(field); 
				}
				state.appendChild(row);
			}
		
		
			Files.createDirectories(path.getParent());
		    Source from = new DOMSource(doc);
		    Result to = new StreamResult(path.toFile());
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
		    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		    transformer.transform(from, to);
		}
		catch (IOException | ParserConfigurationException | TransformerException e) {
			throw new RuntimeException(e);
		}
		return; 
	}
}