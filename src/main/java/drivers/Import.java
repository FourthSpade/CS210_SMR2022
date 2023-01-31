package drivers;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
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
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import apps.Database;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;
import tables.HashArrayTable;
import tables.SearchTable;
import tables.Table;

/*
 * Example:
 *   SHOW TABLE example_table
 *
 * Result:
 * 	 result set: the example_table in the database
 */
public class Import implements Driver {
	static final Pattern pattern = Pattern.compile(
			"IMPORT\\s+([a-z0-9_][a-z0-9_]*.(?:xml|json))\s*(?:\\s+TO\\s+([a-z][a-z0-9_]*))?",
			Pattern.CASE_INSENSITIVE
		);
	
	public Object execute(String query, Database db) throws SQLError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches()) return null;
		
		String filename = matcher.group(1);
		String tablename = null; 
		String[] fileParts = filename.split("\\.");
		String file_type = fileParts[1]; 
		
		if(matcher.group(2) != null) {
			tablename = matcher.group(2);
		}
		
		if(fileParts[1].equalsIgnoreCase("json")) {
			file_type = "JSON"; 
		}else if (fileParts[1].equalsIgnoreCase("xml")) {
			file_type = "XML"; 
		}else {
			throw new SQLError("EXPORT type not recognized");
		}
		
		Table table = null; 
		if(file_type.equalsIgnoreCase("JSON")) {
			Path path = Paths.get("data", "exported", filename);
			table = readJSON(path, tablename, db); 
		}else if(file_type.equalsIgnoreCase("XML")) {
			Path path = Paths.get("data", "exported", filename);
			table = readXML(path, tablename, db); 
		}

		return table; 
	}
	
	public static Table readJSON(Path path, String tablename, Database db) throws SQLError {
		try {
			JsonReader reader = Json.createReader(new FileInputStream(path.toFile()));
			JsonObject root_object = reader.readObject();
			reader.close();
			
			JsonObject Schema = root_object.getJsonObject("schema"); 

			String table_name = tablename; 
			if(table_name == null) {
				table_name = Schema.getString("table_name");
			}
			
			if(db.find(table_name) != null) {
				int i = 1; 
				table_name = table_name + "_" ; 
				int length = table_name.length(); 
				table_name = table_name + i ;
				while(db.find(table_name) != null) {
					table_name = table_name.substring(0, length); 
					i++; 
					table_name = table_name + i;
				}
			}

			JsonArray column_names_array = Schema.getJsonArray("column_names");
			List<String> column_names = new LinkedList<>();
			for (int i = 0; i < column_names_array.size(); i++) {
				column_names.add(column_names_array.getString(i));
			}

			JsonArray column_types_array = Schema.getJsonArray("column_types");
			List<String> column_types = new LinkedList<>();
			for (int i = 0; i < column_types_array.size(); i++) {
				column_types.add(column_types_array.getString(i));
			}

			int primary_index = Schema.getInt("primary_index");

			Table table = new SearchTable(
				table_name,
				column_names,
				column_types,
				primary_index
			);
			db.create(table);
			
			JsonArray state = root_object.getJsonArray("state"); 
			for(int j = 0; j < state.size(); j++) { 
				Object[] newRow = new Object[column_types.size()];
		    	for(int i = 0; i < column_types.size(); i++) {
			    	JsonArray row = state.getJsonArray(j);
					if(row.isNull(i)) {
						newRow[i] = null; 
					}else if(column_types.get(i).equalsIgnoreCase("string")) {
						newRow[i] =  row.getString(i); 
					}else if(column_types.get(i).equalsIgnoreCase("integer")) {
						newRow[i] = row.getInt(i); 
					}else if(column_types.get(i).equalsIgnoreCase("boolean")) {
						newRow[i] = row.getBoolean(i); 
					}else {
						throw new SQLError("Unknown type"); 
					}
				}
				List<Object> validRow = Arrays.asList(newRow);
				table.put(validRow);
			}

			return table;
		}catch (FileNotFoundException e) {
			throw new SQLError("File Not Found");
		}
	}
	
	public static Table readXML(Path path, String tablename, Database db) throws SQLError {
		try {
			try {
				Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(path.toFile());
				Element root = doc.getDocumentElement();
				root.normalize();
				
				String table_name = tablename; 
	
				if(tablename == null) {
					table_name = root.getAttribute("name");
				}
				
				if(db.find(table_name) != null) {
					int i = 1; 
					table_name = table_name + "_" ; 
					int length = table_name.length(); 
					table_name = table_name + i ;
					while(db.find(table_name) != null) {
						table_name = table_name.substring(0, length); 
						i++; 
						table_name = table_name + i;
					}
				}
	
				List<String> column_names = new LinkedList<>();
				List<String> column_types = new LinkedList<>();
	
				Element columns_elem = (Element) root.getElementsByTagName("columns").item(0);
	
				NodeList column_nodes = columns_elem.getElementsByTagName("column");
				for (int i = 0; i < column_nodes.getLength(); i++) {
					Element column_elem = (Element) column_nodes.item(i);
					column_names.add(column_elem.getAttribute("name"));
					column_types.add(column_elem.getAttribute("type"));
				}
	
				int primary_index = Integer.parseInt(columns_elem.getAttribute("primary"));
	
				Table table = new SearchTable(
					table_name,
					column_names,
					column_types,
					primary_index
				);
				db.create(table);
				
				NodeList row_nodes = root.getElementsByTagName("row");
				for (int i = 0; i < row_nodes.getLength(); i++) {
					Element row_elem = (Element) row_nodes.item(i);
					NodeList field_nodes = row_elem.getElementsByTagName("field");
					Object[] newRow = new Object[column_types.size()]; 
					if(column_types.size() != field_nodes.getLength()) {
						throw new SQLError("Fields does not equal column count"); 
					}
					for (int j = 0; j < field_nodes.getLength(); j++) {
						if(field_nodes.item(j).hasAttributes()) {
							newRow[j] = null; 
						}else if(column_types.get(j).equalsIgnoreCase("string")) {
							newRow[j] = field_nodes.item(j).getTextContent(); 
						}else if(column_types.get(j).equalsIgnoreCase("integer")) {
							newRow[j] = Integer.parseInt(field_nodes.item(j).getTextContent()); 
						}else if(column_types.get(j).equalsIgnoreCase("boolean")) {
							newRow[j] = Boolean.parseBoolean(field_nodes.item(j).getTextContent()); 
						}else {
							throw new SQLError("Type for element cast, undeterminable"); 
						}
					}
					
					List<Object> row = Arrays.asList(newRow); 
					
					table.put(row); 
				}
				return table;
			}catch (FileNotFoundException e) {
				throw new SQLError("File not Found"); 
			}
		}
		catch (IOException | ParserConfigurationException | SAXException  e) {
			throw new RuntimeException(e);
		}
	}
	
}