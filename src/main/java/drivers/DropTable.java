package drivers;

import java.util.regex.Pattern;

import apps.Database;
import tables.Table;

public class DropTable implements Driver {
	static final Pattern pattern = Pattern.compile(
			"DROP\\s+TABLE\\s+([a-z][a-z0-9_]*)",
			Pattern.CASE_INSENSITIVE
		);

		@Override
		public Object execute(String query, Database db) throws SQLError {
			var matcher = pattern.matcher(query.strip());
			if (!matcher.matches()) return null;

			String table_name = matcher.group(1);

			if (!db.exists(table_name)) {
				throw new SQLError("Table <%s> does not exist".formatted(table_name));
			}
			else {
				var table = db.find(table_name); 
				db.drop(table_name); 
				int str = table.size(); 
				return str; 
			}
		}
}