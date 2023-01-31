package drivers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;

/*
 * Example:
 *   ECHO "Hello, world!"
 *
 * Result:
 * 	 string: "Hello, world!"
 */
public class Echo implements Driver {
	static final Pattern pattern = Pattern.compile(
		"ECHO\\s*\"([^\"]*)\"",
		Pattern.CASE_INSENSITIVE
	);

	@Override
	public Object execute(String query, Database db) throws SQLError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches()) return null;

		String text = matcher.group(1);

		return text;
	}
}
