package drivers;

import apps.Database;

/*
 * Examples:
 * 	 SQUARES BELOW 20
 * 	 SQUARES BELOW 30 AS a
 * 	 SQUARES BELOW 15 AS a, b
 *
 * Result 1:
 *   result set:
 * 	   primary integer column "x", integer column "x_squared"
 *	   rows [0, 0]; [1, 1]; [2, 4]; [3, 9]; [4, 16]
 *
 * Result 2:
 *   result set:
 * 	   primary integer column "a", integer column "a_squared"
 *	   rows [0, 0]; [1, 1]; [2, 4]; [3, 9]; [4, 16]; [5, 25]
 *
 * Result 3:
 *   result set:
 * 	   primary integer column "a", integer column "b"
 *	   rows [0, 0]; [1, 1]; [2, 4]; [3, 9]
 */
@Deprecated
public class SquaresBelow implements Driver {
	@Override
	public Object execute(String query, Database db) throws SQLError {
//		int iterator = 0; 
//		int value = 0; 
//		while (value < )
		return null; 
	}
}
