package apps.examples.binary;

/**
 * Demonstrates de/serialization in binary
 * for bitmasked rows, assuming:
 * <p>
 * For a row, the bitmask encodes
 * a 0-bit for each null field and
 * a 1-bit for each non-null field.
 * <p>
 * For a non-row, the bitmask encodes
 * all 0-bits for a null or
 * all 1-bits for a tombstone.
 */
public class ExampleB3 {
	public static void main(String[] args) {
		// Encode each bit in sequence.
		short mask = 0;
		mask = (short) ((mask << 1) + 1); explain(mask); // 5th bit
		mask = (short) ((mask << 1) + 0); explain(mask); // 4th bit
		mask = (short) ((mask << 1) + 0); explain(mask); // 3rd bit
		mask = (short) ((mask << 1) + 1); explain(mask); // 2nd bit
		mask = (short) ((mask << 1) + 0); explain(mask); // 1st bit
		mask = (short) ((mask << 1) + 0); explain(mask); // 0th bit

		System.out.println();

		// Mutate arbitrary bits.
//		mask = (short) (mask | (1 << 3)); explain(mask);  // set bit on
//		mask = (short) (mask & ~(1 << 2)); explain(mask); // set bit off
//		mask = (short) (mask ^ (1 << 4)); explain(mask);  // toggle bit
//		mask = (short) (mask ^ (1 << 4)); explain(mask);  // toggle bit
//		mask = (short) (mask ^ (1 << 4)); explain(mask);  // toggle bit

		System.out.println();

		// Access arbitrary bits.
		explain(5, (mask & (1 << 5)) != 0);
		explain(4, (mask & (1 << 4)) != 0);
		explain(3, (mask & (1 << 3)) != 0);
		explain(2, (mask & (1 << 2)) != 0);
		explain(1, (mask & (1 << 1)) != 0);
		explain(0, (mask & (1 << 0)) != 0);
		explain(mask);

		System.out.println();

		// Decode each bit in sequence.
//		explain(0, mask % 2 != 0);
//		mask = (short) (mask >> 1); explain(1, mask % 2 != 0);
//		mask = (short) (mask >> 1); explain(2, mask % 2 != 0);
//		mask = (short) (mask >> 1); explain(3, mask % 2 != 0);
//		mask = (short) (mask >> 1); explain(4, mask % 2 != 0);
//		mask = (short) (mask >> 1); explain(5, mask % 2 != 0);
//		mask = (short) (mask >> 1);
//		explain(mask);

		System.out.println();

		// Encode null instead of row.
		short null_mask = 0;
		explain(null_mask);

		// Encode tombstone instead of row.
		short tombstone_mask = -1;
		explain(tombstone_mask);
	}

	private static void explain(short mask) {
		System.out.printf(
			"Dec %2s = Bin %8s (%s)\n",
			mask,
			bitstring(mask),
			mask == 0 ? "null" : mask < 0 ? "tombstone" : "row"
		);
	}

	private static void explain(int index, boolean bit) {
		System.out.printf(
			"Bit %2s = %s\n",
			index,
			bit ? "on (1)" : "off (0)"
		);
	}

	private static String bitstring(short mask) {
		return "%16s"
			.formatted(Integer.toBinaryString(mask & 0xFFFF))
			.replace(' ', '0');
	}
}