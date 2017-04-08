package org.roaringbitmap;

public class IntUtil {

	/**
	 * Look for value k in array in the range [begin,end). If the value is found, return its index. If
	 * not, return -(i+1) where i is the index where the value would be inserted. The array is assumed
	 * to contain sorted values where shorts are interpreted as unsigned integers.
	 *
	 * @param array array where we search
	 * @param begin first index (inclusive)
	 * @param end   last index (exclusive)
	 * @param k     value we search for
	 * @return count
	 */
	public static int unsignedBinarySearch(final int[] array, final int begin, final int end,
	                                       final int k) {
		if (Util.USE_HYBRID_BINSEARCH) {
			return hybridUnsignedBinarySearch(array, begin, end, k);
		} else {
			return branchyUnsignedBinarySearch(array, begin, end, k);
		}
	}

	protected static long asUnsignedInt(int x) {
		return x & 0xFFFFFFFFL;
	}

	// starts with binary search and finishes with a sequential search
	protected static int hybridUnsignedBinarySearch(
		final int[] array,
		final int begin, final int end,
		final int k) {
		final long ikey = asUnsignedInt(k);
		// next line accelerates the possibly common case where the value would
		// be inserted at the end
		if ((end > 0) && (asUnsignedInt(array[end - 1]) < ikey)) {
			return -end - 1;
		}
		int low = begin;
		int high = end - 1;
		// 16 in the next line matches the size of a cache line (64 bytes)
		while (low + 16 <= high) {
			final int middleIndex = (low + high) >>> 1;
			final long middleValue = asUnsignedInt(array[middleIndex]);

			if (middleValue < ikey) {
				low = middleIndex + 1;
			} else if (middleValue > ikey) {
				high = middleIndex - 1;
			} else {
				return middleIndex;
			}
		}
		// we finish the job with a sequential search
		int x = low;
		for (; x <= high; ++x) {
			final long val = asUnsignedInt(array[x]);
			if (val >= ikey) {
				if (val == ikey) {
					return x;
				}
				break;
			}
		}
		return -(x + 1);
	}

	protected static int branchyUnsignedBinarySearch(
		final int[] array,
		final int begin, final int end,
		final int k) {
		final long ikey = asUnsignedInt(k);
		// next line accelerates the possibly common case where the value would
		// be inserted at the end
		if ((end > 0) && (asUnsignedInt(array[end - 1]) < ikey)) {
			return -end - 1;
		}
		int low = begin;
		int high = end - 1;
		while (low <= high) {
			final int middleIndex = (low + high) >>> 1;
			final long middleValue = asUnsignedInt(array[middleIndex]);

			if (middleValue < ikey) {
				low = middleIndex + 1;
			} else if (middleValue > ikey) {
				high = middleIndex - 1;
			} else {
				return middleIndex;
			}
		}
		return -(low + 1);
	}
}
