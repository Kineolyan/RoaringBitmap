/*
 * (C) Quartet FS 2013-2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package org.roaringbitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

/**
 * @author QuartetFS
 */
public class MewingArray implements Cloneable, Externalizable, StorageArray {

	private static final long serialVersionUID = 1L;

	public static final int INITIAL_CAPACITY = 4;
	public static final int DEFAULT_MAX_SIZE = 4096; // FIXME not sure yet

	// TODO this could be a short, given the expected number of elements
	// May be relevant for the size of the structure
	protected int cardinality;

	private int[] values;

	public MewingArray() {
		this(INITIAL_CAPACITY);
	}

	public MewingArray(final int initialCapacity) {
		this.values = new int[initialCapacity];
		this.cardinality = 0;
	}

	protected MewingArray(final int cardinality, final int[] values) {
		this.cardinality = cardinality;
		this.values = values;
	}

	@Override
	public int size() {
		return this.cardinality;
	}

	@Override
	public long getLongCardinality() {
		return this.cardinality;
	}

	@Override
	public int getCardinality() {
		return cardinality;
	}

	@Override
	public long getLongSizeInBytes() {
		return getSizeInBytes();
	}

	@Override
	public int getSizeInBytes() {
		return 8 // Class header
			+ 4 // integer size
			+ 8 // array reference
			+ 12 // Array header
			+ 4 * this.values.length // Array content
			;
	}

	@Override
	public PeekableIntIterator getIntIterator() {
		// FIXME implement
	}

	@Override
	public IntIterator getReverseIntIterator() {
		// FIXME implement
	}

	@Override
	public boolean contains(int x) {
		final int loc = IntUtil.unsignedBinarySearch(values, 0, cardinality, x);
		return loc >= 0;
	}

	@Override
	public void forEach(final IntConsumer consumer) {
		for (int value : values) {
			consumer.accept(value);
		}
	}

	@Override
	public int select(int i) {
		if (0 <= i && i < cardinality) {
			return values[i];
		} else if (i < 0) {
			throw new IllegalArgumentException("Negative index provided: " + i);
		} else {
			throw new IllegalArgumentException("Exceeding index " + i + " while bitmap contains only " + cardinality + " elements");
		}
	}

	@Override
	public long rank(int x) {
		final int loc = IntUtil.unsignedBinarySearch(values, 0, cardinality, x);
		return loc < 0 ? -loc : loc + 1;
	}

	@Override
	public boolean hasRunCompression() {
		return false;
	}

	@Override
	public StorageArray iadd(final int value) {
		final int loc = IntUtil.unsignedBinarySearch(values, 0, cardinality, value);
		if (loc < 0) {
			// Transform the ArrayContainer to a BitmapContainer
			// when cardinality = DEFAULT_MAX_SIZE
			if (cardinality >= DEFAULT_MAX_SIZE) {
				final RoaringArray a = toRoaringArray();
				a.add(value);
				return a;
			}

			if (cardinality >= this.values.length) {
				final int newCapacity = this.getIncreasedCapacity();
				final int[] newValues = new int[newCapacity];
				System.arraycopy(values, 0, newValues, 0, -loc - 1);
				newValues[-loc - 1] = value;
				System.arraycopy(values, -loc - 1, newValues, -loc, cardinality + loc + 1);
				values = newValues;
			} else {
				// insertion : shift the elements > x by one position to // the right
				// and put x in it's appropriate place
				System.arraycopy(values, -loc - 1, values, -loc, cardinality + loc + 1);
				values[-loc - 1] = value;
			}
			++cardinality;
		}

		return this;
	}

	@Override
	public StorageArray iadd(final long rangeStart, final long rangeEnd) {
		// Using longs to have unsigned int values, in [0, (1 << 32) - 1]
		// FIXME support properly the range with ints, as it is possible to have overflowing ranges
		if (rangeStart == rangeEnd) {
			return this; // Why cloning in
		}
//		if ((begin > end) || (end > (1 << 16))) {
//			throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
//		}
		final int startValue = (int) rangeStart;
		final int endValue = (int) rangeEnd;

		int startLoc = IntUtil.unsignedBinarySearch(values, 0, cardinality, startValue);
		if (startLoc < 0) {
			startLoc = -startLoc - 1;
		}
		int endLoc = IntUtil.unsignedBinarySearch(values, 0, cardinality, endValue - 1);
		if (endLoc < 0) {
			endLoc = -endLoc - 1;
		} else {
			endLoc++;
		}

		final int rangeLength = (int) (rangeEnd - rangeStart);
		final int rangeLocLength = endLoc - startLoc;
		if (rangeLocLength == rangeLength) {
			// All values to insert are already present
			return this;
		}

		final int newCardinality = cardinality - rangeLocLength + rangeLength;
		if (newCardinality > DEFAULT_MAX_SIZE) {
			final RoaringArray a = toRoaringArray();
			a.add(rangeStart, rangeEnd);

			return a;
		}

		final int newCapacity = getIncreasedCapacity();
		final int[] previousValues = values;
		if (newCapacity > values.length) {
			values = new int[newCapacity];
			// Copy the values before the range
			System.arraycopy(values, 0, previousValues, 0, startLoc);
		}

		// Copy the values after the range
		System.arraycopy(previousValues, endLoc, values, startLoc + rangeLength, cardinality - endLoc);
		for (int i = startLoc, v = startValue; i < endLoc; i += 1, v += 1) {
			values[i] = v;
		}
		cardinality = newCardinality;

		return this;
	}

	@Override
	public StorageArray icheckedAdd(int x, boolean[] result) {
		// FIXME implement
	}

	@Override
	public StorageArray add(long rangeStart, long rangeEnd) {
		// FIXME implement
	}

	@Override
	public StorageArray iremove(final int x) {
		final int value = (int) x;
		final int loc = IntUtil.unsignedBinarySearch(values, 0, cardinality, value);
		if (loc >= 0) {
			// insertion
			System.arraycopy(values, loc + 1, values, loc, cardinality - loc - 1);
			--cardinality;
		}

		return this;
	}

	@Override
	public StorageArray remove(long rangeStart, long rangeEnd) {
		// FIXME implement
	}

	@Override
	public StorageArray iremove(final long rangeStart, final long rangeEnd) {
		if (rangeStart == rangeEnd) {
			return this;
		}

//		if ((begin > end) || (end > (1 << 16))) {
//			throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
//		}
		final int startValue = (int) rangeStart;
		final int endValue = (int) rangeEnd;
		int startLoc = IntUtil.unsignedBinarySearch(values, 0, cardinality, startValue);
		if (startLoc < 0) {
			startLoc = -startLoc - 1;
		}
		int endLoc = IntUtil.unsignedBinarySearch(values, 0, cardinality, endValue - 1);
		if (endLoc < 0) {
			endLoc = -endLoc - 1;
		} else {
			endLoc++;
		}

		final int rangeLocLength = endLoc - startLoc;
		if (rangeLocLength > 0) {
			System.arraycopy(values, endLoc, values, startLoc, cardinality - endLoc);
			cardinality -= endLoc - startLoc;
		} // else nothing to do if the values

		return this;
	}

	@Override
	public StorageArray icheckedRemove(int x, boolean[] result) {
		// FIXME implement
	}

	@Override
	public void trim() {
		final int[] newValues = new int[cardinality];
		System.arraycopy(values, 0, newValues, 0, cardinality);
		values = newValues;
	}

	@Override
	public boolean runOptimize() {
		return false;
	}

	@Override
	public boolean removeRunCompression() {
		return false;
	}

	@Override
	public StorageArray and(StorageArray other) {
		// FIXME implement
	}

	@Override
	public StorageArray iand(StorageArray other) {
		// FIXME implement
	}

	@Override
	public int andCardinality(StorageArray other) {
		// FIXME implement
	}

	@Override
	public StorageArray or(StorageArray other) {
		// FIXME implement
	}

	@Override
	public StorageArray ior(StorageArray other) {
		// FIXME implement
	}

	@Override
	public StorageArray iandNot(StorageArray other) {
		// FIXME implement
	}

	@Override
	public StorageArray andNot(StorageArray other) {
		// FIXME implement
	}

	@Override
	public StorageArray add(int... values) {
		// FIXME implement
	}

	@Override
	public StorageArray flip(long rangeStart, long rangeEnd) {
		// FIXME implement
	}

	@Override
	public boolean intersects(StorageArray other) {
		// FIXME implement
	}

	@Override
	public StorageArray xor(StorageArray other) {
		// FIXME implement
	}

	@Override
	public StorageArray iflip(int x) {
		// FIXME implement
	}

	@Override
	public StorageArray iflip(long rangeStart, long rangeEnd) {
		// FIXME implement
	}

	@Override
	public StorageArray ixor(StorageArray other) {
		// FIXME implement
	}

	@Override
	public StorageArray limit(int maxCardinality) {
		final int newCardinality = Math.min(maxCardinality, cardinality);
		return new MewingArray(newCardinality, toArray(0, newCardinality));
	}

	@Override
	public StorageArray selectRangeWithoutCopy(long rangeStart, long rangeEnd) {
		final int startValue, endValue;
		int startLoc = IntUtil.unsignedBinarySearch(values, 0, cardinality, startValue);
		if (startLoc < 0) {
			startLoc = -startLoc + 1;
		}
		int endLoc = IntUtil.unsignedBinarySearch(values, 0, cardinality, endValue);
		if (endLoc < 0) {
			endLoc = -endLoc + 1;
		}

		if (startLoc < endLoc) {
			final int[] newValues = toArray(startLoc, endLoc + 1);
			return new MewingArray(newValues.length, newValues);
		} else {
			// Empty range
			return new MewingArray();
		}
	}

	@Override
	public int[] toArray() {
		return toArray(0, cardinality);
	}

	private int[] toArray(final int startIdx, final int endIdx) {
		final int newCardinality = endIdx - startIdx;
		final int[] newValues = new int[newCardinality];
		System.arraycopy(values, startIdx, newValues, 0, newCardinality);

		return newValues;
	}

	private RoaringArray toRoaringArray() {
		final RoaringArray newStorage = new RoaringArray();
		for (int value : values) {
			newStorage.iadd(value);
		}

		return newStorage;
	}

	private int getIncreasedCapacity() {
		final int newCapacity = (this.values.length == 0) ? INITIAL_CAPACITY
			: this.values.length < 64 ? this.values.length * 2
			: this.values.length < 1067 ? this.values.length * 3 / 2
			: this.values.length * 5 / 4;
		return Math.min(newCapacity, DEFAULT_MAX_SIZE);
	}

	@Override
	public MewingArray clone() {
		return new MewingArray(
			this.cardinality,
			Arrays.copyOf(this.values, this.cardinality)
		);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		serialize(out);
	}

	@Override
	public void serialize(DataOutput out) throws IOException {
		out.writeInt(Integer.reverseBytes(this.cardinality));
		// little endian
		for (int k = 0; k < this.cardinality; ++k) {
			out.writeInt(Integer.reverseBytes(this.values[k]));
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		deserialize(in);
	}

	@Override
	public void deserialize(DataInput in) throws IOException {
		this.cardinality = Integer.reverseBytes(in.readInt());

		if (this.values.length < this.cardinality) {
			this.values = new int[this.cardinality];
		}
		for (int k = 0; k < this.cardinality; ++k) {
			this.values[k] = Integer.reverseBytes(in.readInt());
		}
	}

	@Override
	public int serializedSizeInBytes() {
		return 0;
	}

	@Override
	public Iterator<Integer> iterator() {
		return new Iterator<Integer>() {
			private int pos = 0;

			@Override
			public boolean hasNext() {
				return pos < MewingArray.this.values.length;
			}

			@Override
			public Integer next() {
				if (hasNext()) {
					return MewingArray.this.values[pos++];
				} else {
					throw new ConcurrentModificationException(MewingArray.class.getSimpleName() + " changed while iterating on it. Position " + pos + " does not exist anymore since call to #hasNext");
				}
			}

			@Override
			public void remove() {
				iremove(MewingArray.this.values[pos]);
			}
		};
	}
}
