/*
 * (C) Quartet FS 2013-2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package org.roaringbitmap;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * @author QuartetFS
 */
public class MewingArray implements Cloneable, Externalizable, StorageArray {

	private static final long serialVersionUID = 1L;

	public static final int INITIAL_CAPACITY = 4;
	public static final int DEFAULT_MAX_SIZE = 4096; // FIXME not sure yet

	protected short cardinality;

	private int[] values;

	public MewingArray() {
		this(INITIAL_CAPACITY);
	}

	public MewingArray(final int initialCapacity) {
		this.values = new int[initialCapacity];
		this.cardinality = 0;
	}

	protected MewingArray(final short cardinality, final int[] values) {
		this.cardinality = cardinality;
		this.values = values;
	}

	public StorageArray add(final int value) {
		final int loc = Util.unsignedBinarySearch(values, 0, cardinality, value);
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

	public StorageArray add(final long rangeStart, final long rangeEnd) {
		// Using longs to have unsigned int values, in [0, (1 << 32) - 1]
		// FIXME support properly the range with ints, as it is possible to have overflowing ranges
		if(end == begin) {
			return this; // Why cloning in
		}
		if ((begin > end) || (end > (1 << 16))) {
			throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
		}


		final int newCardinality = cardinality + (int) (rangeEnd - rangeStart);
		if (newCardinality > DEFAULT_MAX_SIZE) {
			final RoaringArray a = toRoaringArray();
			a.add(rangeStart, rangeEnd);

			return a;
		}

		int startLoc = Util.unsignedBinarySearch(values, 0, cardinality, (int) rangeStart);
		if (startLoc < 0) {
			startLoc = -startLoc - 1;
		}
		int endLoc = Util.unsignedBinarySearch(values, 0, cardinality, (int) (rangeEnd - 1));
		if (endLoc < 0) {
			endLoc = -endLoc - 1;
		} else {
			endLoc++;
		}

		if (endLoc - startLoc == 1) {
			// All values insert at a single point
			// Copy before start, insert values, copy after
			final int newCapacity = getIncreasedCapacity();
			final int[] previousValues = values;
			if (newCapacity > values.length) {
				values = new int[newCapacity];
				System.arraycopy(values, 0, previousValues, 0, startLoc);
			}

			System.arraycopy(values, newCapacity - endLoc, previousValues, endLoc, cardinality - endLoc);
		} else {

		}
		System.arraycopy(content, endLoc, answer.content, startLoc + rangelength,
				cardinality - endLoc);
		for (int k = 0; k < rangelength; ++k) {
			answer.content[k + startLoc] = (short) (begin + k);
		}
		answer.cardinality = newcardinality;
		return answer;
	}

	private RoaringArray toRoaringArray() {
		return null;
	}

	private int getIncreasedCapacity() {
		final int newCapacity = (this.values.length == 0) ? INITIAL_CAPACITY
				: this.values.length < 64 ? this.values.length * 2
				: this.values.length < 1067 ? this.values.length * 3 / 2
				: this.values.length * 5 / 4;
		return Math.min(newCapacity, DEFAULT_MAX_SIZE);
	}


	@Override
	protected Object clone() {
		return new MewingArray(
				this.cardinality,
				Arrays.copyOf(this.values, this.cardinality)
		);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeShort(Short.reverseBytes(this.cardinality));
		// little endian
		for (int k = 0; k < this.cardinality; ++k) {
			out.writeInt(Integer.reverseBytes(this.values[k]));
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.cardinality = Short.reverseBytes(in.readShort());

		if (this.values.length < this.cardinality) {
			this.values = new int[this.cardinality];
		}
		for (int k = 0; k < this.cardinality; ++k) {
			this.values[k] = Integer.reverseBytes(in.readInt());
		}
	}
}
