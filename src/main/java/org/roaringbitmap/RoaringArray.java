/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;


import java.io.*;
import java.util.Arrays;
import java.util.Iterator;


/**
 * Specialized array to store the containers used by a RoaringBitmap. This is not meant to be used
 * by end users.
 */
public final class RoaringArray implements Cloneable, Externalizable, StorageArray {
	protected static final short SERIAL_COOKIE_NO_RUNCONTAINER = 12346;
	protected static final short SERIAL_COOKIE = 12347;
	protected static final int NO_OFFSET_THRESHOLD = 4;

	// bumped serialVersionUID with runcontainers, so default serialization
	// will not work...
	private static final long serialVersionUID = 8L;

	static final int INITIAL_CAPACITY = 4;


	short[] keys = null;

	Container[] values = null;

	int size = 0;

	protected RoaringArray() {
		this.keys = new short[INITIAL_CAPACITY];
		this.values = new Container[INITIAL_CAPACITY];
	}

	public void forEach(IntConsumer ic) {
		for (int i = 0; i < size(); i++) {
			getContainerAtIndex(i).forEach(keys[i], ic);
		}
	}

	@Override
	public boolean contains(final int x) {
		final short hb = Util.highbits(x);
		final Container c = getContainer(hb);
		return c != null && c.contains(Util.lowbits(x));
	}

	@Override
	public long rank(final int x) {
		long size = 0;
		short xhigh = Util.highbits(x);

		for (int i = 0; i < size(); i++) {
			short key = getKeyAtIndex(i);
			if (Util.compareUnsigned(key, xhigh) < 0) {
				size += getContainerAtIndex(i).getCardinality();
			} else {
				return size + getContainerAtIndex(i).rank(Util.lowbits(x));
			}
		}
		return size;
	}

	@Override
	public int select(int j) {
		int leftover = j;
		for (int i = 0; i < size(); i++) {
			Container c = getContainerAtIndex(i);
			int thiscard = c.getCardinality();
			if (thiscard > leftover) {
				int keycontrib = getKeyAtIndex(i) << 16;
				int lowcontrib = Util.toIntUnsigned(c.select(leftover));
				return lowcontrib + keycontrib;
			}
			leftover -= thiscard;
		}
		throw new IllegalArgumentException(
			"select " + j + " when the cardinality is " + this.getCardinality());
	}

	/**
	 * Find the smallest integer index larger than pos such that array[index].key&gt;=x. If none can
	 * be found, return size. Based on code by O. Kaser.
	 *
	 * @param x   minimal value
	 * @param pos index to exceed
	 * @return the smallest index greater than pos such that array[index].key is at least as large as
	 * min, or size if it is not possible.
	 */
	protected int advanceUntil(short x, int pos) {
		int lower = pos + 1;

		// special handling for a possibly common sequential case
		if (lower >= size || Util.toIntUnsigned(keys[lower]) >= Util.toIntUnsigned(x)) {
			return lower;
		}

		int spansize = 1; // could set larger
		// bootstrap an upper limit

		while (lower + spansize < size
			&& Util.toIntUnsigned(keys[lower + spansize]) < Util.toIntUnsigned(x)) {
			spansize *= 2; // hoping for compiler will reduce to shift
		}
		int upper = (lower + spansize < size) ? lower + spansize : size - 1;

		// maybe we are lucky (could be common case when the seek ahead
		// expected to be small and sequential will otherwise make us look bad)
		if (keys[upper] == x) {
			return upper;
		}

		if (Util.toIntUnsigned(keys[upper]) < Util.toIntUnsigned(x)) {// means array has no item key >=
			// x
			return size;
		}

		// we know that the next-smallest span was too small
		lower += (spansize / 2);

		// else begin binary search
		// invariant: array[lower]<x && array[upper]>x
		while (lower + 1 != upper) {
			int mid = (lower + upper) / 2;
			if (keys[mid] == x) {
				return mid;
			} else if (Util.toIntUnsigned(keys[mid]) < Util.toIntUnsigned(x)) {
				lower = mid;
			} else {
				upper = mid;
			}
		}
		return upper;
	}

	protected void append(short key, Container value) {
		extendArray(1);
		this.keys[this.size] = key;
		this.values[this.size] = value;
		this.size++;
	}

	/**
	 * Append copies of the values AFTER a specified key (may or may not be present) to end.
	 *
	 * @param sa          other array
	 * @param beforeStart given key is the largest key that we won't copy
	 */
	protected void appendCopiesAfter(RoaringArray sa, short beforeStart) {
		int startLocation = sa.getIndex(beforeStart);
		if (startLocation >= 0) {
			startLocation++;
		} else {
			startLocation = -startLocation - 1;
		}
		extendArray(sa.size - startLocation);

		for (int i = startLocation; i < sa.size; ++i) {
			this.keys[this.size] = sa.keys[i];
			this.values[this.size] = sa.values[i].clone();
			this.size++;
		}
	}

	/**
	 * Append copies of the values from another array, from the start
	 *
	 * @param sourceArray The array to copy from
	 * @param stoppingKey any equal or larger key in other array will terminate copying
	 */
	protected void appendCopiesUntil(RoaringArray sourceArray, short stoppingKey) {
		int stopKey = Util.toIntUnsigned(stoppingKey);
		for (int i = 0; i < sourceArray.size; ++i) {
			if (Util.toIntUnsigned(sourceArray.keys[i]) >= stopKey) {
				break;
			}
			extendArray(1);
			this.keys[this.size] = sourceArray.keys[i];
			this.values[this.size] = sourceArray.values[i].clone();
			this.size++;
		}
	}

	/**
	 * Append copy of the one value from another array
	 *
	 * @param sa    other array
	 * @param index index in the other array
	 */
	protected void appendCopy(RoaringArray sa, int index) {
		extendArray(1);
		this.keys[this.size] = sa.keys[index];
		this.values[this.size] = sa.values[index].clone();
		this.size++;
	}

	/**
	 * Append copies of the values from another array
	 *
	 * @param sa            other array
	 * @param startingIndex starting index in the other array
	 * @param end           endingIndex (exclusive) in the other array
	 */
	protected void appendCopy(RoaringArray sa, int startingIndex, int end) {
		extendArray(end - startingIndex);
		for (int i = startingIndex; i < end; ++i) {
			this.keys[this.size] = sa.keys[i];
			this.values[this.size] = sa.values[i].clone();
			this.size++;
		}
	}


	/**
	 * Append the values from another array, no copy is made (use with care)
	 *
	 * @param sa            other array
	 * @param startingIndex starting index in the other array
	 * @param end           endingIndex (exclusive) in the other array
	 */
	protected void append(RoaringArray sa, int startingIndex, int end) {
		extendArray(end - startingIndex);
		for (int i = startingIndex; i < end; ++i) {
			this.keys[this.size] = sa.keys[i];
			this.values[this.size] = sa.values[i];
			this.size++;
		}
	}


	private int binarySearch(int begin, int end, short key) {
		return Util.unsignedBinarySearch(keys, begin, end, key);
	}

	protected void clear() {
		this.keys = null;
		this.values = null;
		this.size = 0;
	}

	@Override
	public RoaringArray clone() throws CloneNotSupportedException {
		RoaringArray sa;
		sa = (RoaringArray) super.clone();
		sa.keys = Arrays.copyOf(this.keys, this.size);
		sa.values = Arrays.copyOf(this.values, this.size);
		for (int k = 0; k < this.size; ++k) {
			sa.values[k] = sa.values[k].clone();
		}
		sa.size = this.size;
		return sa;
	}

	protected void copyRange(int begin, int end, int newBegin) {
		// assuming begin <= end and newBegin < begin
		final int range = end - begin;
		System.arraycopy(this.keys, begin, this.keys, newBegin, range);
		System.arraycopy(this.values, begin, this.values, newBegin, range);
	}

	/**
	 * Deserialize.
	 *
	 * @param in the DataInput stream
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void deserialize(DataInput in) throws IOException {
		this.clear();
		// little endian
		final int cookie = Integer.reverseBytes(in.readInt());
		if ((cookie & 0xFFFF) != SERIAL_COOKIE && cookie != SERIAL_COOKIE_NO_RUNCONTAINER) {
			throw new IOException("I failed to find one of the right cookies.");
		}
		this.size = ((cookie & 0xFFFF) == SERIAL_COOKIE) ? (cookie >>> 16) + 1
			: Integer.reverseBytes(in.readInt());

		if ((this.keys == null) || (this.keys.length < this.size)) {
			this.keys = new short[this.size];
			this.values = new Container[this.size];
		}


		byte[] bitmapOfRunContainers = null;
		boolean hasrun = (cookie & 0xFFFF) == SERIAL_COOKIE;
		if (hasrun) {
			bitmapOfRunContainers = new byte[(size + 7) / 8];
			in.readFully(bitmapOfRunContainers);
		}

		final short keys[] = new short[this.size];
		final int cardinalities[] = new int[this.size];
		final boolean isBitmap[] = new boolean[this.size];
		for (int k = 0; k < this.size; ++k) {
			keys[k] = Short.reverseBytes(in.readShort());
			cardinalities[k] = 1 + (0xFFFF & Short.reverseBytes(in.readShort()));

			isBitmap[k] = cardinalities[k] > ArrayContainer.DEFAULT_MAX_SIZE;
			if (bitmapOfRunContainers != null && (bitmapOfRunContainers[k / 8] & (1 << (k % 8))) != 0) {
				isBitmap[k] = false;
			}
		}
		if ((!hasrun) || (this.size >= NO_OFFSET_THRESHOLD)) {
			// skipping the offsets
			in.skipBytes(this.size * 4);
		}
		// Reading the containers
		for (int k = 0; k < this.size; ++k) {
			Container val;
			if (isBitmap[k]) {
				final long[] bitmapArray = new long[BitmapContainer.MAX_CAPACITY / 64];
				// little endian
				for (int l = 0; l < bitmapArray.length; ++l) {
					bitmapArray[l] = Long.reverseBytes(in.readLong());
				}
				val = new BitmapContainer(bitmapArray, cardinalities[k]);
			} else if (bitmapOfRunContainers != null
				&& ((bitmapOfRunContainers[k / 8] & (1 << (k % 8))) != 0)) {
				// cf RunContainer.writeArray()
				int nbrruns = Util.toIntUnsigned(Short.reverseBytes(in.readShort()));
				final short lengthsAndValues[] = new short[2 * nbrruns];

				for (int j = 0; j < 2 * nbrruns; ++j) {
					lengthsAndValues[j] = Short.reverseBytes(in.readShort());
				}
				val = new RunContainer(lengthsAndValues, nbrruns);
			} else {
				final short[] shortArray = new short[cardinalities[k]];
				for (int l = 0; l < shortArray.length; ++l) {
					shortArray[l] = Short.reverseBytes(in.readShort());
				}
				val = new ArrayContainer(shortArray);
			}
			this.keys[k] = keys[k];
			this.values[k] = val;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof RoaringArray) {
			RoaringArray srb = (RoaringArray) o;
			if (srb.size != this.size) {
				return false;
			}
			for (int i = 0; i < srb.size; ++i) {
				if (this.keys[i] != srb.keys[i] || !this.values[i].equals(srb.values[i])) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	// make sure there is capacity for at least k more elements
	protected void extendArray(int k) {
		// size + 1 could overflow
		if (this.size + k >= this.keys.length) {
			int newCapacity;
			if (this.keys.length < 1024) {
				newCapacity = 2 * (this.size + k);
			} else {
				newCapacity = 5 * (this.size + k) / 4;
			}
			this.keys = Arrays.copyOf(this.keys, newCapacity);
			this.values = Arrays.copyOf(this.values, newCapacity);
		}
	}

	// involves a binary search
	protected Container getContainer(short x) {
		int i = this.binarySearch(0, size, x);
		if (i < 0) {
			return null;
		}
		return this.values[i];
	}

	protected Container getContainerAtIndex(int i) {
		return this.values[i];
	}

	/**
	 * Create a ContainerPointer for this RoaringArray
	 *
	 * @return a ContainerPointer
	 */
	public ContainerPointer getContainerPointer() {
		return getContainerPointer(0);
	}

	/**
	 * Create a ContainerPointer for this RoaringArray
	 *
	 * @param startIndex starting index in the container list
	 * @return a ContainerPointer
	 */
	public ContainerPointer getContainerPointer(final int startIndex) {
		return new ContainerPointer() {
			int k = startIndex;

			@Override
			public void advance() {
				++k;

			}

			@Override
			public ContainerPointer clone() {
				try {
					return (ContainerPointer) super.clone();
				} catch (CloneNotSupportedException e) {
					return null;// will not happen
				}
			}

			@Override
			public int compareTo(ContainerPointer o) {
				if (key() != o.key()) {
					return Util.toIntUnsigned(key()) - Util.toIntUnsigned(o.key());
				}
				return o.getCardinality() - getCardinality();
			}

			@Override
			public int getCardinality() {
				return getContainer().getCardinality();
			}

			@Override
			public Container getContainer() {
				if (k >= RoaringArray.this.size) {
					return null;
				}
				return RoaringArray.this.values[k];
			}


			@Override
			public boolean isBitmapContainer() {
				return getContainer() instanceof BitmapContainer;
			}

			@Override
			public boolean isRunContainer() {
				return getContainer() instanceof RunContainer;
			}


			@Override
			public short key() {
				return RoaringArray.this.keys[k];

			}
		};
	}

	// involves a binary search
	protected int getIndex(short x) {
		// before the binary search, we optimize for frequent cases
		if ((size == 0) || (keys[size - 1] == x)) {
			return size - 1;
		}
		// no luck we have to go through the list
		return this.binarySearch(0, size, x);
	}

	protected short getKeyAtIndex(int i) {
		return this.keys[i];
	}

	@Override
	public int hashCode() {
		int hashvalue = 0;
		for (int k = 0; k < this.size; ++k) {
			hashvalue = 31 * hashvalue + keys[k] * 0xF0F0F0 + values[k].hashCode();
		}
		return hashvalue;
	}

	boolean hasRunContainer() {
		for (int k = 0; k < size; ++k) {
			Container ck = values[k];
			if (ck instanceof RunContainer) {
				return true;
			}
		}
		return false;
	}

	protected int headerSize() {
		if (hasRunContainer()) {
			if (size < NO_OFFSET_THRESHOLD) {// for small bitmaps, we omit the offsets
				return 4 + (size + 7) / 8 + 4 * size;
			}
			return 4 + (size + 7) / 8 + 8 * size;// - 4 because we pack the size with the cookie
		} else {
			return 4 + 4 + 8 * size;
		}
	}


	// insert a new key, it is assumed that it does not exist
	protected void insertNewKeyValueAt(int i, short key, Container value) {
		extendArray(1);
		System.arraycopy(keys, i, keys, i + 1, size - i);
		keys[i] = key;
		System.arraycopy(values, i, values, i + 1, size - i);
		values[i] = value;
		size++;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		deserialize(in);
	}

	protected void removeAtIndex(int i) {
		System.arraycopy(keys, i + 1, keys, i, size - i - 1);
		keys[size - 1] = 0;
		System.arraycopy(values, i + 1, values, i, size - i - 1);
		values[size - 1] = null;
		size--;
	}

	protected void removeIndexRange(int begin, int end) {
		if (end <= begin) {
			return;
		}
		final int range = end - begin;
		System.arraycopy(keys, end, keys, begin, size - end);
		System.arraycopy(values, end, values, begin, size - end);
		for (int i = 1; i <= range; ++i) {
			keys[size - i] = 0;
			values[size - i] = null;
		}
		size -= range;
	}

	protected void replaceKeyAndContainerAtIndex(int i, short key, Container c) {
		this.keys[i] = key;
		this.values[i] = c;
	}

	protected void resize(int newLength) {
		Arrays.fill(this.keys, newLength, this.size, (short) 0);
		Arrays.fill(this.values, newLength, this.size, null);
		this.size = newLength;
	}


	/**
	 * Serialize.
	 * <p>
	 * The current bitmap is not modified.
	 *
	 * @param out the DataOutput stream
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void serialize(DataOutput out) throws IOException {
		int startOffset = 0;
		boolean hasrun = hasRunContainer();
		if (hasrun) {
			out.writeInt(Integer.reverseBytes(SERIAL_COOKIE | ((size - 1) << 16)));
			byte[] bitmapOfRunContainers = new byte[(size + 7) / 8];
			for (int i = 0; i < size; ++i) {
				if (this.values[i] instanceof RunContainer) {
					bitmapOfRunContainers[i / 8] |= (1 << (i % 8));
				}
			}
			out.write(bitmapOfRunContainers);
			if (this.size < NO_OFFSET_THRESHOLD) {
				startOffset = 4 + 4 * this.size + bitmapOfRunContainers.length;
			} else {
				startOffset = 4 + 8 * this.size + bitmapOfRunContainers.length;
			}
		} else { // backwards compatibility
			out.writeInt(Integer.reverseBytes(SERIAL_COOKIE_NO_RUNCONTAINER));
			out.writeInt(Integer.reverseBytes(size));
			startOffset = 4 + 4 + 4 * this.size + 4 * this.size;
		}
		for (int k = 0; k < size; ++k) {
			out.writeShort(Short.reverseBytes(this.keys[k]));
			out.writeShort(Short.reverseBytes((short) (this.values[k].getCardinality() - 1)));
		}
		if ((!hasrun) || (this.size >= NO_OFFSET_THRESHOLD)) {
			// writing the containers offsets
			for (int k = 0; k < this.size; k++) {
				out.writeInt(Integer.reverseBytes(startOffset));
				startOffset = startOffset + this.values[k].getArraySizeInBytes();
			}
		}
		for (int k = 0; k < size; ++k) {
			values[k].writeArray(out);
		}
	}

	/**
	 * Report the number of bytes required for serialization.
	 *
	 * @return the size in bytes
	 */
	@Override
	public int serializedSizeInBytes() {
		int count = headerSize();
		for (int k = 0; k < size; ++k) {
			count += values[k].getArraySizeInBytes();
		}
		return count;
	}

	protected void setContainerAtIndex(int i, Container c) {
		this.values[i] = c;
	}

	public int size() {
		return this.size;
	}

	@Override
	public long getLongCardinality() {
		long size = 0;
		for (int i = 0; i < size(); i++) {
			size += getContainerAtIndex(i).getCardinality();
		}
		return size;
	}

	@Override
	public int getCardinality() {
		return (int) getLongCardinality();
	}

	@Override
	public long getLongSizeInBytes() {
		long size = 8; // Class header
		for (int i = 0; i < size(); i++) {
			final Container c = getContainerAtIndex(i);
			size += 2 + c.getSizeInBytes();
		}
		return size;
	}

	@Override
	public int getSizeInBytes() {
		return (int) getLongSizeInBytes();
	}

	@Override
	public boolean hasRunCompression() {
		for (int i = 0; i < size(); i++) {
			Container c = getContainerAtIndex(i);
			if (c instanceof RunContainer) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		serialize(out);
	}

	@Override
	public PeekableIntIterator getIntIterator() {
		return new RoaringIntIterator(this);
	}

	@Override
	public IntIterator getReverseIntIterator() {
		return new RoaringReverseIntIterator(this);
	}

	@Override
	public StorageArray add(long rangeStart, long rangeEnd) {
		// Cardinality will not decrease, no need to convert back to a MewingArray
		assert rangeStart < rangeEnd : "Invalid range";

		final RoaringArray answer = new RoaringArray();

		final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
		final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
		final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
		final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));

		answer.appendCopiesUntil(this, (short) hbStart);

		if (hbStart == hbLast) {
			final int i = this.getIndex((short) hbStart);
			final Container c =
				i >= 0 ? this.getContainerAtIndex(i).add(lbStart, lbLast + 1)
					: Container.rangeOfOnes(lbStart, lbLast + 1);
			answer.append((short) hbStart, c);
			answer.appendCopiesAfter(this, (short) hbLast);
			return answer;
		}
		int ifirst = this.getIndex((short) hbStart);
		int ilast = this.getIndex((short) hbLast);

		{
			final Container c = ifirst >= 0
				? this.getContainerAtIndex(ifirst).add(lbStart,
				Util.maxLowBitAsInteger() + 1)
				: Container.rangeOfOnes(lbStart, Util.maxLowBitAsInteger() + 1);
			answer.append((short) hbStart, c);
		}

		for (int hb = hbStart + 1; hb < hbLast; ++hb) {
			Container c = Container.rangeOfOnes(0, Util.maxLowBitAsInteger() + 1);
			answer.append((short) hb, c);
		}

		{
			final Container c = ilast >= 0
				? this.getContainerAtIndex(ilast).add(0, lbLast + 1)
				: Container.rangeOfOnes(0, lbLast + 1);
			answer.append((short) hbLast, c);
		}

		answer.appendCopiesAfter(this, (short) hbLast);
		return answer;
	}

	public StorageArray iand(final StorageArray other_) {
		final RoaringArray other = ensureType(other_);

		int pos1 = 0, pos2 = 0, intersectionSize = 0;
		final int length1 = size(), length2 = other.size();

		while (pos1 < length1 && pos2 < length2) {
			final short s1 = getKeyAtIndex(pos1);
			final short s2 = other.getKeyAtIndex(pos2);
			if (s1 == s2) {
				final Container c1 = getContainerAtIndex(pos1);
				final Container c2 = other.getContainerAtIndex(pos2);
				final Container c = c1.iand(c2);
				if (c.getCardinality() > 0) {
					replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
				}
				++pos1;
				++pos2;
			} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
				pos1 = advanceUntil(s2, pos1);
			} else { // s1 > s2
				pos2 = other.advanceUntil(s1, pos2);
			}
		}
		resize(intersectionSize);

		// FIXME conversion may be needed
		return this;
	}

	@Override
	public StorageArray and(final StorageArray other_) {
		final RoaringArray other = ensureType(other_);

		final RoaringArray answer = new RoaringArray();
		final int length1 = this.size(), length2 = other.size();
		int pos1 = 0, pos2 = 0;

		while (pos1 < length1 && pos2 < length2) {
			final short s1 = this.getKeyAtIndex(pos1);
			final short s2 = other.getKeyAtIndex(pos2);
			if (s1 == s2) {
				final Container c1 = this.getContainerAtIndex(pos1);
				final Container c2 = other.getContainerAtIndex(pos2);
				final Container c = c1.and(c2);
				if (c.getCardinality() > 0) {
					answer.append(s1, c);
				}
				++pos1;
				++pos2;
			} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
				pos1 = this.advanceUntil(s2, pos1);
			} else { // s1 > s2
				pos2 = other.advanceUntil(s1, pos2);
			}
		}

		// FIXME May need to convert the array to a MewingArray
		return answer;
	}

	@Override
	public int andCardinality(final StorageArray other_) {
		final RoaringArray other = ensureType(other_);

		int answer = 0;
		final int length1 = this.size(), length2 = other.size();
		int pos1 = 0, pos2 = 0;

		while (pos1 < length1 && pos2 < length2) {
			final short s1 = this.getKeyAtIndex(pos1);
			final short s2 = other.getKeyAtIndex(pos2);
			if (s1 == s2) {
				final Container c1 = this.getContainerAtIndex(pos1);
				final Container c2 = other.getContainerAtIndex(pos2);
				// TODO: could be made faster if we did not have to materialize container
				answer += c1.andCardinality(c2);
				++pos1;
				++pos2;
			} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
				pos1 = this.advanceUntil(s2, pos1);
			} else { // s1 > s2
				pos2 = other.advanceUntil(s1, pos2);
			}
		}
		return answer;
	}

	@Override
	public StorageArray or(final StorageArray other_) {
		// Size can only increase, no possible conversion
		// FIXME can accept MewingBitmap
		final RoaringArray other = ensureType(other_);

		final RoaringArray answer = new RoaringArray();
		int pos1 = 0, pos2 = 0;
		final int length1 = this.size(), length2 = other.size();
		main:
		if (pos1 < length1 && pos2 < length2) {
			short s1 = this.getKeyAtIndex(pos1);
			short s2 = other.getKeyAtIndex(pos2);

			while (true) {
				if (s1 == s2) {
					answer.append(s1, this.getContainerAtIndex(pos1)
							.or(other.getContainerAtIndex(pos2)));
					pos1++;
					pos2++;
					if ((pos1 == length1) || (pos2 == length2)) {
						break main;
					}
					s1 = this.getKeyAtIndex(pos1);
					s2 = other.getKeyAtIndex(pos2);
				} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
					answer.appendCopy(this, pos1);
					pos1++;
					if (pos1 == length1) {
						break main;
					}
					s1 = this.getKeyAtIndex(pos1);
				} else { // s1 > s2
					answer.appendCopy(other, pos2);
					pos2++;
					if (pos2 == length2) {
						break main;
					}
					s2 = other.getKeyAtIndex(pos2);
				}
			}
		}
		if (pos1 == length1) {
			answer.appendCopy(other, pos2, length2);
		} else if (pos2 == length2) {
			answer.appendCopy(this, pos1, length1);
		}
		return answer;
	}

	@Override
	public StorageArray ior(StorageArray other_) {
		// Size cannot decrease, no possible conversion
		// FIXME can accept MewingBitmap
		final RoaringArray other = ensureType(other_);

		int pos1 = 0, pos2 = 0;
		int length1 = size();
		final int length2 = other.size();
		main: if (pos1 < length1 && pos2 < length2) {
			short s1 = getKeyAtIndex(pos1);
			short s2 = other.getKeyAtIndex(pos2);

			while (true) {
				if (s1 == s2) {
					setContainerAtIndex(pos1, getContainerAtIndex(pos1)
						.ior(other.getContainerAtIndex(pos2)));
					pos1++;
					pos2++;
					if ((pos1 == length1) || (pos2 == length2)) {
						break main;
					}
					s1 = getKeyAtIndex(pos1);
					s2 = other.getKeyAtIndex(pos2);
				} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
					pos1++;
					if (pos1 == length1) {
						break main;
					}
					s1 = getKeyAtIndex(pos1);
				} else { // s1 > s2
					insertNewKeyValueAt(pos1, s2,
						other.getContainerAtIndex(pos2).clone());
					pos1++;
					length1++;
					pos2++;
					if (pos2 == length2) {
						break main;
					}
					s2 = other.getKeyAtIndex(pos2);
				}
			}
		}
		if (pos1 == length1) {
			appendCopy(other, pos2, length2);
		}

		return this;
	}

	@Override
	public StorageArray iandNot(final StorageArray other_) {
		final RoaringArray other = ensureType(other_);

		int pos1 = 0, pos2 = 0, intersectionSize = 0;
		final int length1 = this.size(), length2 = other.size();

		while (pos1 < length1 && pos2 < length2) {
			final short s1 = this.getKeyAtIndex(pos1);
			final short s2 = other.getKeyAtIndex(pos2);
			if (s1 == s2) {
				final Container c1 = this.getContainerAtIndex(pos1);
				final Container c2 = other.getContainerAtIndex(pos2);
				final Container c = c1.iandNot(c2);
				if (c.getCardinality() > 0) {
					this.replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
				}
				++pos1;
				++pos2;
			} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
				if (pos1 != intersectionSize) {
					final Container c1 = this.getContainerAtIndex(pos1);
					this.replaceKeyAndContainerAtIndex(intersectionSize, s1, c1);
				}
				++intersectionSize;
				++pos1;
			} else { // s1 > s2
				pos2 = other.advanceUntil(s1, pos2);
			}
		}
		if (pos1 < length1) {
			this.copyRange(pos1, length1, intersectionSize);
			intersectionSize += length1 - pos1;
		}
		this.resize(intersectionSize);

		// FIXME conversion may be need
		return this;
	}

	@Override
	public StorageArray andNot(final StorageArray other_) {
		final RoaringArray other = ensureType(other_);

		final RoaringArray answer = new RoaringArray();
		int pos1 = 0, pos2 = 0;
		final int length1 = this.size(), length2 = other.size();

		while (pos1 < length1 && pos2 < length2) {
			final short s1 = this.getKeyAtIndex(pos1);
			final short s2 = other.getKeyAtIndex(pos2);
			if (s1 == s2) {
				final Container c1 = this.getContainerAtIndex(pos1);
				final Container c2 = other.getContainerAtIndex(pos2);
				final Container c = c1.andNot(c2);
				if (c.getCardinality() > 0) {
					answer.append(s1, c);
				}
				++pos1;
				++pos2;
			} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
				final int nextPos1 = this.advanceUntil(s2, pos1);
				answer.appendCopy(this, pos1, nextPos1);
				pos1 = nextPos1;
			} else { // s1 > s2
				pos2 = other.advanceUntil(s1, pos2);
			}
		}
		if (pos2 == length2) {
			answer.appendCopy(this, pos1, length1);
		}

		// FIXME may need to be convert to a MewingArray
		return answer;
	}

	@Override
	public StorageArray iadd(int x) {
		// Size cannot decrease, no possible conversion

		final short hb = Util.highbits(x);
		final int i = this.getIndex(hb);
		if (i >= 0) {
			this.setContainerAtIndex(i,
				this.getContainerAtIndex(i).add(Util.lowbits(x)));
		} else {
			final ArrayContainer newac = new ArrayContainer();
			this.insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
		}

		return this;
	}

	@Override
	public StorageArray iadd(long rangeStart, long rangeEnd) {
		// Size can decrease, no possible conversion

		final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
		final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
		final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
		final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));
		for (int hb = hbStart; hb <= hbLast; ++hb) {

			// first container may contain partial range
			final int containerStart = (hb == hbStart) ? lbStart : 0;
			// last container may contain partial range
			final int containerLast = (hb == hbLast) ? lbLast : Util.maxLowBitAsInteger();
			final int i = getIndex((short) hb);

			if (i >= 0) {
				final Container c =
					getContainerAtIndex(i).iadd(containerStart, containerLast + 1);
				setContainerAtIndex(i, c);
			} else {
				insertNewKeyValueAt(-i - 1, (short) hb,
					Container.rangeOfOnes(containerStart, containerLast + 1));
			}
		}

		return this;
	}

	@Override
	public StorageArray icheckedAdd(int x, boolean[] check) {
		// Size cannot decrease, no possible conversion

		final short hb = Util.highbits(x);
		final int i = getIndex(hb);
		if (i >= 0) {
			Container c = getContainerAtIndex(i);
			int oldCard = c.getCardinality();
			// we need to keep the newContainer if a switch between containers type
			// occur, in order to get the new cardinality
			Container newCont = c.add(Util.lowbits(x));
			setContainerAtIndex(i, newCont);
			check[0] = newCont.getCardinality() > oldCard;
		} else {
			final ArrayContainer newac = new ArrayContainer();
			insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
			check[0] = true;
		}

		return this;
	}

	@Override
	public StorageArray add(int... values) {
		// Size cannot reduce, no possible conversion

		Container currentcont = null;
		short currenthb = 0;
		int currentcontainerindex = 0;
		int j = 0;
		if (j < values.length) {
			final int val = values[j];
			currenthb = Util.highbits(val);
			currentcontainerindex = getIndex(currenthb);
			if (currentcontainerindex >= 0) {
				currentcont = getContainerAtIndex(currentcontainerindex);
				Container newcont = currentcont.add(Util.lowbits(val));
				if (newcont != currentcont) {
					setContainerAtIndex(currentcontainerindex, newcont);
					currentcont = newcont;
				}
			} else {
				currentcontainerindex = -currentcontainerindex - 1;
				final ArrayContainer newac = new ArrayContainer();
				currentcont = newac.add(Util.lowbits(val));
				insertNewKeyValueAt(currentcontainerindex, currenthb, currentcont);
			}
			j++;
		}
		for (; j < values.length; ++j) {
			int val = values[j];
			short newhb = Util.highbits(val);
			if (currenthb == newhb) {// easy case
				// this could be quite frequent
				Container newcont = currentcont.add(Util.lowbits(val));
				if (newcont != currentcont) {
					setContainerAtIndex(currentcontainerindex, newcont);
					currentcont = newcont;
				}
			} else {
				currenthb = newhb;
				currentcontainerindex = getIndex(currenthb);
				if (currentcontainerindex >= 0) {
					currentcont = getContainerAtIndex(currentcontainerindex);
					Container newcont = currentcont.add(Util.lowbits(val));
					if (newcont != currentcont) {
						setContainerAtIndex(currentcontainerindex, newcont);
						currentcont = newcont;
					}
				} else {
					currentcontainerindex = -currentcontainerindex - 1;
					final ArrayContainer newac = new ArrayContainer();
					currentcont = newac.add(Util.lowbits(val));
					insertNewKeyValueAt(currentcontainerindex, currenthb, currentcont);
				}
			}
		}

		return this;
	}

	@Override
	public StorageArray iremove(final int x) {
		final short hb = Util.highbits(x);
		final int i = getIndex(hb);
		if (i < 0) {
			return this;
		}

		setContainerAtIndex(i, getContainerAtIndex(i).remove(Util.lowbits(x)));
		if (getContainerAtIndex(i).getCardinality() == 0) {
			removeAtIndex(i);
		}

		// FIXME conversion may be needed
		return null;
	}

	@Override
	public StorageArray iremove(final long rangeStart, final long rangeEnd) {
		final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
		final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
		final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
		final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));
		if (hbStart == hbLast) {
			final int i = getIndex((short) hbStart);
			if (i < 0) {
				return this;
			}
			final Container c = getContainerAtIndex(i).iremove(lbStart, lbLast + 1);
			if (c.getCardinality() > 0) {
				setContainerAtIndex(i, c);
			} else {
				removeAtIndex(i);
			}
			return null;
		}
		int ifirst = getIndex((short) hbStart);
		int ilast = getIndex((short) hbLast);
		if (ifirst >= 0) {
			if (lbStart != 0) {
				final Container c = getContainerAtIndex(ifirst).iremove(lbStart,
					Util.maxLowBitAsInteger() + 1);
				if (c.getCardinality() > 0) {
					setContainerAtIndex(ifirst, c);
					ifirst++;
				}
			}
		} else {
			ifirst = -ifirst - 1;
		}
		if (ilast >= 0) {
			if (lbLast != Util.maxLowBitAsInteger()) {
				final Container c = getContainerAtIndex(ilast).iremove(0, lbLast + 1);
				if (c.getCardinality() > 0) {
					setContainerAtIndex(ilast, c);
				} else {
					ilast++;
				}
			} else {
				ilast++;
			}
		} else {
			ilast = -ilast - 1;
		}
		removeIndexRange(ifirst, ilast);

		// FIXME conversion may be needed
		return null;
	}

	@Override
	public StorageArray remove(final long rangeStart, final long rangeEnd) {
		final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
		final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
		final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
		final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));

		final RoaringArray answer = new RoaringArray();
		answer.appendCopiesUntil(this, (short) hbStart);

		if (hbStart == hbLast) {
			final int i = this.getIndex((short) hbStart);
			if (i >= 0) {
				final Container c = this.getContainerAtIndex(i).remove(lbStart, lbLast + 1);
				if (c.getCardinality() > 0) {
					answer.append((short) hbStart, c);
				}
			}
			answer.appendCopiesAfter(this, (short) hbLast);
			return answer;
		}
		int ifirst = this.getIndex((short) hbStart);
		int ilast = this.getIndex((short) hbLast);
		if ((ifirst >= 0) && (lbStart != 0)) {
			final Container c = this.getContainerAtIndex(ifirst).remove(lbStart,
					Util.maxLowBitAsInteger() + 1);
			if (c.getCardinality() > 0) {
				answer.append((short) hbStart, c);
			}
		}
		if ((ilast >= 0) && (lbLast != Util.maxLowBitAsInteger())) {
			final Container c = this.getContainerAtIndex(ilast).remove(0, lbLast + 1);
			if (c.getCardinality() > 0) {
				answer.append((short) hbLast, c);
			}
		}
		answer.appendCopiesAfter(this, (short) hbLast);

		// FIXME a conversion may be needed
		return answer;
	}

	@Override
	public boolean runOptimize() {
		boolean answer = false;
		for (int i = 0; i < size(); i++) {
			Container c = getContainerAtIndex(i).runOptimize();
			if (c instanceof RunContainer) {
				answer = true;
			}
			setContainerAtIndex(i, c);
		}
		return answer;
	}

	@Override
	public boolean removeRunCompression() {
		boolean answer = false;
		for (int i = 0; i < size(); i++) {
			Container c = getContainerAtIndex(i);
			if (c instanceof RunContainer) {
				Container newc = ((RunContainer) c).toBitmapOrArrayContainer(c.getCardinality());
				setContainerAtIndex(i, newc);
				answer = true;
			}
		}

		return answer;
	}

	@Override
	public StorageArray flip(final long rangeStart, final long rangeEnd) {
		// FIXME the result may need to be converted
		final RoaringArray answer = new RoaringArray();

		final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
		final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
		final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
		final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));

		// copy the containers before the active area
		answer.appendCopiesUntil(this, (short) hbStart);

		for (int hb = hbStart; hb <= hbLast; ++hb) {
			final int containerStart = (hb == hbStart) ? lbStart : 0;
			final int containerLast = (hb == hbLast) ? lbLast : Util.maxLowBitAsInteger();

			final int i = this.getIndex((short) hb);
			final int j = answer.getIndex((short) hb);
			assert j < 0;

			if (i >= 0) {
				Container c =
					this.getContainerAtIndex(i).not(containerStart, containerLast + 1);
				if (c.getCardinality() > 0) {
					answer.insertNewKeyValueAt(-j - 1, (short) hb, c);
				}

			} else { // *think* the range of ones must never be
				// empty.
				answer.insertNewKeyValueAt(-j - 1, (short) hb,
					Container.rangeOfOnes(containerStart, containerLast + 1));
			}
		}
		// copy the containers after the active area.
		answer.appendCopiesAfter(this, (short) hbLast);
		return answer;
	}

	@Override
	public boolean intersects(final StorageArray other_) {
		final RoaringArray other = ensureType(other_);

		final int length1 = this.size(), length2 = other.size();
		int pos1 = 0, pos2 = 0;

		while (pos1 < length1 && pos2 < length2) {
			final short s1 = this.getKeyAtIndex(pos1);
			final short s2 = other.getKeyAtIndex(pos2);
			if (s1 == s2) {
				final Container c1 = this.getContainerAtIndex(pos1);
				final Container c2 = other.getContainerAtIndex(pos2);
				if (c1.intersects(c2)) {
					return true;
				}
				++pos1;
				++pos2;
			} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
				pos1 = this.advanceUntil(s2, pos1);
			} else { // s1 > s2
				pos2 = other.advanceUntil(s1, pos2);
			}
		}

		return false;
	}

	// important: inputs should not have been computed lazily
	protected static RoaringArray lazyor(final RoaringArray x1, final RoaringArray x2) {
		final RoaringArray answer = new RoaringArray();
		int pos1 = 0, pos2 = 0;
		final int length1 = x1.size(), length2 = x2.size();
		main:
		if (pos1 < length1 && pos2 < length2) {
			short s1 = x1.getKeyAtIndex(pos1);
			short s2 = x2.getKeyAtIndex(pos2);

			while (true) {
				if (s1 == s2) {
					answer.append(s1, x1.getContainerAtIndex(pos1)
						.lazyOR(x2.getContainerAtIndex(pos2)));
					pos1++;
					pos2++;
					if ((pos1 == length1) || (pos2 == length2)) {
						break main;
					}
					s1 = x1.getKeyAtIndex(pos1);
					s2 = x2.getKeyAtIndex(pos2);
				} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
					answer.appendCopy(x1, pos1);
					pos1++;
					if (pos1 == length1) {
						break main;
					}
					s1 = x1.getKeyAtIndex(pos1);
				} else { // s1 > s2
					answer.appendCopy(x2, pos2);
					pos2++;
					if (pos2 == length2) {
						break main;
					}
					s2 = x2.getKeyAtIndex(pos2);
				}
			}
		}
		if (pos1 == length1) {
			answer.appendCopy(x2, pos2, length2);
		} else if (pos2 == length2) {
			answer.appendCopy(x1, pos1, length1);
		}
		return answer;
	}

	// important: inputs should not be reused
	protected static RoaringArray lazyorfromlazyinputs(final RoaringArray x1,
	                                                   final RoaringArray x2) {
		final RoaringArray answer = new RoaringArray();
		int pos1 = 0, pos2 = 0;
		final int length1 = x1.size(), length2 = x2.size();
		main:
		if (pos1 < length1 && pos2 < length2) {
			short s1 = x1.getKeyAtIndex(pos1);
			short s2 = x2.getKeyAtIndex(pos2);

			while (true) {
				if (s1 == s2) {
					Container c1 = x1.getContainerAtIndex(pos1);
					Container c2 = x2.getContainerAtIndex(pos2);
					if ((c2 instanceof BitmapContainer) && (!(c1 instanceof BitmapContainer))) {
						Container tmp = c1;
						c1 = c2;
						c2 = tmp;
					}
					answer.append(s1, c1.lazyIOR(c2));
					pos1++;
					pos2++;
					if ((pos1 == length1) || (pos2 == length2)) {
						break main;
					}
					s1 = x1.getKeyAtIndex(pos1);
					s2 = x2.getKeyAtIndex(pos2);
				} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
					Container c1 = x1.getContainerAtIndex(pos1);
					answer.append(s1, c1);
					pos1++;
					if (pos1 == length1) {
						break main;
					}
					s1 = x1.getKeyAtIndex(pos1);
				} else { // s1 > s2
					Container c2 = x2.getContainerAtIndex(pos2);
					answer.append(s2, c2);
					pos2++;
					if (pos2 == length2) {
						break main;
					}
					s2 = x2.getKeyAtIndex(pos2);
				}
			}
		}
		if (pos1 == length1) {
			answer.append(x2, pos2, length2);
		} else if (pos2 == length2) {
			answer.append(x1, pos1, length1);
		}
		return answer;
	}

	@Override
	public StorageArray icheckedRemove(int x, boolean[] result) {
		final short hb = Util.highbits(x);
		final int i = getIndex(hb);
		if (i < 0) {
			result[0] = false;
			return this;
		}

		Container C = getContainerAtIndex(i);
		int oldcard = C.getCardinality();
		C.remove(Util.lowbits(x));
		int newcard = C.getCardinality();
		if (newcard == oldcard) {
			result[0] = false;
			return this;
		}

		result[0] = true;
		if (newcard > 0) {
			setContainerAtIndex(i, C);
		} else {
			removeAtIndex(i);
		}
		// FIXME conversion may be needed
		return this;
	}

	@Override
	public void trim() {
		for (int i = 0; i < size(); i++) {
			getContainerAtIndex(i).trim();
		}
	}

	public StorageArray xor(final StorageArray other_) {
		// FIXME a conversion may be required
		final RoaringArray other = ensureType(other_);

		final RoaringArray answer = new RoaringArray();
		int pos1 = 0, pos2 = 0;
		final int length1 = this.size(), length2 = other.size();

		main:
		if (pos1 < length1 && pos2 < length2) {
			short s1 = this.getKeyAtIndex(pos1);
			short s2 = other.getKeyAtIndex(pos2);

			while (true) {
				if (s1 == s2) {
					final Container c = this.getContainerAtIndex(pos1)
						.xor(other.getContainerAtIndex(pos2));
					if (c.getCardinality() > 0) {
						answer.append(s1, c);
					}
					pos1++;
					pos2++;
					if ((pos1 == length1) || (pos2 == length2)) {
						break main;
					}
					s1 = this.getKeyAtIndex(pos1);
					s2 = other.getKeyAtIndex(pos2);
				} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
					answer.appendCopy(this, pos1);
					pos1++;
					if (pos1 == length1) {
						break main;
					}
					s1 = this.getKeyAtIndex(pos1);
				} else { // s1 > s2
					answer.appendCopy(other, pos2);
					pos2++;
					if (pos2 == length2) {
						break main;
					}
					s2 = other.getKeyAtIndex(pos2);
				}
			}
		}
		if (pos1 == length1) {
			answer.appendCopy(other, pos2, length2);
		} else if (pos2 == length2) {
			answer.appendCopy(this, pos1, length1);
		}

		return answer;
	}

	public StorageArray iflip(final int x) {
		final short hb = Util.highbits(x);
		final int i = getIndex(hb);
		if (i >= 0) {
			Container c = getContainerAtIndex(i).flip(Util.lowbits(x));
			if (c.getCardinality() > 0) {
				setContainerAtIndex(i, c);
			} else {
				removeAtIndex(i);
			}
		} else {
			final ArrayContainer newac = new ArrayContainer();
			insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
		}

		// FIXME conversion may be needed
		return this;
	}

	public StorageArray iflip(final long rangeStart, final long rangeEnd) {
		final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
		final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
		final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
		final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));

		// TODO:this can be accelerated considerably
		for (int hb = hbStart; hb <= hbLast; ++hb) {
			// first container may contain partial range
			final int containerStart = (hb == hbStart) ? lbStart : 0;
			// last container may contain partial range
			final int containerLast = (hb == hbLast) ? lbLast : Util.maxLowBitAsInteger();
			final int i = getIndex((short) hb);

			if (i >= 0) {
				final Container c =
					getContainerAtIndex(i).inot(containerStart, containerLast + 1);
				if (c.getCardinality() > 0) {
					setContainerAtIndex(i, c);
				} else {
					removeAtIndex(i);
				}
			} else {
				insertNewKeyValueAt(-i - 1, (short) hb,
					Container.rangeOfOnes(containerStart, containerLast + 1));
			}
		}

		// FIXME conversion needed
		return this;
	}

	@Override
	public StorageArray ixor(final StorageArray other_) {
		final RoaringArray other = ensureType(other_);

		int pos1 = 0, pos2 = 0;
		int length1 = size();
		final int length2 = other.size();

		main:
		if (pos1 < length1 && pos2 < length2) {
			short s1 = getKeyAtIndex(pos1);
			short s2 = other.getKeyAtIndex(pos2);

			while (true) {
				if (s1 == s2) {
					final Container c = getContainerAtIndex(pos1)
						.ixor(other.getContainerAtIndex(pos2));
					if (c.getCardinality() > 0) {
						this.setContainerAtIndex(pos1, c);
						pos1++;
					} else {
						removeAtIndex(pos1);
						--length1;
					}
					pos2++;
					if ((pos1 == length1) || (pos2 == length2)) {
						break main;
					}
					s1 = getKeyAtIndex(pos1);
					s2 = other.getKeyAtIndex(pos2);
				} else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
					pos1++;
					if (pos1 == length1) {
						break main;
					}
					s1 = getKeyAtIndex(pos1);
				} else { // s1 > s2
					insertNewKeyValueAt(pos1, s2,
						other.getContainerAtIndex(pos2).clone());
					pos1++;
					length1++;
					pos2++;
					if (pos2 == length2) {
						break main;
					}
					s2 = other.getKeyAtIndex(pos2);
				}
			}
		}
		if (pos1 == length1) {
			appendCopy(other, pos2, length2);
		}

		// FIXME conversion may be needed
		return null;
	}

	@Override
	public StorageArray limit(int maxCardinality) {
		final RoaringArray answer = new RoaringArray();

		for (int i = 0, currentCardinality = 0;
		     (currentCardinality < maxCardinality) && (i < size());
		     i++) {
			final Container c = getContainerAtIndex(i);
			if (c.getCardinality() + currentCardinality <= maxCardinality) {
				answer.appendCopy(this, i);
				currentCardinality += c.getCardinality();
			} else {
				int leftover = maxCardinality - currentCardinality;
				final Container limited = c.limit(leftover);
				answer.append(getKeyAtIndex(i), limited);
				break;
			}
		}

		// FIXME conversion may be needed
		return answer;
	}

	@Override
	public StorageArray selectRangeWithoutCopy(
		final long rangeStart,
		final long rangeEnd) {
		final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
		final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
		final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
		final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));
		final RoaringArray answer = new RoaringArray();

		if (hbStart == hbLast) {
			final int i = getIndex((short) hbStart);
			if (i >= 0) {
				final Container c = getContainerAtIndex(i).remove(0, lbStart)
					.iremove(lbLast + 1, Util.maxLowBitAsInteger() + 1);
				if (c.getCardinality() > 0) {
					answer.append((short) hbStart, c);
				}
			}
			return answer;
		}
		int ifirst = getIndex((short) hbStart);
		int ilast = getIndex((short) hbLast);
		if (ifirst >= 0) {
			final Container c = getContainerAtIndex(ifirst).remove(0, lbStart);
			if (c.getCardinality() > 0) {
				answer.append((short) hbStart, c);
			}
		}

		// revised to loop on ints
		for (int hb = hbStart + 1; hb <= hbLast - 1; ++hb) {
			final int i = getIndex((short) hb);
			final int j = answer.getIndex((short) hb);
			assert j < 0;

			if (i >= 0) {
				final Container c = getContainerAtIndex(i);
				answer.insertNewKeyValueAt(-j - 1, (short) hb, c);
			}
		}

		if (ilast >= 0) {
			final Container c = getContainerAtIndex(ilast).remove(lbLast + 1,
				Util.maxLowBitAsInteger() + 1);
			if (c.getCardinality() > 0) {
				answer.append((short) hbLast, c);
			}
		}
		return answer;
	}

	@Override
	public int[] toArray() {
		final int[] array = new int[this.getCardinality()];
		int pos = 0, pos2 = 0;
		while (pos < size()) {
			final int hs = getKeyAtIndex(pos) << 16;
			Container c = getContainerAtIndex(pos++);
			c.fillLeastSignificant16bits(array, pos2, hs);
			pos2 += c.getCardinality();
		}
		return array;
	}

	private static RoaringArray ensureType(StorageArray other_) {
		assert other_ instanceof RoaringArray : "Only performing this operation with RoaringArray. Got " + other_;
		return (RoaringArray) other_;
	}

	/**
	 * iterate over the positions of the true values.
	 *
	 * @return the iterator
	 */
	@Override
	public Iterator<Integer> iterator() {
		return new Iterator<Integer>() {
			private int hs = 0;

			private ShortIterator iter;

			private int pos = 0;

			private int x;

			@Override
			public boolean hasNext() {
				return pos < size();
			}

			private Iterator<Integer> init() {
				if (pos < size()) {
					iter = getContainerAtIndex(pos).getShortIterator();
					hs = getKeyAtIndex(pos) << 16;
				}
				return this;
			}

			@Override
			public Integer next() {
				x = iter.nextAsInt() | hs;
				if (!iter.hasNext()) {
					++pos;
					init();
				}
				return x;
			}

			@Override
			public void remove() {
				if ((x & hs) == hs) {// still in same container
					iter.remove();
				} else {
					// FIXME need a connection to the RoaringBitmap in case a conversion is required
//					remove(x);
				}
			}

		}.init();
	}
}
