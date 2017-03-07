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

/**
 * @author QuartetFS
 */
public interface StorageArray extends Cloneable, Iterable<Integer>, Externalizable {

	StorageArray clone() throws CloneNotSupportedException;

	int size();

	long getLongCardinality();

	int getCardinality();

	long getLongSizeInBytes();

	int getSizeInBytes();

	/**
	 * Check whether this bitmap has had its runs compressed.
	 *
	 * @return whether this bitmap has run compression
	 */
	boolean hasRunCompression();

	PeekableIntIterator getIntIterator();

	IntIterator getReverseIntIterator();

	boolean contains(int x);

	void forEach(IntConsumer ic);

	int select(int i);

	long rank(int x);

	StorageArray iadd(int x);

	/**
	 * Adds in place all integers in [rangeStart,rangeEnd).
	 * <p>
	 *   This array is modified or a new instance may be generated.
	 * </p>
	 *
	 * @param rangeStart inclusive beginning of range
	 * @param rangeEnd exclusive ending of range
	 * @return updated array
	 */
	StorageArray iadd(long rangeStart, long rangeEnd);

	StorageArray icheckedAdd(int x, boolean[] result);

	/**
	 * Generate a new bitmap with all integers in [rangeStart,rangeEnd) added.
	 * <p>
	 *   This array is not modified by the operation.
	 * </p>
	 *
	 * @param rangeStart inclusive beginning of range
	 * @param rangeEnd exclusive ending of range
	 * @return new array
	 */
	StorageArray add(long rangeStart, long rangeEnd);

	StorageArray iremove(int x);

	StorageArray remove(long rangeStart, long rangeEnd);

	StorageArray iremove(long rangeStart, long rangeEnd);

	StorageArray icheckedRemove(int x, boolean[] result);

	/**
	 * Recover allocated but unused memory.
	 */
	void trim();

	/**
	 * Use a run-length encoding where it is more space efficient
	 *
	 * @return whether a change was applied
	 */
	boolean runOptimize();

	/**
	 * Remove run-length encoding even when it is more space efficient
	 *
	 * @return whether a change was applied
	 */
	boolean removeRunCompression();

	StorageArray and(StorageArray other);

	StorageArray iand(StorageArray other);

	/**
	 * Cardinality of Bitwise AND (intersection) operation. The provided bitmaps are *not* modified.
	 * This operation is thread-safe as long as the provided bitmaps remain unchanged.
	 *
	 * @param other other array to consider
	 * @return as if you did and(x2,x2).getCardinality()
	 * @see FastAggregation#and(RoaringBitmap...)
	 */
	int andCardinality(StorageArray other);

	StorageArray or(StorageArray other);

	StorageArray ior(StorageArray other);

	StorageArray iandNot(final StorageArray other);

	/**
	 * Bitwise ANDNOT (difference) operation. The provided bitmaps are *not* modified. This operation
	 * is thread-safe as long as the provided bitmaps remain unchanged.
	 *
	 * @param other other array to consider
	 * @return result of the operation
	 */
	StorageArray andNot(StorageArray other);

	/**
	 * Set all the specified values  to true. This can be expected to be slightly
	 * faster than calling "add" repeatedly. The provided integers values don't
	 * have to be in sorted order, but it may be preferable to sort them from a performance point of
	 * view.
	 *
	 * @param values new set values
	 * @return modified array, may be a new structure if needed
	 */
	StorageArray add(int... values);

	/**
	 * Complements the bits in the given range, from rangeStart (inclusive) rangeEnd (exclusive). The
	 * given bitmap is unchanged.
	 *
	 * @param rangeStart inclusive beginning of range, in [0, 0xffffffff]
	 * @param rangeEnd exclusive ending of range, in [0, 0xffffffff + 1]
	 * @return a new array
	 */
	StorageArray flip(long rangeStart, long rangeEnd);

	/**
	 * Checks whether the two bitmaps intersect. This can be much faster than calling "and" and
	 * checking the cardinality of the result.
	 *
	 * @param other other array to consider
	 * @return true if they intersect
	 */
	boolean intersects(StorageArray other);

	StorageArray xor(StorageArray other);

	StorageArray iflip(int x);

	StorageArray iflip(long rangeStart, long rangeEnd);

	StorageArray ixor(StorageArray other);

	StorageArray limit(int maxCardinality);

	StorageArray selectRangeWithoutCopy(long rangeStart, long rangeEnd);

	int[] toArray();

	void serialize(DataOutput out) throws IOException;

	void deserialize(DataInput in) throws IOException;

	/**
	 * Report the number of bytes required to serialize this bitmap. This is the number of bytes
	 * written out when using the serialize method. When using the writeExternal method, the count
	 * will be higher due to the overhead of Java serialization.
	 *
	 * @return the size in bytes
	 */
	int serializedSizeInBytes();
}
