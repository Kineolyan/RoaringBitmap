/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MappeableContainerPointer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.naming.OperationNotSupportedException;
import java.io.*;
import java.util.Iterator;


/**
 * RoaringBitmap, a compressed alternative to the BitSet.
 *
 * <pre>
 * {@code
 *      import org.roaringbitmap.*;
 *
 *      //...
 *
 *      RoaringBitmap rr = RoaringBitmap.bitmapOf(1,2,3,1000);
 *      RoaringBitmap rr2 = new RoaringBitmap();
 *      for(int k = 4000; k<4255;++k) rr2.add(k);
 *      RoaringBitmap rror = RoaringBitmap.or(rr, rr2);
 *      
 *      //...
 *      DataOutputStream wheretoserialize = ...
 *      rr.runOptimize(); // can help compression 
 *      rr.serialize(wheretoserialize);
 * }
 * </pre>
 * 
 * Integers are added in unsigned sorted order. That is, they are treated as unsigned integers (see
 * Java 8's Integer.toUnsignedLong function).
 *
 * Bitmaps are limited to a maximum of Integer.MAX_VALUE entries. Trying to create larger bitmaps
 * could result in undefined behaviors.
 *
 *
 */


public class RoaringBitmap implements Cloneable, Serializable, Iterable<Integer>, Externalizable,
    ImmutableBitmapDataProvider {

  private static final long serialVersionUID = 6L;

	/**
   * Generate a new bitmap with all integers in [rangeStart,rangeEnd) added.
   * 
   * @param rb initial bitmap (will not be modified)
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new bitmap
   */
  public static RoaringBitmap add(RoaringBitmap rb, final long rangeStart, final long rangeEnd) {
    Util.rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return rb.clone(); // empty range
    }

    final StorageArray newHighLowContainer = rb.highLowContainer.add(rangeStart, rangeEnd);
    return new RoaringBitmap(newHighLowContainer);
  }

  /** 
   *
   * Generate a new bitmap with all integers in [rangeStart,rangeEnd) added.
   * 
   * @param rb initial bitmap (will not be modified)
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new bitmap
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
    public static RoaringBitmap add(RoaringBitmap rb, final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      return add(rb, (long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    return add(rb, rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL); 
  }
  
  /**
   * Bitwise AND (intersection) operation. The provided bitmaps are *not* modified. This operation
   * is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * If you have more than 2 bitmaps, consider using the FastAggregation class.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   * @see FastAggregation#and(RoaringBitmap...)
   */
  public static RoaringBitmap and(final RoaringBitmap x1, final RoaringBitmap x2) {
    return new RoaringBitmap(and(x1.highLowContainer, x2.highLowContainer));
  }

  static StorageArray and(final StorageArray a1, final StorageArray a2) {
	  // TODO move this to some dedicated class
  	return null;
  }

  /**
   * Cardinality of Bitwise AND (intersection) operation. The provided bitmaps are *not* modified.
   * This operation is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return as if you did and(x2,x2).getCardinality()
   * @see FastAggregation#and(RoaringBitmap...)
   */
  protected static int andCardinality(final RoaringBitmap x1, final RoaringBitmap x2) {
  	// FIXME code this dispatch
    return -1;
  }

  /**
   * Bitwise ANDNOT (difference) operation. The provided bitmaps are *not* modified. This operation
   * is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   */
  public static RoaringBitmap andNot(final RoaringBitmap x1, final RoaringBitmap x2) {
	  // FIXME code this dispatch
	  return null;
  }

  /**
   * Set all the specified values  to true. This can be expected to be slightly
   * faster than calling "add" repeatedly. The provided integers values don't
   * have to be in sorted order, but it may be preferable to sort them from a performance point of
   * view.
   *
   * @param dat set values
   */
  public void add(final int... dat) {
  	this.highLowContainer = highLowContainer.add(dat);
  }

  /**
   * Generate a bitmap with the specified values set to true. The provided integers values don't
   * have to be in sorted order, but it may be preferable to sort them from a performance point of
   * view.
   *
   * @param dat set values
   * @return a new bitmap
   */
  public static RoaringBitmap bitmapOf(final int... dat) {
    final RoaringBitmap ans = new RoaringBitmap();
    ans.add(dat);
    return ans;
  }

  /**
   * Complements the bits in the given range, from rangeStart (inclusive) rangeEnd (exclusive). The
   * given bitmap is unchanged.
   *
   * @param bm bitmap being negated
   * @param rangeStart inclusive beginning of range, in [0, 0xffffffff]
   * @param rangeEnd exclusive ending of range, in [0, 0xffffffff + 1]
   * @return a new Bitmap
   */
  public static RoaringBitmap flip(RoaringBitmap bm, final long rangeStart, final long rangeEnd) {
    Util.rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return bm.clone();
    }

    return new RoaringBitmap(bm.highLowContainer.flip(rangeStart, rangeEnd));
  }

  /**
   * Complements the bits in the given range, from rangeStart (inclusive) rangeEnd (exclusive). The
   * given bitmap is unchanged.
   *
   * @param rb bitmap being negated
   * @param rangeStart inclusive beginning of range, in [0, 0xffffffff]
   * @param rangeEnd exclusive ending of range, in [0, 0xffffffff + 1]
   * @return a new Bitmap
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
    public static RoaringBitmap flip(RoaringBitmap rb, final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      return flip(rb, (long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    return flip(rb, rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL); 
  }

  /**
   * Checks whether the two bitmaps intersect. This can be much faster than calling "and" and
   * checking the cardinality of the result.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return true if they intersect
   */
  public static boolean intersects(final RoaringBitmap x1, final RoaringBitmap x2) {
	  // FIXME code this dispatch
	  return false;
  }

  /**
   * Compute overall OR between bitmaps.
   *
   * (Effectively calls {@link FastAggregation#or})
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap or(Iterator<RoaringBitmap> bitmaps) {
    return FastAggregation.or(bitmaps);
  }

  /**
   * Compute overall OR between bitmaps.
   * 
   * (Effectively calls {@link FastAggregation#or})
   * 
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap or(RoaringBitmap... bitmaps) {
    return FastAggregation.or(bitmaps);
  }

  /**
   * Bitwise OR (union) operation. The provided bitmaps are *not* modified. This operation is
   * thread-safe as long as the provided bitmaps remain unchanged.
   *
   * If you have more than 2 bitmaps, consider using the FastAggregation class.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   * @see FastAggregation#or(RoaringBitmap...)
   * @see FastAggregation#horizontal_or(RoaringBitmap...)
   */
  public static RoaringBitmap or(final RoaringBitmap x1, final RoaringBitmap x2) {
    // FIXME code this dispatch
	  return null;
  }

  /**
   * Cardinality of the bitwise OR (union) operation. The provided bitmaps are *not* modified. This
   * operation is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * If you have more than 2 bitmaps, consider using the FastAggregation class.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return cardinality of the union
   * @see FastAggregation#or(RoaringBitmap...)
   * @see FastAggregation#horizontal_or(RoaringBitmap...)
   */
  public static int orCardinality(final RoaringBitmap x1, final RoaringBitmap x2) {
    // we use the fact that the cardinality of the bitmaps is known so that
    // the union is just the total cardinality minus the intersection
    return x1.getCardinality() + x2.getCardinality() - andCardinality(x1, x2);
  }


  /**
   * Generate a new bitmap with all integers in [rangeStart,rangeEnd) removed.
   * 
   * @param rb initial bitmap (will not be modified)
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new bitmap
   */
  public static RoaringBitmap remove(RoaringBitmap rb, final long rangeStart, final long rangeEnd) {
	  Util.rangeSanityCheck(rangeStart, rangeEnd);
	  if (rangeStart >= rangeEnd) {
		  return rb.clone(); // empty range
	  }

  	// FIXME code this dispatch
    return null;
  }

  /**
   * Generate a new bitmap with all integers in [rangeStart,rangeEnd) removed.
   * 
   * @param rb initial bitmap (will not be modified)
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new bitmap
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
    public static RoaringBitmap remove(RoaringBitmap rb, final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      return remove(rb, (long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    return remove(rb, rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL); 
  }

  /**
   * Bitwise XOR (symmetric difference) operation. The provided bitmaps are *not* modified. This
   * operation is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * If you have more than 2 bitmaps, consider using the FastAggregation class.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   * @see FastAggregation#xor(RoaringBitmap...)
   * @see FastAggregation#horizontal_xor(RoaringBitmap...)
   */
  public static RoaringBitmap xor(final RoaringBitmap x1, final RoaringBitmap x2) {
    // FIXME code this dispatch
	  return null;
  }

  StorageArray highLowContainer = null;

  /**
   * Create an empty bitmap
   */
  public RoaringBitmap() {
    highLowContainer = new RoaringArray();
  }

  public RoaringBitmap(final StorageArray highLowContainer) {
  	this.highLowContainer = highLowContainer;
  }

  /**
   * Create a RoaringBitmap from a MutableRoaringBitmap or ImmutableRoaringBitmap. The source is not
   * modified.
   * 
   * @param rb the original bitmap
   */
  public RoaringBitmap(ImmutableRoaringBitmap rb) {
    if (rb.getCardinality() < MewingArray.DEFAULT_MAX_SIZE) {
      highLowContainer = rb.getCardinality() < MewingArray.DEFAULT_MAX_SIZE ? new MewingArray() : new RoaringArray();
      final MappeableContainerPointer cp = rb.getContainerPointer();
      while (cp.getContainer() != null) {
        // FIXME do something for the container
//        highLowContainer.append(cp.key(), cp.getContainer().toContainer());
        cp.advance();
      }
    } else {
      final RoaringArray array = new RoaringArray();
      final MappeableContainerPointer cp = rb.getContainerPointer();
      while (cp.getContainer() != null) {
        array.append(cp.key(), cp.getContainer().toContainer());
        cp.advance();
      }

      highLowContainer = array;
    }
  }

  /**
   * Add the value to the container (set the value to "true"), whether it already appears or not.
   *
   * @param x integer value
   */
  public void add(final int x) {
  	this.highLowContainer = this.highLowContainer.iadd(x);
  }


  /**
   * Add to the current bitmap all integers in [rangeStart,rangeEnd).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   */
  public void add(final long rangeStart, final long rangeEnd) {
    Util.rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return; // empty range
    }

    this.highLowContainer = this.highLowContainer.iadd(rangeStart, rangeEnd);
  }

  /**
   * Add to the current bitmap all integers in [rangeStart,rangeEnd).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
    public void add(final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      add((long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    add(rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL); 
  }


  /**
   * In-place bitwise AND (intersection) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void and(final RoaringBitmap x2) {
  	// FIXME code this dipatch
	  /* If one of the two is a mewing, use it for and. */
  }


  /**
   * Computes AND between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   */
  public static RoaringBitmap and(@SuppressWarnings("rawtypes") final Iterator bitmaps,
      final long rangeStart, final long rangeEnd) {
    Util.rangeSanityCheck(rangeStart, rangeEnd);

    Iterator<RoaringBitmap> bitmapsIterator;
    bitmapsIterator = selectRangeWithoutCopy(bitmaps, rangeStart, rangeEnd);
    return FastAggregation.and(bitmapsIterator);
  }

  /*
   *     In testing, original int-range code failed an assertion with some negative ranges
   *     so presumably nobody relies on negative ranges. rangeEnd=0 also failed. 
   */

  /**
   * Computes AND between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   * @deprecated use the version where longs specify the range. Negative range end are illegal.
   */
  @Deprecated
    public static RoaringBitmap and(@SuppressWarnings("rawtypes") final Iterator bitmaps, 
      final int rangeStart, final int rangeEnd) {
    return and(bitmaps, (long) rangeStart, (long) rangeEnd);
  }




  /**
   * In-place bitwise ANDNOT (difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void andNot(final RoaringBitmap x2) {
    // FIXME code this dispatch
  }


  /**
   * Bitwise ANDNOT (difference) operation for the given range, rangeStart (inclusive) and rangeEnd
   * (exclusive). The provided bitmaps are *not* modified. This operation is thread-safe as long as
   * the provided bitmaps remain unchanged.
   * 
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @param rangeStart starting point of the range (inclusive)
   * @param rangeEnd end point of the range (exclusive)
   * @return result of the operation
   */
  public static RoaringBitmap andNot(final RoaringBitmap x1, final RoaringBitmap x2, 
      long rangeStart, long rangeEnd) {
    Util.rangeSanityCheck(rangeStart, rangeEnd);

    RoaringBitmap rb1 = selectRangeWithoutCopy(x1, rangeStart, rangeEnd);
    RoaringBitmap rb2 = selectRangeWithoutCopy(x2, rangeStart, rangeEnd);
    return andNot(rb1, rb2);
  }

  /**
   * Bitwise ANDNOT (difference) operation for the given range, rangeStart (inclusive) and rangeEnd
   * (exclusive). The provided bitmaps are *not* modified. This operation is thread-safe as long as
   * the provided bitmaps remain unchanged.
   * 
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @param rangeStart starting point of the range (inclusive)
   * @param rangeEnd end point of the range (exclusive)
   * @return result of the operation
   * 
   * @deprecated use the version where longs specify the range. Negative values for range
   *     endpoints are not allowed.
   */
  @Deprecated
    public static RoaringBitmap andNot(final RoaringBitmap x1, final RoaringBitmap x2, 
          final int rangeStart, final int rangeEnd) {
    return andNot(x1, x2, (long) rangeStart, (long) rangeEnd);
  }

  /**
   * Add the value to the container (set the value to "true"), whether it already appears or not.
   *
   * @param x integer value
   * @return true if the added int wasn't already contained in the bitmap. False otherwise.
   */
  public boolean checkedAdd(final int x) {
  	final boolean[] result = new boolean[1];
  	this.highLowContainer = this.highLowContainer.icheckedAdd(x, result);

	  return result[0];
  }

  /**
   * If present remove the specified integer (effectively, sets its bit value to false)
   *
   * @param x integer value representing the index in a bitmap
   * @return true if the unset bit was already in the bitmap
   */
  public boolean checkedRemove(final int x) {
	  final boolean[] result = new boolean[1];
	  this.highLowContainer = this.highLowContainer.icheckedRemove(x, result);

	  return result[0];
  }

  /**
   * reset to an empty bitmap; result occupies as much space a newly created bitmap.
   */
  public void clear() {
    highLowContainer = new RoaringArray(); // lose references
  }

  @Override
  public RoaringBitmap clone() {
    try {
      final RoaringBitmap x = (RoaringBitmap) super.clone();
      x.highLowContainer = highLowContainer.clone();
      return x;
    } catch (final CloneNotSupportedException e) {
      throw new RuntimeException("shouldn't happen with clone", e);
    }
  }

  /**
   * Checks whether the value in included, which is equivalent to checking if the corresponding bit
   * is set (get in BitSet class).
   *
   * @param x integer value
   * @return whether the integer value is included.
   */
  @Override
  public boolean contains(final int x) {
    return this.highLowContainer.contains(x);
  }


  /**
   * Deserialize (retrieve) this bitmap.
   * See format specification at https://github.com/RoaringBitmap/RoaringFormatSpec
   *
   * The current bitmap is overwritten.
   *
   * @param in the DataInput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserialize(DataInput in) throws IOException {
    this.highLowContainer.deserialize(in);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RoaringBitmap) {
      final RoaringBitmap srb = (RoaringBitmap) o;
      return srb.highLowContainer.equals(this.highLowContainer);
    }
    return false;
  }

  /**
   * Add the value if it is not already present, otherwise remove it.
   * 
   * @param x integer value
   */
  public void flip(final int x) {
    this.highLowContainer = this.highLowContainer.iflip(x);
  }

  /**
   * Modifies the current bitmap by complementing the bits in the given range, from rangeStart
   * (inclusive) rangeEnd (exclusive).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   */
  public void flip(final long rangeStart, final long rangeEnd) {
    Util.rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return; // empty range
    }

    this.highLowContainer = this.highLowContainer.iflip(rangeStart, rangeEnd);
  }


 /**
   * Modifies the current bitmap by complementing the bits in the given range, from rangeStart
   * (inclusive) rangeEnd (exclusive).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
    public void flip(final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      flip((long) rangeStart, (long) rangeEnd);
    } else {
      // rangeStart being -ve and rangeEnd being positive is not expected)
      // so assume both -ve
      flip(rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL);
    }
  }

  /**
   * Returns the number of distinct integers added to the bitmap (e.g., number of bits set).
   * 
   * @return the cardinality
   */
  @Override
  public long getLongCardinality() {
    return this.highLowContainer.getLongCardinality();
  }

  @Override
  public int getCardinality() {
    return this.highLowContainer.getCardinality();
  }

  @Override
  public void forEach(final IntConsumer ic) {
    this.highLowContainer.forEach(ic);
  }


  /**
   * Return a low-level container pointer that can be used to access the underlying data structure.
   *
   * @return container pointer
   */
  public ContainerPointer getContainerPointer() {
  	throw new UnsupportedOperationException("Not implemented since sparse bitmaps do not have containers");
//    return ((RoaringArray) this.highLowContainer).getContainerPointer();
  }

  /**
   * 
   * For better performance, consider the Use the {@link #forEach forEach} method.
   *
   * @return a custom iterator over set bits, the bits are traversed in ascending sorted order
   */
  @Override
  public PeekableIntIterator getIntIterator() {
    return new RoaringIntIterator();
  }

  /**
   * @return a custom iterator over set bits, the bits are traversed in descending sorted order
   */
  @Override
  public IntIterator getReverseIntIterator() {
    return new RoaringReverseIntIterator();
  }

  /**
   * Estimate of the memory usage of this data structure. This can be expected to be within 1% of
   * the true memory usage.
   *
   * @return estimated memory usage.
   */
  @Override
  public long getLongSizeInBytes() {
    return this.highLowContainer.getLongSizeInBytes();
  }

  @Override
  public int getSizeInBytes() {
    return this.highLowContainer.getSizeInBytes() ;
  }
  
  @Override
  public int hashCode() {
    return highLowContainer.hashCode();
  }


  /**
   * Check whether this bitmap has had its runs compressed.
   * 
   * @return whether this bitmap has run compression
   */
  public boolean hasRunCompression() {
    return this.highLowContainer.hasRunCompression();
  }

  /**
   * Checks whether the bitmap is empty.
   *
   * @return true if this bitmap contains no set bit
   */
  @Override
  public boolean isEmpty() {
    return highLowContainer.size() == 0;
  }

  // don't forget to call repairAfterLazy() afterward
  // important: x2 should not have been computed lazily
  protected void lazyor(final RoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = highLowContainer.size();
    final int length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      short s1 = highLowContainer.getKeyAtIndex(pos1);
      short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          this.highLowContainer.setContainerAtIndex(pos1, highLowContainer.getContainerAtIndex(pos1)
              .lazyIOR(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
        } else { // s1 > s2
          highLowContainer.insertNewKeyValueAt(pos1, s2,
              x2.highLowContainer.getContainerAtIndex(pos2).clone());
          pos1++;
          length1++;
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
    }
  }
  
  // don't forget to call repairAfterLazy() afterward
  // important: x2 should not have been computed lazily
  // this method is like lazyor except that it will convert
  // the current container to a bitset
  protected void naivelazyor(RoaringBitmap  x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = highLowContainer.size();
    final int length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      short s1 = highLowContainer.getKeyAtIndex(pos1);
      short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          Container c1 = highLowContainer.getContainerAtIndex(pos1);
          c1 = c1.toBitmapContainer();
          this.highLowContainer.setContainerAtIndex(pos1, 
              c1.lazyIOR(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
        } else { // s1 > s2
          highLowContainer.insertNewKeyValueAt(pos1, s2,
              x2.highLowContainer.getContainerAtIndex(pos2).clone());
          pos1++;
          length1++;
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
    }
  }

  /**
   * Create a new Roaring bitmap containing at most maxcardinality integers.
   *
   * @param maxcardinality maximal cardinality
   * @return a new bitmap with cardinality no more than maxcardinality
   */
  @Override
  public RoaringBitmap limit(int maxcardinality) {
  	if (maxcardinality == 0) {
  		return new RoaringBitmap();
	  }

	  return new RoaringBitmap(this.highLowContainer.limit(maxcardinality));
  }

  /**
   * In-place bitwise OR (union) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void or(final RoaringBitmap x2) {
  	// FIXME code this dispatch
  }

  /**
   * Computes OR between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   */
  public static RoaringBitmap or(@SuppressWarnings("rawtypes") final Iterator bitmaps,
      final long rangeStart, final long rangeEnd) {
    Util.rangeSanityCheck(rangeStart, rangeEnd);

    Iterator<RoaringBitmap> bitmapsIterator;
    bitmapsIterator = selectRangeWithoutCopy(bitmaps, rangeStart, rangeEnd);
    return or(bitmapsIterator);
  }


  /**
   * Computes OR between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap 
   * @deprecated use the version where longs specify the range. 
   *     Negative range points are forbidden.
   */
  @Deprecated
    public static RoaringBitmap or(@SuppressWarnings("rawtypes") final Iterator bitmaps, 
          final int rangeStart, final int rangeEnd) {
    return or(bitmaps, (long) rangeStart, (long) rangeEnd);
  }


  /**
   * Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be
   * GetCardinality()).
   * 
   * @param x upper limit
   *
   * @return the rank
   */
  @Override
  public long rankLong(int x) {
    return this.highLowContainer.rank(x);
  }
  
  @Override
  public int rank(int x) {
    return (int) rankLong(x);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.highLowContainer.readExternal(in);
  }

  /**
   * If present remove the specified integer (effectively, sets its bit value to false)
   *
   * @param x integer value representing the index in a bitmap
   */
  public void remove(final int x) {
    this.highLowContainer = this.highLowContainer.iremove(x);
  }

  /**
   * Remove from the current bitmap all integers in [rangeStart,rangeEnd).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   */
  public void remove(final long rangeStart, final long rangeEnd) {
    Util.rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return; // empty range
    }

   this.highLowContainer = this.highLowContainer.iremove(rangeStart, rangeEnd);
  }

  /**
   * Remove from the current bitmap all integers in [rangeStart,rangeEnd).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
    public void remove(final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      remove((long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    remove(rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL); 
  }


  /**
   * Remove run-length encoding even when it is more space efficient
   * 
   * @return whether a change was applied
   */
  public boolean removeRunCompression() {
    return this.highLowContainer.removeRunCompression();
  }

  // to be used with lazyor
  protected void repairAfterLazy() {
    for (int k = 0; k < highLowContainer.size(); ++k) {
      Container c = highLowContainer.getContainerAtIndex(k);
      highLowContainer.setContainerAtIndex(k, c.repairAfterLazy());
    }
  }

  /**
   * Use a run-length encoding where it is more space efficient
   * 
   * @return whether a change was applied
   */
  public boolean runOptimize() {
    return this.highLowContainer.runOptimize();
  }


  /**
   * Return the jth value stored in this bitmap.
   *
   * @param j index of the value
   *
   * @return the value
   */
  @Override
  public int select(int j) {
  	return this.highLowContainer.select(j);
  }

  /**
   * Serialize this bitmap.
   *
   *  See format specification at https://github.com/RoaringBitmap/RoaringFormatSpec
   *  
   * Consider calling {@link #runOptimize} before serialization to improve compression.
   *
   * The current bitmap is not modified.
   * 
   * Advanced example: To serialize your bitmap to a ByteBuffer, you can do the following.
   * 
   * <pre>
   * {@code
   *   //r is your bitmap
   *
   *   r.runOptimize(); // might improve compression
   *   // next we create the ByteBuffer where the data will be stored
   *   ByteBuffer outbb = ByteBuffer.allocate(r.serializedSizeInBytes());
   *   // then we can serialize on a custom OutputStream
   *   mrb.serialize(new DataOutputStream(new OutputStream(){
   *       ByteBuffer mBB;
   *       OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
   *       public void close() {}
   *       public void flush() {}
   *       public void write(int b) {
   *         mBB.put((byte) b);}
   *       public void write(byte[] b) {mBB.put(b);}
   *       public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
   *   }.init(outbb)));
   *   // outbuff will now contain a serialized version of your bitmap
   * }
   * </pre>
   * 
   * Note: Java's data structures are in big endian format. Roaring serializes to a little endian
   * format, so the bytes are flipped by the library during serialization to ensure that what is
   * stored is in little endian---despite Java's big endianness. You can defeat this process by
   * reflipping the bytes again in a custom DataOutput which could lead to serialized Roaring
   * objects with an incorrect byte order.
   *
   * @param out the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Override
  public void serialize(DataOutput out) throws IOException {
    this.highLowContainer.serialize(out);
  }


  /**
   * Assume that one wants to store "cardinality" integers in [0, universe_size), this function
   * returns an upper bound on the serialized size in bytes.
   * 
   * @param cardinality maximal cardinality
   * @param universe_size maximal value
   * @return upper bound on the serialized size in bytes of the bitmap
   */
  public static long maximumSerializedSize(long cardinality, long universe_size) {
    long contnbr = (universe_size + 65535) / 65536;
    if (contnbr > cardinality) {
      contnbr = cardinality;
      // we can't have more containers than we have values
    }
    final long headermax = Math.max(8, 4 + (contnbr + 7) / 8) + 8 * contnbr;
    final long valsarray = 2 * cardinality;
    final long valsbitmap = contnbr * 8192;
    final long valsbest = Math.min(valsarray, valsbitmap);
    return valsbest + headermax;
  }

  /**
   * Report the number of bytes required to serialize this bitmap. This is the number of bytes
   * written out when using the serialize method. When using the writeExternal method, the count
   * will be higher due to the overhead of Java serialization.
   *
   * @return the size in bytes
   */
  @Override
  public int serializedSizeInBytes() {
    return this.highLowContainer.serializedSizeInBytes();
  }

  /**
   * Return new iterator with only values from rangeStart (inclusive) to rangeEnd (exclusive)
   * 
   * @param bitmaps bitmaps iterator
   * @param rangeStart inclusive
   * @param rangeEnd exclusive
   * @return new iterator of bitmaps
   */
  private static Iterator<RoaringBitmap> selectRangeWithoutCopy(final Iterator bitmaps,
      final long rangeStart, final long rangeEnd) {
    Iterator<RoaringBitmap> bitmapsIterator;
    bitmapsIterator = new Iterator<RoaringBitmap>() {
      @Override
      public boolean hasNext() {
        return bitmaps.hasNext();
      }

      @Override
      public RoaringBitmap next() {
        RoaringBitmap next = (RoaringBitmap) bitmaps.next();
        return selectRangeWithoutCopy(next, rangeStart, rangeEnd);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Remove not supported");
      }
    };
    return bitmapsIterator;
  }

  /**
   * 
   * Extracts the values in the specified range, rangeStart (inclusive) and rangeEnd (exclusive)
   * while avoiding copies as much as possible. 
   * 
   * @param rb input bitmap
   * @param rangeStart inclusive
   * @param rangeEnd exclusive
   * @return new bitmap
   */
  // had formerly failed if rangeEnd==0
  private static RoaringBitmap selectRangeWithoutCopy(RoaringBitmap rb, final long rangeStart,
      final long rangeEnd) {
	  assert(rangeStart >= 0 && rangeEnd >= 0);
	  if (rangeEnd <= rangeStart) {
		  return new RoaringBitmap();
	  }

	  return new RoaringBitmap(rb.highLowContainer.selectRangeWithoutCopy(rangeStart, rangeEnd));
  }

  /**
   * Return the set values as an array, if the cardinality is smaller than 2147483648. 
   * The integer values are in sorted order.
   * 
   * @return array representing the set values.
   */
  @Override
  public int[] toArray() {
    return this.highLowContainer.toArray();
  }

  /**
   * 
   * Convert (copies) to a mutable roaring bitmap.
   *
   * @return a copy of this bitmap as a MutableRoaringBitmap
   */
  public MutableRoaringBitmap toMutableRoaringBitmap() {
    return new MutableRoaringBitmap(this);
  }

  /**
   * A string describing the bitmap.
   *
   * @return the string
   */
  @Override
  public String toString() {
    final StringBuilder answer = new StringBuilder();
    final IntIterator i = this.getIntIterator();
    answer.append("{");
    if (i.hasNext()) {
      answer.append(i.next() & 0xFFFFFFFFL);
    }
    while (i.hasNext()) {
      answer.append(",");
      // to avoid using too much memory, we limit the size
      if(answer.length() > 0x80000) {
        answer.append("...");
        break;
      }
      answer.append(i.next() & 0xFFFFFFFFL);
      
    }
    answer.append("}");
    return answer.toString();
  }

  /**
   * Recover allocated but unused memory.
   */
  public void trim() {
    this.highLowContainer.trim();
  }


  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    this.highLowContainer.writeExternal(out);
  }

  /**
   * In-place bitwise XOR (symmetric difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void xor(final RoaringBitmap x2) {
    // FIXME code this dispatch
  }

  /**
   * Computes XOR between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   */
  public static RoaringBitmap xor(@SuppressWarnings("rawtypes") final Iterator bitmaps,
      final long rangeStart, final long rangeEnd) {
    Util.rangeSanityCheck(rangeStart, rangeEnd);
    Iterator<RoaringBitmap> bitmapsIterator;
    bitmapsIterator = selectRangeWithoutCopy(bitmaps, rangeStart, rangeEnd);
    return FastAggregation.xor(bitmapsIterator);
  }

  /**
   * Computes XOR between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bi
   * @deprecated use the version where longs specify the range. 
   *     Negative values not allowed for rangeStart and rangeEnd
   */
  @Deprecated
    public static RoaringBitmap xor(@SuppressWarnings("rawtypes") final Iterator bitmaps, 
          final int rangeStart, final int rangeEnd) {
    return xor(bitmaps, (long) rangeStart, (long) rangeEnd);
  }


}
