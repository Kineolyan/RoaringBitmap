package org.roaringbitmap;

final class RoaringReverseIntIterator implements IntIterator {

	int hs = 0;

	ShortIterator iter;

	private final RoaringArray highLowContainer;

	private short pos;

	public RoaringReverseIntIterator(final RoaringArray highLowContainer) {
		this.highLowContainer = highLowContainer;
		// don't need an int because we go to 0, not Short.MAX_VALUE, and signed shorts underflow well
		// below zero
		this.pos = (short) (this.highLowContainer.size() - 1);

		nextContainer();
	}

	@Override
	public IntIterator clone() {
		try {
			RoaringReverseIntIterator clone = (RoaringReverseIntIterator) super.clone();
			clone.iter = this.iter.clone();
			return clone;
		} catch (CloneNotSupportedException e) {
			return null;// will not happen
		}
	}

	@Override
	public boolean hasNext() {
		return pos >= 0;
	}

	@Override
	public int next() {
		final int x = iter.nextAsInt() | hs;
		if (!iter.hasNext()) {
			--pos;
			nextContainer();
		}
		return x;
	}

	private void nextContainer() {
		if (pos >= 0) {
			iter =
				this.highLowContainer.getContainerAtIndex(pos).getReverseShortIterator();
			hs = this.highLowContainer.getKeyAtIndex(pos) << 16;
		}
	}

}
