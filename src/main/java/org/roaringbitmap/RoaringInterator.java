package org.roaringbitmap;

final class RoaringIntIterator implements PeekableIntIterator {
	private int hs = 0;

	private PeekableShortIterator iter;
	private final RoaringArray highLowContainer;

	private int pos = 0;

	public RoaringIntIterator(final RoaringArray highLowContainer) {
		this.highLowContainer = highLowContainer;
		nextContainer();
	}

	@Override
	public PeekableIntIterator clone() {
		try {
			RoaringIntIterator x = (RoaringIntIterator) super.clone();
			x.iter = this.iter.clone();
			return x;
		} catch (CloneNotSupportedException e) {
			return null;// will not happen
		}
	}

	@Override
	public boolean hasNext() {
		return pos < this.highLowContainer.size();
	}

	@Override
	public int next() {
		final int x = iter.nextAsInt() | hs;
		if (!iter.hasNext()) {
			++pos;
			nextContainer();
		}
		return x;
	}

	private void nextContainer() {
		if (pos < this.highLowContainer.size()) {
			iter = this.highLowContainer.getContainerAtIndex(pos).getShortIterator();
			hs = this.highLowContainer.getKeyAtIndex(pos) << 16;
		}
	}

	@Override
	public void advanceIfNeeded(int minval) {
		while (hasNext() && ((hs >>> 16) < (minval >>> 16))) {
			++pos;
			nextContainer();
		}
		if (hasNext() && ((hs >>> 16) == (minval >>> 16))) {
			iter.advanceIfNeeded(Util.lowbits(minval));
			if (!iter.hasNext()) {
				++pos;
				nextContainer();
			}
		}
	}

	@Override
	public int peekNext() {
		return Util.toIntUnsigned(iter.peekNext()) | hs;
	}


}