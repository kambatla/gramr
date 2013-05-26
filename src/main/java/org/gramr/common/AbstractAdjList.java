package org.gramr.common;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("unchecked")
public abstract class AbstractAdjList implements WritableComparable {
	public boolean isRanked() {
		return false;
	}
}
