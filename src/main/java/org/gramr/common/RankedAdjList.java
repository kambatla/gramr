package org.gramr.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

public class RankedAdjList extends AdjList {
	float rank;

	public RankedAdjList() {
		super();
	}

	public RankedAdjList(int src, float rank, int[] dst) {
		super(src, dst);
		this.rank = rank;
	}

	RankedAdjList(int src, float rank, Collection<Integer> dst) {
		super(src, dst);
		this.rank = rank;
	}

	public static RankedAdjList createRankedAdjList(int src, float rank,
			int[] dst) {
		return new RankedAdjList(src, rank, dst);
	}

	public static RankedAdjList createRankedAdjList(int src, float rank,
			Collection<Integer> dst) {
		return new RankedAdjList(src, rank, dst);
	}

	public float getRank() {
		return this.rank;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeFloat(this.rank);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.rank = in.readFloat();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(" ");
		sb.append(this.rank);
		return sb.toString();
	}

	public static RankedAdjList parseRankedAdjList(String str) {
		AdjList adj = parseAdjList(str);
		if (adj == null) {
			return null;
		}

		String[] splits = str.split(" ");
		return new RankedAdjList(adj.getSrc(), Float
				.parseFloat(splits[splits.length - 1]), adj.getDst());
	}
}
