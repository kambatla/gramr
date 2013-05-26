package org.gramr.kernel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MatVecMapOutputValue implements Writable {
	private boolean isAdjList;
	private float rank;
	private int numOutLinks;
	private int[] dst;

	public MatVecMapOutputValue() {
	}

	public static MatVecMapOutputValue createAdjListValue(float rank, int numOutLinks,
			int[] dst) {
		MatVecMapOutputValue val = new MatVecMapOutputValue();
		val.isAdjList = true;
		val.rank = rank;
		val.numOutLinks = numOutLinks;

		if (val.numOutLinks > 0) {
			val.dst = new int[numOutLinks];
			for (int i = 0; i < dst.length; i++)
				val.dst[i] = dst[i];
		}
		return val;
	}

	public static MatVecMapOutputValue createEdgeValue(float rank) {
		MatVecMapOutputValue val = new MatVecMapOutputValue();
		val.isAdjList = false;
		val.rank = rank;
		return val;
	}

	public boolean isAdjList() {
		return this.isAdjList;
	}

	public float getRank() {
		return this.rank;
	}

	public int getNumOutLinks() {
		return this.numOutLinks;
	}

	public int[] getDst() {
		return this.dst;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(this.isAdjList);

		if (this.isAdjList) {
			out.writeFloat(this.rank);
			out.writeInt(this.numOutLinks);
			if (this.numOutLinks > 0) {
				for (int temp : dst) {
					out.writeInt(temp);
				}
			}
		} else {
			out.writeFloat(this.rank);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		try {
			this.isAdjList = in.readBoolean();

			if (this.isAdjList) {
				this.rank = in.readFloat();
				this.numOutLinks = in.readInt();

				if (this.numOutLinks > 0) {
					this.dst = new int[this.numOutLinks];
					for (int i = 0; i < this.numOutLinks; i++)
						this.dst[i] = in.readInt();
				}
			} else {
				this.rank = in.readFloat();
			}
		} catch (EOFException e) {
			System.out.println(e);
		}
	}
}
