package org.gramr.common;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AdjList extends AbstractAdjList {
	private static Log LOG = LogFactory.getLog(AdjList.class);

	private int src;
	private int numOutLinks;
	private int[] dst;

	public AdjList() {

	}

	public AdjList(int src, int[] dst) {
		this.src = src;

		if (dst != null) {
			this.numOutLinks = dst.length;
			this.dst = new int[this.numOutLinks];
			System.arraycopy(dst, 0, this.dst, 0, this.numOutLinks);
		} else {
			this.numOutLinks = 0;
		}
	}

	public AdjList(int src, Collection<Integer> dst) {
		this.src = src;
		this.numOutLinks = dst.size();
		if (this.numOutLinks > 0) {
			this.dst = new int[this.numOutLinks];
			int i = 0;
			for (Integer dstNode : dst) {
				this.dst[i++] = dstNode;
			}
		}
	}

	public static AdjList createAdjList(int src, int[] dst) {
		return new AdjList(src, dst);
	}

	public static AdjList createAdjList(int src, Collection<Integer> dst) {
		return new AdjList(src, dst);
	}

	public boolean isRanked() {
		return false;
	}

	public int getSrc() {
		return this.src;
	}

	public int getNumOutLinks() {
		return this.numOutLinks;
	}

	public int[] getDst() {
		return this.dst;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(src);
		out.writeInt(this.numOutLinks);
		if (this.numOutLinks > 0) {
			for (long temp : dst) {
				out.writeLong(temp);
			}
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.src = in.readInt();
		this.numOutLinks = in.readInt();
		if (this.numOutLinks > 0) {
			this.dst = new int[this.numOutLinks];
			for (int i = 0; i < this.numOutLinks; i++)
				dst[i] = in.readInt();
		}
	}

	public int compareTo(Object w) {
		long thisValue = this.src;
		long thatValue = ((AdjList) w).src;
		return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.src);
		sb.append(" ");
		sb.append(this.numOutLinks);
		if (this.numOutLinks > 0) {
			for (int dest : this.dst) {
				sb.append(" ");
				sb.append((int) dest);
			}
		}
		return sb.toString();
	}

	public static AdjList parseAdjList(String str) {
		int src, numLinks;
		int[] dst = null;

		String[] splits = str.split(" ");
		if (splits.length < 2)
			return null;
		
		try {
			src = Integer.parseInt(splits[0]);
			numLinks = Integer.parseInt(splits[1]);
		} catch (NumberFormatException nfe) {
			LOG.error("Corrupt adjacency list in graph");
			return null;
		}
		
		if (splits.length < numLinks + 2) {
			LOG.error("Not all outgoing edges listed");
			numLinks = splits.length - 3;
		}

		dst = new int[numLinks];
		int numDstNodesRead = 0;
		for (int j = 2; numDstNodesRead < numLinks; numDstNodesRead++, j++) {
			try {
				dst[numDstNodesRead] = Integer.parseInt(splits[j]);
			} catch (NumberFormatException nfe) {
				LOG.error("Corrupt adjacency list in graph");
				int[] tmp_dst = new int[numDstNodesRead];
				System.arraycopy(dst, 0, tmp_dst, 0, numDstNodesRead);
				dst = tmp_dst;
				tmp_dst = null;
				break;
			}
		}

		return new AdjList(src, dst);
	}
}