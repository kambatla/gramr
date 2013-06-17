package org.gramr.common;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Rank implements WritableComparable{
  private float rank;

  public Rank() {

  }

  public Rank(float rank) {
    this.rank = rank;
  }

  public float getRank() {
    return this.rank;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.rank = in.readFloat();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(this.rank);
  }

  @Override
  public int compareTo(Object other) {
    Rank otherRank = (Rank) other;
    if (this.rank > otherRank.getRank())
      return -1;
    else if (this.rank < otherRank.getRank())
      return 1;
    else
      return 0;
  }
}
