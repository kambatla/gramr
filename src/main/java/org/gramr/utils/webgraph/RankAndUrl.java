package org.gramr.utils.webgraph;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RankAndUrl implements WritableComparable {
  private int src;
  private float rank;
  private String url;

  public RankAndUrl(){}

  public RankAndUrl(int src, float rank, String url) {
    this.src = src;
    this.rank = rank;
    this.url = url;
  }

  public int getSrc() {
    return src;
  }

  public void setSrc(int src) {
    this.src = src;
  }

  public float getRank() {
    return rank;
  }

  public void setRank(float rank) {
    this.rank = rank;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(src);
    dataOutput.writeFloat(rank);
    dataOutput.writeUTF(url);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    src = dataInput.readInt();
    rank = dataInput.readFloat();
    url = dataInput.readUTF();
  }

  @Override
  public int compareTo(Object o) {
    RankAndUrl other = (RankAndUrl) o;
    if (this.rank > other.getRank())
      return -1;
    else if (this.rank < other.getRank())
      return 1;
    else {
      return this.url.compareTo(other.getUrl());
    }
  }

  @Override
  public String toString() {
    return src + " " + rank + " " + url;
  }

  public static RankAndUrl parse(String str) {
    if (str == null) return null;

    String[] splits = str.split(" ");
    if (splits.length >= 2) return null;

    int src = Integer.parseInt(splits[0]);
    float rank = Float.parseFloat(splits[1]);

    String url = null;
    if (splits.length == 3)
      url = splits[2];

    return new RankAndUrl(src, rank, url);
  }
}
