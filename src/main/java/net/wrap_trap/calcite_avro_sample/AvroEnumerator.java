package net.wrap_trap.calcite_avro_sample;

import org.apache.avro.generic.GenericData;
import org.apache.calcite.linq4j.Enumerator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AvroEnumerator implements Enumerator<Object> {

  private List<GenericData.Record> records;
  private Integer[] fields;
  private int pos;

  public AvroEnumerator(List<GenericData.Record> records, Integer[] fields) {
    this.records = records;
    this.fields = fields;
    this.pos = -1;
  }

  @Override
  public Object current() {
    GenericData.Record record = records.get(this.pos);
    return Arrays.stream(this.fields)
             .map(record::get).collect(Collectors.toList()).toArray();
  }

  @Override
  public boolean moveNext() {
    return (this.records.size() > (++this.pos));
  }

  @Override
  public void reset() {
    this.pos = 0;
  }

  @Override
  public void close() {}

  public static Integer[] identityList(int n) {
    Integer[] ret = new Integer[n];
    for (int i = 0; i < n; i ++) {
      ret[i] = i;
    }
    return ret;
  }
}
