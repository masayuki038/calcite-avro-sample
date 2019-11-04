package net.wrap_trap.calcite_avro_sample;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class AvroSchema extends AbstractSchema {
  private Map<String, Table> tableMap;
  private File directory;

  public AvroSchema(File directory) {
    this.directory = directory;
  }

  @Override
  public Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = new HashMap<>();
      File[] avroFiles = directory.listFiles((dir, name) -> name.endsWith(".avro"));
      Arrays.stream(avroFiles).forEach(file -> {
        GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<>();

        try (DataFileReader<GenericData.Record> reader = new DataFileReader<>(file, datum)) {
          Schema schema = reader.getSchema();
          List<GenericData.Record> records = new ArrayList<>();
          while (reader.hasNext()) {
            GenericData.Record record = new GenericData.Record(schema);
            reader.next(record);
            records.add(record);
          }
          tableMap.put(
            trim(file.getName(), ".avro").toUpperCase(),
            new AvroTable(schema, records));
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      });
    }
    return tableMap;
  }

  private String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    if (trimmed == null) {
      return s;
    }
    return trimmed;
  }

  private String trimOrNull(String s, String suffix) {
    if (s.endsWith(suffix)) {
      return s.substring(0, s.length() - suffix.length());
    }
    return null;
  }
}
