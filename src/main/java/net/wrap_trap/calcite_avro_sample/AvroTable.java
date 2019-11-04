package net.wrap_trap.calcite_avro_sample;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;

public class AvroTable extends AbstractTable implements QueryableTable, TranslatableTable {

  private Schema schema;
  private List<GenericData.Record> records;

  public AvroTable(Schema schema, List<GenericData.Record> records) {
    this.schema = schema;
    this.records = records;
  }

  public Enumerable<Object> project(DataContext root, Integer[] fields) {
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        return new AvroEnumerator(records, fields);
      }
    };
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    JavaTypeFactory typeFactory = (JavaTypeFactory) relDataTypeFactory;

    List<Pair<String, RelDataType>> ret = schema.getFields().stream().map(field -> {
      Schema.Type avroFieldType = field.schema().getType();
      if (avroFieldType == Schema.Type.UNION) {
        avroFieldType = getAvroNullableField(field);
      }
      RelDataType relDataType = AvroFieldType.of(avroFieldType).toType(typeFactory);
      return new Pair<>(field.name().toUpperCase(), relDataType);
    }).collect(Collectors.toList());
    return relDataTypeFactory.createStructType(ret);
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus, String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Type getElementType() {
    return Object[].class;
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String tableName, Class clazz) {
    return Schemas.tableExpression(schemaPlus, getElementType(), tableName, clazz);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    int fieldCount = relOptTable.getRowType().getFieldCount();
    Integer[] fields = AvroEnumerator.identityList(fieldCount);
    return new AvroTableScan(context.getCluster(), relOptTable, fields);
  }

  private Schema.Type getAvroNullableField(Schema.Field field) {
    for (Schema schema : field.schema().getTypes()) {
      Schema.Type avroType = schema.getType();
      if (avroType != Schema.Type.NULL) {
        return avroType;
      }
    }
    return null;
  }
}
