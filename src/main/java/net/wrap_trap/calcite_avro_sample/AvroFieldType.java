package net.wrap_trap.calcite_avro_sample;

import org.apache.avro.Schema;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

enum AvroFieldType {
  STRING(String.class, "string"),
  INT(Primitive.INT),
  LONG(Primitive.LONG);

  private final Class clazz;
  private final String simpleName;

  private static final Map<Schema.Type, AvroFieldType> MAP = new HashMap<>();

  static {
    MAP.put(Schema.Type.STRING, STRING);
    MAP.put(Schema.Type.INT, INT);
    MAP.put(Schema.Type.LONG, LONG);
  }

  AvroFieldType(Primitive primitive) {
    this(primitive.boxClass, primitive.primitiveClass.getSimpleName());
  }

  AvroFieldType(Class clazz, String simpleName) {
    this.clazz = clazz;
    this.simpleName = simpleName;
  }

  public static AvroFieldType of(Schema.Type avroType) {
    return MAP.get(avroType);
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }
}
