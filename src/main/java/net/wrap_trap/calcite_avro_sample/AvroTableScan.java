package net.wrap_trap.calcite_avro_sample;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Arrays;
import java.util.List;

public class AvroTableScan extends TableScan implements EnumerableRel {
  private Integer[] fields;

  public AvroTableScan(RelOptCluster cluster, RelOptTable table, Integer[] fields) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);
    this.fields = fields;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new AvroTableScan(getCluster(), this.table, this.fields);
  }

  @Override
  public RelDataType deriveRowType() {
    List<RelDataTypeField> fieldList = getTable().getRowType().getFieldList();
    RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
    Arrays.stream(fields).forEach(field -> builder.add(fieldList.get(field)));
    return builder.build();
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(AvroProjectTableScanRule.INSTANCE);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
    return implementor.result(physType, Blocks.toBlock(
      Expressions.call(
        this.table.getExpression(AvroTable.class),
        "project",
        implementor.getRootExpression(),
        Expressions.constant(this.fields)
      )
    ));
  }
}
