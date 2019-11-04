package net.wrap_trap.calcite_avro_sample;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.stream.Collectors;

public class AvroProjectTableScanRule extends RelOptRule {

  static final AvroProjectTableScanRule INSTANCE = new AvroProjectTableScanRule();

  public AvroProjectTableScanRule() {
    super(RelOptRule.operand(
      LogicalProject.class,
      RelOptRule.operand(AvroTableScan.class, RelOptRule.none())
    ), "AvroProjectTableScanRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject project = call.rel(0);
    AvroTableScan scan = call.rel(1);
    Integer[] fields = getProjectFields(project.getProjects());

    call.transformTo(
      new AvroTableScan(scan.getCluster(), scan.getTable(), fields)
    );
  }

  private Integer[] getProjectFields(List<RexNode> exps) {
    List<Integer> indexes = exps.stream().map(rexNode -> {
      if (rexNode instanceof RexInputRef) {
        return ((RexInputRef)rexNode).getIndex();
      }
      return null;
    }).collect(Collectors.toList());

    Integer[] ret = new Integer[indexes.size()];
    indexes.toArray(ret);
    return ret;
  }
}
