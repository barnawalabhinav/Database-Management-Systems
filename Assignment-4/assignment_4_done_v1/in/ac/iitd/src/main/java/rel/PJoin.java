package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlKind;

import java.util.*;

/*
    * Implement Hash Join
    * The left child is blocking, the right child is streaming
*/
public class PJoin extends Join implements PRel {

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
                super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
                assert getConvention() instanceof PConvention;
    }

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }

    private PRel leftChild = null, rightChild = null;
    private boolean processed = false;
    private int num_left_cols = 0;

    private final HashMap<String, List<Object[]>> groupedRows = new HashMap<>();

    private final Queue<Object[]> results = new LinkedList<>();

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PJoin");
        /* Write your code here */
        leftChild = (PRel) left;
        rightChild = (PRel) right;
        processed = false;
        num_left_cols = left.getRowType().getFieldCount();
        return leftChild.open() && rightChild.open();
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        /* Write your code here */
        leftChild.close();
        rightChild.close();
    }

    private String getKey(Object[] row, RexCall operand, String table) {
        if (operand.getKind().equals(SqlKind.AND)) {
            StringBuilder key = new StringBuilder();
            for (RexNode op : operand.getOperands()) {
                key.append(getKey(row, (RexCall) op, table)).append(',');
            }
            return key.toString();
        }
        assert operand.getKind().equals(SqlKind.EQUALS);
        RexNode op1 = operand.getOperands().get(0);
        RexNode op2 = operand.getOperands().get(1);
        String val1 = null, val2 = null;
        RexInputRef ref1 = null, ref2 = null;
        if (op1 instanceof RexInputRef) {
            ref1 = (RexInputRef) op1;
            if ((table.equals("left") && ref1.getIndex() < num_left_cols) ||
                    (table.equals("right") && ref1.getIndex() >= num_left_cols)) {
                int offset = ref1.getIndex() < num_left_cols ? 0 : num_left_cols;
                return row[ref1.getIndex() - offset].toString();
            }
        }
        if (op2 instanceof RexInputRef) {
            ref2 = (RexInputRef) op2;
            if ((table.equals("left") && ref2.getIndex() < num_left_cols) ||
                    (table.equals("right") && ref2.getIndex() >= num_left_cols)) {
                int offset = ref2.getIndex() < num_left_cols ? 0 : num_left_cols;
                return row[ref2.getIndex() - offset].toString();
            }
        }
        if (op1 instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) op1;
            if ((ref2 != null) || (table.equals("left"))) {
                return literal.getValue().toString();
            }
        }
        if (op2 instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) op2;
            if ((ref1 != null) || (table.equals("right"))) {
                return literal.getValue().toString();
            }
        }
        return null;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        /* Write your code here */
        if (leftChild == null || rightChild == null) {
            return false;
        }
//        System.out.println("Cond: " + condition);
//        System.out.println("VarSet: " + variablesSet);
        if (!processed) {
            processed = true;
            while (leftChild.hasNext()) {
                Object[] leftRow = leftChild.next();
                String key = getKey(leftRow, ((RexCall) condition), "left");
                if (!groupedRows.containsKey(key)) {
                    groupedRows.put(key, new ArrayList<>());
                }
                groupedRows.get(key).add(leftRow);
            }
            while (rightChild.hasNext()) {
                Object[] rightRow = rightChild.next();
                String key = getKey(rightRow, ((RexCall) condition), "right");
                if (groupedRows.containsKey(key)) {
                    for (Object[] leftRow : groupedRows.get(key)) {
                        Object[] row = new Object[leftRow.length + rightRow.length];
                        System.arraycopy(leftRow, 0, row, 0, leftRow.length);
                        System.arraycopy(rightRow, 0, row, leftRow.length, rightRow.length);
                        results.add(row);
                    }
                }
            }
        }
        return !results.isEmpty();
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        /* Write your code here */
        return results.poll();
    }
}
