package rel;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.*;

import convention.PConvention;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class PFilter extends Filter implements PRel {

    public PFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RexNode condition) {
        super(cluster, traits, child, condition);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }
    
    private PRel inputRel = null;
    private Object[] nextRow = null;

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PFilter");
        /* Write your code here */
        inputRel = (PRel) input;
        return inputRel.open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        /* Write your code here */
        inputRel.close();
        return;
    }

    private List<Object> getValues(Object[] row, List<RexNode> operands) {
//                SqlTypeName st = null;
        Object op1 = null, op2 = null;
        if (operands.get(0) instanceof RexInputRef) {
            if (operands.get(1) instanceof RexInputRef) {
                RexInputRef ref = (RexInputRef) operands.get(0);
                op1 = row[ref.getIndex()];
                ref = (RexInputRef) operands.get(1);
                op2 = row[ref.getIndex()];
            } else if (operands.get(1) instanceof RexLiteral) {
                RexInputRef ref = (RexInputRef) operands.get(0);
                op1 = row[ref.getIndex()];
                RexLiteral literal = (RexLiteral) operands.get(1);
                op2 = literal.getValueAs(op1.getClass());
//                        logger.debug(op2.getClass());
            }
//                    st = this.getRowType().getFieldList().get(ref.getIndex()).getType().getSqlTypeName();
        } else if (operands.get(0) instanceof RexLiteral) {
            if (operands.get(1) instanceof RexInputRef) {
                RexInputRef ref = (RexInputRef) operands.get(1);
                op2 = row[ref.getIndex()];
                RexLiteral literal = (RexLiteral) operands.get(0);
                op1 = literal.getValueAs(op2.getClass());
            } else if (operands.get(1) instanceof RexLiteral) {
                RexLiteral literal = (RexLiteral) operands.get(1);
                op2 = literal.getValue();
                literal = (RexLiteral) operands.get(0);
                op1 = literal.getValue();
            }
        }
        return Arrays.asList(op1, op2);
    }

    private Object getValue(Object[] row, List<RexNode> operands) {
        Object op1 = null;
        if (operands.get(0) instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) operands.get(0);
            op1 = row[ref.getIndex()];
        } else if (operands.get(0) instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) operands.get(0);
            op1 = literal.getValue();
        }
        return op1;
    }

    private boolean evaluateCondition(Object[] row, RexNode cond) {
        RexCall call = (RexCall) cond;
        List<RexNode> operands = call.getOperands();

        Object op1, op2;
        List<Object> values;
        switch (call.getKind()) {
            case EQUALS:
                values = getValues(row, operands);
                op1 = values.get(0);
                op2 = values.get(1);
                if (op1 == null || op2 == null) {
                    return false;
                }
                return op1.equals(op2);
            case NOT_EQUALS:
                values = getValues(row, operands);
                op1 = values.get(0);
                op2 = values.get(1);
                if (op1 == null) {
                    return op2 != null;
                }
                return !op1.equals(op2);
            case LESS_THAN:
                values = getValues(row, operands);
                op1 = values.get(0);
                op2 = values.get(1);
                if (op1 == null || op2 == null) {
                    return false;
                }
                return ((Comparable) op1).compareTo(op2) < 0;
            case GREATER_THAN:
                values = getValues(row, operands);
                op1 = values.get(0);
                op2 = values.get(1);
                if (op1 == null || op2 == null) {
                    return false;
                }
                return ((Comparable) op1).compareTo(op2) > 0;
            case LESS_THAN_OR_EQUAL:
                values = getValues(row, operands);
                op1 = values.get(0);
                op2 = values.get(1);
                if (op1 == null || op2 == null) {
                    return false;
                }
                return ((Comparable) op1).compareTo(op2) <= 0;
            case GREATER_THAN_OR_EQUAL:
                values = getValues(row, operands);
                op1 = values.get(0);
                op2 = values.get(1);
                if (op1 == null || op2 == null) {
                    return false;
                }
                return ((Comparable) op1).compareTo(op2) >= 0;
            case LIKE:
                values = getValues(row, operands);
                op1 = values.get(0);
                op2 = values.get(1);
                if (op1 == null || op2 == null) {
                    return false;
                }
                if (!(op1 instanceof String) || !(op2 instanceof String)) {
                    throw new UnsupportedOperationException("LIKE can only be used with String values");
                }
                String pattern = ((String) op2).replace("%", ".*");
                pattern = pattern.replace("_", ".");
                return ((String) op1).matches(pattern);
            case IS_NULL:
                return getValue(row, operands) == null;
            case IS_NOT_NULL:
                return getValue(row, operands) != null;
            case AND:
                boolean resAnd = true;
                for (RexNode operand : operands) {
                    resAnd = resAnd && evaluateCondition(row, operand);
                }
                return resAnd;
//                return evaluateCondition(row, operands.get(0)) && evaluateCondition(row, operands.get(1));
            case OR:
                boolean resOr = false;
                for (RexNode operand : operands) {
                    resOr = resOr || evaluateCondition(row, operand);
                }
                return resOr;
//                return evaluateCondition(row, operands.get(0)) || evaluateCondition(row, operands.get(1));
            case NOT:
                return !evaluateCondition(row, operands.get(0));
            default:
                return false;
        }
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PFilter has next");
        /* Write your code here */
        if (inputRel == null) {
            return false;
        }
        while (inputRel.hasNext()) {
            Object[] row = inputRel.next();
            if (evaluateCondition(row, condition)) {
                logger.debug("Row passed filter: " + Arrays.toString(row));
                nextRow = row;
                return true;
            }
        }
        return false;
    }

    // returns the next row
    // Hint: Try looking at different possible filter conditions
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PFilter");
        /* Write your code here */
        assert nextRow != null;
        return nextRow;
    }
}
