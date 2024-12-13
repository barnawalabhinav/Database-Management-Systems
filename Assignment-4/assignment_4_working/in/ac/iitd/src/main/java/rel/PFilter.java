package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.*;

import convention.PConvention;
import org.apache.calcite.sql.type.SqlTypeName;

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

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PFilter");
        /* Write your code here */
        return ((PRel) input).open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        /* Write your code here */
        ((PRel) input).close();
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PFilter has next");
        /* Write your code here */
        return (input != null) && ((PRel) input).hasNext();
    }

    private boolean evaluateCondition(Object[] row, RexNode cond) {
        RexCall call = (RexCall) cond;
        List<RexNode> operands = call.getOperands();

        switch (call.getKind()) {
            case EQUALS:
                Object op1 = null, op2 = null;
//                SqlTypeName st = null;
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
                        logger.debug(op2.getClass());
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
                if (op1 == null || op2 == null) {
                    return false;
                }
                return op1.equals(op2);
            case NOT_EQUALS:
                break;
            case LESS_THAN:
                break;
            case GREATER_THAN:
                break;
            case LESS_THAN_OR_EQUAL:
                break;
            case GREATER_THAN_OR_EQUAL:
                break;
            case BETWEEN:
                break;
            case IN:
                break;
            case NOT_IN:
                break;
            case LIKE:
                break;
            case IS_NULL:
                break;
            case IS_NOT_NULL:
                break;
            case AND:
                break;
            case OR:
                break;
            case NOT:
                break;
            default:
                return false;
        }
        return false;
    }

    // returns the next row
    // Hint: Try looking at different possible filter conditions
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PFilter");
        /* Write your code here */
        while (((PRel) input).hasNext()) {
            Object[] row = ((PRel) input).next();
            if (row != null && evaluateCondition(row, condition)) {
                logger.debug("Row passed filter: " + Arrays.toString(row));
                return row;
            }
        }
        return null;
    }
}
