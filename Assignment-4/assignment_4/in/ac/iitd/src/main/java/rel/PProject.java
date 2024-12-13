package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.util.NlsString;

import java.util.List;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {

    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                            List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    private PRel inputRel = null;
    private Object[] nextRow = null;

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        /* Write your code here */
        inputRel = (PRel) input;
        return inputRel.open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        /* Write your code here */
        inputRel.close();
    }

    private Object evaluateProject(RexNode project, Object[] row) {
        if (project instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) project;
            return row[inputRef.getIndex()];
        } else if (project instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) project;
            if (literal.getValue() instanceof NlsString) {
                return ((NlsString) literal.getValue()).getValue();
            }
            return literal.getValue();
        } else if (project instanceof RexCall) {
            RexCall call = (RexCall) project;
            List<RexNode> operands = call.getOperands();
            Comparable op1, op2;
            switch (call.getKind()) {
                case PLUS:
                    return ((Number) evaluateProject(operands.get(0), row)).doubleValue() + ((Number) evaluateProject(operands.get(1), row)).doubleValue();
                case MINUS:
                    return ((Number) evaluateProject(operands.get(0), row)).doubleValue() - ((Number) evaluateProject(operands.get(1), row)).doubleValue();
                case TIMES:
                    return ((Number) evaluateProject(operands.get(0), row)).doubleValue() * ((Number) evaluateProject(operands.get(1), row)).doubleValue();
                case DIVIDE:
                    return ((Number) evaluateProject(operands.get(0), row)).doubleValue() / ((Number) evaluateProject(operands.get(1), row)).doubleValue();
                case MOD:
                    return ((Number) evaluateProject(operands.get(0), row)).doubleValue() % ((Number) evaluateProject(operands.get(1), row)).doubleValue();
                case MINUS_PREFIX:
                    return -((Number) evaluateProject(operands.get(0), row)).doubleValue();
                case AND:
                    return ((Boolean) evaluateProject(operands.get(0), row)) && ((Boolean) evaluateProject(operands.get(1), row));
                case OR:
                    return ((Boolean) evaluateProject(operands.get(0), row)) || ((Boolean) evaluateProject(operands.get(1), row));
                case NOT:
                    return !((Boolean) evaluateProject(operands.get(0), row));
                case EQUALS:
                    return evaluateProject(operands.get(0), row).equals(evaluateProject(operands.get(1), row));
                case NOT_EQUALS:
                    return !evaluateProject(operands.get(0), row).equals(evaluateProject(operands.get(1), row));
                case GREATER_THAN:
                    op1 = (Comparable) evaluateProject(operands.get(0), row);
                    op2 = (Comparable) evaluateProject(operands.get(1), row);
                    if (op1 instanceof Number) {
                        op1 = ((Number) op1).doubleValue();
                    }
                    if (op2 instanceof Number) {
                        op2 = ((Number) op2).doubleValue();
                    }
                    return (op1.compareTo(op2)) > 0;
                case GREATER_THAN_OR_EQUAL:
                    op1 = (Comparable) evaluateProject(operands.get(0), row);
                    op2 = (Comparable) evaluateProject(operands.get(1), row);
                    if (op1 instanceof Number) {
                        op1 = ((Number) op1).doubleValue();
                    }
                    if (op2 instanceof Number) {
                        op2 = ((Number) op2).doubleValue();
                    }
                    return (op1.compareTo(op2)) >= 0;
                case LESS_THAN:
                    op1 = (Comparable) evaluateProject(operands.get(0), row);
                    op2 = (Comparable) evaluateProject(operands.get(1), row);
                    if (op1 instanceof Number) {
                        op1 = ((Number) op1).doubleValue();
                    }
                    if (op2 instanceof Number) {
                        op2 = ((Number) op2).doubleValue();
                    }
                    return op1.compareTo(op2) < 0;
                case LESS_THAN_OR_EQUAL:
                    op1 = (Comparable) evaluateProject(operands.get(0), row);
                    op2 = (Comparable) evaluateProject(operands.get(1), row);
                    if (op1 instanceof Number) {
                        op1 = ((Number) op1).doubleValue();
                    }
                    if (op2 instanceof Number) {
                        op2 = ((Number) op2).doubleValue();
                    }
                    return (op1.compareTo(op2)) <= 0;
                case CASE:
                    for (int i = 0; i < operands.size() - 1; i += 2) {
                        if ((Boolean) evaluateProject(operands.get(i), row)) {
                            return evaluateProject(operands.get(i + 1), row);
                        }
                    }
                    return evaluateProject(operands.get(operands.size() - 1), row);
                case IS_NULL:
                    return evaluateProject(operands.get(0), row) == null;
                case IS_NOT_NULL:
                    return evaluateProject(operands.get(0), row) != null;
                case CONCAT2:
                    return evaluateProject(operands.get(0), row).toString() + evaluateProject(operands.get(1), row).toString();
                default:
                    throw new UnsupportedOperationException("Unsupported operator: " + call.getKind());
            }
        } else {
            throw new UnsupportedOperationException("Unsupported RexNode type: " + project.getClass());
        }
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
        if (input == null) {
            return false;
        }
        if (inputRel.hasNext()) {
            Object[] row = inputRel.next();

            // Apply the projects to the row
            Object[] result = new Object[this.exps.size()];
            for (int i = 0; i < this.exps.size(); i++) {
                RexNode project = this.exps.get(i);
//                System.out.println(project);

                // Evaluate the project against the row
                result[i] = evaluateProject(project, row);
            }
            nextRow = result;
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        /* Write your code here */
        assert nextRow != null;
        return nextRow;
    }
}
