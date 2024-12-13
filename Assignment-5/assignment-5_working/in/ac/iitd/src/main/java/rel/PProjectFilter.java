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

import java.util.Arrays;
import java.util.List;

/*
    * PProjectFilter is a relational operator that represents a Project followed by a Filter.
    * You need to write the entire code in this file.
    * To implement PProjectFilter, you can extend either Project or Filter class.
    * Define the constructor accordingly and override the methods as required.
*/
public class PProjectFilter extends Project implements PRel {
    protected final RexNode condition;

    public PProjectFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType,
            RexNode condition) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        this.condition = condition;
//        assert getConvention() instanceof PConvention;
    }

    @Override
    public Project copy(RelTraitSet relTraitSet, RelNode relNode, List<RexNode> list, RelDataType relDataType) {
        return new PProjectFilter(getCluster(), relTraitSet, relNode, list, relDataType, condition);
    }

    public Project copy(RelTraitSet relTraitSet, RelNode relNode, List<RexNode> list, RelDataType relDataType, RexNode condition) {
        return new PProjectFilter(getCluster(), relTraitSet, relNode, list, relDataType, condition);
    }

    @Override
    public String toString() {
        return "PProjectFilter";
    }

    private PRel inputRel = null;
    private Object[] nextRow = null;

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProjectFilter");
        /* Write your code here */
        inputRel = (PRel) input;
        return inputRel.open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProjectFilter");
        /* Write your code here */
        inputRel.close();
    }

    private List<Object> getValues(Object[] row, List<RexNode> operands) {
        Object op1 = null, op2 = null;
        if (operands.get(0) instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) operands.get(0);
            op1 = row[ref.getIndex()];
            if (operands.get(1) instanceof RexInputRef) {
                ref = (RexInputRef) operands.get(1);
                op2 = row[ref.getIndex()];
            } else if (operands.get(1) instanceof RexLiteral) {
                RexLiteral literal = (RexLiteral) operands.get(1);
                op2 = literal.getValueAs(op1.getClass());
            } else {
                op2 = evaluateProject(operands.get(1), row);
            }
        } else if (operands.get(0) instanceof RexLiteral) {
            if (operands.get(1) instanceof RexInputRef) {
                RexInputRef ref = (RexInputRef) operands.get(1);
                op2 = row[ref.getIndex()];
            } else if (operands.get(1) instanceof RexLiteral) {
                RexLiteral literal = (RexLiteral) operands.get(1);
                op2 = literal.getValue();
            } else {
                op2 = evaluateProject(operands.get(1), row);
            }
            RexLiteral literal = (RexLiteral) operands.get(0);
            op1 = literal.getValueAs(op2.getClass());
        } else {
            op1 = evaluateProject(operands.get(0), row);
            if (operands.get(1) instanceof RexInputRef) {
                RexInputRef ref = (RexInputRef) operands.get(1);
                op2 = row[ref.getIndex()];
            } else if (operands.get(1) instanceof RexLiteral) {
                RexLiteral literal = (RexLiteral) operands.get(1);
                op2 = literal.getValueAs(op1.getClass());
            } else {
                op2 = evaluateProject(operands.get(1), row);
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
                String pattern = ((String) op2).replace("*", "\\*");
                pattern = pattern.replace(".", "\\.");
                pattern = pattern.replace("%", ".*");
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
            case OR:
                boolean resOr = false;
                for (RexNode operand : operands) {
                    resOr = resOr || evaluateCondition(row, operand);
                }
                return resOr;
            case NOT:
                return !evaluateCondition(row, operands.get(0));
            default:
                return false;
        }
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
        logger.trace("Checking if PProjectFilter has next");
        /* Write your code here */
        if (input == null) {
            return false;
        }
        while (inputRel.hasNext()) {
            Object[] row = inputRel.next();

            // Check if the row satisfies the condition
            if (evaluateCondition(row, condition)) {
                // Apply the projects to the row
                Object[] result = new Object[this.exps.size()];
                for (int i = 0; i < this.exps.size(); i++) {
                    RexNode project = this.exps.get(i);

                    // Evaluate the project against the row
                    result[i] = evaluateProject(project, row);
                }
                nextRow = result;
                return true;
            }
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProjectFilter");
        /* Write your code here */
        assert nextRow != null;
        return nextRow;
    }
}