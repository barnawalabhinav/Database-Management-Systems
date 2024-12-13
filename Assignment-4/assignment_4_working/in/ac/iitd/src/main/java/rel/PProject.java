package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

import java.util.Arrays;
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

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        /* Write your code here */
        for (RexNode project : this.exps) {
            if (project instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) project;
                System.out.print("Index: " + inputRef.getIndex() + ", ");
            }
            System.out.println();
        }
        return ((PRel) input).open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        /* Write your code here */
        ((PRel) input).close();
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
        return (input != null) && ((PRel) input).hasNext() && ((PRel) input).next() != null;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        /* Write your code here */
        while (((PRel) input).hasNext()) {
            Object[] row = ((PRel) input).next();
            if (row == null) {
                continue;
            }

            // Apply the projects to the row
            Object[] result = new Object[this.exps.size()];
            for (int i = 0; i < this.exps.size(); i++) {
                RexNode project = this.exps.get(i);

                // Evaluate the project against the row
                if (project instanceof RexInputRef) {
                    // If the project is a column reference, return the corresponding column value
                    RexInputRef inputRef = (RexInputRef) project;
                    result[i] = row[inputRef.getIndex()];
                } else if (project instanceof RexLiteral) {
                    // If the project is a literal, return the literal value
                    RexLiteral literal = (RexLiteral) project;
                    result[i] = literal.getValue();
                } else {
                    throw new UnsupportedOperationException("Unsupported RexNode type: " + project.getClass());
                }
            }
            return result;
        }
        return null;
    }
}
