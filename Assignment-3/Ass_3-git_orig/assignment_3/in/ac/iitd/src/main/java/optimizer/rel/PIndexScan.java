package optimizer.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import manager.StorageManager;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {
    
        private final List<RexNode> projects;
        private final RelDataType rowType;
        private final RelOptTable table;
        private final RexNode filter;
    
        public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
            super(cluster, traitSet, table);
            this.table = table;
            this.rowType = deriveRowType();
            this.filter = filter;
            this.projects = projects;
        }
    
        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new PIndexScan(getCluster(), traitSet, table, filter, projects);
        }
    
        @Override
        public RelOptTable getTable() {
            return table;
        }

        @Override
        public String toString() {
            return "PIndexScan";
        }

        public String getTableName() {
            return table.getQualifiedName().get(1);
        }

        @Override
        public List<Object[]> evaluate(StorageManager storage_manager) {
            String tableName = getTableName();
            System.out.println("Evaluating PIndexScan for table: " + tableName);

            /* Write your code here */
            RexCall filterCall = (RexCall) filter;
            RexNode columnRef = filterCall.getOperands().get(0);
            RexInputRef inputRef = (RexInputRef) columnRef;
            int columnInd = inputRef.getIndex();
            String op_name = filterCall.getOperator().getName();
//            System.out.println("Column ind: " + columnInd);
//            System.out.println("Filter condition: " + op_name);

            byte[] schemaBlock = storage_manager.get_data_block(tableName, 0);
            String columnName = "";
            int num_fixed = 0, num_var = 0;
            int col_offset = (schemaBlock[2 + 2 * columnInd] & 0xFF) | (schemaBlock[3 + 2 * columnInd] << 8);
            int col_name_len = schemaBlock[col_offset + 1] & 0xFF;
            byte[] col_name_bytes = new byte[col_name_len];
            for (int j = 0; j < col_name_len; j++) {
                col_name_bytes[j] = schemaBlock[col_offset + 2 + j];
            }
            columnName = new String(col_name_bytes);
//            System.out.println("Column Name: " + columnName);

            RexLiteral key_to_search = (RexLiteral) filterCall.getOperands().get(1);
            String operand_type = key_to_search.getType().toString();
//            System.out.println("Operand: " + key_to_search + " of type " + key_to_search.getType().toString());
//            assert (key_to_search.getType().toString().equals("INTEGER"));

            HashSet<Integer> explored_block_records = new HashSet<>();

            List<Object[]> records = new ArrayList<>();
            int leaf_block_id = storage_manager.search(tableName, columnName, key_to_search);
//            System.out.println("LEAF BLOCK ID: " + leaf_block_id);
            if (leaf_block_id == -1) {
                if (op_name.equals("<=") || op_name.equals("<")) {
                    int block_id = 1;
                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                    while (record_i != null) {
                        records.addAll(record_i);
                        block_id++;
                        record_i = storage_manager.get_records_from_block(tableName, block_id);
                    }
                }
                return records;
            } else {
                byte[] leaf_data = storage_manager.get_data_block(tableName + "_" + columnName + "_index", leaf_block_id);
                if (op_name.equals("=")) {
                    while (true) {
                        int num_keys = (leaf_data[0] << 8) | (leaf_data[1] & 0xFF);
                        //System.out.println(num_keys);
                        int next_leaf_block_id = (leaf_data[4] << 8) | (leaf_data[5] & 0xFF);
                        int nextFreeOffset = (leaf_data[6] << 8) | (leaf_data[7] & 0xFF);
                        int pointer = 8, i = 0;
                        while (pointer < nextFreeOffset) {
                            int block_id = (leaf_data[pointer] << 8) | (leaf_data[pointer + 1] & 0xFF);
                            int keyLen = (leaf_data[pointer + 2] << 8) | (leaf_data[pointer + 3] & 0xFF);
                            if (explored_block_records.contains(block_id)) {
                                pointer += keyLen + 4;
                                continue;
                            }

                            byte[] fnd_key_bytes = new byte[keyLen];
                            System.arraycopy(leaf_data, pointer + 4, fnd_key_bytes, 0, keyLen);
                            if (operand_type.equals("INTEGER")) {
                                Integer key = Integer.valueOf((fnd_key_bytes[0] << 24) | (fnd_key_bytes[1] << 16) | (fnd_key_bytes[2] << 8) | (fnd_key_bytes[3] & 0xFF));
                                //System.out.println("parsed KEY: " + key);

                                Integer search_key = key_to_search.getValueAs(Integer.class);
                                if (key.equals(search_key)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if (record[columnInd].equals(search_key)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                }
                                if (key.compareTo(search_key) > 0) {
                                    break;
                                }
                            } else if (operand_type.equals("BOOLEAN")) {
                                Boolean key = Boolean.valueOf(fnd_key_bytes[0] == 1);
                                //System.out.println("parsed KEY: " + key);

                                Boolean search_key = key_to_search.getValueAs(Boolean.class);
                                if (key.equals(search_key)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if (record[columnInd].equals(search_key)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                }
                                if (key.compareTo(search_key) > 0) {
                                    break;
                                }
                            } else if (operand_type.equals("DOUBLE")) {
                                Double key = Double.valueOf(ByteBuffer.wrap(fnd_key_bytes).getDouble());
                                //System.out.println("parsed KEY: " + key);

                                Double search_key = key_to_search.getValueAs(Double.class);
                                if (key.equals(search_key)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if (record[columnInd].equals(search_key)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                }
                                if (key.compareTo(search_key) > 0) {
                                    break;
                                }
                            } else if (operand_type.equals("FLOAT")) {
                                Float key = Float.valueOf(ByteBuffer.wrap(fnd_key_bytes).getFloat());
                                //System.out.println("parsed KEY: " + key);

                                Float search_key = key_to_search.getValueAs(Float.class);
                                if (key.equals(search_key)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if (record[columnInd].equals(search_key)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                }
                                if (key.compareTo(search_key) > 0) {
                                    break;
                                }
                            } else if (operand_type.equals("VARCHAR")) {
                                String key = new String(fnd_key_bytes);
                                //System.out.println("parsed KEY: " + key);

                                String search_key = key_to_search.getValueAs(String.class);
                                if (key.equals(search_key)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if (record[columnInd].equals(search_key)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                }
                                if (key.compareTo(search_key) > 0) {
                                    break;
                                }
                            }
                            pointer += keyLen + 4;
                        }
                        if ((pointer < nextFreeOffset) || (next_leaf_block_id <= 0)) {
                            break;
                        }
                        leaf_data = storage_manager.get_data_block(tableName + "_" + columnName + "_index", next_leaf_block_id);
                    }
                } else if (op_name.equals(">") || op_name.equals(">=")) {
                    while (true) {
                        int num_keys = (leaf_data[0] << 8) | (leaf_data[1] & 0xFF);
                        //System.out.println(num_keys);
                        int next_leaf_block_id = (leaf_data[4] << 8) | (leaf_data[5] & 0xFF);
                        int nextFreeOffset = (leaf_data[6] << 8) | (leaf_data[7] & 0xFF);
                        int pointer = 8, i = 0;
                        while (pointer < nextFreeOffset) {
                            int block_id = (leaf_data[pointer] << 8) | (leaf_data[pointer + 1] & 0xFF);
                            int keyLen = (leaf_data[pointer + 2] << 8) | (leaf_data[pointer + 3] & 0xFF);
                            if (explored_block_records.contains(block_id)) {
                                pointer += keyLen + 4;
                                continue;
                            }

                            byte[] fnd_key_bytes = new byte[keyLen];
                            System.arraycopy(leaf_data, pointer + 4, fnd_key_bytes, 0, keyLen);
                            if (operand_type.equals("INTEGER")) {
                                Integer key = Integer.valueOf((fnd_key_bytes[0] << 24) | (fnd_key_bytes[1] << 16) | (fnd_key_bytes[2] << 8) | (fnd_key_bytes[3] & 0xFF));
                                System.out.println("parsed KEY: " + key);

                                Integer search_key = key_to_search.getValueAs(Integer.class);
                                if ((op_name.equals(">") && key.compareTo(search_key) > 0) || (op_name.equals(">=") && key.compareTo(search_key) >= 0)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if ((op_name.equals(">") && search_key.compareTo((Integer) record[columnInd]) < 0) || (op_name.equals(">=") && search_key.compareTo((Integer) record[columnInd]) <= 0)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                }
                            } else if (operand_type.equals("BOOLEAN")) {
                                Boolean key = Boolean.valueOf(fnd_key_bytes[0] == 1);
                                //System.out.println("parsed KEY: " + key);

                                Boolean search_key = key_to_search.getValueAs(Boolean.class);
                                if ((op_name.equals(">") && key.compareTo(search_key) > 0) || (op_name.equals(">=") && key.compareTo(search_key) >= 0)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if ((op_name.equals(">") && search_key.compareTo((Boolean) record[columnInd]) < 0) || (op_name.equals(">=") && search_key.compareTo((Boolean) record[columnInd]) <= 0)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                }
                            } else if (operand_type.equals("DOUBLE")) {
                                Double key = Double.valueOf(ByteBuffer.wrap(fnd_key_bytes).getDouble());
                                //System.out.println("parsed KEY: " + key);

                                Double search_key = key_to_search.getValueAs(Double.class);
                                if ((op_name.equals(">") && key.compareTo(search_key) > 0) || (op_name.equals(">=") && key.compareTo(search_key) >= 0)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if ((op_name.equals(">") && search_key.compareTo((Double) record[columnInd]) < 0) || (op_name.equals(">=") && search_key.compareTo((Double) record[columnInd]) <= 0)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                }
                            } else if (operand_type.equals("FLOAT")) {
                                Float key = Float.valueOf(ByteBuffer.wrap(fnd_key_bytes).getFloat());
                                //System.out.println("parsed KEY: " + key);

                                Float search_key = key_to_search.getValueAs(Float.class);
                                if ((op_name.equals(">") && key.compareTo(search_key) > 0) || (op_name.equals(">=") && key.compareTo(search_key) >= 0)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if ((op_name.equals(">") && search_key.compareTo((Float) record[columnInd]) < 0) || (op_name.equals(">=") && search_key.compareTo((Float) record[columnInd]) <= 0)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                }
                            } else if (operand_type.equals("VARCHAR")) {
                                String key = new String(fnd_key_bytes);
                                //System.out.println("parsed KEY: " + key);

                                String search_key = key_to_search.getValueAs(String.class);
                                if ((op_name.equals(">") && key.compareTo(search_key) > 0) || (op_name.equals(">=") && key.compareTo(search_key) >= 0)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if ((op_name.equals(">") && search_key.compareTo((String) record[columnInd]) < 0) || (op_name.equals(">=") && search_key.compareTo((String) record[columnInd]) <= 0)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                }
                            }
                            pointer += keyLen + 4;
                        }
                        if (next_leaf_block_id <= 0) {
                            break;
                        }
                        leaf_data = storage_manager.get_data_block(tableName + "_" + columnName + "_index", next_leaf_block_id);
                    }
                } else if (op_name.equals("<=") || op_name.equals("<")) {
                    int prev_leaf_block_id = (leaf_data[2] << 8) | (leaf_data[3] & 0xFF);
                    while (prev_leaf_block_id > 0) {
                        leaf_data = storage_manager.get_data_block(tableName + "_" + columnName + "_index", prev_leaf_block_id);
                        prev_leaf_block_id = (leaf_data[2] << 8) | (leaf_data[3] & 0xFF);
                    }

                    while (true) {
                        int num_keys = (leaf_data[0] << 8) | (leaf_data[1] & 0xFF);
                        //System.out.println(num_keys);
                        int next_leaf_block_id = (leaf_data[4] << 8) | (leaf_data[5] & 0xFF);
                        int nextFreeOffset = (leaf_data[6] << 8) | (leaf_data[7] & 0xFF);
                        int pointer = 8, i = 0;
                        while (pointer < nextFreeOffset) {
                            int block_id = (leaf_data[pointer] << 8) | (leaf_data[pointer + 1] & 0xFF);
                            int keyLen = (leaf_data[pointer + 2] << 8) | (leaf_data[pointer + 3] & 0xFF);
                            if (explored_block_records.contains(block_id)) {
                                pointer += keyLen + 4;
                                continue;
                            }

                            byte[] fnd_key_bytes = new byte[keyLen];
                            System.arraycopy(leaf_data, pointer + 4, fnd_key_bytes, 0, keyLen);
                            if (operand_type.equals("INTEGER")) {
                                Integer key = Integer.valueOf((fnd_key_bytes[0] << 24) | (fnd_key_bytes[1] << 16) | (fnd_key_bytes[2] << 8) | (fnd_key_bytes[3] & 0xFF));
                                //System.out.println("parsed KEY: " + key);

                                Integer search_key = key_to_search.getValueAs(Integer.class);
                                if ((op_name.equals("<") && key.compareTo(search_key) < 0) || (op_name.equals("<=") && key.compareTo(search_key) <= 0)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if ((op_name.equals("<") && search_key.compareTo((Integer) record[columnInd]) > 0) || (op_name.equals("<=") && search_key.compareTo((Integer) record[columnInd]) >= 0)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                } else {
                                    break;
                                }
                            } else if (operand_type.equals("BOOLEAN")) {
                                Boolean key = Boolean.valueOf(fnd_key_bytes[0] == 1);
                                //System.out.println("parsed KEY: " + key);

                                Boolean search_key = key_to_search.getValueAs(Boolean.class);
                                if ((op_name.equals("<") && key.compareTo(search_key) < 0) || (op_name.equals("<=") && key.compareTo(search_key) <= 0)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if ((op_name.equals("<") && search_key.compareTo((Boolean) record[columnInd]) > 0) || (op_name.equals("<=") && search_key.compareTo((Boolean) record[columnInd]) >= 0)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                } else {
                                    break;
                                }
                            } else if (operand_type.equals("DOUBLE")) {
                                Double key = Double.valueOf(ByteBuffer.wrap(fnd_key_bytes).getDouble());
                                //System.out.println("parsed KEY: " + key);

                                Double search_key = key_to_search.getValueAs(Double.class);
                                if ((op_name.equals("<") && key.compareTo(search_key) < 0) || (op_name.equals("<=") && key.compareTo(search_key) <= 0)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if ((op_name.equals("<") && search_key.compareTo((Double) record[columnInd]) > 0) || (op_name.equals("<=") && search_key.compareTo((Double) record[columnInd]) >= 0)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                } else {
                                    break;
                                }
                            } else if (operand_type.equals("FLOAT")) {
                                Float key = Float.valueOf(ByteBuffer.wrap(fnd_key_bytes).getFloat());
                                //System.out.println("parsed KEY: " + key);

                                Float search_key = key_to_search.getValueAs(Float.class);
                                if ((op_name.equals("<") && key.compareTo(search_key) < 0) || (op_name.equals("<=") && key.compareTo(search_key) <= 0)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if ((op_name.equals("<") && search_key.compareTo((Float) record[columnInd]) > 0) || (op_name.equals("<=") && search_key.compareTo((Float) record[columnInd]) >= 0)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                } else {
                                    break;
                                }
                            } else if (operand_type.equals("VARCHAR")) {
                                String key = new String(fnd_key_bytes);
                                //System.out.println("parsed KEY: " + key);

                                String search_key = key_to_search.getValueAs(String.class);
                                if ((op_name.equals("<") && key.compareTo(search_key) < 0) || (op_name.equals("<=") && key.compareTo(search_key) <= 0)) {
                                    List<Object[]> record_i = storage_manager.get_records_from_block(tableName, block_id);
                                    for (Object[] record : record_i) {
                                        if ((op_name.equals("<") && search_key.compareTo((String) record[columnInd]) > 0) || (op_name.equals("<=") && search_key.compareTo((String) record[columnInd]) >= 0)) {
                                            System.out.println("Adding " + record[columnInd]);
records.add(record);
                                        }
                                    }
                                    explored_block_records.add(block_id);
                                } else {
                                    break;
                                }
                            }
                            pointer += keyLen + 4;
                        }
                        if ((pointer < nextFreeOffset) || (next_leaf_block_id <= 0)) {
                            break;
                        }
                        leaf_data = storage_manager.get_data_block(tableName + "_" + columnName + "_index", next_leaf_block_id);
                    }
                }

                return records;
            }
        }
}