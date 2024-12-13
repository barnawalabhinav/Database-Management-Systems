package manager;

import index.bplusTree.BPlusTreeIndexFile;
import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {         
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

//        System.out.println("Number of columns injected: " + columnNames.size());

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
//                System.out.println("Column name type:" + column_name_type[0] + ", " + columnNames.get(i));
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        // return null if file does not exist, or block_id is invalid
        // return list of records otherwise

        if(!check_file_exists(table_name)) {
            return null;
        }
        if (block_id <= 0 || db.get_data(file_to_fileid.get(table_name), block_id) == null) {
            return null;
        }
        byte[] schemaBlock = get_data_block(table_name, 0);
        int num_columns = (schemaBlock[0] & 0xFF) | (schemaBlock[1] << 8);

//        System.out.println("NUM cols:" + num_columns);

        String[] typeList = new String[num_columns];
        int num_fixed = 0, num_var = 0;
        for (int i = 0; i < num_columns; i++) {
            byte[] offsetBytes = new byte[2];
            offsetBytes[0] = schemaBlock[2 + 2 * i];
            offsetBytes[1] = schemaBlock[3 + 2 * i];
            int col_offsets = (offsetBytes[0] & 0xFF) | (offsetBytes[1] << 8);
            int col_type_ind = schemaBlock[col_offsets] & 0xFF;
            typeList[i] = ColumnType.values()[col_type_ind].name();
            if (typeList[i].equals("VARCHAR")) {
                num_var++;
            } else {
                num_fixed++;
            }
        }
//        System.out.println("FIXED = " + num_fixed + " VAR = " + num_var);
        byte[] blockBytes = get_data_block(table_name, block_id);
        int num_records = (blockBytes[0] << 8) | (blockBytes[1] & 0xFF);
//        System.out.println("NUM Records: " + num_records);
        List<Object[]> records = new ArrayList<>();
        int offset_pointer = 2;
        for (int i = 0; i < num_records; i++) {
            int pointer = (blockBytes[offset_pointer] << 8) | (blockBytes[offset_pointer + 1] & 0xFF);
            offset_pointer += 2;

            Object[] record = new Object[num_columns];
            int[] var_offset = new int[num_var];
            int[] var_length = new int[num_var];
            for (int j = 0; j < num_var; j++) {
                var_offset[j] = (blockBytes[pointer] & 0xFF) | (blockBytes[pointer + 1] << 8);
                var_length[j] = (blockBytes[pointer + 2] & 0xFF) | (blockBytes[pointer + 3] << 8);
                pointer += 4;
            }
            byte[] null_bitmap = new byte[(num_columns + 7) / 8];
//            System.out.println("NULL BITMAP LENGTH: " + null_bitmap.length);
//            System.out.println("VAR_offset 0 : " + var_offset[0]);
            System.arraycopy(blockBytes, var_offset[0] - null_bitmap.length, null_bitmap, 0, null_bitmap.length);

            for (int j = 0; j < num_fixed; j++) {
                if ((null_bitmap[j / 8] & (1 << (7 - j % 8))) == 1) {
                    record[j] = null;
                } else {
                    switch (typeList[j]) {
                        case "INTEGER": {
                            int val = (blockBytes[pointer] & 0xFF) | (blockBytes[pointer + 1] << 8) | (blockBytes[pointer + 2] << 16) | (blockBytes[pointer + 3] << 24);
                            record[j] = val;
                            break;
                        }
                        case "BOOLEAN":
                            record[j] = blockBytes[pointer] == 1;
                            break;
                        case "FLOAT": {
                            int val = (blockBytes[pointer] & 0xFF) | (blockBytes[pointer + 1] << 8) | (blockBytes[pointer + 2] << 16) | (blockBytes[pointer + 3] << 24);
                            record[j] = Float.intBitsToFloat(val);
                            break;
                        }
                        case "DOUBLE": {
                            long val = (blockBytes[pointer] & 0xFF) | (blockBytes[pointer + 1] << 8) | (blockBytes[pointer + 2] << 16) | (blockBytes[pointer + 3] << 24) | ((long) blockBytes[pointer + 4] << 32) | ((long) blockBytes[pointer + 5] << 40) | ((long) blockBytes[pointer + 6] << 48) | ((long) blockBytes[pointer + 7] << 56);
                            record[j] = Double.longBitsToDouble(val);
                            break;
                        }
                    }
                }
            }

            for (int j = 0; j < num_var; j++) {
                byte[] col_data = new byte[var_length[j]];
                if (var_length[j] >= 0) System.arraycopy(blockBytes, var_offset[j], col_data, 0, var_length[j]);
                if ((null_bitmap[(num_fixed + j) / 8] & (1 << (7 - (num_fixed + j) % 8))) == 1) {
                    record[num_fixed + j] = null;
                } else {
                    record[num_fixed + j] = new String(col_data);
                }
            }
            records.add(record);
        }

        return records;
    }

    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */
        // Hint: You need to create a new index file and write the index blocks

        byte[] schemaBlock = get_data_block(table_name, 0);
        int num_columns = (schemaBlock[0] & 0xFF) | (schemaBlock[1] << 8);
//        System.out.println("Number of columns retrieved: " + num_columns);
//        System.out.println("Column name to index: " + column_name);
        String[] typeList = new String[num_columns];
        String[] col_names = new String[num_columns];
        int num_fixed = 0, num_var = 0;
        int col_ind_in_record = -1;
        for (int i = 0; i < num_columns; i++) {
            int col_offset = (schemaBlock[2 + 2 * i] & 0xFF) | (schemaBlock[3 + 2 * i] << 8);
//            System.out.println("COL Offset:" + col_offset);
            int col_type_ind = schemaBlock[col_offset] & 0xFF;
//            System.out.println("COL TYPE ind:" + col_type_ind);
            int col_name_len = schemaBlock[col_offset + 1] & 0xFF;
            typeList[i] = ColumnType.values()[col_type_ind].name();
//            System.out.println("Retrieved Typename:" + typeList[i]);
            if (typeList[i].equals("VARCHAR")) {
                num_var++;
            } else {
                num_fixed++;
            }
            byte[] col_name_bytes = new byte[col_name_len];
            for (int j = 0; j < col_name_len; j++) {
                col_name_bytes[j] = schemaBlock[col_offset + 2 + j];
            }
            col_names[i] = new String(col_name_bytes);
            if (col_names[i].equals(column_name)) {
                col_ind_in_record = i;
            }
        }
        System.out.println();
        System.out.println(col_ind_in_record);
        assert (col_ind_in_record >= 0);

        BPlusTreeIndexFile indexFile = null;
        switch (typeList[col_ind_in_record]) {
            case "INTEGER":
                indexFile = new BPlusTreeIndexFile<>(order, Integer.class);
                break;
            case "BOOLEAN":
                indexFile = new BPlusTreeIndexFile<>(order, Boolean.class);
                break;
            case "FLOAT":
                indexFile = new BPlusTreeIndexFile<>(order, Float.class);
                break;
            case "DOUBLE":
                indexFile = new BPlusTreeIndexFile<>(order, Double.class);
                break;
            case "VARCHAR":
                indexFile = new BPlusTreeIndexFile<>(order, String.class);
                break;
        }

        int block_id = 1;
        List<Object[]> records = get_records_from_block(table_name, block_id);
        while (records != null) {
//            System.out.println("NUM Records: " + records.size());
            for (Object[] record : records) {
//                System.out.println("REcord = " + record[col_ind_in_record]);
                indexFile.insert(record[col_ind_in_record], block_id);
//                System.out.println("Index Block = " + indexFile.get_num_blocks());
            }
            block_id++;
//            System.out.println("Block id = "+ block_id);
            records = get_records_from_block(table_name, block_id);
        }
//        System.out.println("NUM Blocks: " + indexFile.get_num_blocks());
        int index_file_id = db.addFile(indexFile);
        file_to_fileid.put(table_name + "_" + column_name + "_index", index_file_id);
        return true;
    }

    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
        return db.search_index(file_id, value);
//        return -1;
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

}