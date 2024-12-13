package index.bplusTree;

import com.sun.org.apache.xpath.internal.operations.Bool;

/*
    * A LeafNode contains keys and block ids.
    * Looks Like -
    * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
    *
    * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        byte[] nextFreeOffsetBytes = this.get_data(6, 2);
        int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);


        int pointer = 10, i = 0;
        while (pointer < nextFreeOffset) {
            byte[] keyLenBytes = this.get_data(pointer, 2);
            int keyLen = (keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF);

            keys[i++] = convertBytesToT(this.get_data(pointer + 2, keyLen), typeClass);
            pointer += keyLen + 4;
        }

        return keys;

    }

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();

        int[] block_ids = new int[numKeys];

        /* Write your code here */
        byte[] nextFreeOffsetBytes = this.get_data(6, 2);
        int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);

        int pointer = 8, i = 0;
        while (pointer < nextFreeOffset) {
            byte[] blockId_keyLen_Bytes = this.get_data(pointer, 4);
            block_ids[i++] = (blockId_keyLen_Bytes[0] << 8) | (blockId_keyLen_Bytes[1] & 0xFF);
            int keyLen = (blockId_keyLen_Bytes[2] << 8) | (blockId_keyLen_Bytes[3] & 0xFF);
            pointer += keyLen + 4;
        }

        return block_ids;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int block_id) {

        /* Write your code here */
        byte[] nextFreeOffsetBytes = this.get_data(6, 2);
        int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);

        byte[] blockIdBytes = new byte[2];
        blockIdBytes[0] = (byte) ((block_id >> 8) & 0xFF);
        blockIdBytes[1] = (byte) (block_id & 0xFF);

        byte[] keyLenBytes = new byte[2];
        if (typeClass == Integer.class) {
            keyLenBytes[0] = 0;
            keyLenBytes[1] = 4;
        } else if (typeClass == Boolean.class) {
            keyLenBytes[0] = 0;
            keyLenBytes[1] = 1;
        } else if (typeClass == Double.class) {
            keyLenBytes[0] = 0;
            keyLenBytes[1] = 8;
        } else if (typeClass == Float.class) {
            keyLenBytes[0] = 0;
            keyLenBytes[1] = 4;
        } else if (typeClass == String.class) {
            keyLenBytes[0] = (byte) ((key.toString().length() >> 8) & 0xFF);
            keyLenBytes[1] = (byte) (key.toString().length() & 0xFF);
        }
        int keyLen = (keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF);

        byte[] keyBytes = new byte[keyLen];
        if (typeClass == Integer.class) {
            keyBytes[0] = (byte) (((Integer) key >> 24) & 0xFF);
            keyBytes[1] = (byte) (((Integer) key >> 16) & 0xFF);
            keyBytes[2] = (byte) (((Integer) key >> 8) & 0xFF);
            keyBytes[3] = (byte) ((Integer) key & 0xFF);
        } else if (typeClass == Boolean.class) {
            keyBytes[0] = (byte) ((Boolean) key ? 1 : 0);
        } else if (typeClass == Double.class) {
            keyBytes = new byte[8];
            for (int i = 0; i < 8; i++) {
                keyBytes[i] = (byte) ((Double.doubleToLongBits((Double) key) >> (8 * (7 - i))) & 0xFF);
            }
        } else if (typeClass == Float.class) {
            keyBytes = new byte[4];
            for (int i = 0; i < 4; i++) {
                keyBytes[i] = (byte) ((Float.floatToIntBits((Float) key) >> (8 * (3 - i))) & 0xFF);
            }
        } else if (typeClass == String.class) {
            keyBytes = key.toString().getBytes();
        }

        int pointer = 8, i = 0;
        while (pointer < nextFreeOffset) {
            byte[] blockId_keyLen_Bytes = this.get_data(pointer, 4);
            int keyLen_ = (blockId_keyLen_Bytes[2] << 8) | (blockId_keyLen_Bytes[3] & 0xFF);
            byte[] fnd_key_bytes = this.get_data(pointer + 4, keyLen_);
            T fnd_key = convertBytesToT(fnd_key_bytes, typeClass);
            if (compare(key, fnd_key) < 0) {
                break;
            }
            pointer += keyLen_ + 4;
        }

        if (pointer < nextFreeOffset) {
            byte[] temp = this.get_data(pointer, nextFreeOffset - pointer);
            this.write_data(pointer + 4 + keyLen, temp);
        }
        this.write_data(pointer, blockIdBytes);
        this.write_data(pointer + 2, keyLenBytes);
        this.write_data(pointer + 4, keyBytes);

        nextFreeOffset += 4 + keyLen;
        nextFreeOffsetBytes[0] = (byte) ((nextFreeOffset >> 8) & 0xFF);
        nextFreeOffsetBytes[1] = (byte) (nextFreeOffset & 0xFF);
        this.write_data(6, nextFreeOffsetBytes);

        int numEntries = getNumKeys() + 1;
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = (byte) ((numEntries >> 8) & 0xFF);
        numEntriesBytes[1] = (byte) (numEntries & 0xFF);
        this.write_data(0, numEntriesBytes);
        return;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        /* Write your code here */
        byte[] nextFreeOffsetBytes = this.get_data(6, 2);
        int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);

        int ans_block_id = -1;
        int pointer = 8, i = 0;
        while (pointer < nextFreeOffset) {
            byte[] blockId_keyLen_Bytes = this.get_data(pointer, 4);
            int block_id = (blockId_keyLen_Bytes[0] << 8) | (blockId_keyLen_Bytes[1] & 0xFF);
            int keyLen = (blockId_keyLen_Bytes[2] << 8) | (blockId_keyLen_Bytes[3] & 0xFF);
            byte[] fnd_key_bytes = this.get_data(pointer + 4, keyLen);
            T fnd_key = convertBytesToT(fnd_key_bytes, typeClass);
//            if (compare(key, fnd_key) < 0) {
//                return ans_block_id;
//            }
            if (compare(key, fnd_key) == 0) {
                return block_id;
            }
//            ans_block_id = block_id;
            pointer += keyLen + 4;
        }
        return ans_block_id;
    }

    public void set_prev_node_id(int prev_node_id) {
        byte[] prev_node_id_bytes = new byte[2];
        prev_node_id_bytes[0] = (byte) ((prev_node_id >> 8) & 0xFF);
        prev_node_id_bytes[1] = (byte) (prev_node_id & 0xFF);
        this.write_data(2, prev_node_id_bytes);
        return;
    }

    public void set_next_node_id(int next_node_id) {
        byte[] next_node_id_bytes = new byte[2];
        next_node_id_bytes[0] = (byte) ((next_node_id >> 8) & 0xFF);
        next_node_id_bytes[1] = (byte) (next_node_id & 0xFF);
        this.write_data(4, next_node_id_bytes);
        return;
    }

    public int get_prev_node_id() {
        byte[] prev_node_id_bytes = this.get_data(2, 2);
        return (prev_node_id_bytes[0] << 8) | (prev_node_id_bytes[1] & 0xFF);
    }

    public int get_next_node_id() {
        byte[] next_node_id_bytes = this.get_data(4, 2);
        return (next_node_id_bytes[0] << 8) | (next_node_id_bytes[1] & 0xFF);
    }

}
