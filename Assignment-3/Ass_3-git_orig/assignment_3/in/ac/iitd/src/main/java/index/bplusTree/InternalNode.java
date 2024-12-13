package index.bplusTree;

/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        byte[] nextFreeOffsetBytes = this.get_data(2, 2);
        int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);
        int pointer = 6, i = 0;
        while (pointer < nextFreeOffset) {
            byte[] keyLenBytes = this.get_data(pointer, 2);
            int keyLen = (keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF);
            keys[i++] = convertBytesToT(this.get_data(pointer + 2, keyLen), typeClass);
            pointer += keyLen + 4;
        }

        return keys;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int right_block_id) {
        /* Write your code here */
        byte[] nextFreeOffsetBytes = this.get_data(2, 2);
        int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);

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

        byte[] blockIdBytes = new byte[2];
        blockIdBytes[0] = (byte) ((right_block_id >> 8) & 0xFF);
        blockIdBytes[1] = (byte) (right_block_id & 0xFF);

        int pointer = 6, i = 0;
        while (pointer < nextFreeOffset) {
            byte[] keyLenBytes_ = this.get_data(pointer, 2);
            int keyLen_ = (keyLenBytes_[0] << 8) | (keyLenBytes_[1] & 0xFF);
            byte[] keyBytes_ = this.get_data(pointer + 2, keyLen_);
            T key_ = convertBytesToT(keyBytes_, typeClass);
            if (compare(key, key_) < 0) {
                break;
            }
            pointer += keyLen_ + 4;
        }
        if (pointer < nextFreeOffset) {
            byte[] temp = this.get_data(pointer, nextFreeOffset - pointer);
            this.write_data(pointer + 4 + keyLen, temp);
        }
        this.write_data(pointer, keyLenBytes);
        this.write_data(pointer + 2, keyBytes);
        this.write_data(pointer + 2 + keyLen, blockIdBytes);

        nextFreeOffset += 4 + keyLen;
        nextFreeOffsetBytes[0] = (byte) ((nextFreeOffset >> 8) & 0xFF);
        nextFreeOffsetBytes[1] = (byte) (nextFreeOffset & 0xFF);
        this.write_data(2, nextFreeOffsetBytes);

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
        byte[] nextFreeOffsetBytes = this.get_data(2, 2);
        int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);

        byte[] childIdBytes = this.get_data(4, 2);
        int childId = (childIdBytes[0] << 8) | (childIdBytes[1] & 0xFF);

        int pointer = 6, i = 1;
        while (pointer < nextFreeOffset) {
            byte[] keyLenBytes = this.get_data(pointer, 2);
            int keyLen = (keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF);

            byte[] fnd_key_bytes = this.get_data(pointer + 2, keyLen);
            T fnd_key = convertBytesToT(fnd_key_bytes, typeClass);
            if (compare(key, fnd_key) < 0) {
//                System.out.println("key: " + key + " fnd_key: " + fnd_key);
                return childId;
            }

            childIdBytes = this.get_data(pointer + keyLen + 2, 2);
            childId = (childIdBytes[0] << 8) | (childIdBytes[1] & 0xFF);
//            if (compare(key, fnd_key) == 0) {
//                return childId;
//            }

            pointer += keyLen + 4;
        }
        return childId;
    }

    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];

        /* Write your code here */
        byte[] nextFreeOffsetBytes = this.get_data(2, 2);
        int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);

        byte[] childIdBytes = this.get_data(4, 2);
        children[0] = (childIdBytes[0] << 8) | (childIdBytes[1] & 0xFF);

        int pointer = 6, i = 1;
        while (pointer < nextFreeOffset) {
            byte[] keyLenBytes = this.get_data(pointer, 2);
            int keyLen = (keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF);

            childIdBytes = this.get_data(pointer + keyLen + 2, 2);
            int childId = (childIdBytes[0] << 8) | (childIdBytes[1] & 0xFF);

            children[i++] = childId;
            pointer += keyLen + 4;
        }

        return children;
    }

}
