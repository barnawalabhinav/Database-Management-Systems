package index.bplusTree;

import java.nio.ByteBuffer;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    
    // Might be useful for you - will not be evaluated
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        
        /* Write your code here */
        if (typeClass == Integer.class) {
            return (T) Integer.valueOf((bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | (bytes[3] & 0xFF));
        }
        if (typeClass == Boolean.class) {
            return (T) Boolean.valueOf(bytes[0] == 1);
        }
        if (typeClass == Double.class) {
            return (T) Double.valueOf(ByteBuffer.wrap(bytes).getDouble());
        }
        if (typeClass == Float.class) {
            return (T) Float.valueOf(ByteBuffer.wrap(bytes).getFloat());
        }
        if (typeClass == String.class) {
            return (T) new String(bytes);
        }
        return null;
    }

    default public int compare(T key1, T key2) {
        if (key1 instanceof Integer) {
            return Integer.compare((Integer) key1, (Integer) key2);
        }
        if (key1 instanceof Boolean) {
            return Boolean.compare((Boolean) key1, (Boolean) key2);
        }
        if (key1 instanceof Double) {
            return Double.compare((Double) key1, (Double) key2);
        }
        if (key1 instanceof Float) {
            return Float.compare((Float) key1, (Float) key2);
        }
        if (key2 instanceof String) {
            return ((String) key1).compareTo((String) key2);
        }
        return 0;
    }

}