package index.bplusTree;

import storage.AbstractFile;

import java.util.Queue;

import java.util.ArrayList;
import java.util.LinkedList;

/*
    * Tree is a collection of BlockNodes
    * The first BlockNode is the metadata block - stores the order and the block_id of the root node

    * The total number of keys in all leaf nodes is the total number of records in the records file.
*/

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return (orderBytes[0] << 8) | (orderBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    // will be evaluated
    public void insert(T key, int block_id) {

        /* Write your code here */
        int order = getOrder();
        int split_point = order / 2;

//        System.out.println("-KB pair- " + key + " : " + block_id);

        // Root is a LeafNode
        if (isLeaf(blocks.get(getRootId()))) {
            LeafNode<T> root = (LeafNode<T>) blocks.get(getRootId());
            if (!isFull(getRootId())) {
                root.insert(key, block_id);
            }
            else {
                LeafNode<T> newNode1 = new LeafNode<>(typeClass);
                LeafNode<T> newNode2 = new LeafNode<>(typeClass);
                T[] keys = root.getKeys();
                int[] blockIds = root.getBlockIds();
                T new_root_key = null;
                int i = 0, j = 0;
                while (i < split_point) {
                    if (j == i && root.compare(keys[j], key) > 0) {
                        newNode1.insert(key, block_id);
//                        if (i == (order-1) / 2) {
//                            new_root_key = key;
//                        }
                    } else {
                        newNode1.insert(keys[j], blockIds[j]);
//                        if (i == (order-1) / 2) {
//                            new_root_key = keys[j];
//                        }
                        j++;
                    }
                    i++;
                }
//                System.out.println("--t2--- ");
//                for (T eokey : newNode1.getKeys()) {
//                    System.out.print(eokey + " ");
//                }
//                System.out.println();
                while (i < order) {
                    if (j == order - 1 || (j == i && root.compare(keys[j], key) > 0)) {
                        newNode2.insert(key, block_id);
                        if (i == split_point) {
                            new_root_key = key;
                        }
                    } else {
                        newNode2.insert(keys[j], blockIds[j]);
                        if (i == split_point) {
                            new_root_key = keys[j];
                        }
                        j++;
                    }
                    i++;
                }
//                System.out.println("--t2--- ");
//                for (T eokey : newNode2.getKeys()) {
//                    System.out.print(eokey + " ");
//                }
//                System.out.println();
                int node1_id = blocks.size();
                int node2_id = node1_id + 1;
                newNode2.set_prev_node_id(node1_id);
                newNode1.set_next_node_id(node2_id);
                blocks.add(newNode1);
                blocks.add(newNode2);
                InternalNode<T> newRoot = new InternalNode<>(new_root_key, node1_id, node2_id, typeClass);
                blocks.set(getRootId(), newRoot);
            }
        } else {
            int node_id = getRootId();
            ArrayList<Integer> parents = new ArrayList<>();
            while (!isLeaf(node_id)) {
                parents.add(node_id);
                InternalNode<T> node = (InternalNode<T>) blocks.get(node_id);
                node_id = node.search(key);
            }
            LeafNode<T> node = (LeafNode<T>) blocks.get(node_id);
            T[] keys = node.getKeys();

//            System.out.println("Key-Node: " + key + ", " + node_id);

            //System.out.println("-----LEN----" + keys.length);
//            for (T eokey : keys) {
                //System.out.println(eokey);
//            }
            //System.out.println("------------");

//            ----------- To check for other empty eligible leaf nodes.
//            while (isFull(node_id) && node.compare(key, keys[keys.length-1]) == 0 && node.get_next_node_id() != 0) {
//                node_id = node.get_next_node_id();
//                node = (LeafNode<T>) blocks.get(node_id);
//            }

            if (!isFull(node_id)) {
                node.insert(key, block_id);
            } else {
                LeafNode<T> newNode1 = new LeafNode<>(typeClass);
                LeafNode<T> newNode2 = new LeafNode<>(typeClass);
                int[] blockIds = node.getBlockIds();
                T new_key = null;
                int i = 0, j = 0;
                while (i < split_point) {
                    if (j == i && node.compare(keys[j], key) > 0) {
                        newNode1.insert(key, block_id);
//                        if (i == (order - 1) / 2) {
//                            new_key = key;
//                        }
                    } else {
                        newNode1.insert(keys[j], blockIds[j]);
//                        if (i == (order - 1) / 2) {
//                            new_key = keys[j];
//                        }
                        j++;
                    }
                    i++;
                }

                //System.out.println("--t2--- ");
//                for (T eokey : newNode1.getKeys()) {
                      //System.out.print(eokey + " ");
//                }
                while (i < order) {
                    if (j == order - 1 || (j == i && node.compare(keys[j], key) > 0)) {
                        newNode2.insert(key, block_id);
                        if (i == split_point) {
                            new_key = key;
                        }
                    } else {
                        newNode2.insert(keys[j], blockIds[j]);
                        if (i == split_point) {
                            new_key = keys[j];
                        }
                        j++;
                    }
                    i++;
                }
                int right_child_id = blocks.size();
                int next_node_id = node.get_next_node_id();

                newNode1.set_prev_node_id(node.get_prev_node_id());
                newNode1.set_next_node_id(right_child_id);
                newNode2.set_prev_node_id(node_id);

                //System.out.print("--t3--- ");
//                for (T eokey : newNode2.getKeys()) {
                      //System.out.print(eokey + " ");
//                }
                //System.out.println("--************-- ");

                blocks.set(node_id, newNode1);
                blocks.add(newNode2);

                if (next_node_id > 0) {
                    LeafNode<T> next_node = (LeafNode<T>) blocks.get(next_node_id);
                    newNode2.set_next_node_id(next_node_id);
                    next_node.set_prev_node_id(right_child_id);
                    blocks.set(next_node_id, next_node);
                }
                //System.out.println("---- " + next_node_id + " ----");
                //System.out.println("--NK-- " + new_key + " ----");

                for (int pid = parents.size() - 1; pid >= 0; pid--) {
                    InternalNode<T> parent = (InternalNode<T>) blocks.get(parents.get(pid));
                    //System.out.println("Parent id: " + pid);
                    if (!isFull(parents.get(pid))) {
                        parent.insert(new_key, right_child_id);
                        break;
                    } else {
                        int[] child_ids = parent.getChildren();
                        T[] parent_keys = parent.getKeys();
                        ArrayList<T> keys1 = new ArrayList<>();
                        ArrayList<T> keys2 = new ArrayList<>();
                        ArrayList<Integer> children1 = new ArrayList<>();
                        ArrayList<Integer> children2 = new ArrayList<>();
                        children1.add(child_ids[0]);
                        i = 0; j = 0;
                        while (i < order / 2) {
                            if (j == i && node.compare(parent_keys[j], new_key) > 0) {
                                keys1.add(new_key);
                                children1.add(right_child_id);
                            } else {
                                keys1.add(parent_keys[j]);
                                children1.add(child_ids[j+1]);
                                j++;
                            }
                            i++;
                        }

                        // // ////System.out.println("Keys1");
//                        for (T eokey : keys1) {
                            // // ////System.out.print(eokey + " ");
//                        }
                        // // ////System.out.println();

                        T old_key = new_key;
                        if (j == order -1 || (j == i && node.compare(parent_keys[j], new_key) > 0)) {
                            children2.add(right_child_id);
                        } else {
                            new_key = parent_keys[j];
                            children2.add(child_ids[j+1]);
                            j++;
                        }
                        i++;

                        // // ////System.out.println("NewKey");
                        // // ////System.out.println(new_key);
                        // // ////System.out.println();

                        while (i < order) {
                            if (j == order -1 || (j == i && node.compare(parent_keys[j], new_key) > 0)) {
                                keys2.add(old_key);
                                children2.add(right_child_id);
                            } else {
                                keys2.add(parent_keys[j]);
                                children2.add(child_ids[j+1]);
                                j++;
                            }
                            i++;
                        }

                        // // ////System.out.println("Keys2");
//                        for (T eokey : keys2) {
                            // // ////System.out.print(eokey + " ");
//                        }
                        // // ////System.out.println();

                        InternalNode<T> inode1 = new InternalNode<>(keys1.get(0), children1.get(0), children1.get(1), typeClass);
                        InternalNode<T> inode2 = new InternalNode<>(keys2.get(0), children2.get(0), children2.get(1), typeClass);
                        for (i = 1; i < keys1.size(); i++) {
                            inode1.insert(keys1.get(i), children1.get(i+1));
                        }

//                        for (T eokey : inode1.getKeys()) {
                            //System.out.print(eokey + " ");
//                        }
                        //System.out.println();

                        for (i = 1; i < keys2.size(); i++) {
                            inode2.insert(keys2.get(i), children2.get(i+1));
                        }

//                        for (T eokey : inode2.getKeys()) {
                            //System.out.print(eokey + " ");
//                        }
                        //System.out.println("#### " + pid + " ####");
                        //System.out.println("--- " + new_key + " ---");

                        if (pid == 0) {
                            assert (parents.get(pid) == getRootId());
                            int left_child_id = blocks.size();
                            blocks.add(inode1);
                            right_child_id = blocks.size();
                            blocks.add(inode2);
                            InternalNode<T> newRoot = new InternalNode<>(new_key, left_child_id, right_child_id, typeClass);
                            blocks.set(parents.get(pid), newRoot);
                        } else {
                            blocks.set(parents.get(pid), inode1);
                            right_child_id = blocks.size();
                            blocks.add(inode2);
                        }
                    }
                }
            }
        }
        return;
    }

    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {

        /* Write your code here */
        int node_id = getRootId();
        while (!isLeaf(node_id)) {
            InternalNode<T> node = (InternalNode<T>) blocks.get(node_id);
            node_id = node.search(key);
//            for (T eokey : node.getKeys()) {
//                System.out.print(eokey + " ");
//            }
//            System.out.println();
        }
        LeafNode<T> node = (LeafNode<T>) blocks.get(node_id);
        if (node.search(key) == -1) {
            T[] keys = node.getKeys();
            for (T key_ : keys) {
                if (node.compare(key_, key) > 0) {
                    return node_id;
                }
            }
            if (node.get_next_node_id() == 0) {
                return -1;
            }
            return node.get_next_node_id();
        }
//        for (T eokey : node.getKeys()) {
//            System.out.print(eokey + " ");
//        }
//        System.out.println();
//        System.out.println("Last node:" + node_id);

        while (node.get_prev_node_id() != 0) {
            int prev_node_id = node.get_prev_node_id();
            node = (LeafNode<T>) blocks.get(prev_node_id);
//            for (T eokey : node.getKeys()) {
//                System.out.print(eokey + " ");
//            }
//            System.out.println();
            if (node.search(key) == -1) {
                break;
            }
            node_id = prev_node_id;
        }
//        LeafNode<T> node = (LeafNode<T>) blocks.get(node_id);
//        return node.search(key);
        return node_id;
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        Queue<Integer> level = new LinkedList<>();
        queue.add(root);
        level.add(0);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            int lev = level.remove();
//            System.out.print("\nLevel " + lev + ": ");
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                    level.add(lev + 1);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

}