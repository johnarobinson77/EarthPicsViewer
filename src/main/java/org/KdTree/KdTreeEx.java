package org.KdTree;
/*
 * Copyright (c) 2019, John A. Robinson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * <p>
 * The KdTreeEx class extends the KdTree class with some functions that aid in speeding up DBSCAN clustering.
 * </p>
 *
 * @author John Robinson
 */
public class KdTreeEx<VALUE_TYPE_EX> extends KdTree<VALUE_TYPE_EX> {

    public KdTreeEx(int numPoints, int numDimensions) {
        super(numPoints, numDimensions);
    }

    private class NodeReference {
        long[] key;
        VALUE_TYPE_EX value;
    } // ValueReference


    private int pickValue(final KdNode myThis, final NodeReference valuePtr, final long selector,
                          boolean removePick, final int[] permutation, final int depth){

        // Look up the partition.
        final int p = permutation[depth];
        // init the return result to 0
        int returnResult = 0;

        boolean goGtThan = (selector & 0x1) == 1;

        // if greater than or less than, continue the search on  the appropriate child node
        if ((!goGtThan || myThis.gtChild == null) && myThis.ltChild != null) {
            returnResult = pickValue(myThis.ltChild, valuePtr, selector >> 1, removePick,  permutation, depth+1);
            // if the node below declared itself dead, remove the link
            if (removePick && returnResult == -1) myThis.ltChild = null;
        } else if((goGtThan || myThis.ltChild == null) && myThis.gtChild != null) {
            returnResult = pickValue(myThis.gtChild, valuePtr, selector >> 1, removePick,  permutation, depth+1);
            // if the node below declared itself dead, remove the link
            if (removePick && returnResult == -1) myThis.gtChild = null;
        } else {// both child pointers must be null to get here so this is a leaf node
            if (myThis.value.size() > 0) {
                valuePtr.value = (VALUE_TYPE_EX) myThis.value.get(myThis.value.size()-1);  // assuming that taking the last value is the fastest
                valuePtr.key = myThis.tuple;
                if (removePick) {
                    myThis.value.remove(myThis.value.size() - 1);
                    returnResult = -1;
                }  else {
                    returnResult = 1;
                }
            }
        }
        // Now figure out what to return.  If something was found either here or below return 1 if this node
        // is still active or -1 if this node is dead.  Otherwise return a 0.
        if (returnResult == -1 && (myThis.value.size() != 0 || myThis.ltChild != null || myThis.gtChild != null)) {
            returnResult = 1;
        }
        return returnResult;
    }

    /**
     * <p>
     * The {@code pickValue) Picks an arbitrary value from the tree.
     * </p>
     *
     * @param key - array where the kee will be put.
     * @param query - Array containing the search point
     * @param valueToRemove - value to remove at that point
     * @returns a boolean which is true if that value was removed.
     */
    public VALUE_TYPE_EX pickValue(long[] key, int selectionBias, boolean remove) {
        // if the tree is not built yet, build it
        if (root == null) {
            buildTree();
            // if root is still null; return a null
            if (root == null) return null;
        }
        // descent selector
        long selector = 0;
        switch (selectionBias) {
            case 0 : // less than side of the tree
                selector = 0L;
                break;
            case 1: // greater than side of the tree.
                selector = 0x7FFFFFFFFFFFFFFFL;
                break;
            case 2: // middle case
                selector = 0x2AAAAAAAAAAAAAAAL;
                break;
            case 3: // make a random selector
                selector = new Random().nextLong();
                break;
            default:
                System.out.println("Selection Bias " + selectionBias + " not avaiable");
                return null;
        }
        // search the tree to find the node to remove.
        NodeReference nodeRef = new NodeReference();
        int returnResult = pickValue(root, nodeRef, selector, remove, permutation, 0);
        // A result of 0 indicates that no value was found to pick which is an internal error.
        // -1 or +1 indicate a value was removed.
        if (returnResult == 0) return null;
        for(int i = 0; i < nodeRef.key.length; i++){
            key[i] = nodeRef.key[i];
        }
        return (VALUE_TYPE_EX)nodeRef.value;
    }

    /**
     * <p>
     * The {@code searchKdTree} method searches the k-d tree and finds the KdNodes
     * that lie within a cutoff distance from a query node in all k dimensions.
     * </p>
     *
     * @param myThis - Reference to the current KdNode of interest.  Basically a explicit instead of implicit "this"
     * @param result - ArrayList to which the values of k-d nodes that lie within the cutoff
     *               distance of the query node will be added.
     * @param queryPlus - Array containing the lager search bound for each dimension
     * @param queryMinus - Array containing the smaller search bound for each dimension
     * @param permutation - an array that indicates permutation of the reference arrays
     * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
     * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
     * @param depth - the depth in the k-d tree
     * @return void
     */
    private static void searchKdTree(final KdNode myThis, final List<KdTree.KdNode> result, final long[] queryPlus,
                                     final long[] queryMinus, final int[] permutation, final ExecutorService executor,
                                     final int maximumSubmitDepth, final int depth) {

        // Look up the partition.
        final int p = permutation[depth];
        // get a list ready for the other thread to use
        List<KdNode> threadList = null;

        // If the distance from the query node to the k-d node is within the cutoff distance
        // in all k dimensions, add the k-d node to a list.

        boolean inside = true;
        for (int i = 0; i < myThis.tuple.length; i++) {
            if ((queryPlus[i]  <= myThis.tuple[i]) ||
                    (queryMinus[i] > myThis.tuple[i]) ) {
                inside = false;
                break;
            }
        }
        if (inside) {
            result.add(myThis);
        }

        // Search the < branch of the k-d tree if the partition coordinate of the query point minus
        // the cutoff distance is <= the partition coordinate of the k-d node. The < branch
        // must be searched when the cutoff distance equals the partition coordinate because the super
        // key may assign a point to either branch of the tree if the sorting or partition coordinate,
        // which forms the most significant portion of the super key, shows equality.
        Future<List<KdNode>> future = null;
        if (myThis.ltChild != null) {
            if (queryMinus[p] <= myThis.tuple[p]) {

                // Search the < branch with a child thread at as many levels of the tree as possible.
                // Create the child threads as high in the tree as possible for greater utilization.
                // If maxSubmitDepth == -1, there are no child threads.
                if (maximumSubmitDepth > -1 && depth <= maximumSubmitDepth) {
                    threadList = new ArrayList<>();
                    future =
                            executor.submit(searchKdTreeWithThread(myThis.ltChild, result, queryPlus, queryMinus,
                                    permutation, executor, maximumSubmitDepth, depth + 1) );
                } else {
                    searchKdTree(myThis.ltChild, result, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth,depth + 1);
                }
            }
        }

        // Search the > branch of the k-d tree if the partition coordinate of the query point plus
        // the cutoff distance is >= the partition coordinate of the k-d node.  Note the transformation
        // of this test: (query[p] + cut >= tuple[p]) -> (tuple[p] - query[p] <= cut).  The < branch
        // must be searched when the cutoff distance equals the partition coordinate because the super
        // key may assign a point to either branch of the tree if the sorting or partition coordinate,
        // which forms the most significant portion of the super key, shows equality.
        if (myThis.gtChild != null) {
            if (queryPlus[p] >= myThis.tuple[p]) {
                searchKdTree(myThis. gtChild, result, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth,
                        depth + 1);
            }
        }

        // If a child thread searched the < branch, get the result.
        if (future != null) {
            try {
                future.get();
                result.addAll(threadList);
            } catch (Exception e) {
                throw new RuntimeException( "future exception: " + e.getMessage() );
            }
        }
        return;
    }

    /**
     * <p>
     * The {@code searchKdTreeWithThread} method returns a
     * {@link java.util.concurrent.Callable Callable} whose call() method executes the
     * searchTree method.
     * </p>
     *
     * @param result - ArrayList to which the values of k-d nodes that lie within the cutoff
     *               distance of the query node will be added.
     * @param queryPlus - Array containing the lager search bound for each dimension
     * @param queryMinus - Array containing the smaller search bound for each dimension
     * @param permutation - an array that indicates permutation of the reference arrays
     * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
     * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
     * @param depth - the depth in the k-d tree
     * that contains the k-d nodes that lie within the cutoff distance of the query node
     */
    private static Callable searchKdTreeWithThread(final KdNode myThis, final List<KdNode> result,
                                                   final long[] queryPlus, final long[] queryMinus,
                                                   final int[] permutation, final ExecutorService executor,
                                                   final int maximumSubmitDepth, final int depth) {
        return new Callable() {
            @Override
            public Object call() {
                searchKdTree(myThis, result, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, depth);
                return null;
            }
        };
    }

    /**
     * <p>
     * The {@code searchTree} search the tree for all nodes contained within the bounds
     * set by queryPlus and queryMinus and return the values and tuples associated with those nodes.
     * </p>
     *
     * @param tuples - Array that upon return will contain the tuples found within the query BB
     * @param values - Array that upon return will contain the values found within the query BB
     * @param queryPlus - Array containing the lager search bound for each dimension
     * @param queryMinus - Array containing the smaller search bound for each dimension
     */
    public void searchTree(List<long[]> tuples, List<VALUE_TYPE_EX> values, final long[] queryPlus, final long[] queryMinus) {

        // check that the values in Plus are > than the values in Minus
        for(int i = 0;  i < queryMinus.length; i++) {
            if (queryMinus[i] > queryPlus[i]) {
                long T = queryMinus[i];
                queryMinus[i] = queryPlus[i];
                queryPlus[i] = T;
            }
        }
        List<KdNode> results = new ArrayList<KdNode>();
        searchKdTree(root, results, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, 0);
        for(KdNode kn : results){
            for(Object val : kn.value) {
                tuples.add(kn.tuple);
                values.add((VALUE_TYPE_EX) val);
            }
        }
    }
    /**
     * <p>
     * The {@code getNumDimensions} return the number of dimensions of each tuple, set in the constructor.
     * </p>
     */
    public int getNumDimensions() { return numDimensions; }


    private void test(KdTree myKdTree){
        /*****************************
         // remove test
         *****************************/
        /*
        // get the current node count.
        //int removeCount = myKdTree.root.verifyKdTree(myKdTree.permutation, myKdTree.executor, myKdTree.maximumSubmitDepth, 0);
        // cfind a new query point to test remove()
        val = myKdTree.pickValue(2, false);
        if (val == null) {
            System.out.println("pickValue should have picked a value but didn't");
        }
        // get the tuple for that picked point.  Only works because the value in this case in index of the KdNode
        KdTree.KdNode<Integer> pickNode = myKdTree.kdNodes[val];
        //pickQuery = pickNode.tuple;
        // search the tree with that point and make sure it returns something.
        //kdNodeValuesAlt = myKdTree.searchTree(pickQuery, 1);
        //if (!kdNodeValuesAlt.contains(val)) {
            System.out.println("Search before remove should have returned the picked value");
        }
        // remove the value and check to see that it was successful.
        //if (!myKdTree.remove(pickQuery, val)){
            System.out.println("Call to remove was not successful");
        }
        // check to see that that value is not in the tree any more.
        kdNodeValuesAlt = myKdTree.searchTree(query, 1);
        //if (kdNodeValuesAlt.contains(val)) {
            System.out.println("Search after remove should not have returned the picked value");
        }
        // verify the tree and make sure not to many points have been removed.
        removeCount -= myKdTree.root.verifyKdTree(myKdTree.permutation, myKdTree.executor, myKdTree.maximumSubmitDepth, 0);
        if (removeCount != 0 && removeCount != 1) {
            System.out.println("remove() cause an unexpected number of node changes.");
        }
        */
        /*****************************
         // pick and remove test
         *****************************/
        /*
        // verify the tree and get a node count.
        int removeCount = myKdTree.root.verifyKdTree(myKdTree.permutation, myKdTree.executor, myKdTree.maximumSubmitDepth, 0);
        // pick a value from the tree with removal anc check to see that it was successfull
        Integer val = myKdTree.pickValue(3, true);
        if (val == null) {
            System.out.println("pickValue should have picked a value but didn't");
        }
        // see if that value was removed from the tree.  This works because the value is the index into the kdnodes[]
        long[] pickQuery = myKdTree.kdNodes[val].tuple;
        ArrayList<Integer> notPickedValues = (ArrayList)myKdTree.searchTree(pickQuery, 1);
        if ( notPickedValues.contains(val) ){
            System.out.println("pickValue did not delete the picked value from the tree.");
        }
        // verify the tree and make sure not to many points have been removed.
        removeCount -= myKdTree.root.verifyKdTree(myKdTree.permutation, myKdTree.executor, myKdTree.maximumSubmitDepth, 0);
        if (removeCount != 0 && removeCount != 1) {
            System.out.println("pickValue() cause an unexpected number of node changes.");
        }

        */
    }



}
