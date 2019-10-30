package org.KdTree;
/*
 * Copyright (c) 2015, 2019, Russell A. Brown and John A. Robinson
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * <p>
 * The k-d tree was described by Jon Bentley in "Multidimensional Binary Search Trees Used
 * for Associative Searching", CACM 18(9): 509-517, 1975.  For k dimensions and n elements
 * of data, a balanced k-d tree is built in O(kn log n) + O((k+1)n log n) time by first
 * sorting the data in each of k dimensions, then building the k-d tree in a manner that
 * preserves the order of the k sorts while recursively partitioning the data at each level
 * of the k-d tree.  No further sorting is necessary.  Moreover, it is possible to replace
 * the O((k+1)n log n) term with a O((k-1)n log n) term but this approach sacrifices the
 * generality of building the k-d tree for points of any number of dimensions.
 * </p>
 *
 * @author Russell A. Brown
 */
public class KdTree<VALUE_TYPE> {
    // enables printing the time stats.
    private final static boolean printstats = false;

    /**
     * <p>
     * The {@code KdNode} class stores a point of any number of dimensions
     * as well as references to the "less than" and "greater than" sub-trees.
     * </p>
     */
    static class KdNode<VALUE_TYPE> {

        private static final int INSERTION_SORT_CUTOFF = 15;

        // Change to whatever value type desired; NOTE that a List requires a lot of memory.
        List<VALUE_TYPE> value;
        long[] tuple;
        KdNode ltChild, gtChild;
        static KdNode[] KdNodes;

        /**
         * <p>
         * KdNode constructor
         * <p>
         */
        protected KdNode(int numDimentions) {
            this.tuple = new long[numDimentions];
            this.value = new ArrayList<VALUE_TYPE>(1);
            this.ltChild = null; // Redundant
            this.gtChild = null;
        }

        /**
         * <p>
         * The {@code initializeReference} method initializes one reference array.
         * </p>
         *
         * @param coordinates - a KdNode[]
         * @param reference - a KdNode[]
         */
        private static void initializeReference(final KdNode[] coordinates,
                                                final KdNode[] reference) {
            for (int i = 0; i < reference.length; i++) {
                reference[i] = coordinates[i];
            }
        }

        /**
         * <p>
         * The {@code initializeReferenceWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#initializeReference initializeReference} method.
         * </p>
         *
         * @param coordinates - a KdNode[]
         * @param reference - a KdNode[]
         */
        private static Callable<Void> initializeReferenceWithThread(final KdNode[] coordinates,
                                                                    final KdNode[] reference) {

            return new Callable<Void>() {
                @Override
                public Void call() {
                    initializeReference(coordinates, reference);
                    return null;
                }
            };
        }

        /**
         * <p>
         * The {@code superKeyCompare} method compares two int[] in as few coordinates as possible
         * and uses the sorting or partition coordinate as the most significant coordinate.
         * </p>
         *
         * @param a - a int[]
         * @param b - a int[]
         * @param p - the most significant dimension
         * @return an int that represents the result of comparing two super keys
         */
        private static long superKeyCompare(final long[] a, final long[] b, final int p) {
            long diff = a[p] - b[p];
            for (int i = 1; diff == 0 && i < a.length; i++) {
                int r = i + p;
                // A fast alternative to the modulus operator for (i + p) < 2 * a.length.
                r = (r < a.length) ? r : r - a.length;
                diff = a[r] - b[r];
            }
            return diff;
        }

        /**
         * <p> The {@code mergeResultsAscending} method compares the results in the source array in order of
         * ascending address and merges them into the destination array in order of ascending value.
         * </p>
         *
         * @param destination - a KdNode[] from which to merge results
         * @param source - a KdNode[] into which to merge results
         * @param iStart - the initial value of the i-index
         * @param jStart - the initial value of the j-index
         * @param kStart - the initial value of the k-index
         * @param kEnd - the final value of the k-index
         * @param p - the sorting partition (x, y, z, w...)
         */
        private static void mergeResultsAscending(final KdNode[] destination, final KdNode[] source,
                                                  final int iStart, final int jStart, final int kStart,
                                                  final int kEnd, final int p) {

            for (int i = iStart, j = jStart, k = kStart; k <= kEnd; k++) {
                destination[k] = (superKeyCompare(source[i].tuple, source[j].tuple, p) <= 0) ? source[i++] : source[j--];
            }
        }

        /**
         * <p> The {@code mergeResultsDescending} method compares the results in the source array in order of
         * ascending address and merges them into the destination array in order of descending value.
         * </p>
         *
         * @param destination - a KdNode[] from which to merge results
         * @param source - a KdNode[] into which to merge results
         * @param iStart - the initial value of the i-index
         * @param jStart - the initial value of the j-index
         * @param kStart - the initial value of the k-index
         * @param kEnd - the final value of the k-index
         * @param p - the sorting partition (x, y, z, w...)
         */
        private static void mergeResultsDescending(final KdNode[] destination, final KdNode[] source,
                                                   final int iStart, final int jStart, final int kStart,
                                                   final int kEnd, final int p) {

            for (int i = iStart, j = jStart, k = kStart; k <= kEnd; k++) {
                destination[k] = (superKeyCompare(source[i].tuple, source[j].tuple, p) >= 0) ? source[i++] : source[j--];
            }
        }

        /**
         * <p>
         * The {@code mergeResultsAscendingWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#mergeResultsAscending mergeResultsAscending} method.
         * </p>
         *
         * @param destination - a KdNode[] from which to merge results
         * @param source - a KdNode[] into which to merge results
         * @param iStart - the initial value of the i-index
         * @param jStart - the initial value of the j-index
         * @param kStart - the initial value of the k-index
         * @param kEnd - the final value of the k-index
         * @param p - the sorting partition (x, y, z, w...)
         */
        private static Callable<Void> mergeResultsAscendingWithThread(final KdNode[] destination, final KdNode[] source,
                                                                      final int iStart, final int jStart, final int kStart,
                                                                      final int kEnd, final int p) {

            return new Callable<Void>() {
                @Override
                public Void call() {
                    mergeResultsAscending(destination, source, iStart, jStart, kStart, kEnd, p);
                    return null;
                }
            };
        }

        /**
         * <p>
         * The {@code mergeResultsDescendingWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#mergeResultsDescending mergeResultsDescending} method.
         * </p>
         *
         * @param destination - a KdNode[] from which to merge results
         * @param source - a KdNode[] into which to merge results
         * @param iStart - the initial value of the i-index
         * @param jStart - the initial value of the j-index
         * @param kStart - the initial value of the k-index
         * @param kEnd - the final value of the k-index
         * @param p - the sorting partition (x, y, z, w...)
         */
        private static Callable<Void> mergeResultsDescendingWithThread(final KdNode[] destination, final KdNode[] source,
                                                                       final int iStart, final int jStart, final int kStart,
                                                                       final int kEnd, final int p) {

            return new Callable<Void>() {
                @Override
                public Void call() {
                    mergeResultsDescending(destination, source, iStart, jStart, kStart, kEnd, p);
                    return null;
                }
            };
        }

        /**
         * <p>
         * The following four merge sort methods are adapted from the mergesort function that is shown
         * on p. 166 of Robert Sedgewick's "Algorithms in C++", Addison-Wesley, Reading, MA, 1992.
         * That elegant implementation of the merge sort algorithm eliminates the requirement to test
         * whether the upper and lower halves of an auxiliary array have become exhausted during the
         * merge operation that copies from the auxiliary array to a result array.  This elimination is
         * made possible by inverting the order of the upper half of the auxiliary array and by accessing
         * elements of the upper half of the auxiliary array from highest address to lowest address while
         * accessing elements of the lower half of the auxiliary array from lowest address to highest
         * address.
         * </p>
         * <p>
         * The following four merge sort methods also implement two suggestions from p. 275 of Robert
         * Sedgewick's and Kevin Wayne's "Algorithms 4th Edition", Addison-Wesley, New York, 2011.  The
         * first suggestion is to replace merge sort with insertion sort when the size of the array to
         * sort falls below a threshold.  The second suggestion is to avoid unnecessary copying to the
         * auxiliary array prior to the merge step of the algorithm by implementing two versions of
         * merge sort and by applying some "recursive trickery" to arrange that the required result is
         * returned in the auxiliary array by one version and in the result array by the other version.
         * The following four merge sort methods build upon this suggestion and return their result in
         * either ascending or descending (inverted) order.
         * </p>
         * <p>
         * During multi-threaded execution, the upper and lower halves of the result array may be filled
         * from the auxiliary array (or vice versa) simultaneously by two threads.  The lower half of the
         * result array is filled by accessing elements of the upper half of the auxiliary array from highest
         * address to lowest address while accessing elements of the lower half of the auxiliary array from
         * lowest address to highest address, as explained above for elimination of the test for exhaustion.
         * The upper half of the result array is filled by addressing elements from the upper half of the
         * auxiliary array from lowest address to highest address while accessing the elements from the lower
         * half of the auxiliary array from highest address to lowest address.  Note: for the upper half
         * of the result array, there is no requirement to test for exhaustion provided that upper half of
         * the result array never comprises more elements than the lower half of the result array.  This
         * provision is satisfied by computing the median address of the result array as shown below for
         * all four merge sort methods.
         * </p>
         * <p>
         * The {@code mergeSortReferenceAscending} method recursively subdivides the array to be sorted
         * then merges the elements in ascending order and leaves the result in the reference array.
         * </p>
         *
         * @param reference - a KdNode[]
         * @param temporary - a scratch KdNode[] from which to copy results;
         *                    this array must be as large as the reference array
         * @param low - the start index of the region of the reference array
         * @param high - the high index of the region of the reference array
         * @param p - the sorting partition (x, y, z, w...)
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param depth - the depth of subdivision
         */
        private static void mergeSortReferenceAscending(final KdNode[] reference, final KdNode[] temporary,
                                                        final int low, final int high,
                                                        final int p, final ExecutorService executor,
                                                        final int maximumSubmitDepth, int depth) {

            if (high - low > INSERTION_SORT_CUTOFF) {

                // Avoid overflow when calculating the median address.
                final int mid = low + ( (high - low) >> 1 );

                // Subdivide the lower half of the tree with a child thread at as many levels of the tree as possible.
                // Create the child threads as high in the subdivision hierarchy as possible for greater utilization.

                // Is a child thread available to subdivide the lower half of the reference array?
                if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

                    // No, recursively subdivide the lower half of the reference array with the current
                    // thread and return the result in the temporary array in ascending order.
                    mergeSortTemporaryAscending(reference, temporary, low, mid, p, executor, maximumSubmitDepth, depth + 1);

                    // Then recursively subdivide the upper half of the reference array with the current
                    // thread and return the result in the temporary array in descending order.
                    mergeSortTemporaryDescending(reference, temporary, mid + 1, high, p, executor, maximumSubmitDepth, depth + 1);

                    // Compare the results in the temporary array in ascending order and merge them into
                    // the reference array in ascending order.
                    for (int i = low, j = high, k = low; k <= high; k++) {
                        reference[k] =
                                (superKeyCompare(temporary[i].tuple, temporary[j].tuple, p) < 0) ? temporary[i++] : temporary[j--];
                    }

                } else {

                    // Yes, a child thread is available, so recursively subdivide the lower half of the reference
                    // array with a child thread and return the result in the temporary array in ascending order.
                    final Future<Void> sortFuture =
                            executor.submit( mergeSortTemporaryAscendingWithThread(reference, temporary,
                                    low, mid, p, executor, maximumSubmitDepth, depth + 1) );

                    // And simultaneously, recursively subdivide the upper half of the reference array with
                    // the current thread and return the result in the temporary array in descending order.
                    mergeSortTemporaryDescending(reference, temporary, mid + 1, high, p, executor, maximumSubmitDepth, depth + 1);

                    // Then get the result of subdividing the lower half of the reference array with the child thread.
                    try {
                        sortFuture.get();
                    } catch (Exception e) {
                        throw new RuntimeException( "sort future exception: " + e.getMessage() );
                    }

                    // Compare the results in the temporary array in ascending order with a child thread
                    // and merge them into the lower half of the reference array in ascending order.
                    final Future<Void> mergeFuture =
                            executor.submit( mergeResultsAscendingWithThread(reference, temporary, low, high, low, mid, p) );

                    // And simultaneously compare the results in the temporary array in descending order with the
                    // current thread and merge them into the upper half of the reference array in ascending order.
                    for (int i = mid, j = mid + 1, k = high; k > mid; k--) {
                        reference[k] =
                                (superKeyCompare(temporary[i].tuple, temporary[j].tuple, p) > 0) ? temporary[i--] : temporary[j++];
                    }

                    // Then get the result of merging into the lower half of the reference array with the child thread.
                    try {
                        mergeFuture.get();
                    } catch (Exception e) {
                        throw new RuntimeException( "merge future exception: " + e.getMessage() );
                    }
                }

            } else {

                // Here is Jon Benley's implementation of insertion sort from "Programming Pearls", pp. 115-116,
                // Addison-Wesley, 1999, that sorts in ascending order and leaves the result in the reference array.
                for (int i = low + 1; i <= high; i++) {
                    KdNode tmp = reference[i];
                    int j;
                    for (j = i; j > low && superKeyCompare(reference[j-1].tuple, tmp.tuple, p) > 0; j--) {
                        reference[j] = reference[j-1];
                    }
                    reference[j] = tmp;
                }
            }
        }

        /**
         * <p>
         * The {@code mergeSortReferenceDescending} method recursively subdivides the array to be sorted
         * then merges the elements in descending order and leaves the result in the reference array.
         * </p>
         *
         * @param reference - a KdNode[]
         * @param temporary - a scratch KdNode[] from which to copy results;
         *                    this array must be as large as the reference array
         * @param low - the start index of the region of the reference array
         * @param high - the high index of the region of the reference array
         * @param p - the sorting partition (x, y, z, w...)
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param depth - the depth of subdivision
         */
        private static void mergeSortReferenceDescending(final KdNode[] reference, final KdNode[] temporary,
                                                         final int low, final int high,
                                                         final int p, final ExecutorService executor,
                                                         final int maximumSubmitDepth, int depth) {

            if (high - low > INSERTION_SORT_CUTOFF) {

                // Avoid overflow when calculating the median address.
                final int mid = low + ( (high - low) >> 1 );

                // Subdivide the lower half of the tree with a child thread at as many levels of the tree as possible.
                // Create the child threads as high in the subdivision hierarchy as possible for greater utilization.

                // Is a child thread available to subdivide the lower half of the reference array?
                if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

                    // No, recursively subdivide the lower half of the reference array with the current
                    // thread and return the result in the temporary array in descending order.
                    mergeSortTemporaryDescending(reference, temporary, low, mid, p, executor, maximumSubmitDepth, depth + 1);

                    // Then recursively subdivide the upper half of the reference array with the current
                    // thread and return the result in the temporary array in ascending order.
                    mergeSortTemporaryAscending(reference, temporary, mid + 1, high, p, executor, maximumSubmitDepth, depth + 1);

                    // Compare the results in the temporary array in ascending order and merge them into
                    // the reference array in descending order.
                    for (int i = low, j = high, k = low; k <= high; k++) {
                        reference[k] =
                                (superKeyCompare(temporary[i].tuple, temporary[j].tuple, p) > 0) ? temporary[i++] : temporary[j--];
                    }

                } else {

                    // Yes, a child thread is available, so recursively subdivide the lower half of the reference
                    // array with a child thread and return the result in the temporary array in descending order.
                    final Future<Void> sortFuture =
                            executor.submit( mergeSortTemporaryDescendingWithThread(reference, temporary,
                                    low, mid, p, executor, maximumSubmitDepth, depth + 1) );

                    // And simultaneously, recursively subdivide the upper half of the reference array with
                    // the current thread and return the result in the temporary array in ascending order.
                    mergeSortTemporaryAscending(reference, temporary, mid + 1, high, p, executor, maximumSubmitDepth, depth + 1);

                    // Then get the result of subdividing the lower half of the reference array with the child thread.
                    try {
                        sortFuture.get();
                    } catch (Exception e) {
                        throw new RuntimeException( "sort future exception: " + e.getMessage() );
                    }

                    // Compare the results in the temporary array in ascending order with a child thread
                    // and merge them into the lower half of the reference array in descending order.
                    final Future<Void> mergeFuture =
                            executor.submit( mergeResultsDescendingWithThread(reference, temporary, low, high, low, mid, p) );

                    // And simultaneously compare the results in the temporary array in descending order with the
                    // current thread and merge them into the upper half of the reference array in descending order.
                    for (int i = mid, j = mid + 1, k = high; k > mid; k--) {
                        reference[k] =
                                (superKeyCompare(temporary[i].tuple, temporary[j].tuple, p) < 0) ? temporary[i--] : temporary[j++];
                    }

                    // Then get the result of merging into the lower half of the reference array with the child thread.
                    try {
                        mergeFuture.get();
                    } catch (Exception e) {
                        throw new RuntimeException( "merge future exception: " + e.getMessage() );
                    }
                }

            } else {

                // Here is Jon Benley's implementation of insertion sort from "Programming Pearls", pp. 115-116,
                // Addison-Wesley, 1999, that sorts in descending order and leaves the result in the reference array.
                for (int i = low + 1; i <= high; i++) {
                    KdNode tmp = reference[i];
                    int j;
                    for (j = i; j > low && superKeyCompare(reference[j-1].tuple, tmp.tuple, p) < 0; j--) {
                        reference[j] = reference[j-1];
                    }
                    reference[j] = tmp;
                }
            }
        }

        /**
         * <p>
         * The {@code mergeSortTemporaryAscending} method recursively subdivides the array to be sorted
         * then merges the elements in ascending order and leaves the result in the temporary array.
         * </p>
         *
         * @param reference - a KdNode[]
         * @param temporary - a scratch KdNode[] into which to copy results;
         *                    this array must be as large as the reference array
         * @param low - the start index of the region of the reference array
         * @param high - the high index of the region of the reference array
         * @param p - the sorting partition (x, y, z, w...)
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param depth - the depth of subdivision
         */
        private static void mergeSortTemporaryAscending(final KdNode[] reference, final KdNode[] temporary,
                                                        final int low, final int high,
                                                        final int p, final ExecutorService executor,
                                                        final int maximumSubmitDepth, int depth) {

            if (high - low > INSERTION_SORT_CUTOFF) {

                // Avoid overflow when calculating the median address.
                final int mid = low + ( (high - low) >> 1 );

                // Subdivide the lower half of the tree with a child thread at as many levels of the tree as possible.
                // Create the child threads as high in the subdivision hierarchy as possible for greater utilization.

                // Is a child thread available to subdivide the lower half of the reference array?
                if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

                    // No, recursively subdivide the lower half of the reference array with the current
                    // thread and return the result in the reference array in ascending order.
                    mergeSortReferenceAscending(reference, temporary, low, mid, p, executor, maximumSubmitDepth, depth + 1);

                    // Then recursively subdivide the upper half of the reference array with the current
                    // thread and return the result in the reference array in descending order.
                    mergeSortReferenceDescending(reference, temporary, mid + 1, high, p, executor, maximumSubmitDepth, depth + 1);

                    // Compare the results in the reference array in ascending order and merge them into
                    // the temporary array in ascending order.
                    for (int i = low, j = high, k = low; k <= high; k++) {
                        temporary[k] =
                                (superKeyCompare(reference[i].tuple, reference[j].tuple, p) < 0) ? reference[i++] : reference[j--];
                    }


                } else {

                    // Yes, a child thread is available, so recursively subdivide the lower half of the reference
                    // array with a child thread and return the result in the reference array in ascending order.
                    final Future<Void> sortFuture =
                            executor.submit( mergeSortReferenceAscendingWithThread(reference, temporary,
                                    low, mid, p, executor, maximumSubmitDepth, depth + 1) );

                    // And simultaneously, recursively subdivide the upper half of the reference array with
                    // the current thread and return the result in the reference array in descending order.
                    mergeSortReferenceDescending(reference, temporary, mid + 1, high, p, executor, maximumSubmitDepth, depth + 1);

                    // Then get the result of subdividing the lower half of the reference array with the child thread.
                    try {
                        sortFuture.get();
                    } catch (Exception e) {
                        throw new RuntimeException( "sort future exception: " + e.getMessage() );
                    }

                    // Compare the results in the reference array in ascending order with a child thread
                    // and merge them into the lower half of the temporary array in ascending order.
                    final Future<Void> mergeFuture =
                            executor.submit( mergeResultsAscendingWithThread(temporary, reference, low, high, low, mid, p) );

                    // And simultaneously compare the results in the reference array in descending order with the
                    // current thread and merge them into the upper half of the temporary array in ascending order.
                    for (int i = mid, j = mid + 1, k = high; k > mid; k--) {
                        temporary[k] =
                                (superKeyCompare(reference[i].tuple, reference[j].tuple, p) > 0) ? reference[i--] : reference[j++];
                    }

                    // Then get the result of merging into the lower half of the temporary array with the child thread.
                    try {
                        mergeFuture.get();
                    } catch (Exception e) {
                        throw new RuntimeException( "merge future exception: " + e.getMessage() );
                    }
                }

            } else {

                // This implementation of insertion sort leaves the result in the temporary array in ascending order.
                temporary[high] = reference[high];
                int i, j;
                for (j = high - 1; j >= low; j--) {
                    for (i = j; i < high; i++) {
                        if(superKeyCompare(reference[j].tuple, temporary[i + 1].tuple, p) > 0) {
                            temporary[i] = temporary[i + 1];
                        } else {
                            break;
                        }
                    }
                    temporary[i] = reference[j];
                }
            }
        }

        /**
         * <p>
         * The {@code mergeSortTemporaryDescending} method recursively subdivides the array to be sorted
         * then merges the elements in descending order and leaves the result in the temporary array.
         * </p>
         *
         * @param reference - a KdNode[]
         * @param temporary - a scratch array into which to copy results;
         *                    this array must be as large as the reference array
         * @param low - the start index of the region of the reference array
         * @param high - the high index of the region of the reference array
         * @param p - the sorting partition (x, y, z, w...)
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param depth - the depth of subdivision
         */
        private static void mergeSortTemporaryDescending(final KdNode[] reference, final KdNode[] temporary,
                                                         final int low, final int high,
                                                         final int p, final ExecutorService executor,
                                                         final int maximumSubmitDepth, int depth) {

            if (high - low > INSERTION_SORT_CUTOFF) {

                // Avoid overflow when calculating the median address.
                final int mid = low + ( (high - low) >> 1 );

                // Subdivide the lower half of the tree with a child thread at as many levels of the tree as possible.
                // Create the child threads as high in the subdivision hierarchy as possible for greater utilization.

                // Is a child thread available to subdivide the lower half of the reference array?
                if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

                    // No, recursively subdivide the lower half of the reference array with the current
                    // thread and return the result in the reference array in descending order.
                    mergeSortReferenceDescending(reference, temporary, low, mid, p, executor, maximumSubmitDepth, depth + 1);

                    // Then recursively subdivide the upper half of the reference array with the current thread
                    // thread and return the result in the reference array in ascending order.
                    mergeSortReferenceAscending(reference, temporary, mid + 1, high, p, executor, maximumSubmitDepth, depth + 1);

                    // Compare the results in the reference array in ascending order and merge them into
                    // the temporary array in descending order.
                    for (int i = low, j = high, k = low; k <= high; k++) {
                        temporary[k] =
                                (superKeyCompare(reference[i].tuple, reference[j].tuple, p) > 0) ? reference[i++] : reference[j--];
                    }


                } else {

                    // Yes, a child thread is available, so recursively subdivide the lower half of the reference
                    // array with a child thread and return the result in the reference array in descending order.
                    final Future<Void> sortFuture =
                            executor.submit( mergeSortReferenceDescendingWithThread(reference, temporary,
                                    low, mid, p, executor, maximumSubmitDepth, depth + 1) );

                    // And simultaneously, recursively subdivide the upper half of the reference array with
                    // the current thread and return the result in the reference array in ascending order.
                    mergeSortReferenceAscending(reference, temporary, mid + 1, high, p, executor, maximumSubmitDepth, depth + 1);

                    // Then get the result of subdividing the lower half of the reference array with the child thread.
                    try {
                        sortFuture.get();
                    } catch (Exception e) {
                        throw new RuntimeException( "sort future exception: " + e.getMessage() );
                    }

                    // Compare the results in the reference array in ascending order with a child thread
                    // and merge them into the lower half of the temporary array in descending order.
                    final Future<Void> mergeFuture =
                            executor.submit( mergeResultsDescendingWithThread(temporary, reference, low, high, low, mid, p) );

                    // And simultaneously compare the results in the reference array in descending order with the
                    // current thread and merge them into the upper half of the temporary array in descending order.
                    for (int i = mid, j = mid + 1, k = high; k > mid; k--) {
                        temporary[k] =
                                (superKeyCompare(reference[i].tuple, reference[j].tuple, p) < 0) ? reference[i--] : reference[j++];
                    }

                    // Then get the result of merging into the lower half of the temporary array with the child thread.
                    try {
                        mergeFuture.get();
                    } catch (Exception e) {
                        throw new RuntimeException( "merge future exception: " + e.getMessage() );
                    }
                }

            } else {

                // This implementation of insertion sort leaves the result in the temporary array in descending order.
                temporary[high] = reference[high];
                int i, j;
                for (j = high - 1; j >= low; j--) {
                    for (i = j; i < high; i++) {
                        if (superKeyCompare(reference[j].tuple, temporary[i + 1].tuple, p) < 0) {
                            temporary[i] = temporary[i + 1];
                        } else {
                            break;
                        }
                    }
                    temporary[i] = reference[j];
                }
            }
        }

        /**
         * <p>
         * The {@code mergeSortReferenceAscendingWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#mergeSortReferenceAscending mergeSortReferenceAscending} method.
         * </p>
         *
         * @param reference - a KdNode[]
         * @param temporary - a scratch KdNode[] from which to copy results;
         *                    this array must be as large as the reference array.
         * @param low - the start index of the region of the reference array
         * @param high - the high index of the region of the reference array
         * @param p - the sorting partition (x, y, z, w...)
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param depth - the depth of subdivision
         */
        private static Callable<Void> mergeSortReferenceAscendingWithThread(final KdNode[] reference, final KdNode[] temporary,
                                                                            final int low, final int high, final int p,
                                                                            final ExecutorService executor,
                                                                            final int maximumSubmitDepth, final int depth) {

            return new Callable<Void>() {
                @Override
                public Void call() {
                    mergeSortReferenceAscending(reference, temporary, low, high, p, executor, maximumSubmitDepth, depth);
                    return null;
                }
            };
        }

        /**
         * <p>
         * The {@code mergeSortReferenceDescendingWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#mergeSortReferenceDescending mergeSortReferenceDescending} method.
         * </p>
         *
         * @param reference - a KdNode[]
         * @param temporary - a scratch KdNode[] from which to copy results;
         *                    this array must be as large as the reference array.
         * @param low - the start index of the region of the reference array
         * @param high - the high index of the region of the reference array
         * @param p - the sorting partition (x, y, z, w...)
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param depth - the depth of subdivision
         */
        private static Callable<Void> mergeSortReferenceDescendingWithThread(final KdNode[] reference, final KdNode[] temporary,
                                                                             final int low, final int high, final int p,
                                                                             final ExecutorService executor,
                                                                             final int maximumSubmitDepth, final int depth) {

            return new Callable<Void>() {
                @Override
                public Void call() {
                    mergeSortReferenceDescending(reference, temporary, low, high, p, executor, maximumSubmitDepth, depth);
                    return null;
                }
            };
        }

        /**
         * <p>
         * The {@code mergeSortTemporaryAscendingWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#mergeSortTemporaryAscending mergeSortTemporaryAscending} method.
         * </p>
         *
         * @param reference - a KdNode[]
         * @param temporary - a scratch KdNode[] into which to copy results;
         *                    this array must be as large as the reference array.
         * @param low - the start index of the region of the reference array
         * @param high - the high index of the region of the reference array
         * @param p - the sorting partition (x, y, z, w...)
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param depth - the depth of subdivision
         */
        private static Callable<Void> mergeSortTemporaryAscendingWithThread(final KdNode[] reference, final KdNode[] temporary,
                                                                            final int low, final int high, final int p,
                                                                            final ExecutorService executor,
                                                                            final int maximumSubmitDepth, final int depth) {

            return new Callable<Void>() {
                @Override
                public Void call() {
                    mergeSortTemporaryAscending(reference, temporary, low, high, p, executor, maximumSubmitDepth, depth);
                    return null;
                }
            };
        }

        /**
         * <p>
         * The {@code mergeSortTemporaryDescendingWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#mergeSortTemporaryDescending mergeSortTemporaryDescending} method.
         * </p>
         *
         * @param reference - a KdNode[]
         * @param temporary - a scratch KdNode[] into which to copy results;
         *                    this array must be as large as the reference array.
         * @param low - the start index of the region of the reference array
         * @param high - the high index of the region of the reference
         * @param p - the sorting partition (x, y, z, w...)
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param depth - the depth of subdivision
         */
        private static Callable<Void> mergeSortTemporaryDescendingWithThread(final KdNode[] reference, final KdNode[] temporary,
                                                                             final int low, final int high, final int p,
                                                                             final ExecutorService executor,
                                                                             final int maximumSubmitDepth, final int depth) {

            return new Callable<Void>() {
                @Override
                public Void call() {
                    mergeSortTemporaryDescending(reference, temporary, low, high, p, executor, maximumSubmitDepth, depth);
                    return null;
                }
            };
        }

        /**
         * <p>
         * The {@code removeDuplicates} method} checks the validity of the merge sort
         * and removes from the reference array all but one of a set of references that
         * reference duplicate tuples.
         * </p>
         *
         * @param reference - a KdNode[]
         * @param p - the index of the most significant coordinate in the super key
         * @return the address of the last element of the references array following duplicate removal
         */
        private static int removeDuplicates(final KdNode[] reference, final int p) {
            int end = 0;
            for (int i = 1; i < reference.length; i++) {
                long compare = superKeyCompare(reference[i].tuple, reference[i-1].tuple, p);
                if (compare < 0) {
                    throw new RuntimeException( "merge sort failure: superKeyCompare(ref[" +
                            Integer.toString(i) + "], ref[" + Integer.toString(i-1) +
                            "], (" + Integer.toString(p) + ") = " + Long.toString(compare) );
                } else if (compare > 0) {
                    reference[++end] = reference[i]; // Keep this element of the reference array.
                } else {
                    // Discard this element of the reference array and
                    // add its value list to the list of the prior element.
                    reference[end].value.addAll(reference[i].value);
                }
            }
            return end;
        }


        /**
         * <p>
         * Build a k-d tree by recursively partitioning the reference arrays and adding nodes to the tree.
         * These arrays are permuted cyclically for successive levels of the tree in order that sorting use
         * x, y, z, etc. as the most significant portion of the sorting or partitioning key.  The contents
         * of the reference arrays are scrambled by each recursive partitioning.
         * </p>
         *
         * @param references - multiple arrays of KdNode[]
         * @param temporary - a scratch KdNode[] for use in partitioning
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param start - the first element of the reference array
         * @param end - the last element of the reference array
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param depth - the depth in the k-d tree
         * @returns the root of the k-d tree
         */
        private static KdNode buildKdTree(final KdNode[][] references, final KdNode[] temporary,
                                          final int[] permutation, final int start, final int end,
                                          final ExecutorService executor, final int maximumSubmitDepth,
                                          final int depth) {

            final KdNode node;

            // The partition cycles as x, y, z, etc.
            final int p = permutation[depth];

            if (end == start) {

                // Only one reference was passed to this method, so store it at this level of the tree.
                node = references[0][start];

            } else if (end == start + 1) {

                // Two references were passed to this method in sorted order, so store the start
                // element at this level of the tree and store the end element as the > child.
                node = references[0][start];
                node.gtChild = references[0][end];

            } else if (end == start + 2) {

                // Three references were passed to this method in sorted order, so
                // store the median element at this level of the tree, store the start
                // element as the < child and store the end element as the > child.
                node = references[0][start + 1];
                node.ltChild = references[0][start];
                node.gtChild = references[0][end];

            } else if (end > start + 2) {

                // Four or more references were passed to this method.  Partitioning of the other reference
                // arrays will occur about the median element of references[0].  Avoid overflow when
                // calculating the median.  Store the median element of references[0] in a new k-d node.
                int median = start + ((end - start) >> 1);
                if (median <= start || median >= end) {
                    throw new RuntimeException("error in median calculation at depth = " + depth +
                            " : start = " + start + "  median = " + median + "  end = " + end);
                }
                node = references[0][median];

                // Copy references[0] to the temporary array before partitioning.
                for (int i = start; i <= end; i++) {
                    temporary[i] = references[0][i];
                }

                // Sweep through each of the other reference arrays in its a priori sorted order
                // and partition it into "less than" and "greater than" halves by comparing
                // super keys.  Store the result from references[i] in references[i-1], thus
                // permuting the reference arrays.  Skip the element of references[i] that
                // references a point that equals the point that is stored in the new k-d node.
                for (int i = 1; i < references.length; i++) {

                    // Is a child thread available to partition the lower half of one reference array?
                    if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

                        // No, so partiion the lower half of the reference array with the current thread.
                        scanAndPartitionLower(references, node, p, i, start, median);

                        // Then partition the upper half of the reference array with the current thread.
                        scanAndPartitionUpper(references, node, p, i, median, end);

                    } else {

                        // Yes, a child thread is available, so partition the lower half
                        // of the reference array with a child thread.
                        final Future<Void> future =
                                executor.submit( scanAndPartitionLowerWithThread(references, node, p, i,
                                        start, median) );
                        // And simultaneously partition the upper half of the reference
                        // array with the current thread.
                        scanAndPartitionUpper(references, node, p, i, median, end);

                        // Then get the result of partitioning the lower half
                        // of the references array with the child thread.
                        try {
                            future.get();
                        } catch (Exception e) {
                            throw new RuntimeException( "partition future exception: " + e.getMessage() );
                        }
                    }
                }

                // Copy the temporary array to the last reference array to finish permutation.  The copies to
                // and from the temporary array produce the O((k+1)n log n)  term of the computational
                // complexity.  This term may be reduced to a O((k-1)n log n) term for (x,y,z) coordinates
                // by eliminating these copies and explicitly passing x, y, z and t (temporary) arrays to this
                // buildKdTree method, then copying t<-x, x<-y and y<-z, then explicitly passing x, y, t and z
                // to the next level of recursion.  However, this approach would sacrifice the generality
                // of sorting points of any number of dimensions because explicit calling parameters
                // would need to be passed to this method for each specific number of dimensions.
                for (int i = start; i <= end; i++) {
                    references[references.length - 1][i] = temporary[i];
                }

                // Build the < branch with a child thread at as many levels of the tree as possible.
                // Create the child threads as high in the tree as possible for greater utilization.

                // Is a child thread available to build the < branch?
                if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

                    // No, so recursively build the < branch of the tree with the current thread.
                    node.ltChild = buildKdTree(references, temporary, permutation,
                            start, median - 1, executor, maximumSubmitDepth, depth + 1);

                    // Then recursively build the > branch of the tree with the current thread.
                    node.gtChild = buildKdTree(references, temporary, permutation,
                            median + 1, end, executor, maximumSubmitDepth, depth + 1);

                } else {

                    // Yes, a child thread is available, so recursively build the < branch with a child thread.
                    final Future<KdNode> future =
                            executor.submit( buildKdTreeWithThread(references, temporary, permutation,
                                    start, median - 1, executor, maximumSubmitDepth,
                                    depth + 1) );

                    // And simultaneously, recursively build the > branch of the tree with the current thread.
                    node.gtChild = buildKdTree(references, temporary, permutation,
                            median + 1, end, executor, maximumSubmitDepth, depth + 1);

                    // Then get the result of building the < branch with the child thread.
                    try {
                        node.ltChild = future.get();
                    } catch (Exception e) {
                        throw new RuntimeException( "recursive future exception: " + e.getMessage() );
                    }
                }

            } else 	if (end < start) {

                // This is an illegal condition that should never occur, so test for it last.
                throw new RuntimeException("end < start");

            } else {

                // This final else block is added to keep the Java compiler from complaining.
                throw new RuntimeException("unknown configuration of  start and end");
            }

            return node;
        }

        /**
         * <p>
         * Return a {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#buildKdTree buildKdTree} method.
         * </p>
         *
         * @param references - multiple arrays of KdNode[]
         * @param temporary - a scratch KdNode[] for use in partitioning
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param start - the first element of the reference array
         * @param end - the last element of the reference array
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param depth - the depth in the k-d tree
         * @return a {@link KdNode}
         */
        private static Callable<KdNode> buildKdTreeWithThread(final KdNode[][] references, final KdNode[] temporary,
                                                              final int[] permutation, final int start, final int end,
                                                              final ExecutorService executor,
                                                              final int maximumSubmitDepth, final int depth) {

            return new Callable<KdNode>() {
                @Override
                public KdNode call() {
                    return buildKdTree(references, temporary, permutation,
                            start, end, executor, maximumSubmitDepth, depth);
                }
            };
        }

        /**
         * <p>
         * Scan and partition the lower half of a reference array.
         * </p>
         *
         * @param references - multiple arrays of KdNode[]
         * @param node - a {@link KdNode} that contains the coordinate about which to partition
         * @param i - selector for one reference array
         * @param start - the first element of the reference array
         * @param median - the median element of the reference array
         * @param p  - the partition that cycles as x, y, z, etc.
         */
        private static void scanAndPartitionLower(final KdNode references[][], final KdNode node, final int p,
                                                  final int i, final int start, final int median) {
            KdNode src[] = references[i];
            KdNode dst[] = references[i - 1];
            for (int lower = start - 1, upper = median, j = start; j <= median; ++j) {
                final long compare = superKeyCompare(src[j].tuple, node.tuple, p);
                if (compare < 0) {
                    dst[++lower] = src[j];
                } else if (compare > 0) {
                    dst[++upper] = src[j];
                }
            }
        }

        /**
         * <p>
         * Return a {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#buildKdTree scanAndPartitionLower} method.
         * </p>
         *
         * @param references - multiple arrays of KdNode[]
         * @param node - a {@link KdNode} that contains the coordinate about which to partition
         * @param i - selector for one reference array
         * @param start - the first element of the reference array
         * @param median - the median element of the reference array
         * @param p  - the partition that cycles as x, y, z, etc.
         */
        private static Callable<Void> scanAndPartitionLowerWithThread(final KdNode references[][],
                                                                      final KdNode node,
                                                                      final int p, final int i,
                                                                      final int start, final int median) {
            return new Callable<Void>() {
                @Override
                public Void call() {
                    scanAndPartitionLower(references, node, p, i, start, median);
                    return null;
                }
            };
        }

        /**
         * <p>
         * Scan and partition the upper half of a reference array.
         * </p>
         *
         * @param references - multiple arrays of KdNode[]
         * @param node - a {@link KdNode} that contains the coordinate about which to partition
         * @param i - selector for one reference array
         * @param median - the median element of the reference array
         * @param end - the last element of the reference array
         * @param p  - the partition that cycles as x, y, z, etc.
         */
        private static void scanAndPartitionUpper(final KdNode references[][], final KdNode node, final int p,
                                                  final int i, final int median, final int end) {
            KdNode src[] = references[i];
            KdNode dst[] = references[i - 1];
            for (int lower = median, upper = end + 1, k = end; k > median; --k) {
                final long compare = superKeyCompare(src[k].tuple, node.tuple, p);
                if (compare < 0) {
                    dst[--lower] = src[k];
                } else if (compare > 0) {
                    dst[--upper] = src[k];
                }
            }
        }

        /**
         * <p>
         * The {@code verifyKdTree} method checks that the children of each node of the k-d tree
         * are correctly sorted relative to that node.
         * </p>
         *
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param depth - the depth in the k-d tree
         * @return the number of nodes in the k-d tree
         */
        private int verifyKdTree(final int[] permutation, final ExecutorService executor,
                                 final int maximumSubmitDepth, final int depth) {

            if (tuple == null) {
                throw new RuntimeException("point is null");
            }

            // Look up the partition.
            final int p = permutation[depth];

            if (ltChild != null) {
                if (ltChild.tuple[p] > tuple[p]) {
                    throw new RuntimeException("node is > partition!");
                }
                if (superKeyCompare(ltChild.tuple, tuple, p) >= 0) {
                    throw new RuntimeException("node is >= partition!");
                }
            }
            if (gtChild != null) {
                if (gtChild.tuple[p] < tuple[p]) {
                    throw new RuntimeException("node is < partition!");
                }
                if (superKeyCompare(gtChild.tuple, tuple, p) <= 0) {
                    throw new RuntimeException("node is <= partition!");
                }
            }

            // Count this node.
            int count = 1 ;

            // Search the < branch with a child thread at as many levels of the tree as possible.
            // Create the child thread as high in the tree as possible for greater utilization.

            // Is a child thread available to build the < branch?
            if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

                // No, so search the < branch with the current thread.
                if (ltChild != null) {
                    count += ltChild.verifyKdTree(permutation, executor, maximumSubmitDepth, depth + 1);
                }

                // Then search the > branch with the current thread.
                if (gtChild != null) {
                    count += gtChild.verifyKdTree(permutation, executor, maximumSubmitDepth, depth + 1);
                }
            } else {

                // Yes, so launch a child thread to search the < branch.
                Future<Integer> future = null;
                if (ltChild != null) {
                    future = executor.submit( ltChild.verifyKdTreeWithThread(permutation, executor,
                            maximumSubmitDepth, depth + 1) );
                }

                // And simultaneously search the > branch with the current thread.
                if (gtChild != null) {
                    count += gtChild.verifyKdTree(permutation, executor, maximumSubmitDepth, depth + 1);
                }

                // If a child thread searched the < branch, get the result.
                if (future != null) {
                    try {
                        count += future.get();
                    } catch (Exception e) {
                        throw new RuntimeException( "future exception: " + e.getMessage() );
                    }
                }
            }

            return count;
        }

        /**
         * <p>
         * The {@code verifyKdTreeWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#verifyKdTree verifyKdTree} method.
         * </p>
         *
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param depth - the depth in the k-d tree
         * @return the number of nodes in the k-d tree
         */
        private Callable<Integer> verifyKdTreeWithThread(final int[] permutation, final ExecutorService executor,
                                                         final int maximumSubmitDepth, final int depth) {

            return new Callable<Integer>() {
                @Override
                public Integer call() {
                    return verifyKdTree(permutation, executor, maximumSubmitDepth, depth);
                }
            };
        }

        /*
         * The swap function swaps two elements in an array.
         *
         * calling parameters:
         *
         * a - the array
         * i - the index of the first element
         * j - the index of the second element
         */
        public static void swap(int a[], int i, int j) {
            int t = a[i];
            a[i] = a[j];
            a[j] = t;
        }

        /**
         * <p>
         * The {@code createKdTree} method builds a k-d tree from a KdNode[]
         * where the coordinates of each point are stored in KdNode.tuple
         * </p>
         *
         * @param coordinates - a KdNode[]
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @returns the root of the k-d tree
         */
        public static KdNode createKdTree(KdNode[] coordinates, final int numPoints, final int[] permutation,
                                          final ExecutorService executor, final int maximumSubmitDepth) {

            // Declare all reference arrays and initialize one of them. The number of dimensions
            // may be obtained from either coordinates[0].length or references.length.  The number
            // of points may be obtained from either coordinates.length or references[0].length.
            long initTime;
            if (KdTree.printstats) initTime = System.currentTimeMillis();
            final KdNode[][] references = new KdNode[coordinates[0].tuple.length][numPoints];
            initializeReference(coordinates, references[0]);
            if (KdTree.printstats) initTime = System.currentTimeMillis() - initTime;

            // Sort one of the reference arrays using the first dimension (0) as the most significant
            // key of the super key.
            final KdNode[] temporary = new KdNode[numPoints];
            final int key = 0;
            long sortTime;
            if (KdTree.printstats) sortTime = System.currentTimeMillis();
            mergeSortReferenceAscending(references[0], temporary, 0, numPoints - 1,
                    key, executor, maximumSubmitDepth, 0);
            if (KdTree.printstats) sortTime = System.currentTimeMillis() - sortTime;

            // Remove references to duplicate tuples via one pass through the sorted reference array.
            // The same most significant key of the super key should be used for sort and de-duping.
            long removeTime;
            if (KdTree.printstats) removeTime = System.currentTimeMillis();
            final int end = removeDuplicates(references[0], key);
            if (KdTree.printstats) removeTime = System.currentTimeMillis() - removeTime;

            // Copy the de-duplicated reference array to the other reference arrays.
            final List<Future<Void>> initializeFutures = new ArrayList<Future<Void>>();
            long initTime2;
            if (KdTree.printstats) initTime2 = System.currentTimeMillis();
            for (int i = 1; i < references.length; i++) {
                // Initialize all dimensions in parallel if possible.
                if (executor == null) {
                    initializeReference(references[0], references[i]);
                } else {
                    Future<Void> future =
                            executor.submit( initializeReferenceWithThread(references[0], references[i]) );
                    initializeFutures.add(future);
                }
            }
            for (Future<Void> future : initializeFutures) {
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RuntimeException( "future exception: " + e.getMessage() );
                }
            }
            if (KdTree.printstats) initTime += System.currentTimeMillis() - initTime2;

            // Sort the other reference arrays but sort only the de-duplicated region
            // of those reference arrays. Use the remaining dimensions (not 0 that
            // was used for sorting above) for sorting the other reference arrays such
            // that each reference array is sorted using a different key as the most
            // signifcant key of the super key. The permutation array guarantees cycling
            // through the keys so that each key is used as the most significant key of
            // the super key; hence the reference arrays must be sorted so that the most
            // significant key of the super key is as specified by the permutation array.
            long sortTime2;
            if (KdTree.printstats) sortTime2 = System.currentTimeMillis();
            for (int i = 1; i < references.length; i++) {
                mergeSortReferenceAscending(references[i], temporary, 0, end,
                        i, executor, maximumSubmitDepth, 0);
            }
            if (KdTree.printstats) sortTime += System.currentTimeMillis() - sortTime2;

            // Build the k-d tree via heirarchical multi-threading if possible.
            long kdTime;
            if (KdTree.printstats) kdTime = System.currentTimeMillis();
            final KdNode root = buildKdTree(references, temporary, permutation, 0,
                    end, executor, maximumSubmitDepth, 0);
            if (KdTree.printstats) kdTime = System.currentTimeMillis() - kdTime;

            // Verify the k-d tree via hierarchical multi-threading if possible and report the number of nodes.
            long verifyTime;
            if (KdTree.printstats) verifyTime = System.currentTimeMillis();
            final int numberOfNodes = root.verifyKdTree(permutation, executor, maximumSubmitDepth, 0);
            if (KdTree.printstats) verifyTime = System.currentTimeMillis() - verifyTime;

            if (KdTree.printstats) {
                System.out.println("Number of nodes = " + numberOfNodes);

                final double iT = (double) initTime / 1000.;
                final double sT = (double) sortTime / 1000.;
                final double rT = (double) removeTime / 1000.;
                final double kT = (double) kdTime / 1000.;
                final double vT = verifyTime / 1000.;
                System.out.printf("\ntotalTime = %.2f  initTime = %.2f  sortTime = %.2f"
                                + "  removeTime = %.2f  kdTime = %.2f  verifyTime = %.2f\n\n",
                        iT + sT + rT + kT + vT, iT, sT, rT, kT, vT);
            }

            // Return the root of the tree.
            return root;
        }

        /**
         * <p>
         * The {@code searchKdTree} method searches the k-d tree and finds the KdNodes
         * that lie within a cutoff distance from a query node in all k dimensions.
         * </p>
         *
         * @param query - the query point
         * @param cut - the cutoff distance
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param depth - the depth in the k-d tree
         * @return a {@link java.util.List List}{@code <}{@link KdNode}{@code >}
         * that contains the k-d nodes that lie within the cutoff distance of the query node
         */
        private List<KdNode> searchKdTree(final long[] query, final long cut, final int[] permutation,
                                          final ExecutorService executor, final int maximumSubmitDepth,
                                          final int depth) {

            // Look up the partition.
            final int p = permutation[depth];

            // If the distance from the query node to the k-d node is within the cutoff distance
            // in all k dimensions, add the k-d node to a list.
            final List<KdNode> result = new ArrayList<KdNode>();
            boolean inside = true;
            for (int i = 0; i < tuple.length; i++) {
                if (Math.abs(query[i] - tuple[i]) > cut) {
                    inside = false;
                    break;
                }
            }
            if (inside) {
                result.add(this);
            }

            // Search the < branch of the k-d tree if the partition coordinate of the query point minus
            // the cutoff distance is <= the partition coordinate of the k-d node.  Note the tranformation
            // of this test: (query[p] - cut <= tuple[p]) -> (query[p] - tuple[p] <= cut).  The < branch
            // must be searched when the cutoff distance equals the partition coordinate because the super
            // key may assign a point to either branch of the tree if the sorting or partition coordinate,
            // which forms the most significant portion of the super key, shows equality.
            Future<List<KdNode>> future = null;
            if (ltChild != null) {
                if (query[p] - tuple[p] <= cut) {

                    // Search the < branch with a child thread at as many levels of the tree as possible.
                    // Create the child threads as high in the tree as possible for greater utilization.
                    // If maxSubmitDepth == -1, there are no child threads.
                    if (maximumSubmitDepth > -1 && depth <= maximumSubmitDepth) {
                        future =
                                executor.submit( ltChild.searchKdTreeWithThread(query, cut, permutation,
                                        executor, maximumSubmitDepth, depth + 1) );
                    } else {
                        result.addAll( ltChild.searchKdTree(query, cut, permutation, executor,
                                maximumSubmitDepth, depth + 1) );
                    }
                }
            }

            // Search the > branch of the k-d tree if the partition coordinate of the query point plus
            // the cutoff distance is >= the partition coordinate of the k-d node.  Note the transformation
            // of this test: (query[p] + cut >= tuple[p]) -> (tuple[p] - query[p] <= cut).  The < branch
            // must be searched when the cutoff distance equals the partition coordinate because the super
            // key may assign a point to either branch of the tree if the sorting or partition coordinate,
            // which forms the most significant portion of the super key, shows equality.
            if (gtChild != null) {
                if (tuple[p] - query[p] <= cut) {
                    result.addAll( gtChild.searchKdTree(query, cut, permutation, executor, maximumSubmitDepth, depth + 1) );
                }
            }

            // If a child thread searched the < branch, get the result.
            if (future != null) {
                try {
                    result.addAll( future.get() );
                } catch (Exception e) {
                    throw new RuntimeException( "future exception: " + e.getMessage() );
                }
            }
            return result;
        }

        /**
         * <p>
         * The {@code searchKdTreeWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#searchKdTree searchKdTree} method.
         * </p>
         *
         * @param query - the query point
         * @param cut - the cutoff distance
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param depth - the depth in the k-d tree
         * @return a {@link java.util.List List}{@code <}{@link KdNode}{@code >}
         * that contains the k-d nodes that lie within the cutoff distance of the query node
         */
        private Callable<List<KdNode>> searchKdTreeWithThread(final long[] query, final long cut, final int[] permutation,
                                                              final ExecutorService executor, final int maximumSubmitDepth,
                                                              final int depth) {

            return new Callable<List<KdNode>>() {
                @Override
                public List<KdNode> call() {
                    return searchKdTree(query, cut, permutation, executor, maximumSubmitDepth, depth);
                }
            };
        }

        /**
         * <p>
         * The {@code searchKdTree} method searches the k-d tree and finds the KdNodes
         * that lie within a cutoff distance from a query node in all k dimensions.
         * </p>
         *
         /**
         * <p>
         * The {@code searchKdTree} method searches the k-d tree and finds the KdNodes
         * that lie within a cutoff distance from a query node in all k dimensions.
         * </p>
         *
         * @param result - ArrayList to the k-d nodes that lie the query hypercube will be added.
         * @param queryPlus - Array containing the lager search bound for each dimension
         * @param queryMinus - Array containing the smaller search bound for each dimension
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param depth - the depth in the k-d tree
         * @return void
         */
        void searchKdTree(final ArrayList<KdNode<VALUE_TYPE>> result, final long[] queryPlus, final long[] queryMinus,
                          final int[] permutation, final ExecutorService executor,
                          final int maximumSubmitDepth, final int depth) {

            // Look up the partition.
            final int p = permutation[depth];

            // the branchCode will be used later to select the actual branch configuration in the switch statement
            // below.  0 = no branch, 1 = < branch only, 2 = > branch only, 3 = bothe branches.
            int branchCode = 0;

            // Search the < branch of the k-d tree if the partition coordinate of the queryPlus is
            // <= the partition coordinate of the k-d node.  The < branch
            // must be searched when the cutoff distance equals the partition coordinate because the super
            // key may assign a point to either branch of the tree if the sorting or partition coordinate,
            // which forms the most significant portion of the super key, shows equality.
            if (queryMinus[p] <= tuple[p]) {
                // but only search if the ltChild pointer is not null;
                if (ltChild != null) branchCode = 1;
                // Search the > branch of the k-d tree if the partition coordinate of the queryPlus is
                // >= the partition coordinate of the k-d node.  The < branch
                // must be searched when the cutoff distance equals the partition coordinate because the super
                // key may assign a point to either branch of the tree if the sorting or partition coordinate,
                // which forms the most significant portion of the super key, shows equality.
                if (queryPlus[p] >= tuple[p]) {
                    // but only if the gtChild pointer is not null;
                    if (gtChild != null) branchCode += 2;
                    // while here check to see if there are values that could be inside the the
                    // hypercube.
                    if (value != null){
                        // If the distance from the query node to the k-d node is within the cutoff distance
                        // in all k dimensions, add the k-d node to a list.
                        boolean inside = true;
                        for (int i = 0; i < tuple.length; i++) {
                            if ((queryPlus[i]  <= tuple[i]) ||
                                    (queryMinus[i] > tuple[i]) ) {
                                inside = false;
                                break;
                            }
                        }
                        if (inside) {
                            result.add(this);
                        }
                    }
                }
            } else {
                if (gtChild != null && queryPlus[p] >= tuple[p]) branchCode = 2;
            }

            switch (branchCode) {
                case 0: // child pointer are both null so just return
                    return;
                case 1: // only go down the less than branch
                    ltChild.searchKdTree(result, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, depth + 1);
                    return;
                case 2: // only go down the greater than branch
                    gtChild.searchKdTree(result, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, depth + 1);
                    return;
                case 3: // go down both branches
                    // get a future and another list ready in case a child thread is spawned
                    Future<List<VALUE_TYPE>> future = null;
                    ArrayList<KdNode<VALUE_TYPE>> threadResult = null;
                    // check to see if there is a thread available and descend the less than branch with that thread.
                    if (maximumSubmitDepth > -1 && depth <= maximumSubmitDepth) {
                        threadResult = new ArrayList<>();
                        future =
                                executor.submit(ltChild.searchKdTreeWithThread(threadResult, queryPlus, queryMinus,
                                        permutation, executor, maximumSubmitDepth, depth + 1));
                    } else {
                        // if no thread, just descend directly
                        ltChild.searchKdTree(result, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, depth + 1);
                    }

                    gtChild.searchKdTree(result, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, depth + 1);

                    // If a child thread searched the < branch, get the result.
                    if (future != null) {
                        try {
                            future.get();
                            result.addAll(threadResult);
                        } catch (Exception e) {
                            throw new RuntimeException("future exception: " + e.getMessage());
                        }
                    }
                    return;
            }
        }

        /**
         * <p>
         * The {@code searchKdTreeWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#searchKdTree searchKdTree} method.
         * </p>
         *
         * @param result - ArrayList to which the k-d nodes that lie within the hypercube will be added.
         * @param queryPlus - Array containing the lager search bound for each dimension
         * @param queryMinus - Array containing the smaller search bound for each dimension
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param depth - the depth in the k-d tree
         * @return a {@link java.util.List List}{@code <}{@link KdNode}{@code >}
         * that contains the k-d nodes that lie within the cutoff distance of the query node
         */
        private Callable searchKdTreeWithThread(final ArrayList<KdNode<VALUE_TYPE>> result, final long[] queryPlus, final long[] queryMinus, final int[] permutation,
                                                final ExecutorService executor, final int maximumSubmitDepth,
                                                final int depth) {
            return new Callable() {
                @Override
                public Object call() {
                    searchKdTree(result, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, depth);
                    return null;
                }
            };
        }

        /**
         * <p>
         * The {@code searchKdTree} method searches the k-d tree and finds the KdNodes
         * that lie within a cutoff distance from a query node in all k dimensions.
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
         * @return a code that indicates something about the search below.
         *           0 - nothing found or removed
         *           1 - something found and removed but the node returned from is still needed
         *          -1 - something found and removed the node below is dead so can be pruned.
         */
        private int searchAndRemoveKdTree(final List<VALUE_TYPE> result, final long[] queryPlus, final long[] queryMinus, final int[] permutation,
                                          final ExecutorService executor, final int maximumSubmitDepth,
                                          final int depth) {

            // Look up the partition.
            final int p = permutation[depth];
            // get a list ready for the other thread to use
            List<VALUE_TYPE> threadResult = null;

            // this int ge set to the return value of the chield decent.  +1 means values were
            int returnResultLt = 0;
            int returnResultGt = 0;
            int returnResult = 0;

            // If the distance from the query node to the k-d node is within the cutoff distance
            // in all k dimensions, add the k-d node to a list.

            boolean inside = true;
            for (int i = 0; i < tuple.length; i++) {
                if ((queryPlus[i]  <= tuple[i]) ||
                        (queryMinus[i] > tuple[i]) ) {
                    inside = false;
                    break;
                }
            }
            if (inside) {
                // this node's values to the result array
                result.addAll(this.value);
                // and empty the values list.
                this.value.clear();
                returnResult = 1;
            }



            // Search the < branch of the k-d tree if the partition coordinate of the query point minus
            // the cutoff distance is <= the partition coordinate of the k-d node. The < branch
            // must be searched when the cutoff distance equals the partition coordinate because the super
            // key may assign a point to either branch of the tree
            // if the sorting or partition coordinate,
            // which forms the most significant portion of the super key, shows equality.
            Future<Integer> future = null;
            if (ltChild != null) {
                if (queryMinus[p] <= tuple[p]) {

                    // Search the < branch with a child thread at as many levels of the tree as possible.
                    // Create the child threads as high in the tree as possible for greater utilization.
                    // If maxSubmitDepth == -1, there are no child threads.
                    if (maximumSubmitDepth > -1 && depth <= maximumSubmitDepth) {
                        threadResult = new ArrayList<>();
                        future =
                                executor.submit( ltChild.searchAndRemoveKdTreeWithThread(threadResult, queryPlus, queryMinus,
                                        permutation, executor, maximumSubmitDepth, depth + 1) );
                    } else {
                        returnResultLt = ltChild.searchAndRemoveKdTree(result, queryPlus, queryMinus, permutation, executor,
                                maximumSubmitDepth, depth + 1);
                        if (returnResultLt == -1) ltChild = null;
                    }
                }
            } else {
                returnResultLt = -1;
            }

            // Search the > branch of the k-d tree if the partition coordinate of the query point plus
            // the cutoff distance is >= the partition coordinate of the k-d node.  Note the transformation
            // of this test: (query[p] + cut >= tuple[p]) -> (tuple[p] - query[p] <= cut).  The < branch
            // must be searched when the cutoff distance equals the partition coordinate because the super
            // key may assign a point to either branch of the tree if the sorting or partition coordinate,
            // which forms the most significant portion of the super key, shows equality.
            if (gtChild != null) {
                if (queryPlus[p] >= tuple[p]) {
                    returnResultGt = gtChild.searchAndRemoveKdTree(result, queryPlus, queryMinus, permutation,
                            executor, maximumSubmitDepth, depth + 1);
                    if (returnResultGt == -1) gtChild = null;

                }
            } else {
                returnResultGt = -1;
            }

            // If a child thread searched the < branch, get the result.
            if (future != null) {
                try {
                    returnResultLt = future.get();
                    if(returnResultLt == -1){
                        ltChild = null;
                    }
                    result.addAll(threadResult);
                } catch (Exception e) {
                    throw new RuntimeException( "future exception: " + e.getMessage() );
                }
            }

            if (returnResultGt ==  0 && returnResultLt ==  0)
                return returnResult;
            if (returnResultGt == -1 && returnResultLt ==  0)
                return returnResult;
            if (returnResultGt ==  0 && returnResultLt == -1)
                return returnResult;
            if (returnResultGt ==  1 || returnResultLt ==  1)
                return 1;
            if (returnResultGt == -1 && returnResultLt == -1)
                return this.value.size() > 0 ? 1 : -1;
            return returnResult;
        }

        /**
         * <p>
         * The {@code searchKdTreeWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the
         * {@link KdNode#searchKdTree searchKdTree} method.
         * </p>
         *
         * @param result - ArrayList to which the values of k-d nodes that lie within the cutoff
         *                 distance of the query node will be added.
         * @param queryPlus - Array containing the lager search bound for each dimension
         * @param queryMinus - Array containing the smaller search bound for each dimension
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
         * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
         * @param depth - the depth in the k-d tree
         * @return a code that indicates somthing about the search below.
         *           0 - nothing found or removed
         *           1 - somthing found and removed but the node returned from is still needed
         *          -1 - sonthing found and removed the node below is dead so can be pruned.
         */
        private Callable<Integer>
        searchAndRemoveKdTreeWithThread(final List<VALUE_TYPE> result, final long[] queryPlus, final long[] queryMinus,
                                        final int[] permutation, final ExecutorService executor,
                                        final int maximumSubmitDepth, final int depth) {

            return new Callable<Integer>() {
                @Override
                public Integer call() {
                    return searchAndRemoveKdTree(result, queryPlus, queryMinus, permutation,
                            executor, maximumSubmitDepth, depth);
                }
            };
        }

        /**
         * <p>
         * The {@code nearestNeighbor} method is used to search the tree for all possible M
         * nearest geometric neighbors by adding them the the NearestNeighborHeap.  It
         * excludes branches of the tree where it is guaranteed that all the nodes in that
         * branch are farther way than the current farthest node in the NearestNeighborHeap.
         * </p>
         *
         * @param nnHeap - Instance of the NearestNeighborHeap.
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param depth - the depth in the k-d tree
         */
        private void nearestNeighbor(final NearestNeighborHeap nnHeap, final int[] permutation, int depth) {

            final int p = permutation[depth];

            // If query[p] < tuple[p], descend the < branch to the bottom of the tree before adding a point to the
            // nearestNeighborHeap, which increases the probability that closer nodes to the query point will get added
            // earlier, thus reducing the likelihood of adding more distant points that get kicked out of the heap later.
            if (nnHeap.query[p] < tuple[p]) {
                if (ltChild != null) {  // if not at the bottom of the tree yet descend the near branch unconditionally
                    ltChild.nearestNeighbor(nnHeap, permutation, depth+1);
                }
                // If the current node is closer to the query point than the farthest item in the nearestNeighborHeap,
                // or if this component of the array is not part of the nearest neighbor search, or if the heap is not
                // full, descend the other branch and then attempt to add the node to the nearest neighbor heap.
                if (tuple[p] - nnHeap.query[p] <= nnHeap.curMaxDist() || !nnHeap.enable[p] || !nnHeap.heapFull()) {
                    if (gtChild != null) { // and if not at the bottom, descend the far branch
                        gtChild.nearestNeighbor(nnHeap, permutation, depth+1);
                    }
                    nnHeap.add(this);  // attempt to add the current node to the heap
                }
            }
            // If query[p] > tuple[p], descend the > branch to the bottom of the tree before adding a point to the
            // nearestNeighborHeap, which increases the probability that closer nodes to the query point will get added
            // earlier, thus reducing the likelihood of adding more distant points that get kicked out of the heap later.
            else if (nnHeap.query[p] > tuple[p]) {
                if (gtChild != null) {  // if not at the bottom of the tree yet descend the near branch unconditionally
                    gtChild.nearestNeighbor(nnHeap, permutation, depth+1);
                }
                // If the current node is closer to the query point than the farthest item in the nearestNeighborHeap,
                // or if this component of the array is not part of the nearest neighbor search, or if the heap is not
                // full, descend the other branch and then attempt to add the node to the nearest neighbor list.
                if (nnHeap.query[p] - tuple[p] <= nnHeap.curMaxDist() || !nnHeap.enable[p] || !nnHeap.heapFull()) {
                    if (ltChild != null) {
                        ltChild.nearestNeighbor(nnHeap, permutation, depth+1);
                    }
                    nnHeap.add(this);  // attempt to add the current node to the heap
                }
            }
            // Because query[p] == tuple[p], the probability of finding nearest neighbors is equal for both branches
            // of the tree, so descend both branches. Then attempt to add the node to the nearest neighbor heap.
            else {
                if (ltChild != null) {
                    ltChild.nearestNeighbor(nnHeap, permutation, depth+1);
                }
                if (gtChild != null) {
                    gtChild.nearestNeighbor(nnHeap, permutation, depth+1);
                }
                nnHeap.add(this);  // attempt to add the current node to the heap
            }
            return;
        }

        /**
         * <p>
         * The {@code removeValue} removes a value from the tree if found.
         *
         * @param query - Array containing the lager search bound for each dimension
         * @param valueToRemove - value to be removed
         * @param permutation - an array that indicates permutation of the reference arrays
         * @param depth - the depth in the k-d tree
         * @return a code that indicates somthing about the search below.
         *           0 - nothing found or removed
         *           1 - somthing found and removed but the node returned from is still needed
         *          -1 - sonthing found and removed the node below is dead so can be pruned.
         */
        private int removeValue(final long[] query, VALUE_TYPE valueToRemove, final int[] permutation, final int depth){

            // Look up the partition.
            final int p = permutation[depth];
            // init the return result to 0
            int returnResult = 0;

            // compare query with current node tuple
            long compResult = superKeyCompare(query,tuple,p);
            // if greter than or less than, continue the search on  the appropriate child node
            if (0 > compResult) {
                if (ltChild != null) {
                    returnResult = ltChild.removeValue(query, valueToRemove, permutation, depth+1);
                    // if the node below declared itself dead, remove the link
                    if (returnResult == -1) ltChild = null;
                }
            } else if(0 < compResult) {
                if (gtChild != null) {
                    returnResult = gtChild.removeValue(query, valueToRemove, permutation, depth+1);
                    // if the node below declared itself dead, remove the link
                    if (returnResult == -1) gtChild = null;
                }
            } else {// must be equal to remove value from the list.
                returnResult = value.remove(valueToRemove) ? -1 : 0;  // record if somthing was actually removed
            }
            // Now figure out what to return.  if something was found either here or below return 1 if this node
            // is still active or -1 if this node is dead
            if (returnResult == -1 && (value.size() != 0 || ltChild != null || gtChild != null)) {
                returnResult = 1;
            }
            return returnResult;
        }

        /**
         * <p>
         * The {@code printKdTree} method prints the k-d tree "sideways" with the root at the left.
         * </p>
         *
         * @param depth - the depth in the k-d tree
         */
        public void printKdTree(final long depth) {
            if (gtChild != null) {
                gtChild.printKdTree(depth+1);
            }
            for (int i = 0; i < depth; i++) {
                System.out.print("        ");
            }
            printTuple(tuple);
            System.out.println();
            if (ltChild != null) {
                ltChild.printKdTree(depth+1);
            }
        }

        /**
         * <p>
         * The {@code printTuple} method prints a tuple.
         * </p>
         *
         * @param p - the tuple
         */
        public static void printTuple(final long[] p) {
            System.out.print("(");
            for (int i = 0; i < p.length; i++) {
                System.out.print(p[i]);
                if (i < p.length - 1) {
                    System.out.print(", ");
                }
            }
            System.out.print(")");
        }
    } // Class KdNode

    /**
     * <p>
     * The HeapPair Class holds return values from the NearestNeighborHeap.removeTop method.
     * </p>
     */
    static class HeapPair {
        private long dist;
        private KdNode node;

        private HeapPair(long dist, KdNode node) {
            this.dist = dist;
            this.node = node;
        }
    } // class HeapPair

    /**
     * <p>
     * The NearestNeighborHeap Class implements a fixed length heap of both containing both a KdNode and euclidean distance
     * from the tuple in the node to a query point.  When a KdNode is added to the heap it is unconditionally placed in
     * the heap until the heap is full.  After the heap is full, a KdNode is added to the heap only if the calculated
     * distance from the query point to the tuple is less than the farthest KdNode currently in the heap; and in that
     * case, the current farthest KdNode and distance are removed from the heap to make room for it.
     *
     * The heap is maintained in two corresponding fixed length arrays, one for the KdNodes and one for the distance to
     * the query point.  These arrays are stored in order of increasing distance.  When a new node is added, regardless
     * of whether or not the heap is full, an insertion sort is done to place the new KdNode in the proper order in the heap.
     * A binary search is done to find the correct location, then the farther entries are moved down to make room for the new
     * one, which results in the old farthest KdNode being dropped.  In this way, the farthest KdNode is always at the end of
     * the heap when the heap is full.
     *
     * The "top" of the heap, i.e., dists[1], is initialized to the maximum int value so that all KdNodes will be added
     * to the heap until the heap is full.  Once the heap is full, dists[1] represents the distance to the farthest point
     * in the heap to which distance comparisons are made.
     * <p>
     */
    static class NearestNeighborHeap {
        private long query[]; // point for the which the nearest neighbors will be found
        private int reqDepth; // requested number of nearest neighbors and therefore size of the above arrays
        private KdNode nodes[];  // set of nodes that are the nearest neighbors
        private long dists[]; // set of distances from the query point to the above nodes
        private int curDepth; // number of nearest nodes/distances on the heap
        private long curMaxDist; // distance to the last (and farthest) KdNode on the heap
        private boolean enable[];  // per component enable for distance calculation

        /**
         * <p>
         * The {@code NearestNeighborHeap} constructor for NearestNeighborHeap class
         * </p>
         *
         * @param query - array containing the point to which the nearest neighbors will be found
         * @param numNeighbors - number of nearest neighbors to be found and hence size of the heap
         */
        private NearestNeighborHeap( final long query[], final int numNeighbors) {
            this.nodes = new KdNode[numNeighbors+1];  // heap of KdNodes (address 0 is unused)
            this.dists = new long[numNeighbors+1];  // corresponding heap of distances
            this.reqDepth = numNeighbors;
            this.curDepth = 0;  // redundant
            this.dists[1] = 0;  // redundant
            this.query = new long[query.length];
            this.enable = new boolean[query.length];
            for (int i = 0; i < query.length; i++) {
                this.query[i] = query[i];
                this.enable[i] = true;
            }
        }

        private NearestNeighborHeap(final long query[], final int numNeighbors, final boolean enable[]) {
            this.nodes = new KdNode[numNeighbors+1];  // heap of KdNodes (address 0 is unused)
            this.dists = new long[numNeighbors+1];  // corresponding heap of distances
            this.reqDepth = numNeighbors;
            this.curDepth = 0;  // redundant
            this.dists[1] = 0;  // redundant
            this.query = new long[query.length];
            this.enable = new boolean[query.length];
            for (int i = 0; i < query.length; i++) {
                this.query[i] = query[i];
                this.enable[i] = enable[i];
            }
        }

        /**
         * <p>
         * The {@code swap} method swaps two elements in the heap
         * </p>
         *
         * @param i - the index of the first element
         * @param j - the index of the second element
         */
        private void swap(final int i, final int j) {
            long tempDist = dists[i];
            KdNode tempNode = nodes[i];
            dists[i] = dists[j];
            nodes[i] = nodes[j];
            dists[j] = tempDist;
            nodes[j] = tempNode;
        }

        /**
         * <p>
         * The {@code rise} method allows an element to rise upward through the heap
         * </p>
         *
         * @param k - the element index
         */
        private void rise(int k) {
            while (k > 1 && dists[k/2] < dists[k]) {
                swap(k/2, k);
                k = k/2;
            }
        }

        /**
         * <p>
         * The {@code fall} method allows an element to fall downward through the heap
         * </p>
         *
         * @param k - the element index
         */
        private void fall(int k) {
            while (2*k <= curDepth) {
                int j = 2*k;
                if (j < curDepth && dists[j] < dists[j+1]) j++;
                if (dists[k] >= dists[j]) {
                    break;
                }
                swap(k, j);
                k = j;
            }
        }

        /**
         * <p>
         * The {@code removeTop} method removes the top element of the heap
         * </p>
         *
         * @return the distance of the removed top element
         */
        private HeapPair removeTop() {
            HeapPair max = new HeapPair(dists[1], nodes[1]);
            swap(1, curDepth--);
            nodes[curDepth+1] = null; // permit garbage collection
            fall(1);
            return max;
        }

        /**
         * <p>
         * The {@code add} method adds a newNode to this NearestNeighborHeap if its distance to the
         * query point is less than curMaxDistance
         * </p>
         *
         * @param newNode - KdNode to potentially be added to the heap
         */
        private void add(final KdNode newNode) {
            // if the number of values associated with this node is 0, don't add it to the nn list.
            if (newNode.value.size() == 0) return;
            // find the distance by subtracting the query from the tuple and
            // calculating the sqrt of the sum of the squares
            double dDist = 0.0;
            for (int i = 0; i < newNode.tuple.length; i++) {
                // add the coordinate distance only if the dimension is enabled
                if (enable[i]) {
                    long comp = newNode.tuple[i] - query[i];
                    dDist += (double)comp * (double)comp;
                }
            }
            long lDist = (long)Math.sqrt(dDist);
            // If the queue is not full, add the point to the bottom of the heap unconditionally and let it rise;
            if (!heapFull()) {
                dists[++curDepth] = lDist;
                nodes[curDepth] = newNode;
                rise(curDepth);
            }
            // otherwise, if the point is closer than the top of the heap, overwrite the top and let it fall.
            else if (lDist < curMaxDist()) {
                dists[1] = lDist;
                nodes[1] = newNode;
                fall(1);
            }
            return;
        }

        /**
         * <p>
         * The {@code curMaxDist} method returns the maximum distance, i.e., dists[1]
         * </p>
         *
         * @return dists[1]
         */
        private long curMaxDist() {
            return dists[1];
        }

        /**
         * <p>
         * The {@code heapFull} method returns true if the queue is full
         * </p>
         *
         * @return curDepth >= reqDepth
         */
        private boolean heapFull() {
            return curDepth >= reqDepth;
        }

    } // class NearestNeighborHeap


    // KdTree class variables.
    private int numPointsAllocated; // This variable sores the number of point of memory allocated;
    private int numPointsInTree; // this variable stores the number of points to be mapped
    int numDimensions; // this variable holds the number of dimensions of each point
    private KdNode<VALUE_TYPE>[] kdNodes; // The array of KdNodes
    KdNode root = null; // the root of the KdTree
    int[] permutation;
    protected int maximumSubmitDepth;  // number of threads to used in executing
    protected ExecutorService executor = null;

    /**
     * <p>
     * The {@code KdTree} Constructor for KdTree
     * </p>
     *
     * @param numPoints - Indicates the total number of KdNodes to be allocated
     * @param numPoints - Indicates the dimensionality of each point
     */
    public KdTree(final int numPoints, final int numDimensions){
        this.numPointsAllocated = numPoints;
        this.numDimensions = numDimensions;
        this.numPointsInTree = 0;
        kdNodes = new KdNode[numPoints];
        setNumThreads(-1);  // default number of threads to use to build and search the tree.
    }

    /**
     * <p>
     * The {@code KdTree} Constructor for KdTree
     * </p>
     *
     * @param from - kdTree to be copied.
     */
    public KdTree(KdTree from){
        if (from == null || from.root == null) {
            // TODO throw an error
        }
        this.numPointsAllocated = from.numPointsAllocated;
        this.numDimensions = from.numDimensions;
        this.numPointsInTree = from.numPointsInTree;
        setNumThreads(-1);  // default number of threads to use to build and search the tree.
        this.permutation = new int[from.permutation.length];
        for (int i = 0;  i < from.permutation.length; i++)
            this.permutation[i] = from.permutation[i];
        root = copyTree(from.root);
    }

    private KdNode copyTree(KdNode copyFromNode){
        // allocate a new node
        KdNode copyToNode = new KdNode(copyFromNode.tuple.length);
        // if there is a less than pointer in the copyFrom node, copy the less than branch
        if (copyFromNode.ltChild != null) {
            copyToNode.ltChild = copyTree(copyFromNode.ltChild);
        } else {
            copyToNode.ltChild = null;
        }
        // if there is a greater than pointer in the copyFrom node, copy the greater than tree
        if (copyFromNode.gtChild != null) {
            copyToNode.gtChild = copyTree(copyFromNode.gtChild);
        } else {
            copyToNode.gtChild = null;
        }
        // copy the tuple data from the from node
        for (int i = 0;  i < copyFromNode.tuple.length; i++) {
            copyToNode.tuple[i] = copyFromNode.tuple[i];
        }
        // copy the values from the other tree node.
        if (copyFromNode.value != null) {
            copyToNode.value.addAll(copyFromNode.value);
        } else {
            copyToNode.value = null;
        }
        return copyToNode;
    }

    /**
     * <p>
     * The {@code getNumDimensions} return the number of dimensions of each tuple, set in the constructor.
     * </p>
     */
    public int getNumDimensions() { return numDimensions; }

    /**
     * <p>
     * The {@code size} return the number of tuples added to the tree.
     * </p>
     */
    public int size() { return numPointsInTree; }


    /**
     * <p>
     * The {@code setNumThreads} sets the number of threads used to build
     * and search the KdTree.
     * </p>
     *
     * @param numThreads - the total number of threads.  0 or negative
     * indicate no multithreading
     */
    public void setNumThreads(int numThreads) {
        // Calculate the number of child threads to be the number of threads minus 1, then
        // calculate the maximum tree depth at which to launch a child thread.  Truncate
        // this depth such that the total number of threads, including the master thread, is
        // an integer power of 2, hence simplifying the launching of child threads by restricting
        // them to only the < branch of the tree for some number of levels of the tree.
        int n = 0;
        if (numThreads > 0) {
            while (numThreads > 0) {
                n++;
                numThreads >>= 1;
            }
            numThreads = 1 << (n - 1);
        } else {
            numThreads = 0;
        }

        final int childThreads = numThreads - 1;
        maximumSubmitDepth = -1;
        if (numThreads < 2) {
            maximumSubmitDepth = -1; // The sentinel value -1 specifies no child threads.
        } else if (numThreads == 2) {
            maximumSubmitDepth = 0;
        } else {
            maximumSubmitDepth = (int) Math.floor( Math.log( (double) childThreads ) / Math.log(2.) );
        }
        if (KdTree.printstats) System.out.println("\nNumber of child threads = " + childThreads + "  maximum submit depth = " + maximumSubmitDepth + "\n");

        // Create a fixed thread pool ExecutorService.
        if (childThreads > 0) {
            try {
                executor = Executors.newFixedThreadPool(childThreads);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("executor exception " + e.getMessage());
            }
        }
    }

    /**
     * <p>
     * The {@code shutdown} Shuts down the executor used for multithreading.
     * </p>
     *
     */
    private void shutdown() {
        if (executor != null) executor.shutdown();
    }

    /**
     * <p>
     * The {@code add} Adds a point and value to the KdTree
     * </p>
     *
     * @param point - Array containing the point to be added.  lenth must match
     * dimensions specified in constructor
     * @param value - Value that is associated with that point.
     * @returns the number of points in the KdTree after this add
     * or -1 if there is an error.
     *
     */
    public int add(long point[], VALUE_TYPE value) {
        // check to see that the number of nodes in the tree will not exceed the number allocated.
        if (numPointsInTree == numPointsAllocated) return -1;
        // check that the length of the submitted point mathes the lenth of the KdNode tuple
        if (point.length != numDimensions) return -1;
        // allocated the node
        kdNodes[numPointsInTree] = new KdNode(numDimensions);
        // store the point
        for (int i = 0; i< numDimensions; i++)
            kdNodes[numPointsInTree].tuple[i] = point[i];
        // store the value
        kdNodes[numPointsInTree].value.add(value);
        // invalidate the current tree.
        root = null;
        // return the current number of points in the tree.
        return (++numPointsInTree);
    }

    /**
     * <p>
     * The {@code buildTree} method builds the tree.  While not necessary for the user to
     * call this function, it can be called preemptively  to make the first search later.
     * </p>
     *
     */

    public void buildTree() {
        // Determine the maximum depth of the k-d tree, which is log2( coordinates.length ).
        int size = numPointsInTree;
        int maxDepth = 1;
        while (size > 0) {
            maxDepth++;
            size >>= 1;
        }

        // It is unnecessary to compute the partition coordinate upon each recursive call
        // of the buildKdTree function because that coordinate depends only on the depth of
        // recursion, so it may be pre-computed.
        permutation = new int[maxDepth];
        for (int i = 0; i < permutation.length; ++i) {
            permutation[i] = i % numDimensions;
        }

        // Build the k-d tree.
        root = KdNode.createKdTree(kdNodes, numPointsInTree, permutation, executor, maximumSubmitDepth);
    }

    /**
     * <p>
     * The {@code searchTree} search the tree for all nodes contained within the searchDistanece
     *  query and return the values associated with those nodes.
     * </p>
     *
     * @param query - Array containing the center point of a search region.  lenth
     * must match dimensions specified in constructor
     * @param searchDistance - the distance from the query point to be searched.
     * @returns list of KdNodes found in the search region
     */
    private List<KdNode> searchTreeForKdNodesIn(long query[], long searchDistance) {
        // if the tree is not built yet, build it
        if (root == null) {
            buildTree();
            // if root is null; return a null
            if (root == null) return null;
        }
        // calculate the plus and minus arrays while checking for overflow
        long queryPlus[]  = new long[numDimensions];
        long queryMinus[] = new long[numDimensions];
        for (int i=0; i<numDimensions; i++) {
            if (query[i] > 0 && (Long.MAX_VALUE - query[i]) < searchDistance) {
                queryPlus[i]  =  Long.MAX_VALUE;
            } else {
                queryPlus[i]  =  query[i] + searchDistance;
            }
            if ((query[i] < 0) && ((query[i] - Long.MIN_VALUE) < searchDistance)) {
                queryMinus[i] = Long.MIN_VALUE;
            } else {
                queryMinus[i] = query[i] - searchDistance;
            }
        }
        return root.searchKdTree(queryPlus, searchDistance, permutation, executor, maximumSubmitDepth, 0);
    }

    /**
     * <p>
     * The {@code searchAndRemove} search the tree for all nodes contained within the searchDistanece
     *  query and return the values associated with those knodes.
     * </p>
     *
     * @param query - Array containing the center point of a search region.  lenth
     * must match dimensions specified in constructor
     * @param searchDistance - the distance from the query point to be searched.
     * @returns list of Values found in the search region
     */
    public List<VALUE_TYPE> searchTree(long query[], long searchDistance) {
        // if the tree is not built yet, build it

        // calculate the plus and minus arrays while checking for overflow
        long queryPlus[]  = new long[numDimensions];
        long queryMinus[] = new long[numDimensions];
        for (int i=0; i<numDimensions; i++) {
            if (query[i] > 0 && (Long.MAX_VALUE - query[i]) < searchDistance) {
                queryPlus[i]  =  Long.MAX_VALUE;
            } else {
                queryPlus[i]  =  query[i] + searchDistance;
            }
            if ((query[i] < 0) && ((query[i] - Long.MIN_VALUE) < searchDistance)) {
                queryMinus[i] = Long.MIN_VALUE;
            } else {
                queryMinus[i] = query[i] - searchDistance;
            }
        }
        ArrayList<KdNode<VALUE_TYPE>> results = new ArrayList<KdNode<VALUE_TYPE>>();
        root.searchKdTree(results, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, 0);
        ArrayList<VALUE_TYPE> values = new ArrayList<>();
        for(KdNode kn : results){
            values.addAll(kn.value);
        }
        return values;
    }

    /**
     * <p>
     * The {@code searchAndRemove} search the tree for all nodes contained within the bounds
     * set by queryPlus and queryMinus and return the values associated with those nodes.
     * </p>
     *
     * @param queryPlus - Array containing the lager search bound for each dimension
     * @param queryMinus - Array containing the smaller search bound for each dimension
     * @returns list of Values found in the search region
     */
    public List<VALUE_TYPE> searchTree(final long[] queryPlus, final long[] queryMinus) {
        // if the tree is not built yet, build it
        if (root == null) {
            buildTree();
            // if root is still null; return a null
            if (root == null) return null;
        }
        // check that the values in Plus are > than the values in Minus
        for(int i = 0;  i < queryMinus.length; i++) {
            if (queryMinus[i] > queryPlus[i]) {
                long T = queryMinus[i];
                queryMinus[i] = queryPlus[i];
                queryPlus[i] = T;
            }
        }
        // search the tree to get the list of values
        List<VALUE_TYPE> resultValues = new ArrayList<VALUE_TYPE>();
        ArrayList<KdNode<VALUE_TYPE>> results = new ArrayList<KdNode<VALUE_TYPE>>();
        root.searchKdTree(results, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, 0);
        ArrayList<VALUE_TYPE> values = new ArrayList<>();
        for(KdNode kn : results){
            values.addAll(kn.value);
        }
        return values;
    }


    /**
     * <p>
     * The {@code searchAndRemove} search the tree for all nodes contained within the searchDistanece
     *  query and return the values associated with those knodes.
     * </p>
     *
     * @param query - Array containing the center point of a search region.  lenth
     * must match dimensions specified in constructor
     * @param searchDistance - the distance from the query point to be searched.
     * @returns list of Values found in the search region
     */
    public void searchTree(List<long[]> tuples, List<VALUE_TYPE> values, long query[], long searchDistance) {
        // if the tree is not built yet, build it

        // calculate the plus and minus arrays while checking for overflow
        long queryPlus[]  = new long[numDimensions];
        long queryMinus[] = new long[numDimensions];
        for (int i=0; i<numDimensions; i++) {
            if (query[i] > 0 && (Long.MAX_VALUE - query[i]) < searchDistance) {
                queryPlus[i]  =  Long.MAX_VALUE;
            } else {
                queryPlus[i]  =  query[i] + searchDistance;
            }
            if ((query[i] < 0) && ((query[i] - Long.MIN_VALUE) < searchDistance)) {
                queryMinus[i] = Long.MIN_VALUE;
            } else {
                queryMinus[i] = query[i] - searchDistance;
            }
        }
        searchTree(tuples, values, queryPlus, queryMinus);
        return;
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
    public void searchTree(List<long[]> tuples, List<VALUE_TYPE> values, final long[] queryPlus, final long[] queryMinus) {

        // check that the values in Plus are > than the values in Minus
        for(int i = 0;  i < queryMinus.length; i++) {
            if (queryMinus[i] > queryPlus[i]) {
                long T = queryMinus[i];
                queryMinus[i] = queryPlus[i];
                queryPlus[i] = T;
            }
        }
        ArrayList<KdNode<VALUE_TYPE>> results = new ArrayList<KdNode<VALUE_TYPE>>();
        root.searchKdTree(results, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, 0);
        for(KdNode kn : results){
            for(Object val : kn.value) {
                tuples.add(kn.tuple);
                values.add((VALUE_TYPE) val);
            }
        }
    }

    /**
     * <p>
     * The {@code searchAndRemove} search the tree for all nodes contained within the searchDistanece
     *  query and return the values associated with those nodes.  Those values are removed from the tree.
     * </p>
     *
     * @param query - Array containing the center point of a search region.  lenth
     * must match dimensions specified in constructor
     * @param searchDistance - the distance from the query point to be searched.
     * @returns list of values found in the search region
     */
    public List<VALUE_TYPE> searchAndRemove(long query[], long searchDistance) {
        // if the tree is not built yet, build it

        // calculate the plus and minus arrays while checking for overflow
        long queryPlus[]  = new long[numDimensions];
        long queryMinus[] = new long[numDimensions];
        for (int i=0; i<numDimensions; i++) {
            if (query[i] > 0 && (Long.MAX_VALUE - query[i]) < searchDistance) {
                queryPlus[i]  =  Long.MAX_VALUE;
            } else {
                queryPlus[i]  =  query[i] + searchDistance;
            }
            if ((query[i] < 0) && ((query[i] - Long.MIN_VALUE) < searchDistance)) {
                queryMinus[i] = Long.MIN_VALUE;
            } else {
                queryMinus[i] = query[i] - searchDistance;
            }
        }
        List<VALUE_TYPE> resultValues = new ArrayList<VALUE_TYPE>();
        root.searchAndRemoveKdTree(resultValues, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, 0);
        return resultValues;
    }

    /**
     * <p>
     * The {@code searchAndRemove} search the tree for all nodes contained within the bounds
     * set by queryPlus and queryMinus and return the values associated with those nodes.
     * Those values are removed from the tree
     * </p>
     *
     * @param queryPlus - Array containing the lager search bound for each dimension
     * @param queryMinus - Array containing the smaller search bound for each dimension
     * @returns list of values found in the search region
     */
    public List<VALUE_TYPE> searchAndRemove(final long[] queryPlus, final long[] queryMinus) {
        // if the tree is not built yet, build it
        if (root == null) {
            buildTree();
            // if root is still null; return a null
            if (root == null) return null;
        }
        // check that the values in Plus are > than the values in Minus
        for(int i = 0;  i < queryMinus.length; i++) {
            if (queryMinus[i] > queryPlus[i]) {
                long T = queryMinus[i];
                queryMinus[i] = queryPlus[i];
                queryPlus[i] = T;
            }
        }
        // search the tree to get the list of values
        List<VALUE_TYPE> resultValues = new ArrayList<VALUE_TYPE>();
        root.searchAndRemoveKdTree(resultValues, queryPlus, queryMinus, permutation, executor, maximumSubmitDepth, 0);
        return resultValues;
    }

    /**
     * <p>
     * The {@code nearestNeighborSearch} searches the KdTree to find number of closest
     * neighbors from the point query set by numNeighbors. NOTE that the number of values
     * returned may be greater than numNeighbors if any of the KdTree were combined because
     * they were duplicates.
     * </p>
     *
     * @param query - Array containing the search point
     * @param numNeighbors - number of neighbors to return.
     * @returns a NearestNeighborHeap that contains the KdNodes found in the nearest neighbors.
     */
    public NearestNeighborHeap nearestNeighborSearch(final long[] query, final int numNeighbors) {
        // if the tree is not built yet, build it
        if (root == null) {
            buildTree();
            // if root is still null; return a null
            if (root == null) return null;
        }
        // search the tree to get the heap of KdNodes
        NearestNeighborHeap nnHeap = new NearestNeighborHeap(query, numNeighbors);
        root.nearestNeighbor(nnHeap, permutation, 0);
        return nnHeap;
    }

    /**
     * <p>
     * The {@code nearestNeighborSearch} searches the KdTree to find number of closest
     * neighbors from the point query set by numNeighbors. NOTE that the number of values
     * returned may be greater than numNeighbors if any of the KdTree were combined because
     * they were duplicates.
     * </p>
     *
     * @param query - Array containing the search point
     * @param numNeighbors - number of neighbors to return.
     * @param enable - a boolean array indicating which components should be included in the search.
     * @returns list of Values found in the nearest neighbors.
     */
    public List<VALUE_TYPE> nearestNeighborSearch(final long[] query, final int numNeighbors, final boolean[] enable) {
        // if the tree is not built yet, build it
        if (root == null) {
            buildTree();
            // if root is still null; return a null
            if (root == null) return null;
        }
        // search the tree to get the list of KdNodes
        NearestNeighborHeap nnHeap = new NearestNeighborHeap(query, numNeighbors, enable);
        root.nearestNeighbor(nnHeap, permutation, 0);
        // copy the list values in each KdNode to a single list for return but note
        // that heap stores nothing in nodes[0] and that copying the elements of nodes
        // in order (as below) does not produce a list of nodes sorted by dist.
        List<VALUE_TYPE> resultValues = new ArrayList<VALUE_TYPE>();
        for (int i = 1; i < nnHeap.curDepth; i++) {
            resultValues.addAll(nnHeap.nodes[i].value);
        }
        return resultValues;
    }

    /**
     * <p>
     * The {@code remove) removes the valueToRemove and the query point.
     * </p>
     *
     * @param query - Array containing the search point
     * @param valueToRemove - value to remove at that point
     * @returns a boolean which is true if that calue was removed.
     */
    public boolean remove(final long[] query, VALUE_TYPE valueToRemove) {
        // if the tree is not built yet, build it
        if (root == null) {
            buildTree();
            // if root is still null; return a null
            if (root == null) return false;
        }
        // search the tree to find the node to remove.
        int returnResult = root.removeValue(query, valueToRemove, permutation, 0);
        // A result of 0 indicates that no value was found to remove.  -1 or +1 indicate a value was removed.
        return returnResult != 0;
    }

    /**
     * <p>
     * The {@code randomIntegerInInterval} method creates a random int in the interval [min, max].
     * See http://stackoverflow.com/questions/6218399/how-to-generate-a-random-number-between-0-and-1
     * </p>
     *
     * @param min - the minimum int value desired
     * @param max - the maximum int value desired
     * @returns a random int
     */
    private static int randomIntegerInInterval(final int min, final int max) {
        return min + (int) ( Math.random() * (max - min) );
    }

    /**
     * <p>
     * Define a simple data set then build a k-d tree.
     * </p>
     */
    public static void main(String[] args) {

        // Set the defaults then parse the input arguments.
        int numPoints = 16*262144;
        int extraPoints = 100;
        int numDimensions = 4;
        int numThreads = 8;
        int searchDistance = 100000000;
        int maximumNumberOfNodesToPrint = 5;
        int numNearestNeighbors = 100;

        for (int i = 0; i < args.length; i++) {
            if ( args[i].equals("-n") || args[i].equals("--numPoints") ) {
                numPoints = Integer.parseInt(args[++i]);
                continue;
            }
            if ( args[i].equals("-x") || args[i].equals("--extraPoints") ) {
                extraPoints = Integer.parseInt(args[++i]);
                continue;
            }
            if ( args[i].equals("-d") || args[i].equals("--numDimensions") ) {
                numDimensions = Integer.parseInt(args[++i]);
                continue;
            }
            if ( args[i].equals("-t") || args[i].equals("--numThreads") ) {
                numThreads = Integer.parseInt(args[++i]);
                continue;
            }
            if ( args[i].equals("-s") || args[i].equals("--searchDistance") ) {
                searchDistance = Integer.parseInt(args[++i]);
                continue;
            }
            if ( args[i].equals("-p") || args[i].equals("--maximumNodesToPrint") ) {
                maximumNumberOfNodesToPrint = Integer.parseInt(args[++i]);
                continue;
            }
            if ( args[i].equals("-m") || args[i].equals("--nearestNeighborCount") ) {
                numNearestNeighbors = Integer.parseInt(args[++i]);
                continue;
            }
            throw new RuntimeException("illegal command-line argument: " + args[i]);
        }

        // Declare and initialize the the KdTree class add n dimensional tuples as keys and an int
        // as a value in the half-open interval [0, Integer.MAXIMUM_VALUE].  Create extraPoints-1
        // duplicate coordinates, where extraPoints <= numPoints, to test the removal of duplicate points.
        extraPoints = (extraPoints <= numPoints) ? extraPoints: numPoints;
        KdTree<Integer> myKdTree = new KdTree<Integer>(numPoints, numDimensions);
        myKdTree.setNumThreads(numThreads);
        long[] coordinates= new long[numDimensions];
        for (int i = 0; i < numPoints + extraPoints - 1; i++) {
            for (int j = 0; j < numDimensions; j++) {
                coordinates[j] = randomIntegerInInterval(0, Integer.MAX_VALUE);
            }
            myKdTree.add(coordinates, i);
        }

        // create some duplicate points.
        for (int i = 1; i < extraPoints; i++) {
            for (int j = 0; j < myKdTree.kdNodes[0].tuple.length; j++) {
                myKdTree.kdNodes[i].tuple[j] = myKdTree.kdNodes[numPoints - 1 - i].tuple[j];
            }
        }
        myKdTree.buildTree();

        /*****************************
         // value validity test
         *****************************/
        // set the bounds of a tre search to return all values
        long[] qp = new long[numPoints];
        long[] qm = new long[numPoints];
        for (int i = 0;  i < numPoints; i++) {
            qp[i] = Integer.MAX_VALUE;
            qm[i] = Integer.MIN_VALUE;
        }
        ArrayList<Integer> allValues = (ArrayList)myKdTree.searchTree(qp, qm);
        // check to see of the number of values is correct.
        if (allValues.size() != numPoints) {
            System.out.println("Number of returned values from full tree search " + allValues.size() +
                    " does not equal number of input values " + numPoints);
        }
        // sort the results which should be in order from 0 to numPoints-1
        Collections.sort(allValues);
        for (int i = 0;  i<numPoints; i++){
            if (allValues.get(i) != i){
                System.out.println("Full tree value list is incorrect at " + i);
                System.exit(1);
            }
        }

        /*****************************
         // search region test
         *****************************/
        // Search the k-d tree for all points that lie within a hypercube centered at the first point.
        final long[] query = myKdTree.kdNodes[0].tuple;
        long searchTime = System.currentTimeMillis();
        List<Integer> kdNodeValues = myKdTree.searchTree(query, searchDistance);
        searchTime = System.currentTimeMillis() - searchTime;
        final double sT = (double) searchTime / 1000.;
        System.out.printf("searchTime for hypercube search = %.3f\n", sT);
        System.out.print("\n" + kdNodeValues.size() + " nodes within " + searchDistance + " units of ");
        KdNode.printTuple(query);
        System.out.println(" in all dimensions.\n");
        if ( !kdNodeValues.isEmpty() ) {
            maximumNumberOfNodesToPrint = Math.min( maximumNumberOfNodesToPrint, kdNodeValues.size() );
            System.out.println("List of values associated with the first " + maximumNumberOfNodesToPrint + " k-d nodes within " +
                    searchDistance + "-unit search distance follows:\n");
            for (int i = 0; i < maximumNumberOfNodesToPrint; i++) {
                System.out.println(kdNodeValues.get(i));
            }
            System.out.println();
        }
        // check that single threaded search returns the same results.
        myKdTree.setNumThreads(1);
        List<Integer> kdNodeValuesAlt = myKdTree.searchTree(query, searchDistance);
        if (kdNodeValues.size() != kdNodeValuesAlt.size() || !kdNodeValues.containsAll(kdNodeValuesAlt))
            System.out.println("Single threaded search does not match multithreaded search");
        myKdTree.setNumThreads(8);

        List<long[]> tuples = new ArrayList<long[]>();
        kdNodeValuesAlt.clear();
        myKdTree.searchTree(tuples, kdNodeValuesAlt, query, searchDistance);
        if (kdNodeValues.size() != kdNodeValuesAlt.size() || !kdNodeValues.containsAll(kdNodeValuesAlt))
            System.out.println("tuple + value search != value only search");


        /*****************************
         // copy constructor test
         *****************************/
        int numKdTreeNodes = myKdTree.root.verifyKdTree(myKdTree.permutation, myKdTree.executor, myKdTree.maximumSubmitDepth, 0);
        long copyTime = System.currentTimeMillis();
        KdTree copiedTree = new KdTree(myKdTree);
        copyTime = System.currentTimeMillis() - copyTime;
        final double sC = (double) copyTime / 1000.;
        System.out.printf("copy KdTree time = %.3f\n", sC);
        copiedTree.setNumThreads(numThreads);
        int numCopiedNodes = copiedTree.root.verifyKdTree(copiedTree.permutation, copiedTree.executor, copiedTree.maximumSubmitDepth, 0);
        if (numKdTreeNodes != numCopiedNodes)
            System.out.println("Number of nodes in the copied tree, " + numCopiedNodes +
                    " does not equal number of node in the original tree, " + numKdTreeNodes);
        allValues = (ArrayList)copiedTree.searchTree(qp, qm);
        // check to see of the number of values is correct.
        if (allValues.size() != numPoints) {
            System.out.println("Number of returned values from the copied tree search " + allValues.size() +
                    " does not equal number of input values " + numPoints);
        }
        // sort the results which should be in order from 0 to numPoints-1
        Collections.sort(allValues);
        for (int i = 0;  i<numPoints; i++){
            if (allValues.get(i) != i){
                System.out.println("Copied tree value list is incorrect at " + i);
                System.exit(1);
            }
        }


        /*****************************
         // nearest neighbor test
         *****************************/

        // It's not possible to find more nearest neighbors than there are points.
        numNearestNeighbors = Math.min(numNearestNeighbors, numPoints + extraPoints - 1);

        // Search the k-d tree for the numNearestNeighbors nearest neighbors to the first point.
        long nnTime = System.currentTimeMillis();
        // search the tree to get the heap of KdNodes
        NearestNeighborHeap nns = myKdTree.nearestNeighborSearch(query, numNearestNeighbors);
        nnTime = System.currentTimeMillis() - nnTime;
        final double nT = (double) nnTime / 1000.;
        System.out.print("searchTime for " + numNearestNeighbors + " nearest neighbors = ");
        System.out.printf("%.3f\n\n", nT);

        // Now perform a brute-force search for nearest neighbors and compare the results.
        // start by creating a list of KdNodes still in the tree.
        List<KdNode<Integer>> allKdNodes = myKdTree.root.searchKdTree(myKdTree.root.tuple, (long)Integer.MAX_VALUE,
                myKdTree.permutation, myKdTree.executor, myKdTree.maximumSubmitDepth, 0);
        NearestNeighborHeap nnl = new NearestNeighborHeap(query, numNearestNeighbors);
        for (int i = 0; i < allKdNodes.size(); i++) {
            nnl.add(allKdNodes.get(i));
        }

        // Unload the heaps and verify that they are ordered correctly and are in the same sorted order,
        // and that corresponding KdNodes from the two heaps have identical value lists.
        HeapPair s1 = nns.removeTop();
        HeapPair l1 = nnl.removeTop();
        if (s1.dist != l1.dist) {
            System.out.println("nns dist1 = " + s1.dist + " != nnl dist1 = " + l1.dist);
        }
        for (int j = 0; j < s1.node.value.size(); j++) {
            if (l1.node.value.get(j) != s1.node.value.get(j)) {
                System.out.println("nnSearch values at 0, " + j + " do not match");
            }
        }
        for (int i = 1; i < numNearestNeighbors; i++) {
            HeapPair s2 = nns.removeTop();
            HeapPair l2 = nnl.removeTop();
            if (s1.dist < s2.dist) {
                System.out.println("at index = " + i + " nns dist1 = " + s1.dist + " < dist2 = " + s2.dist);
            }
            if (l1.dist < l2.dist) {
                System.out.println("at index = " + i + " nnl dist1 = " + l1.dist + " < dist2 = " + l2.dist);
            }
            if (s2.dist != l2.dist) {
                System.out.println("nns dist2 = " + s2.dist + " != nnl dist2 = " + l2.dist);
            }
            for (int j = 0; j < s2.node.value.size(); j++) {
                if (l2.node.value.get(j) != s2.node.value.get(j)) {
                    System.out.println("nnSearch values at " + i + ", " + j + " do not match");
                }
            }
            s1 = s2;
            l1 = l2;
        }

        /*****************************
         // search and remove region test
         *****************************/
        // verify the tree before starting
        myKdTree.root.verifyKdTree(myKdTree.permutation, myKdTree.executor, myKdTree.maximumSubmitDepth, 0);
        // call search and remove with the same arguments as the original search
        kdNodeValuesAlt = myKdTree.searchAndRemove(query, searchDistance);
        // make sure the returned results match.
        if (kdNodeValues.size() != kdNodeValuesAlt.size()|| !kdNodeValues.containsAll(kdNodeValuesAlt))
            System.out.print("searchAndRemove does not match regular search search");
        // repeat the searchAndRemove.
        kdNodeValuesAlt = myKdTree.searchAndRemove(query, searchDistance);
        // check to see that no values are returned
        if ( kdNodeValuesAlt.size() != 0)
            System.out.print("Repeated call to searchAndRemove returned some values when it should not");
        //Verify the tree after searchAndRemove
        myKdTree.root.verifyKdTree(myKdTree.permutation, myKdTree.executor, myKdTree.maximumSubmitDepth, 0);


        // Shut down the ExecutorService.
        myKdTree.shutdown();

        System.exit(0);
    }

} //class KdTree


