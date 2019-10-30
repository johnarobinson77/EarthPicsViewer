package org.jar;
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

import org.KdTree.KdTreeEx;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * <p>
 * DBSCAN_Clusters implements a version of the popular DBSCAN clustering algorithm.  It depends upon the existence of
 * the KdTreeEx class implemented elsewhere to provide a mechanism for fast searches.
 * </p>
 *
 * @author John A. Robinson
 */
class DBSCAN_Clusters {

    int numDimensions;// holds the number of dimensions in the clustering data
    ArrayList<Cluster> clusters; // holds the array of individual Cluster objects

    // the primary constructor.  It needs to know the number of dimensions.
    /*
     * <p>
     * The {@code DBSCAN_Clusters} is the primary constructor.  It needs to know the number of dimensions.
     * </p>
     *
     * @param numDimensions - number of dimension each cluster point will have.  Needs to match the KdTreeEx dimensions
     */
    DBSCAN_Clusters(int numDimensions) {
        clusters = new ArrayList<>();
        this.numDimensions = numDimensions;
    }

    /* The Cluster object holds the list of indices to point in the cluster.  In addition it
     * calculate and hold the bounds of that cluster as an arbitrary tag
     */
    class Cluster {
        private ArrayList<Integer> clusterIdxs; // array of indices in the cluster
        private ArrayList<long[]> clusterPoints; // temp array to old point associated with the above idxs.
        private long[] upperBounds; // upper point of the hypercube bounding the cluster.
        private long[] lowerBounds; // lower point of the hypercube bounding the cluster.
        private String tag;  // arbitrary tag string

        // Cluster constructor
        Cluster() {
            clusterIdxs = new ArrayList<>();
            clusterPoints = new ArrayList<>();
            upperBounds = new long[numDimensions];
            lowerBounds = new long[numDimensions];
            for (int i = 0; i < numDimensions; i++) {
                upperBounds[i] = Long.MIN_VALUE;
                lowerBounds[i] = Long.MAX_VALUE;
            }
            tag = null;
        }

        /* adds a point to the current Cluster. */
        boolean add(long[] point, Integer locIdx) {
            // get maintain the min and max.  Add an epsilon to the max to make sure its > all points.
            for (int i = 0; i < numDimensions; i++) {
                if (point[i]+1 > upperBounds[i]) upperBounds[i] = point[i]+1;
                if (point[i]   < lowerBounds[i]) lowerBounds[i] = point[i];
            }
            clusterIdxs.add(locIdx);
            clusterPoints.add(point);
            return true;
        }

        // returns the number of points in the cluster
        int size () {
            return clusterIdxs.size();
        }

        // return a point index in the cluster.
        int get (int i) {
            return clusterIdxs.get(i);
        }

        // returns the bounds found during the building of the cluster.
        public long[][] getBoundsLong() {
            long[][] ret = new long[2][];
            ret[0] = upperBounds;
            ret[1] = lowerBounds;
            return ret;
        }

        // returns the bounds found during the building of the cluster in double format
        // assuming that the data was in E7 format.
        public double[][] getBoundsDouble() {
            double[][] ret = new double[2][numDimensions];
            for (int i=0;  i < numDimensions; i++) {
                ret[0][i] = upperBounds[i] * 1.0e-7;
                ret[1][i] = lowerBounds[i] * 1.0e-7;
            }
            return ret;
        }

        // access tag data
        public String getTag() {
            return tag;
        }
        public void setTag(String tag) {
            this.tag = tag;
        }
        public boolean hasTag(String tag) {
            if (this.tag == null || tag == null) {
                return false;
            }
            return this.tag.equals(tag);
        }

        public void sort(Comparator<Integer> integerComparator) {
            clusterIdxs.sort(integerComparator);
        }

        // returns the array holding the points int eh cluster
        public ArrayList<Integer> clusterIdxs() {
            return clusterIdxs;
        }

        // clears the cluster to constructor state.
        public void clear() {
            clusterIdxs.clear();
            for (int i = 0; i < numDimensions; i++) {
                upperBounds[i] = Long.MIN_VALUE;
                lowerBounds[i] = Long.MAX_VALUE;
            }
        }
    } //Cluster

    // external Cluster construction.
    public Cluster getNewCluster() {
        return new Cluster();
    }

    /*
     * <p>
     * The {@code buildCluster} method builds the clusters
     * </p>
     *
     * @param KdTreeEx - THe KdTree holding the data that is to be clustered.
     * @param searchRadius - Array holding the cluster window size +/- from the center point.
     * @returns void
     */
    void buildCluster(final KdTreeEx<Integer> kdTree, final long[] searchRadius) {

        if(kdTree.getNumDimensions() != numDimensions) {
            System.out.println("KdTreee and DBSCAN_Cluster number of dimentions do not match");
        }
        long[] qp = new long[numDimensions];
        long[] qm = new long[numDimensions];
        // Start by picking an arbitrary point from the KdTree.  If none returned, the the KdTree is empty so
        // its done.
        Integer nextIdx;
        long[] picPoint = new long[numDimensions];
        while (null != (nextIdx = kdTree.pickValue(picPoint,1, true))) {
            // add a new cluster
            Cluster cluster = new Cluster();
            // and add the seed point to it.
            cluster.add(picPoint, nextIdx);
            // Step through each element in the cluster list and add to the cluster list all locations that are
            // within searchRad of the current element of the list.  The list will only contain 1 reference to
            // each item.  So the list will stop growing when there are no items closer searchRadius from any point
            // in the list
            int n = 0;
            long[] point;
            while (n < cluster.size()) {
                // get the search window for the k-d tree
                point = cluster.clusterPoints.get(n);

                for (int i = 0; i < numDimensions; i++) {
                    qp[i] = point[i] + searchRadius[i];
                    qm[i] = point[i] - searchRadius[i];
                }
                // get the array of indices to the points in that search window
                ArrayList<long[]> keys = new ArrayList<long[]>();
                ArrayList<Integer> values = new ArrayList<Integer>();
                kdTree.searchTree(keys, values, qp, qm);
                // add each point to the cluster and remove it from the kdtree.
                for (int m = 0; m < values.size(); m++) {
                    // get the point
                    point = keys.get(m);
                    // add location index to the cluster
                    cluster.add(point, values.get(m));
                    // remove the index from the KdTree
                    if (!kdTree.remove(point, values.get(m))) {
                        System.out.println("KdTree Removal Value Error.");
                    }
                }
                n++;
            }
            cluster.clusterPoints = null;  // save space
            clusters.add(cluster);
        }
    }

    // Some helper Clusters methods
    // Sort the clusters by size
    void sortBySize(){
        clusters.sort(new Comparator<Cluster>() {
            @Override
            public int compare(Cluster o1, Cluster o2) {
                long t = o2.size() - o1.size();
                return t>0 ? +1 : t<0 ? -1 : 0;
            }
        });
    }

    // search thr cluster list to see if this point is inside the bounding box
    public ArrayList<Cluster> searchClusters(long[] point) {
        ArrayList<Cluster> cList = new ArrayList<>();
        for(Cluster clstr : clusters){
            boolean inside = true;
            for (int i = 0;  i < numDimensions; i++) {
                if (point[i] >= clstr.upperBounds[i] ||
                        point[i] < clstr.lowerBounds[i]) {
                    inside = false;
                    break;
                }
            }
            if (inside) {
                cList.add(clstr);
            }
        }
        return cList;
    }


    /*
     * <p>
     * The {@code checkClusters} method  prints statistics about the clusters.  All cluster is tag is null
     * </p>
     *
     * @param numLocations - THe number of expected point in the clusters to check that nothing got dropped
     * @returns boolean indicating pass (true) or fail (false)
     */
    boolean checkClusters(final int numLocations) {
        return checkClusters(numLocations, null);
    }

    /*
     * <p>
     * The {@code checkClusters} method  prints statistics about the clusters by tag.  All cluster is tag is null
     * </p>
     *
     * @param numLocations - THe number of expected point in the clusters to check that nothing got dropped
     * @param tag - for printing the stats of a particular cluster tag.
     * @returns boolean indicating pass (true) or fail (false)
     */
    boolean checkClusters(final int numLocations, final String tag) {
        int max = 0;
        int min = Integer.MAX_VALUE;
        int avgSize = 0;
        int count = 0;
        boolean rb = true;
        for (int i = 0; i < clusters.size(); i++) {
            Cluster c = clusters.get(i);
            if (tag == null || c.tag!=null && c.hasTag(tag)) {
                int t = c.clusterIdxs.size();
                if (t == 0) {
                    System.out.println("Cluster " + i + " has 0 entries");
                }
                if (t > max) max = t;
                if (t < min) min = t;
                avgSize += t;
                count++;
            }
        }
        if (numLocations != -1 && avgSize != numLocations) {
            System.out.println("Number of locations in all clusters not equal input locations.");
            rb = false;
        }
        avgSize = (count > 0) ? avgSize/count : 0;
        min = (count > 0) ? min : 0;
        max = (count > 0) ? max : 0;
        String t_tag = new String(tag == null ? "" : tag + " ");
        System.out.println("Cluster " + t_tag + "Count = " + count + " Max = " + max +
                "  Min = " + min + " Average = " + avgSize);
        return rb;
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
    private static long randomIntegerInInterval(final long min, final long max) {
        return min + (long) ( Math.random() * (max - min) );
    }

    // this main() function provides a test simple case and usage examples and is not necessary.
    public static void main(String[] args) {
        final Locations locations = new Locations();
        final int numClusters = 1600;
        final int numPointsPer = 1600;
        final int clusterSpan = 3;
        final int numDimensions = 3;
        final long searchRad = 1000;

        // The test case provided here generates <numClusters> clusters where each cluster has <numPointsPer> points
        // in them.  The test is a pass if checkClusters() prints <numClusters> number of clusters and the average
        // points per cluster matches <numPointsPer>.  Occasionally, the random number generator generator clusters
        // that overlap reducing the cluster count but that run should be ignored.

        // create an array of cluster center points at random positions
        long[][] clusterCenters = new long[numClusters][numDimensions];
        for (int i = 0; i<numClusters; i++) {
            for (int j = 0; j < numDimensions; j++)
                clusterCenters[i][j] = randomIntegerInInterval(-1000000000, 1000000000);
        }

        // create a set of points that are clustered within <cluster span> X <search radius>
        // around each cluster center point.  Make sure the cluster points are not adjacent in the Locations list.
        for (int i = 0; i < numClusters; i++) {
            for (int j = 0;  j < numPointsPer; j++) {
                Locations.Location loc = locations.add();
                loc.setLatitudeE7(clusterCenters[i][0] +
                        randomIntegerInInterval(searchRad * -clusterSpan, searchRad * clusterSpan));
                loc.setLongitudeE7(clusterCenters[i][1] +
                        randomIntegerInInterval(searchRad * -clusterSpan, searchRad * clusterSpan));
                loc.setTimestampMs (Long.toString(clusterCenters[i][2] +
                        randomIntegerInInterval(0, searchRad * clusterSpan)));
            }
        }

        // create, fill and build the KdTree
        long[] latLonTime = new long[3];
        KdTreeEx<Integer> fKdTree = new KdTreeEx<Integer>((int)locations.size(), 3);
        fKdTree.setNumThreads(Runtime.getRuntime().availableProcessors());

        for (int idx = 0;  idx < locations.size(); idx++){
            // feed the kdTree
            latLonTime[0] = locations.get(idx).getLatitudeE7();
            latLonTime[1] = locations.get(idx).getLongitudeE7();
            latLonTime[2] = locations.get(idx).getTimeStampLong();
            if (0 > fKdTree.add(latLonTime, idx)) {
                System.out.println("fKdTree data input error at " + idx);
            }
        }
        long overallTime = System.currentTimeMillis();
        fKdTree.buildTree();

        // create a DBSCAN_Clusters object.
        DBSCAN_Clusters visitCluster = new DBSCAN_Clusters(numDimensions);

        long clusterTime = System.currentTimeMillis();

        // get the search range to about cluster distance window, and build clusters
        long[] window = {searchRad, searchRad,searchRad};
        visitCluster.buildCluster(fKdTree, window);

        long currentTime =  System.currentTimeMillis();
        final double sC = (double) (currentTime - clusterTime) / 1000.;
        final double sO = (double) (currentTime - overallTime) / 1000;
        System.out.printf("Cluster time = %.3f\n", sC);
        System.out.printf("Overall time = %.3f\n", sO);

        // check for errors in the clusters and print stats
        if (!visitCluster.checkClusters((int)locations.size())) {
            System.exit(1);
        }
        System.exit(0);
    }



    /*  Not ready for prime time.
    static void clipHyperCube(final ArrayList<long[][]> clippedCubes, final long[] nqp, final long[] nqm, final long[] oqp, final long[] oqm,
                              int dim) {
        long[][]outside = null;
        for(int plane = 0;  plane < dim; plane++) {
            if (nqm[plane] >= oqp[plane] || nqp[plane] <= oqm[plane]) { // check if completely ouside this other cube
                // output the new cube and exit;
                outside = new long[2][dim];
                for (int i = 0; i < dim; i++) {
                    outside[0][i] = nqp[i];
                    outside[1][i] = nqm[i];
                }
                clippedCubes.add(outside);
                return;
            }
            if (nqp[plane] <= oqp[plane] && nqm[plane] >= oqm[plane]) // check if completely inside
                continue;
            if (nqp[plane] > oqp[plane] && nqm[plane] < oqp[plane]) { // check agaist other plus side
                // output the outside half
                outside = new long[2][dim];
                for (int i = 0; i < dim; i++) {
                    outside[0][i] = nqp[i];
                    if (i == plane) {
                        nqp[i] = oqp[i];
                        outside[1][i] = oqp[i];
                    } else {
                        outside[1][i] = nqm[i];
                    }
                }
                clippedCubes.add(outside);
            }
            if (nqp[plane] > oqm[plane] && nqm[plane] < oqm[plane]) { // check against other minus side
                // output the outside half
                outside = new long[2][dim];
                for (int i = 0; i < dim; i++) {
                    outside[1][i] = nqm[i];
                    if (i == plane) {
                        nqm[i] = oqm[i];
                        outside[0][i] = oqm[i];
                    } else {
                        outside[0][i] = nqp[i];
                    }
                }
                clippedCubes.add(outside);
            }
        }
        return;
    }
     */

}

