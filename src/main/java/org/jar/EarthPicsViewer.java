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

import de.micromata.opengis.kml.v_2_2_0.*;
import org.KdTree.KdTree;
import org.KdTree.KdTreeEx;
import org.apache.commons.imaging.ImageReadException;
import org.apache.commons.imaging.Imaging;
import org.apache.commons.imaging.common.ImageMetadata;
import org.apache.commons.imaging.common.RationalNumber;
import org.apache.commons.imaging.formats.jpeg.JpegImageMetadata;
import org.apache.commons.imaging.formats.tiff.TiffField;
import org.apache.commons.imaging.formats.tiff.constants.ExifTagConstants;
import org.apache.commons.imaging.formats.tiff.constants.GpsTagConstants;
import org.apache.commons.imaging.formats.tiff.constants.TiffTagConstants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;

public class EarthPicsViewer {

    final static private boolean runTest = false;

    final static int cores = Runtime.getRuntime().availableProcessors();

    static InputForm inputForm = null;

    private static class FilterParameter {
        private ArrayList<long[][]> includeRegions;
        private ArrayList<long[][]> excludeRegions;

        FilterParameter() {
            includeRegions = new ArrayList<>();
            excludeRegions = new ArrayList<>();
        }

        public void addIncludeRegion(final long maxLat, final long maxLon, final long maxTime,
                                     final long minLat, final long minLon, final long minTime) {
            long[][] region = new long[][]{{maxLat, maxLon, maxTime}, {minLat, minLon, minTime}};
            includeRegions.add(region);
        }

        public long[][] getIncludeRegion(int i) {
            if (i < includeRegions.size())
                return includeRegions.get(i);
            else
                return null;
        }
        public int numExcludeRegions() {
            return excludeRegions.size();
        }

        public void addExcludeRegion(final long maxLat, final long maxLon, final long maxTime,
                                     final long minLat, final long minLon, final long minTime) {
            long[][] region = new long[][]{{maxLat, maxLon, maxTime}, {minLat, minLon, minTime}};
            excludeRegions.add(region);
        }

        public long[][] getExcludeRegion(int i) {
            if (i < excludeRegions.size())
                return excludeRegions.get(i);
            else
                return null;
        }
        public int numIncludeRegions() {
            return includeRegions.size();
        }
    }

    // The PicturesMdata class reads in and holds the metadata for all jpeg files.  It also provides the
    // container for all the instances.
    public static class PicturesMdata {

        Locations picLocations = new Locations();

        //The PictureMdata class reads in and hold the metadata for one jpeg file.
        public class PictureMdata {
            String srcFileName;
            String dstFileName;
            Locations.Location location;

            private PictureMdata(String srcFileName, String dstFileName, JpegImageMetadata jpegMetadata)
                    throws ImageReadException, ParseException {
                this.srcFileName = srcFileName;
                this.dstFileName = dstFileName;
                location = picLocations.add();
                // parse time and add it to location data
                String origDate = null;
                if (jpegMetadata.findEXIFValueWithExactMatch(
                        ExifTagConstants.EXIF_TAG_DATE_TIME_ORIGINAL)!=null) {
                    origDate = jpegMetadata.findEXIFValueWithExactMatch(
                            ExifTagConstants.EXIF_TAG_DATE_TIME_ORIGINAL).getStringValue();
                } else {
                    throw new ImageReadException("Date Read Error");
                }
                DateFormat df = new SimpleDateFormat("yyyy:MM:dd hh:mm:ss");
                Timestamp ts = new Timestamp(((java.util.Date) df.parse(origDate)).getTime());
                location.setTimestampMs(ts);
                // parse location and add it to the location data.
                final TiffField gpsLatitudeRefField = jpegMetadata.findEXIFValueWithExactMatch(
                        GpsTagConstants.GPS_TAG_GPS_LATITUDE_REF);
                final TiffField gpsLatitudeField = jpegMetadata.findEXIFValueWithExactMatch(
                        GpsTagConstants.GPS_TAG_GPS_LATITUDE);
                final TiffField gpsLongitudeRefField = jpegMetadata.findEXIFValueWithExactMatch(
                        GpsTagConstants.GPS_TAG_GPS_LONGITUDE_REF);
                final TiffField gpsLongitudeField = jpegMetadata.findEXIFValueWithExactMatch(
                        GpsTagConstants.GPS_TAG_GPS_LONGITUDE);
                if (gpsLatitudeRefField != null && gpsLatitudeField != null &&
                        gpsLongitudeRefField != null &&
                        gpsLongitudeField != null) {
                    // all of these values are strings.
                    final String gpsLatitudeRef = (String) gpsLatitudeRefField.getValue();
                    final RationalNumber gpsLatitude[] = (RationalNumber[]) (gpsLatitudeField.getValue());
                    final String gpsLongitudeRef = (String) gpsLongitudeRefField.getValue();
                    final RationalNumber gpsLongitude[] = (RationalNumber[]) gpsLongitudeField.getValue();

                    Double dLongitude = gpsLongitude[0].doubleValue() + gpsLongitude[1].doubleValue() / 60.0 +
                            gpsLongitude[2].doubleValue() / 3600.0;
                    if (gpsLongitudeRef.equals("W")) {
                        dLongitude = -dLongitude;
                    }
                    Double dLatitude = gpsLatitude[0].doubleValue() + gpsLatitude[1].doubleValue() / 60.0 +
                            gpsLatitude[2].doubleValue() / 3600.0;
                    if (gpsLatitudeRef.equals("S")) {
                        dLatitude = -dLatitude;
                    }
                    location.setLatitude(dLatitude);
                    location.setLongitude(dLongitude);
                } else {
                    throw new ImageReadException("GPS Read Error");
                }
            }
        }

        ArrayList<PictureMdata> pictureMdataList;

        public PicturesMdata() {
            pictureMdataList = new ArrayList();
        }

        public PictureMdata add(String srcFileName, String dstFilename)
                throws IOException, ImageReadException, ParseException {
            PictureMdata pm = null;
            final ImageMetadata metadata = Imaging.getMetadata(new File(srcFileName));
            if (metadata instanceof JpegImageMetadata) {
                JpegImageMetadata jpegMetadata = (JpegImageMetadata) metadata;
                pm = new PictureMdata(srcFileName, dstFilename, jpegMetadata);
            }
            pictureMdataList.add(pm);
            return pm;
        }

        public PictureMdata get(int i) {
            return pictureMdataList.get(i);
        }

        public int size() {
            return pictureMdataList.size();
        }
    }

    // This class if from the apache commons-imaging library and does the heavy lifting for reading
    // metadata from the jpeg files
    public JpegImageMetadata jpegMetadata;

    /* this constructor is just test code for the metadata reader and not use
    public EarthPicsViewer(String fullFileName) throws IOException, ImageReadException, ParseException {
        File file = new File(fullFileName);
        // get all metadata stored in EXIF format.
        final ImageMetadata metadata = Imaging.getMetadata(file);

        if (metadata instanceof JpegImageMetadata) {
            jpegMetadata = (JpegImageMetadata) metadata;
            String manufacturer = jpegMetadata.findEXIFValueWithExactMatch(TiffTagConstants.TIFF_TAG_MAKE).getStringValue();
            String model = jpegMetadata.findEXIFValueWithExactMatch(TiffTagConstants.TIFF_TAG_MODEL).getStringValue();
            String origDate = jpegMetadata.findEXIFValueWithExactMatch(ExifTagConstants.EXIF_TAG_DATE_TIME_ORIGINAL).getStringValue();
            System.out.println("Manufacturer: "+ manufacturer+", Model: " + model);
            System.out.println("Date: " + origDate);
            DateFormat df = new SimpleDateFormat("yyyy:mm:dd hh:mm:ss");
            Timestamp ts = new Timestamp(((java.util.Date)df.parse(origDate)).getTime());
            System.out.println(ts);

            // more specific example of how to manually access GPS values
            final TiffField gpsLatitudeRefField = jpegMetadata.findEXIFValueWithExactMatch(
                    GpsTagConstants.GPS_TAG_GPS_LATITUDE_REF);
            final TiffField gpsLatitudeField = jpegMetadata.findEXIFValueWithExactMatch(
                    GpsTagConstants.GPS_TAG_GPS_LATITUDE);
            final TiffField gpsLongitudeRefField = jpegMetadata.findEXIFValueWithExactMatch(
                    GpsTagConstants.GPS_TAG_GPS_LONGITUDE_REF);
            final TiffField gpsLongitudeField = jpegMetadata.findEXIFValueWithExactMatch(
                    GpsTagConstants.GPS_TAG_GPS_LONGITUDE);
            if (gpsLatitudeRefField != null && gpsLatitudeField != null &&
                    gpsLongitudeRefField != null &&
                    gpsLongitudeField != null) {
                // all of these values are strings.
                final String gpsLatitudeRef = (String) gpsLatitudeRefField.getValue();
                final RationalNumber gpsLatitude[] = (RationalNumber[]) (gpsLatitudeField.getValue());
                final String gpsLongitudeRef = (String) gpsLongitudeRefField.getValue();
                final RationalNumber gpsLongitude[] = (RationalNumber[]) gpsLongitudeField.getValue();

                // This will format the gps info like so:
                //38°27'9.104"N 121°12'24.194"W
                System.out.print("GPS Coordinates: "
                        + gpsLatitude[0].toDisplayString() + "°"
                        + gpsLatitude[1].toDisplayString() + "'"
                        + gpsLatitude[2].toDisplayString() + "\""
                        + gpsLatitudeRef + " ");
                System.out.println(
                        gpsLongitude[0].toDisplayString() + "°"
                                + gpsLongitude[1].toDisplayString() + "'"
                                + gpsLongitude[2].toDisplayString() + "\""
                                + gpsLongitudeRef);

                Double dLongitude = gpsLongitude[0].doubleValue() + gpsLongitude[1].doubleValue()/60.0 +
                        gpsLongitude[2].doubleValue()/3600.0;
                if (gpsLongitudeRef.equals("W")) {
                    dLongitude = -dLongitude;
                }
                Double dLatitude = gpsLatitude[0].doubleValue() + gpsLatitude[1].doubleValue()/60.0 +
                        gpsLatitude[2].doubleValue()/3600.0;
                if (gpsLatitudeRef.equals("S")) {
                    dLatitude = -dLatitude;
                }
                System.out.println("("+dLatitude.toString() + " " + dLongitude.toString()+")");

            }
            System.out.println();
        }
    }
    */

    public static ArrayList<String> listFilesForFolder(final File folder, int recursions) {
        ArrayList<String> fn = new ArrayList<>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                if (recursions > 0) fn.addAll(listFilesForFolder(fileEntry, recursions - 1));
            } else {
                String ffn = fileEntry.getPath();
                if (ffn.endsWith(".jpg") || ffn.endsWith(".JPG")) {
                    if (runTest) System.out.println(ffn);
                    fn.add(ffn);
                }
            }
        }
        return fn;
    }

    // The LocHier class build the visual hierarchy of regions of photos and
    // builds the KML file
    public static class LocHier {
        final static int childLimit = 10;
        final static long windowLimit = (long)((1.0/69.0) / 100.0 * 10e7); //~ 52.8 feet.
        final static int windowDivisor = 10;
        final static SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
        final static SimpleDateFormat timeFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        static PicturesMdata picturesMdata = null;
        static String outputFolderName = null;
        static HashSet<String> iconList = null;
        ArrayList<LocHier> childLocHier = null;
        DBSCAN_Clusters.Cluster cluster = null;
        long[] window = new long[2];

        public LocHier(PicturesMdata picturesMdata, String outputFolderName){
            LocHier.picturesMdata = picturesMdata;
            LocHier.outputFolderName = new String(outputFolderName);
            iconList = new HashSet<>();
        }

        public LocHier(){}

        public void buildLocHier (ArrayList<Integer> locIdx) {
            if (picturesMdata == null) {
                System.out.println("Class not initialized correctly");
                return;
            }
            DBSCAN_Clusters clusters = new DBSCAN_Clusters(2);
            cluster = clusters.getNewCluster();
            for (Integer idx : locIdx){
                cluster.add(new long[]{
                        picturesMdata.get(idx).location.getLatitudeE7(),
                        picturesMdata.get(idx).location.getLongitudeE7(),
                },idx);
            }
            // calculate the window for the top node
            long[][] lltBounds = cluster.getBoundsLong();
            window[0] = (lltBounds[0][0]-lltBounds[1][0])/windowDivisor + 1;
            window[1] = (lltBounds[0][1]-lltBounds[1][1])/windowDivisor + 1;
            if(window[0] > window[1])
                window[1] =  window[0];
            else
                window[0] = window[1];
            buildLocHier();
        }

        private void buildLocHier () {

            ArrayList<Integer> locIdx = cluster.clusterIdxs();

            long[] latLonTime = new long[2];
            if (locIdx.size() > childLimit && window[0] > windowLimit) {
                // build a KdTree from the input data
                KdTreeEx<Integer> fKdTree = new KdTreeEx<Integer>((int)locIdx.size(), 2);
                fKdTree.setNumThreads(cores);
                for (Integer idx : locIdx){
                    // feed the kdTree
                    latLonTime[0] = picturesMdata.get(idx).location.getLatitudeE7();
                    latLonTime[1] = picturesMdata.get(idx).location.getLongitudeE7();
                    if (0 > fKdTree.add(latLonTime, idx)) {
                        System.out.println("fKdTree data input error at " + idx);
                    }
                }
                fKdTree.buildTree();

                // create a DBSCAN_Clusters object and override the getPoint fuction to get access to the location data.
                DBSCAN_Clusters clusters = new DBSCAN_Clusters(2);
                // get the search range to about cluster distance window.  If window = null, then
                // get the window from the bounds. Otherwise divide the passed in one by windowDevisor
                // make the window a fraction of the upper level or top level lltbounds
                clusters.buildCluster(fKdTree, window);
                for (DBSCAN_Clusters.Cluster c : clusters.clusters){
                    // create a lower level
                    // create a place to put the nodes for the next hier level down
                    if (childLocHier == null) childLocHier = new ArrayList<LocHier>();
                    LocHier lh = new LocHier();
                    // create a copy of window and create the lower level.
                    lh.window[0] = window[0]/windowDivisor + 1;
                    lh.window[1] = window[1]/windowDivisor + 1;
                    lh.cluster = c;
                    childLocHier.add(lh);
                    lh.buildLocHier();
                }
            } else {
            }
        }

        protected void writeoutKML(Document doc, int depth, int cnt) {

            if (cluster == null) {
                System.out.println("Class not initialized correctly");
                return;
            }
            String folderName = "lev" + depth + "num" + cnt;
            final Folder folder = doc.createAndAddFolder();
            folder.withName(folderName).withOpen(childLocHier == null); // close unless bottom level
            // for the region used for LOD, calculate center of the bounding box of the cluster
            double[][] cbounds = cluster.getBoundsDouble();
            double[][] lcbounds = new double[2][2];
            double longitude = (cbounds[1][1] + cbounds[0][1]) / 2.0d;
            double latitude = (cbounds[1][0] + cbounds[0][0]) / 2.0d;
            // and then calculate a region that is the size of the search window
            lcbounds[0][0] = latitude + window[0] * 10E-7;
            lcbounds[1][0] = latitude - window[0] * 10E-7;
            lcbounds[0][1] = longitude + window[1] * 10E-7;
            lcbounds[1][1] = longitude - window[1] * 10E-7;
            // if this is the top node in the tree make LOD max be infinite
            long lodmin = depth == 0 ? 0 : 50;
            // if this is a leaf node  make LOD min be infinite
            long lodmax = childLocHier == null ? -1 : 500;
            final Region region = createRegion(folder, folderName+"Rgn", lcbounds, lodmax, lodmin);
            folder.withRegion(region);
            // if top level, use the just calculated center region for lookat point
            if (depth == 0) {
                final LookAt lookAt = doc.createAndSetLookAt().withLongitude(longitude).withLatitude(latitude).
                        withAltitude(0).withRange(12000000);
            }
            int lcnt = 0;
            if (childLocHier != null) { // not at a leaf node
                for (LocHier locHier : childLocHier) {
                    String styleURL = null;
                    if (locHier.cluster.size() <= 400)
                        styleURL = "number_" + locHier.cluster.size();
                    else
                        styleURL = "symbol_blank";
                    createStyle(doc, styleURL);
                    cbounds = locHier.cluster.getBoundsDouble();
                    longitude = (cbounds[1][1] + cbounds[0][1]) / 2.0d;
                    latitude = (cbounds[1][0] + cbounds[0][0]) / 2.0d;
                    createPlacemark(doc, folder, longitude, latitude,
                            null, null, null, styleURL);
                    locHier.writeoutKML(doc, depth + 1, lcnt++);
                }
            } else { // leaf node
                int blcnt = 0;
                for (Integer idx : cluster.clusterIdxs()) {
                    String styleURL = "photo";
                    createStyle(doc, styleURL);
                    longitude = picturesMdata.get(idx).location.getLongitude();
                    latitude  = picturesMdata.get(idx).location.getLatitude();
                    String fn = new File(picturesMdata.get(idx).srcFileName).getName();
                    String timeString = dateFormat.format(picturesMdata.get(idx).location.getTimestampMsTs());
                    String picLink = "<center><img src=\"" +
                            picturesMdata.get(idx).dstFileName + "\" width=500<br></center>" +
                            "<center><big>" + fn + "</big></center>";
                    createPlacemark(doc, folder, longitude, latitude,timeString,
                            null, picLink, styleURL);
                }
            }
        }

        private static void createStyle(Document doc, String styleURL){
            if (iconList.contains(styleURL)) return;  // already have that icon
            iconList.add(styleURL); // mark it as used
            Style style = doc.createAndAddStyle().withId(styleURL);
            Icon icon = style.createAndSetIconStyle().withScale(1).createAndSetIcon();
            icon.withHref("icons" + File.separator + styleURL + ".png");
            icon.setViewBoundScale(1.0);
            icon.setRefreshInterval(100000000.0);
        }

        public void copyIcons(String outputFolderNameutputFolderName) throws IOException {
            File destDir = new File(outputFolderName + File.separator + "icons" );
            if (!destDir.exists()) {
                Files.createDirectories(destDir.toPath());
                //System.out.println("mkdir " + destDir.toString());
            }
            for (String styleURL : iconList) {
                Path srcPath = new File("icons" + File.separator + styleURL + ".png").toPath();
                Path destPath = new File(outputFolderName + File.separator + srcPath.toString()).toPath();
                Files.copy(srcPath, destPath, StandardCopyOption.REPLACE_EXISTING);
                //System.out.println(srcPath.toString() + "->" + destPath.toString() );
            }
        }

        private static void createPlacemark(Document document, Folder folder, double longitude, double latitude,
                                            String timeString, Region region, String description, String styleURL) {
            final Placemark placemark = folder.createAndAddPlacemark();
            if (region != null){
                placemark.withRegion(region);
            }
            if (timeString != null) {
                placemark.withName(timeString);
            }
            if (description != null) {
                placemark.withDescription(description);
            }
            if (description != null) {
                placemark.withDescription(description);
            }
            if (styleURL != null) {
                placemark.withStyleUrl("#"+styleURL);
            }
            placemark.withName(timeString);
            placemark.createAndSetPoint().addToCoordinates(longitude, latitude); // set coordinates
        }


        public static Region createRegion(Folder folder, String regionName, double[][] bounds, double max, double min) {
            final Region region = new Region();
            region.setId(regionName);

            final LatLonAltBox latlonBox = new LatLonAltBox();
            region.setLatLonAltBox(latlonBox);
            latlonBox.setNorth(bounds[0][0]);
            latlonBox.setSouth(bounds[1][0]);
            latlonBox.setEast(bounds[0][1]);
            latlonBox.setWest(bounds[1][1]);
            latlonBox.setMinAltitude(0.0);
            latlonBox.setMaxAltitude(0.0);
            latlonBox.setAltitudeMode(AltitudeMode.CLAMP_TO_GROUND);

            final Lod lod = new Lod();
            region.setLod(lod);
            lod.setMinLodPixels(min);
            lod.setMaxLodPixels(max);
            //lod.setMinFadeExtent(0.0);
            //lod.setMaxFadeExtent(0.0);
            return region;
        }

    }

    public static void main(String[] args) {
        // create instance of inputForms class that handles the gui.
        inputForm = new InputForm("Earth Pics Viewer"){
            @Override
            public void start(String[] inputs){
                buildKMLfile(inputs);
            }
        };
        if (args != null && args.length > 0) {
            //if (false) {
            // if there are cmd line args, use those and call buildKML file directly
            String[] inputs = new String[6];
            for(int i = 0; i < 6; i++){
                inputs[i] = i < args.length ? args[i] : "";
            }
            buildKMLfile(inputs);
            System.exit(0);
        } else {
            // if there are no cmd line args, go to the GUI
            inputForm.getUserInput();
        }
    }

    private static void buildKMLfile(String[] inputs) {

        if (inputs[0] == null || inputs[1].equals("")) {
            inputForm.messageAppendLn("Input directory must be specified");
            System.out.println("Input directory must be specified");
            return;
        }
        String fullInputFolderName = inputs[0];
        File inputDir = new File(fullInputFolderName);
        if (!inputDir.isDirectory()) {
            inputForm.messageAppendLn(fullInputFolderName + " is not a directory");
            System.out.println(fullInputFolderName + " is not a directory");
            return;
        }
        if (inputs[1] == null || inputs[1].equals("")) {
            inputForm.messageAppendLn("Input directory must be specified");
            return;
        }
        String fullOutputFolderName = inputs[1];

        ArrayList<String> fnl = listFilesForFolder(inputDir, 0);

        if (inputs[1] == null || inputs[1].equals("")) {
            inputForm.messageAppendLn("Output directory must be specified");
            System.out.println("Output directory must be specified");
            return;
        }
        File outputDir = new File(fullOutputFolderName);
        if (!outputDir.isDirectory()) {
            inputForm.messageAppendLn(fullOutputFolderName + " is not a directory");
            System.out.println(fullOutputFolderName + " is not a directory");
            return;
        }

        final Kml kml = new Kml();
        Document doc = kml.createAndSetDocument().withName("Picture Locations").withOpen(true);
        int cores = Runtime.getRuntime().availableProcessors();

        // create the list of picture metadata records
        PicturesMdata picturesMdata = new PicturesMdata();
        // start be assuming files will not be copied to set the output folder to input folder
        String outputFolderName = new String(fullInputFolderName);
        // if the files are to be copied, create the destination folder name and folder
        if (inputs[5].equals("Yes")) {
            outputFolderName = fullOutputFolderName + File.separator + "jpegs" + File.separator;
            File destDir = new File(outputFolderName);
            if (!destDir.exists()) {
                try {
                    Files.createDirectories(destDir.toPath());
                } catch (IOException e) {
                    e.printStackTrace();
                    inputForm.messageAppendLn("Failed to create output jpeg directory.");
                    return;
                }
            }
        }

        // Handle user input of the date range
        SimpleDateFormat dateF = null;
        Timestamp afterTime = new Timestamp(0);
        Timestamp beforeTime = new Timestamp(System.currentTimeMillis());
        //Timestamp beforeTime = new Timestamp(Long.MAX_VALUE);
        try {
            if (inputs[2] != null && inputs[2].length() > 0) {
                if (inputs[2].split( " ").length == 1) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy");
                } else if(inputs[2].split( ":").length == 1) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH");
                } else if(inputs[2].split( ":").length == 2) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH:mm");
                } else if(inputs[2].split( ":").length == 3) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                }
                afterTime = new Timestamp(dateF.parse(inputs[2]).getTime());
            }
            if (inputs[3] != null && inputs[3].length() > 0) {
                if (inputs[3].split(" ").length == 1) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy");
                } else if (inputs[3].split(":").length == 1) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH");
                } else if (inputs[3].split(":").length == 2) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH:mm");
                } else if (inputs[3].split(":").length == 3) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                }
                beforeTime = new Timestamp(dateF.parse(inputs[3]).getTime());
            }
        } catch (ParseException e) {
            System.out.println("Earliest or Latest Time not understood");
            inputForm.messageAppendLn("Earliest or Latest Time not understood");
            return;
        }

        // now create a pictureMdata record for each file
        inputForm.messageAppendLn("Reading metadata from jpeg files");
        for (String fn : fnl) {
            try {
                if (inputs[5].equals("Yes")) {
                    File tf = new File(fn);
                    String nfn = "jpegs" + File.separator + tf.getName();
                    picturesMdata.add(fn, nfn);
                } else {
                    picturesMdata.add(fn, fn);
                }
            }
            catch(ParseException | IOException | ImageReadException e) {
                inputForm.messageAppendLn("Read Error on file " + fn);
                inputForm.messageAppendLn(e.getMessage());
                System.out.println("Read Error on file " + fn);
                System.out.println(e.getMessage());
                if (!(e instanceof ImageReadException)) return;
                else e.printStackTrace();
            }
        }

        // create a KdTree and filter the data to the region.
        inputForm.messageAppendLn("Filtering jpeg files");
        KdTree<Integer> kdTree = new KdTree<Integer>((int) picturesMdata.size(), 3);
        long[] latLonTime = new long[3];
        kdTree.setNumThreads(cores);
        for (int i = 0; i < picturesMdata.size(); i++) {
            latLonTime[0] = picturesMdata.get(i).location.getLatitudeE7();
            latLonTime[1] = picturesMdata.get(i).location.getLongitudeE7();
            latLonTime[2] = picturesMdata.get(i).location.getTimeStampLong();
            if (0 > kdTree.add(latLonTime, i)) {
                System.out.println("KdTree data input error at " + i);
            }
        }
        kdTree.buildTree();

        // this will be used for region filtering in future enhancements
        FilterParameter fp = new FilterParameter();
        fp.addIncludeRegion(Long.MAX_VALUE, Long.MAX_VALUE, afterTime.getTime(),
                Long.MIN_VALUE, Long.MIN_VALUE, beforeTime.getTime() );


        ArrayList<Integer> fclusterIdxs = new ArrayList<>();
        {  // temporary data structures used for the input filter
            // collect include region points in a has set to eliminate point
            // in overlapping regions
            HashSet<Integer> hashIdx = new HashSet<>();
            // get the list of indices for all of the filtered locations
            for (int i = 0; i < fp.numIncludeRegions(); i++) {
                long[][] includeFilter = fp.getIncludeRegion(i);
                hashIdx.addAll(kdTree.searchTree(includeFilter[0], includeFilter[1]));
            }
            ArrayList<Integer> exIdxs = new ArrayList<>();
            for (int i = 0; i < fp.numExcludeRegions(); i++) {
                long[][] excludeFilter = fp.getExcludeRegion(i);
                exIdxs.addAll(kdTree.searchTree(excludeFilter[0], excludeFilter[1]));
                hashIdx.removeAll(exIdxs);
            }
            fclusterIdxs.addAll(hashIdx);
        }
        if (fclusterIdxs.size() == 0) {
            inputForm.messageAppendLn("No locations left to processes.");
            System.out.println("No locations left to processes.");
            return;
        }
        kdTree = null;  // don't need this tree anymore so let it be garbage collected

        // create the location hierarchy
        inputForm.messageAppendLn("Creating KML file");
        LocHier locHier = new LocHier(picturesMdata, fullOutputFolderName);
        locHier.buildLocHier(fclusterIdxs);
        // create the KML file from the location hierarchy
        locHier.writeoutKML(doc, 0,0);

        //marshals to console
        //kml.marshal();
        //marshals into file
        boolean marshal = false;
        try {
            marshal = kml.marshal(new File(fullOutputFolderName,"EarthPicsView.kml"));
        } catch (FileNotFoundException e) {
            //e.getMessage();
        }
        if (!marshal) {
            inputForm.messageAppendLn("KML file save failed.");
            return;
        }

        inputForm.messageAppendLn("Copying icon files");
        try {
            locHier.copyIcons(fullOutputFolderName);
        } catch (IOException e) {
            //e.printStackTrace();
            inputForm.messageAppendLn("Icon Copy Error : " + e.getMessage());
            inputForm.messageAppendLn("Check for missing Icons directory");
            return;
        }

        // copy the jpeg files if required
        if (inputs[5].equals("Yes")) {
            inputForm.messageAppendLn("Copying jpeg files");
            for (int i = 0;  i < picturesMdata.size(); i++){
                Path destPath = new File(fullOutputFolderName + File.separator +
                        picturesMdata.get(i).dstFileName).toPath();
                Path srcPath = new File(picturesMdata.get(i).srcFileName).toPath();
                try {
                    Files.copy(srcPath, destPath, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    //e.printStackTrace();
                    inputForm.messageAppendLn("Failed to copy " + srcPath.toString() + "->" + destPath.toString() );
                    return;
                }
                //System.out.println(srcPath.toString() + "->" + destPath.toString() );
            }
        }
        inputForm.messageAppendLn(fclusterIdxs.size() + " images processed.");
        System.out.println(fclusterIdxs.size() + " images processed.");
    }

}
