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

//import com.google.gson.Gson;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
/**
 * <p>
 * Locations is a generic class for storing a list of timestamped locations.  It happens top be field
 * compatible with the Location "History.json" file that Google delivers when asked to send all of the
 * data they have stored on a person and when used with GSON will read that file
 * ie.
 *         final Locations locations = Locations.getLocations(
 *             new File("/Users/john/Desktop/gis", "Location History.json"));
 *         if (locations == null) System.exit(1);
 * </p>
 *
 * @author John A Robinson
 */
public class Locations {
    /* A Location object stores the individual timestamped locations.  It also has a number of accessor
     * functions.
     */
    public class Location {

        protected class Activity {
            protected String type;
            protected short confidence;
            public Activity () {
                type = null;
                confidence = 0;
            }

            public String getType() {
                return type;
            }
            public void setType(String type) {
                this.type = type;
            }

            public short getConfidence() {
                return confidence;
            }
            public void setConfidence(short confidence) {
                this.confidence = confidence;
            }
        }

        protected class Activities {
            protected String timestampMs;
            protected ArrayList<Activity> activity;
            public Activities () {
                timestampMs = "";
                activity = new ArrayList<Activity>();
            }
            //String
            public String getTimestampMs() {
                return timestampMs;
            }
            public void setTimestampMs(String timestampMs) {
                this.timestampMs = timestampMs;
            }
            // Timestamp access
            public Timestamp getTimestampMsLong() {
                Timestamp ts = new Timestamp(Long.getLong(timestampMs));
                return ts;
            }
            public void setTimestampMsLong(Timestamp ts) {
                Long tsl = ts.getTime();
                timestampMs = tsl.toString();
                return;
            }
            public ArrayList<Activity> getActivity() {
                return activity;
            }


        }

        private String timestampMs;
        private long latitudeE7;
        private long longitudeE7;
        private short accuracy;
        private short velocity;
        private short altitude;
        private short verticalAccuracy;
        private ArrayList<Activities> activity;


        public Location() {
            // these are the default values of the fields if data does not exist in the file
            timestampMs = null;
            latitudeE7 = 0;
            longitudeE7 = 0;
            accuracy = 0;
            velocity = 0;
            altitude = 0;
            verticalAccuracy = 0;
            activity = new ArrayList<Activities>();
        }

        //String
        public String getTimestampMs() {
            return timestampMs;
        }
        public void setTimestampMs(String timestampMs) {
            this.timestampMs = timestampMs;
        }
        // Timestamp access
        public Timestamp getTimestampMsTs() {
            Timestamp ts = new Timestamp(Long.parseLong(timestampMs));
            return ts;
        }
        public void setTimestampMs(Timestamp ts) {
            Long tsl = ts.getTime();
            timestampMs = tsl.toString();
            return;
        }
        public long[] getTimeStampPtr() { return new long[]{Long.parseLong(timestampMs)}; };
        public long getTimeStampLong() { return Long.parseLong(timestampMs); };

        // set and get latitude in E7 long and double format
        public long getLatitudeE7() {
            return latitudeE7;
        }
        public void setLatitudeE7(long latitudeE7) {
            this.latitudeE7 = latitudeE7;
        }
        public double getLatitude() {
            return (double)latitudeE7 * 1.0E-7;
        }
        public void setLatitude(double latitude) {
            this.latitudeE7 = (long)(latitude * 1.0E7);
        }

        // set and get longitude in E7 long and double format
        public long getLongitudeE7() {
            return longitudeE7;
        }
        public void setLongitudeE7(long longitudeE7) {
            this.longitudeE7 = longitudeE7;
        }
        public double getLongitude() {
            return (double)longitudeE7 * 1.0E-7;
        }
        public void setLongitude(double longitude) {
            this.longitudeE7 = (long)(longitude * 1.0E7);
        }

        // return latitude and longitude in a 2d array.
        public long[] getCoordinatesE7() { return new long[]{latitudeE7, longitudeE7}; }
        public double[] getCoordinates() { return new double[]{latitudeE7*1.0E-7, longitudeE7*1.0E-7}; }

        public short getAccuracy() {
            return accuracy;
        }
        public void setAccuracy(short accuracy) {
            this.accuracy = accuracy;
        }

        public short getVelocity() {
            return velocity;
        }
        public void setVelocity(short velocity) {
            this.velocity = velocity;
        }

        public short getAltitude() {
            return altitude;
        }
        public void setAltitude(short altitude) {
            this.altitude = altitude;
        }

        public short getVerticalAccuracy() {
            return verticalAccuracy;
        }
        public void setVerticalAccuracy(short verticalAccuracy) {
            this.verticalAccuracy = verticalAccuracy;
        }

        public ArrayList<Activities> getActivity() {
            return activity;
        }

    }

    // locations holds the array of individual location objects

    private ArrayList<Location> locations;

    /* main constructor */
    public Locations() {
        locations = new ArrayList<Location>(){};
    }

    /* Accessor functions */
    public void setLocations(ArrayList<Location> locations) {
        this.locations = locations;
    }

    public Location get(int i) {
        return locations.get(i);
    }
    public Location add() {
        Location tmp = new Location();
        locations.add(tmp);
        return tmp;
    }
    public long size() {
        return locations.size();
    }

    /* boundary finding functions */
    /*
    void findBounds(lh2kml.latlon ur, lh2kml.latlon ll) {
        ur.set(-Double.MAX_VALUE, -Double.MAX_VALUE);
        ll.set( Double.MAX_VALUE,  Double.MAX_VALUE);
        for (int i = 0; i < locations.size(); i++) {
            lh2kml.latlon t = new lh2kml.latlon(locations.get(i));
            ur.maxLatLon(t);
            ll.minLatLon(t);
        }
        // add an epsilon to max to is't greater than the largest point
        ur.accum(new lh2kml.latlon(1.0e-8, 1.0e-8));
    }

    void findBounds(lh2kml.latlon ur, lh2kml.latlon ll, ArrayList<Integer> idxList) {
        ur.set(-Double.MAX_VALUE, -Double.MAX_VALUE);
        ll.set( Double.MAX_VALUE,  Double.MAX_VALUE);
        for (int i = 0; i < idxList.size(); i++) {
            lh2kml.latlon t = new lh2kml.latlon(locations.get(idxList.get(i)));
            ur.maxLatLon(t);
            ll.minLatLon(t);
        }
        // add an epsilon to max to is't greater than the largest point
        ur.accum(new lh2kml.latlon(1.0e-8, 1.0e-8));
    }

    static Locations getLocations(File file) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            Gson gson = new Gson();
            //convert the json string object
            final Locations locations = gson.fromJson(br, Locations.class);

            return locations;
        } catch (FileNotFoundException e) {
            System.out.println("Json file " + file.toString() + " not found");
            return null;
        } catch (IOException e) {
            System.out.println("Json file " + file.toString() + " read error");
            return null;
        }
    }
*/


}
