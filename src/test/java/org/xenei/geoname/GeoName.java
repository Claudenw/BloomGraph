/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.geoname;

/*
 The main 'geoname' table has the following fields :

 geonameid         : integer id of record in geonames database
 name              : name of geographical point (utf8) varchar(200)
 asciiname         : name of geographical point in plain ascii characters, varchar(200)
 alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
 latitude          : latitude in decimal degrees (wgs84)
 longitude         : longitude in decimal degrees (wgs84)
 feature class     : see http://www.geonames.org/export/codes.html, char(1)
 feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
 country code      : ISO-3166 2-letter country code, 2 characters
 cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
 admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
 admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
 admin3 code       : code for third level administrative division, varchar(20)
 admin4 code       : code for fourth level administrative division, varchar(20)
 population        : bigint (8 byte int) 
 elevation         : in meters, integer
 dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
 timezone          : the timezone id (see file timeZone.txt) varchar(40)
 modification date : date of last modification in yyyy-MM-dd format
 */
public class GeoName {
	public String geonameid;
	public String name;
	public String asciiname;
	public String alternatenames;
	public String latitude;
	public String longitude;
	public String feature_class;
	public String feature_code;
	public String country_code;
	public String cc2;
	public String admin1_code;
	public String admin2_code;
	public String admin3_code;
	public String admin4_code;
	public String population;
	public String elevation;
	public String dem;
	public String timezone;
	public String modification_date;

	public static GeoName parse(final String txt) {
		final String[] parts = txt.split("\t");
		final GeoName retval = new GeoName();
		retval.geonameid = parts[0];
		retval.name = parts[1];
		retval.asciiname = parts[2];
		retval.alternatenames = parts[3];
		retval.latitude = parts[4];
		retval.longitude = parts[5];
		retval.feature_class = parts[6];
		retval.feature_code = parts[7];
		retval.country_code = parts[8];
		retval.cc2 = parts[9];
		retval.admin1_code = parts[10];
		retval.admin2_code = parts[11];
		retval.admin3_code = parts[12];
		retval.admin4_code = parts[13];
		retval.population = parts[14];
		retval.elevation = parts[15];
		retval.dem = parts[16];
		retval.timezone = parts[17];
		retval.modification_date = parts[18];
		return retval;
	}

}
