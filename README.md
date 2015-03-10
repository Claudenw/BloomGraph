# BloomGraph
A Jena graph implementations that uses bloom filters to locate triples.

# Notes

The test code requires that allCountries.txt from http://download.geonames.org/export/dump/allCountries.zip (277MB zip)

There is only a MySQL based implementation of the DB storage layer. It requires the Bloom filter UDF found at https://github.com/Claudenw/mysql_bloom

There are 2 implementations of the bloom filter graph included here; a memory based one and the MySQL based DB implementation.


