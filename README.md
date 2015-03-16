# BloomGraph
A Jena graph implementations that uses [bloom filters|http://en.wikipedia.org/wiki/Bloom_filter] to locate triples.

# Notes

The test code requires that allCountries.txt from http://download.geonames.org/export/dump/allCountries.zip (277MB zip)

There is only a MySQL based implementation of the DB storage layer. It requires the Bloom filter UDF found at https://github.com/Claudenw/mysql_bloom

There are 2 implementations of the bloom filter graph included here; a memory based one and the MySQL based DB implementation.

# Design Goals

This is an implementation of [Jena Graph|http://jena.apache.org/documentation/javadoc/jena/com/hp/hpl/jena/graph/Graph.html] is intended to explore the possibility of using Bloom Filters to search a triple store.

Triples are stored in "pages" of 10K entries.  The page has a bloom filter that can store 10000 triples and has a 1 in 100K collision rate.

Each triple has a filter that comprises the three nodes.  The triple bloom filter can store 3 nodes and has a 1 in 100K collision rate. 

The overall strategy is for the storage level to use bloom filters to reduce the number of candidates to a small enough number to be filtered by direct comparison.

When searching for a triple in the in-memory implementation the page size bloom filter for the triple is calculated and each page triple is checked.  Pages with matching filters are scanned for matching triples.  Matching triples are then checked against the requests subject, predicate and objects.

When searching for a triple in the DB backed implementation the pages are scanned as they are in the in-memory version.  In the case of an exact match (when the subject, predicate and object are specified and there are no wild cards) the page is searched by the hash code of the triple, and matching triples are checked as above.  If it is not an exact search the triples bloom filters are scanned for matches and the results are then checked as above.

The database implementation utilizes 2 indexes on the page index table:

* A primary key that comprises an auto increment field in the table.  This key is only used to identify the page table.

* An index on the page bloom filter [hamming value|http://en.wikipedia.org/wiki/Hamming_weight] and an estimated base 2 log of the value of the bloom filter interpreted as a large unsigned integer value.

The database implementation utilizes 3 indexes on the page tables: 

* A primary key that comprises an auto increment field in the table.  This key is only used during delete operations when the triple has been located by other means.

* An index on the hash value of the triple for locating exact matches.

* An index on the bloom filter hamming value and an estimated base 2 log of the value of the bloom filter interpreted as a large unsigned integer value.

The hamming value index works on the following premise:

* A bloom filter can not match any bloom filter that has a lower hamming value.

* A bloom filter when interpreted as an unsigned integer can not match any bloom filter that has a lower value.  Therefore a bloom filter can not match any bloom filter for which the the log of the unsigned integer representation of the first bloom filter is less than the log of the second bloom filter.

Using these principles, the index search looks for bloom filters that have hamming values >= to the hamming value of the search target and that have estimated base 2 log values >=  the base 2 log value of the search target and for which the bloom filters "match".  In some cases the database determines that the index search is not efficient and the page table is scanned.  The worse case is no worse than not having the index.







