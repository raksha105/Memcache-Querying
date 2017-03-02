# Memcache-Querying
Queries are performed on large dataset(90k-100k tuples using memcache on AWS. Users can upload large datasets in .csv, and perform query operations on them using memcache. 

upload_file() handles uploading of the file to AWS

All queries are handled in query_normal(), min_max_query().Queries are handled in memcache in the function memecache().

This project was used to test the earthquake.csv file with 95k tuples. You can modify it to query your dataset.
