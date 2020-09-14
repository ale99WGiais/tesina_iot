create keyspace metaserver with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};
use metaserver;

create table object(
    uid uuid primary key,
    path text,
    created timestamp,
    owner text,
    size bigint,
    min_copies int
);

create table stored_object(
    id uuid primary key,
    uid uuid,
    server text,
    created timestamp,
    complete boolean
);

create table operation_log(
    id uuid primary key,
    uid uuid,
    type text,
    source_addr text,
    dest_addr text,
    created timestamp,
    completed timestamp
);

create custom index object_path_idx on object(path)
using 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 'case_sensitive': 'true'};
