
drop keyspace metaserver;
create keyspace metaserver with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1};
use metaserver;

create table dataserver(
	server text,
	online boolean,
	capacity bigint,
	remaining_capacity bigint,
	available_down float,
	available_up float,
	primary key(server)
);


insert into dataserver(server, online) values ('localhost:10010', false);
insert into dataserver(server, online) values ('localhost:10011', false);
insert into dataserver(server, online) values ('localhost:10012', false);

create table object(
    uid uuid,
    path text,
    created timestamp,
    owner text,
    size bigint,
    priority int,
    checksum text,
    deleted timestamp,
    primary key(uid, created)
);

create table object_lock(
    path text,
    user text,
    primary key(path)
);

create table performance_log(
    server text,
    time timestamp,
    online boolean,
	capacity bigint,
	remaining_capacity bigint,
	available_down float,
	available_up float,
	primary key(server, time)
);

create table pending_object(
    uid uuid,
    enabled boolean,
    primary key(uid)
);


create table stored_object(
    uid uuid,
    server text,
    created timestamp,
    complete boolean,
    primary key(uid, server)
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
using 'org.apache.cassandra.index.sasi.SASIIndex';

create materialized view pathToObject as select * from object where path is not null and created is not null
primary key (path, created, uid)
with clustering order by (created desc);


