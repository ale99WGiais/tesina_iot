

use metaserver;

truncate table dataserver;
truncate table object;
truncate table operation_log;
truncate table pending_object;
truncate table performance_log;
truncate table stored_object;
truncate table object_lock;



insert into dataserver(server, online) values ('localhost:10010', false);
insert into dataserver(server, online) values ('localhost:10011', false);
insert into dataserver(server, online) values ('localhost:10012', false);

