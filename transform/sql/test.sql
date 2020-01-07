create table if not exists test(foo int);

truncate table test;

insert into test (foo) values (1);

select * from test;