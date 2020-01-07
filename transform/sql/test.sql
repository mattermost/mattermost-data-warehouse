create table if not exists test(int foo);

truncate table test;

insert into test (foo) values (1);

select * from test;