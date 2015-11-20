--
-- Append pull-up across Join
--

--
-- Build a table for testing
--
-- CREATE Partition Table (Modulation is used for dividing)
create temp table check_test_div (
id integer,
data_x float8,
data_y float8
);

create temp table check_test_div_0 (
check(id % 3 = 0)
) inherits(check_test_div);

create temp table check_test_div_1 (
check(id % 3 = 1)
) inherits(check_test_div);

create temp table check_test_div_2 (
check(id % 3 = 2)
) inherits(check_test_div);

-- CREATE table for inner relation
create temp table inner_t as
select generate_series(0,3000)::integer as id, ceil(random()*10000)::integer as num;

begin;

insert INTO check_test_div_0
select (ceil(random()*1000)*3)::integer as id, random(), random() as data
from generate_series(0,5000);

insert INTO check_test_div_1
select (ceil(random()*1000)*3+1)::integer as id, random(), random() as data
from generate_series(0,5000);

insert INTO check_test_div_2
select (ceil(random()*1000)*3+2)::integer as id, random(), random() as data
from generate_series(0,5000);

commit;

-- CREATE table for verifying
create temp table test_appended (
data_x float8,
data_y float8,
num integer
);

begin;
insert into test_appended (data_x, data_y, num)
select data_x, data_y, num from only check_test_div join inner_t on check_test_div.id = inner_t.id;

insert into test_appended (data_x, data_y, num)
select data_x, data_y, num from check_test_div_0 join inner_t on check_test_div_0.id = inner_t.id;

insert into test_appended (data_x, data_y, num)
select data_x, data_y, num from check_test_div_1 join inner_t on check_test_div_1.id = inner_t.id;

insert into test_appended (data_x, data_y, num)
select data_x, data_y, num from check_test_div_2 join inner_t on check_test_div_2.id = inner_t.id;
commit;

set enable_hashjoin to on;
set enable_mergejoin to off;
set enable_nestloop to off;

--
-- Check plan
--
explain (costs off)
select data_x, data_y, num from check_test_div join inner_t on check_test_div.id = inner_t.id;

--
-- Verify its results
--
select data_x, data_y, num from check_test_div join inner_t on check_test_div.id = inner_t.id
except (select * from test_appended);

select * from test_appended
except (
select data_x, data_y, num from check_test_div join inner_t on check_test_div.id = inner_t.id
);

drop table check_test_div cascade;
drop table test_appended;

--
-- Build a table for testing
--
-- CREATE Partition Table (Simple; Greater-than/Less-than marks are used for dividing)
create temp table check_test_div (
id integer,
data_x float8,
data_y float8
);

create temp table check_test_div_0 (
check(id < 1000)
) inherits(check_test_div);

create temp table check_test_div_1 (
check(id between 1000 and 1999)
) inherits(check_test_div);

create temp table check_test_div_2 (
check(id > 1999)
) inherits(check_test_div);

-- Table for inner relation is already created.

begin;

insert INTO check_test_div_0
select (ceil(random()*999))::integer as id, random(), random() as data
from generate_series(0,5000);

insert INTO check_test_div_1
select (ceil(random()*999)+1000)::integer as id, random(), random() as data
from generate_series(0,5000);

insert INTO check_test_div_2
select (ceil(random()*999)+2000)::integer as id, random(), random() as data
from generate_series(0,5000);

commit;

-- CREATE table for verifying
create temp table test_appended (
data_x float8,
data_y float8,
num integer
);

begin;
insert into test_appended (data_x, data_y, num)
select data_x, data_y, num from only check_test_div join inner_t on check_test_div.id = inner_t.id;

insert into test_appended (data_x, data_y, num)
select data_x, data_y, num from check_test_div_0 join inner_t on check_test_div_0.id = inner_t.id;

insert into test_appended (data_x, data_y, num)
select data_x, data_y, num from check_test_div_1 join inner_t on check_test_div_1.id = inner_t.id;

insert into test_appended (data_x, data_y, num)
select data_x, data_y, num from check_test_div_2 join inner_t on check_test_div_2.id = inner_t.id;
commit;

set enable_hashjoin to on;
set enable_mergejoin to off;
set enable_nestloop to off;

--
-- Check plan
--
explain (costs off)
select data_x, data_y, num from check_test_div join inner_t on check_test_div.id = inner_t.id;

--
-- Verify its results
--
select data_x, data_y, num from check_test_div join inner_t on check_test_div.id = inner_t.id
except (select * from test_appended);

select * from test_appended
except (
select data_x, data_y, num from check_test_div join inner_t on check_test_div.id = inner_t.id
);
