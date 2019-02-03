create domain guard_action as text
check (
    VALUE in ('starts sleeping', 'wakes up', 'starts shift')
);

create table log_shift (
    log_date timestamp primary key,
    action guard_action,
    guard text
);

-- adapt the path to your directory layout
with input as (
    select pg_read_file('/home/yolo/projects/adventofcode-sql/src/input1') as lines
),
raw_log_entry as (
    select unnest(q.lines) as record
    from (
        select regexp_split_to_array(lines, '\n') as lines
        from input
    ) q
),
log_entry as (
    select
        substring(raw_log_entry.record from '\[\d+-\d+-\d+ \d+:\d+\]')::timestamp as date
        ,substring(raw_log_entry.record from '(?: Guard #)(\d+)') as start_guard
        ,substring(raw_log_entry.record from 'falls asleep') as start_sleeping
        ,substring(raw_log_entry.record from 'wakes up') as wakes_up
    from raw_log_entry
    where (
        length(raw_log_entry.record) > 0
    )
)
insert into log_shift
select
    date,
    case
        when (start_sleeping is not null) then 'starts sleeping'
        when (wakes_up is not null) then 'wakes up'
        when (start_guard is not null) then 'starts shift'
    end as action,
    case
        when (start_guard is not null) then start_guard
        else null
    end as guard
from log_entry;

-- at this point we can qualify each log entry with an action. However there is
-- no way to link this action to the context of a guard shift.
select * from log_shift;

begin;
    -- inside this transaction we build a pseudo table:
    -- shift (
    --      start,
    --      end,
    --      guard
    --   )
    -- we then use this result set to update the log_shift table and add a
    -- static constraint on its guard column.
with arr_intervals as (
    select
        guard,
        log_date,
        array_agg(log_date) over (
            order by log_date
            rows between current row and 1 following
        ) as end_start
    from log_shift
    where guard is not null
    order by log_date asc
),
shift as (
    select
        guard,
        end_start[1]::timestamp as start,
        case when (array_length(end_start, 1) = 1) then now()::timestamp
        else end_start[2]::timestamp
        end as end
    from arr_intervals
),
shift_with_guard  as (
    select
        shift.guard
        ,l_s.log_date
    from shift
    join log_shift l_s on (
        l_s.log_date between shift.start and shift.end
        and l_s.action <> 'starts shift'
    )
)
update log_shift
set
    guard = s.guard
from (
    select * from shift_with_guard
) as s
where s.log_date = log_shift.log_date
;
alter table log_shift
alter column guard set not null
;
select * from log_shift order by log_date asc;
;
commit;

-- Returns the guard which slept the most time
with shifts as (
    select
        guard,
        log_date,
        action,
        array_agg(log_date) over (
            order by log_date
            rows between current row and 1 following
        ) as end_start
    from log_shift
    where action <> 'starts shift'
    order by log_date asc
)
select
    sum(end_start[2]::timestamp - end_start[1]::timestamp) as sleep_time,
    guard
from shifts where (action = 'starts sleeping')
group by guard
order by sleep_time desc
limit 1
;

-- Returns the for each minute the number of times the guard was asleep. Filter
-- by the guard id to get the most slept minute for a given guard.
with shifts as (
    select
        guard,
        log_date,
        action,
        array_agg(log_date) over (
            order by log_date
            rows between current row and 1 following
        ) as end_start
    from log_shift
    where action <> 'starts shift'
    order by log_date asc
), instant_slept as (
    select
        generate_series(end_start[1]::timestamp, end_start[2]::timestamp - interval '1 minute', '1 minute') as m
        ,guard
    from shifts
    where action = 'starts sleeping'
), minute_slept as (
    select
    extract(minute from instant_slept.m) as minute
    ,guard
    from instant_slept
)
select
    guard,
    minute
    ,count(*)
from minute_slept
group by guard, minute
order by count desc
;
