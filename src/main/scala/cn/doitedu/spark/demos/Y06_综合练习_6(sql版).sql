/*
-- 关键在正确分组
1,   2020-02-18 14:20:30,   2020-02-18 14:46:30,20    ,0   ,0
1,   2020-02-18 14:47:20,   2020-02-18 15:20:30,30    ,0   ,0
1,   2020-02-18 15:37:23,   2020-02-18 16:05:26,40    ,1   ,1
1,   2020-02-18 16:06:27,   2020-02-18 17:20:49,50    ,0   ,1
1,   2020-02-18 17:21:50,   2020-02-18 18:03:27,60    ,0   ,1


2,   2020-02-18 14:18:24,   2020-02-18 15:01:40,20

2,   2020-02-18 15:20:49,   2020-02-18 15:30:24,30

2,   2020-02-18 16:01:23,   2020-02-18 16:40:32,40
2,   2020-02-18 16:44:56,   2020-02-18 17:40:52,50

3,   2020-02-18 14:39:58,   2020-02-18 15:35:53,20
3,   2020-02-18 15:36:39,   2020-02-18 15:24:54,30
*/


WITH tmp as (
    SELECT
        uid,
        start_time,
        end_time,
        lag(end_time,1,null) over(partition by uid order by start_time) as pre_end_time
    FROM t
)

SELECT
    uid,
    min(start_time) as start_time,
    max(end_time) as end_time,
    sum(num) as amount
FROM
    (
        SELECT
            uid,
            start_time,
            end_time,
            num,
            sum(flag) over(partition by uid order by start_time rows between unbounded preceding and current row) as groupid
        FROM
            (
                SELECT
                    uid,
                    start_time,
                    end_time,
                    num,
                    if(unix_timestamp(start_time) - nvl(unix_timestamp(pre_end_time),unix_timestamp(start_time))< 10*60,0,1) as flag
                FROM tmp
            ) o1
    )o2
GROUP BY uid,groupid