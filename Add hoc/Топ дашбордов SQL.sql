select
  jurnal.id, jurnal.dashboard_title,
  count(jurnal.username) as cnt,
  sum(case when position('@nitec.kz' in jurnal.username) > 0 and 
      jurnal.username not in ('admin', 'Marzhan.Imanbazarova@nitec.kz', 'aidarbek436@gmail.com') then 1 else 0 
      end) as cnt_nit,
  sum(case when jurnal.username = 'admin' then 1 else 0 end) as cnt_admin,
  sum(case when jurnal.username = 'Marzhan.Imanbazarova@nitec.kz' then 1 else 0 end) as cnt_Marzhan,
  sum(case when jurnal.username = 'aidarbek436@gmail.com' then 1 else 0 end) as cnt_Aidarbek
from
  (select * from
    (SELECT TO_TIMESTAMP(cast(a.dttm as TEXT), 'YYYY-MM-DD HH24') as logtime,
               b.id,
               b.dashboard_title,
               c.username
        FROM public.logs as a
        LEFT JOIN public.dashboards as b on a.dashboard_id =b.id
        LEFT JOIN public.ab_user as c on a.user_id = c.id
        WHERE action = 'log'
          and dashboard_id is not null
          and dashboard_title is not null
          and dashboard_title <> '.'
          and date_part('YEAR', TO_TIMESTAMP(cast(a.dttm as TEXT), 'YYYY-MM-DD HH24')) = 2023) as log
   group by log.logtime,
            log.id,
            log.dashboard_title,
            log.username) as jurnal
group by jurnal.id, jurnal.dashboard_title
order by cnt desc
