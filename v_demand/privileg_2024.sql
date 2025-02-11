select
    l.org_nick "МО",
    date_part('month',  p.personprivilege_begdate) as "Месяц 2024 года",
    count(distinct (p.person_id)) as "Количество пациентов"
    from v_personprivilege p
    join v_lpu l using (lpu_id)

where
    p.personprivilege_begdate between '2024-01-01' and '2024-12-31'
and (p.personprivilege_enddate isnull or p.personprivilege_enddate > '2024-12-31')
and p.person_id in {ids}
group by 1, 2;

