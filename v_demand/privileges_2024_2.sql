select
    p.person_id,
    to_char(p.personprivilege_begdate, 'YYYY-MM-DD') as "Дата включения в регистр ЛЛО",
    to_char(p.personprivilege_begdate, 'YYYY-MM') as "Месяц года",
    l.org_nick "МО"
    from v_personprivilege p
    left join v_lpu l using (lpu_id)
where 1=1
