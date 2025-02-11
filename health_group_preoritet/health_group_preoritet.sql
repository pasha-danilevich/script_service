select COUNT(*) from promeddispdn p;



select
         p.person_id,
         p."МО ДН",
         p."ФИО",
         p."ДР",
         p."Диагноз код",
         p."Диагноз группа"
    from promeddispdn p
    where p."Дата смерти" isnull and p."Дата снятия с ДН" isnull
    and (p."Диагноз код" in {codes} or p."Диагноз группа" in {groups})

