-- select count(*) from v_evnpldispprof where evnpldispprof_setdt between '2025-01-01' and current_date;



select d.evnpldispprof_id,
              d.person_id,
              dcl.dispclass_id,
              d.healthkind_id,
       lpu.org_nick as "МО",
       dcl.dispclass_name,
       to_char(d.evnpldispprof_disdt, 'dd.mm.yyyy') as "Дата профмероприятия",

       concat(
           ps.person_surname, ' ', ps.person_firname, ' ', coalesce(ps.person_secname, '')) as "ФИО",
       to_char(ps.person_birthday, 'dd.mm.yyyy') as "ДР",
--        a.healthkind_id,
       m.person_fio as "Врач",
       h.healthkind_name as "Группа здоровья",
--        v.diag_id,
       v.diag_code as "Диагноз"


from v_evnpldispprof d
    join v_lpu lpu using (lpu_id)
    join v_personstate ps using (person_id)
    join dispclass dcl on d.dispclass_id = dcl.dispclass_id --and dcl.dispclass_id not in (10, 7) --in (1, 2, 5, 31, 32, 35, 36)
    join lateral (select v.*, ds.* from v_evnvizitdisp v
                           join diag ds on ds.diag_id = v.diag_id
                           where d.evnpldispprof_id = v.evnvizitdisp_rid
        order by v.evnvizitdisp_id desc limit 1
        ) v on true

--     left join assessmenthealth a on d.evnpldispprof_id = a.evnpldisp_id
    join healthkind h on d.healthkind_id = h.healthkind_id
    left join v_medstafffactcache m on d.medstafffact_id = m.medstafffact_id
where {condition}
--and d.healthkind_id is not null
-- limit 100


