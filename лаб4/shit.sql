select p.*, b.kol
from p
full join (select t.n_det, count(t.n_izd) kol
      from (select n_izd, (select a.n_det
             from
             (select x.n_det, sum(kol) vol
              from spj x 
              join p on x.n_det = p.n_det
              where n_izd = j.n_izd
                    and p.ves = (select max(ves)
                                 from spj
                                 join p on p.n_det = spj.n_det
                                 where n_izd = j.n_izd)
              group by x.n_det
              ) a
              join p on p.n_det = a.n_det
              order by vol desc, p.name desc
              limit 1)
       from j) t
       group by t.n_det) b on b.n_det = p.n_det
order by 1