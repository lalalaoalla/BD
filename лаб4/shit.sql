select p.n_det, spj.n_post, sum(spj.kol) as total_quantity
from p
left join spj on p.n_det = spj.n_det
group by p.n_det, spj.n_post
having count(spj.kol) = (select max(total_count)
                         from (select spj.n_post, count(spj.kol) as total_count
                               from spj
                               where spj.n_det = p.n_det
                               group by spj.n_post
                             ) as supplier_totals
                         where spj.n_post in (select n_post
                                              from (select spj.n_post, count(spj.n_izd) as count_post_izd
                                                    from spj
                                                    where spj.n_det = p.n_det
                                                    group by spj.n_post
                                                    ) as product_counts
                                               where count_post_izd = (select max(count_post_izd)
                                                                       from (select spj.n_post, count(spj.n_izd) as count_post_izd
                                                                             from spj
                                                                             where spj.n_det = p.n_det
                                                                             group by spj.n_post) as inner_counts
                                                                       )
                                                )
                        )

order by p.n_det, spj.n_post desc