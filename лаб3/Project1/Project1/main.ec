#include <sqlca.h>
#include <stdio.h>
#include <stdlib.h>
// сообщение об ошибке
int error_msg(char *desc)
{
    printf("\n%s\nКод: %d\nОписание ошибки: %s\n", desc, sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
    return -1;
}
void connectBD()
{
    // подключение к бд
    exec sql connect to students@students.ami.nstu.ru user "pmi-b1303" using "Shlasow0";
    if(sqlca.sqlcode < 0)
    {
        printf("\nНеверный логин или пароль\nОписание ошибки: %s\n*Выход из программы*\n\n", 
sqlca.sqlerrm.sqlerrmc);
        exit(1);
    }
}
void closeBD()
{
    // закрываем сессию
    exec sql disconnect current;
}
int useScheme()
{
    // путь к схеме
    exec sql set search_path to "pmib1303", public;
    if (sqlca.sqlcode < 0)
    {
        printf("Ошибка выбора схемы");
        return 1;
    }
    else
        printf("Схема выбрана успешно!\n");
    return 0;
}
int main()
{
    // секция объявления переменнфх
    exec sql begin declare section;//начало
    int reiting, count_post;
    char n_post[7], name[21], town[21], n_izd[7], n_det[7], task_num;
    exec sql end declare section;// конец
    
    connectBD();
    
    if(!useScheme())
    {
            // менюшечка
        printf("\n1. Выдать число поставок, выполненных для изделий с деталями зеленого цвета. ");
        printf("\n2. Поменять местами города, с самым коротким и самым длинным названием изделия.");
        printf("\n3. Найти детали, имеющие поставки, вес которых меньше среднего веса поставок этой детали для изделий из Лондона.");
        printf("\n4. Выбрать поставщиков, не поставляющих ни одной из деталей, поставляемых поставщиками, находящимися в Лондоне.");
        printf("\n5. Выдать полную информацию о поставщиках, выполнивших поставки ТОЛЬКО с объемом от 200 до 500 деталей. ");
        printf("\nВыберите задание (1-5): ");
        while(scanf(" %c", &task_num) && task_num > '0' && task_num < '6')
        {
            switch(task_num)
            {
                case '1':
                    // начало транзакции
                    exec sql begin work;
                    
                    // первый запрос into  -сохраняем туда данные
                    exec sql select count(n_izd) 
                    into :count_post
                    from spj
                    where spj.n_izd in(select n_izd
                                     from spj
                                     join p on p.n_det=spj.n_det
                                     where p.cvet='Зелёный');

                    // проверяем результат rollback - отменяем все сделанные изменения в рамках транзакции
                    if (sqlca.sqlcode < 0)
                    {
                        error_msg("В запросе есть ошибка.");
                        exec sql rollback work;
                        break;
                    }
                    else
                        if (sqlca.sqlcode == 0)
                            printf("\nЧисло поставок = %d\n", count_post);
                    //подтверждение транзакции
                    exec sql commit work;
                break;
                case '2':
                    exec sql begin work;
                    // запрос намбер ту
                    exec sql update j set town = (case when length(j.name) = 
                                                 (select max(length(name))
                                                 from j)
                     then (select j1.town
                            from j j1
                            order by length(j1.name), j1.town
                            limit 1)
                     else (select j2.town
                           from j j2
                           order by length(j2.name) desc, j2.town
                           limit 1)
                     end)
                        where length(j.name) = (select min(length(name)) 
                                                from j j5)
                                                or length(j.name) = (select max(length(name)) 
                                                                     from j j6);
                    if (sqlca.sqlcode < 0)
                    {
                        error_msg("Ошибка при изменении (UPDATE).");
                        exec sql rollback work;
                        break;
                    }
                    else
                        if (sqlca.sqlcode == 0)
                        {
                            printf("\nКоличество обработанных записей: %d\n", sqlca.sqlerrd[2]);
                        };
                    exec sql commit work;
                break;
                case '3':
                    exec sql begin work;
                    // запрос 3
                    //курсор помогает просматривать результаты построчно
                    exec sql declare cursor_3 cursor for
                    select spj.n_det
                    into :n_det
                    from spj
                    join p on p.n_det=spj.n_det
                    join (select spj.n_det,avg(spj.kol*p.ves) mves
                          from spj
                          join p on p.n_det=spj.n_det
                          join j on j.n_izd=spj.n_izd
                          where j.town='Лондон'
                          group by spj.n_det
                        ) zap on zap.n_det=spj.n_det
                    where spj.kol*p.ves<mves;

                    exec sql open cursor_3;
                    if (sqlca.sqlcode < 0)
                    {
                        error_msg("Ошибка при открытии курсора (OPEN).");
                        exec sql close cursor_3;
                        exec sql rollback work;
                        break;
                    }
                    //команда, запрашивающая следующую строку курсора
                    exec sql fetch cursor_3;
                    if (sqlca.sqlcode < 0)
                    {
                        error_msg("Ошибка при чтении курсора(FETCH).");
                        exec sql close cursor_3;
                        exec sql rollback work;
                        break;
                    }
                    else
                        if (sqlca.sqlcode == 100)
                            printf("\nДанные отсутствуют.\n");
                        else
                        {
                            printf("\nНомер детали\n%s\n", n_det);
                            while (sqlca.sqlcode == 0)
                            {
                                exec sql fetch cursor_3;
                                if (sqlca.sqlcode < 0)
                                {
                                    error_msg("Ошибка при чтении курсора(FETCH).");
                                    exec sql close cursor_3;
                                    exec sql rollback work;
                                    break;
                                }
                                else
                                if (sqlca.sqlcode == 0)
                                    printf("%s\n", n_det);
                            }
                        }
                    exec sql close cursor_3;
                    exec sql commit work;
                break;
                case '4':
                    exec sql begin work;
                    //запрос 4
                    exec sql declare cursor_4 cursor for
                    select n_post
                    into :n_post
                    from spj
                    except
                    select distinct n_post
                    from spj 
                    where n_det in (select n_det 
                                    from spj
                                    join s on s.n_post=spj.n_post
                                    where town='Лондон');

                    exec sql open cursor_4;
                    if (sqlca.sqlcode < 0)
                    {
                        error_msg("Ошибка при открытии курсора(OPEN).");
                        exec sql close cursor_4;
                        exec sql rollback work;
                        break;
                    }
                    //читаем курсор
                    exec sql fetch cursor_4;
                    if (sqlca.sqlcode < 0)
                    {
                        error_msg("Ошибка при чтении курсора (FETCH).");
                        exec sql close cursor_4;
                        exec sql rollback work;
                        break;
                    }
                    else
                        if (sqlca.sqlcode == 100)
                            printf("\nДанные отсутствуют.\n");
                        else
                        {
                            printf("\nСписок поставщиков:\n%s\n", n_post);
                            while (sqlca.sqlcode == 0)
                            {
                                exec sql fetch cursor_4;
                                if (sqlca.sqlcode < 0)
                                {
                                    error_msg("Ошибка при чтении курсора(FETCH).");
                                    exec sql close cursor_4;
                                    exec sql rollback work;
                                    break;
                                }
                                else
                                    if (sqlca.sqlcode == 0)
                                        printf("%s\n", n_post);
                            }
                        }
                    exec sql close cursor_4;
                    exec sql commit work;
                break;
                case '5':
                    exec sql begin work;
                    //запрос номер 5
                    exec sql declare cursor_5 cursor for
                    select *
                    into :n_post, :name, :reiting, :town
                    from s
                    where n_post in(select n_post
                                    from spj
                                    except
                                    select n_post
                                    from spj
                                    where not (kol<=500 and kol>=200)
                    );
                    exec sql open cursor_5;
                    if (sqlca.sqlcode < 0)
                    {
                        error_msg("Ошибка при открытии курсора(OPEN).");
                        exec sql close cursor_5;
                        exec sql rollback work;
                        break;
                    }
                    exec sql fetch cursor_5;
                    if (sqlca.sqlcode < 0)
                    {
                        error_msg("Ошибка при чтении курсора (FETCH).");
                        exec sql close cursor_5;
                        exec sql rollback work;
                        break;
                    }
                    else
                        if (sqlca.sqlcode == 100)
                            printf("\nДанные отсутствуют.\n");
                        else
                        {
                            printf("\nНомер\tИмя\t\t\tРейтинг\tГород\n%s\t%s\t%d\t%s\n", n_post, name, reiting, town);
                            while (sqlca.sqlcode == 0)
                            {
                                exec sql fetch cursor_5;
                                if (sqlca.sqlcode < 0)
                                {
                                    error_msg("Ошибка при чтении курсора(FETCH).");
                                    exec sql close cursor_5;
                                    exec sql rollback work;
                                    break;
                                }
                                else
                                    if (sqlca.sqlcode == 0)
                                        printf("%s\t%s\t%d\t%s\n", n_post, name, reiting, town);
                            }
                        }
                    exec sql close cursor_5;
                    exec sql commit work;
                break;
            }
            printf("\n1. Выдать число поставок, выполненных для изделий с деталями зеленого цвета. ");
            printf("\n2. Поменять местами города, с самым коротким и самым длинным названием изделия.");
            printf("\n3. Найти детали, имеющие поставки, вес которых меньше среднего веса поставок этой детали для изделий из Лондона.");
            printf("\n4. Выбрать поставщиков, не поставляющих ни одной из деталей, поставляемых поставщиками, находящимися в Лондоне.");
            printf("\n5. Выдать полную информацию о поставщиках, выполнивших поставки ТОЛЬКО с объемом от 200 до 500 деталей. ");
            printf("\nВыберите задание (1-5): ");
        }
    }
    closeBD();
    return 0;
}