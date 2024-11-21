#include <sqlca.h>
#include <stdio.h>
#include <stdlib.h>
// сообщение об ошибке
int error_msg(char *desc) 
{
    printf("\n%s\nКод: %d\nОписание: %s\n", desc, sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
    return -1;
}
void connectBD() 
{
    // подсоединение к базе данных
    exec sql connect to students@students.ami.nstu.ru user "pmi-b1303" using "Shlasow0";
    if(sqlca.sqlcode < 0) 
    {
        printf("\nОшибка подключения\nОписание ошибки: %s\n*Выход из программы*\n\n", 
sqlca.sqlerrm.sqlerrmc);
        exit(1);
    }
}
void closeBD() 
{
    // закрытие сессии
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
    } else 
    {
        printf("Вы успешно выбрали схему \n");
    }
    return 0;
}
int main() 
{
    int is_task_1_prepaired = 0;
    int is_task_2_prepaired = 0;
    int is_task_3_prepaired = 0;
    // секция объявления переменных
    exec sql begin declare section;
        int count_a, count_b;
        int ind_count_a, ind_count_b;
        double pr, ave_v_post,round;
        int ind_pr, ind_ave_v_post, ind_round;
        char n_izd[70], town[210], name[210], cvet[210], n_post[70];
        int task_num = -1;
        int ind_name, ind_town, ind_n_izd, ind_cvet,ind_n_post;
        char task_1[] = "select round(avg(a.max),2)\
                            from (select n_izd, max(kol) \
                                from spj \
                                group by n_izd) a";
        char task_2[] = "select j.n_izd, j.name, j.town,a.средний_объем_поставки \
                            from (select spj.n_izd, avg(kol) as средний_объем_поставки \
                                    from spj \
                                    where spj.n_post = ? \
                                    group by spj.n_izd) a \
                            join j on j.n_izd=a.n_izd \
                            order by j.n_izd";
        char task_3[] = "select a.cvet, a.количество_пост_этого_цвета, b.общее_число_поставок, \
                                    round(a.количество_пост_этого_цвета*100.0/b.общее_число_поставок,2) pr \
                            from (select p.cvet as cvet, count(spj.n_izd) as количество_пост_этого_цвета \
                                    from spj \
                                    join p on p.n_det=spj.n_det \
                                    where spj.n_izd = ? \
                                    group by p.cvet) a \
                            cross join (select count(n_izd) as общее_число_поставок \
                                        from spj \
                                        where spj.n_izd = ?) b ";
    exec sql end declare section;

    connectBD();

    exec sql prepare task_1 from :task_1;
    // обработка ошибки запроса
    if (sqlca.sqlcode < 0)
    {
        is_task_1_prepaired = 0;
        error_msg("Ошибка при подготовке первого запроса.");
    } else 
    {
        is_task_1_prepaired = 1;
        printf("Подготовка первого запроса выполнена успешно\n");
    }

    // Подготовка второго запроса к выполнению
    exec sql prepare task_2 from :task_2;
    // обработка ошибки запроса
    if (sqlca.sqlcode < 0)
    {
        is_task_2_prepaired = 0;
        error_msg("Ошибка при подготовке второго запроса.");
    } else 
    {
        is_task_2_prepaired = 1;
        printf("Подготовка второго запроса выполнена успешно\n");
    }

    // Подготовка третьего запроса к выполнению
    exec sql prepare task_3 from :task_3;
    // обработка ошибки запроса
    if (sqlca.sqlcode < 0)
    {
        is_task_3_prepaired = 0;
        error_msg("Ошибка при подготовке третьего запроса.");
    } else 
    {
        is_task_3_prepaired = 1;
        printf("Подготовка третьего запроса выполнена успешно\n");
    }
    

    
    if(!useScheme()) 
    {
        // Выбор задания
            printf("\n1)Получить наибольший объем поставки для каждого изделия и найти их среднее.");
            printf("\n2)Для указанного поставщика S* найти средний объем его поставок для каждого из изделий (для которых он поставлял детали). Вывести номер изделия, название изделия, город изделия, средний объем поставок для изделия. ");
            printf("\n3)Ввести номер изделия J*. Найти цвета деталей, поставлявшихся для изделия J*, и определить, какой процент поставки деталей каждого цвета составляют от общего числа поставок для изделия. Вывести цвет детали, число поставок деталей этого цвета, общее число поставок для изделия J*, процент.");
            printf("\nВыберите задание (1-3): ");

        while(scanf("%d", &task_num) && task_num > 0 && task_num < 4) 
        {
            switch(task_num) 
            {
                case 1: 
                {
                    printf("Задача 1\n");
                    if (!is_task_1_prepaired) 
                    {
                        printf("Задача 1 не может быть выполнена из-за ошибки подготовки запроса.\n");
                        break;
                    }

                    // начало транзакции
                    exec sql begin work;
                    // выполнение запроса
                    exec sql execute task_1 into :round:ind_round;
                    if (sqlca.sqlcode < 0)
                    {
                        error_msg("Ошибка при выполнении запроса.");
                        exec sql rollback work;
                        break;
                    } else if (sqlca.sqlcode == 0) 
                    { // Вывод результата запроса
                        if (ind_round == -1) 
                        {
                            printf("\nРезультат запроса: NULL (0)\n");
                        } else 
                        {
                            printf("\nРезультат запроса: %.2f\n", round);
                        }
                    }
                    // подтверждение транзакции
                    exec sql commit work;
                    break;
                }

                //запрос номер 2
                case 2: 
                {
                    printf("Задача 2\n");
                    if (!is_task_2_prepaired) 
                    {
                        printf("Задача 2 не может быть выполнена из-за ошибки подготовки запроса.\n");
                        break;
                    }
                    printf("Введите поставщика: ");
                    scanf("%s", n_post);
    
                    //объявление курсора_2 
                    exec sql declare cursor_2 cursor for task_2;
                    if(sqlca.sqlcode < 0)
                    {
                        error_msg("Ошибка при объявлении курсора\n");
                        exec sql rollback;
                        break;
                    }

                    // начало транзакции
                    exec sql begin work;
                    // открытие курсора_2
                    exec sql open cursor_2 using :n_post;
                    if (sqlca.sqlcode < 0) 
                    {
                        error_msg("Ошибка при открытии курсора.");
                        exec sql close cursor_2;
                        exec sql rollback work;
                        break;
                    }
                    // считывание строки данных и их дальнейшая обработка
                    exec sql fetch cursor_2 into :n_izd:ind_n_izd, :name:ind_name, :town:ind_town, 
:ave_v_post:ind_ave_v_post;
                    if (sqlca.sqlcode < 0) 
                    {
                        error_msg("Ошибка при чтении курсора (FETCH).");
                        exec sql close cursor_2;
                        exec sql rollback work;
                        break;
                    } else if (sqlca.sqlcode == 100) 
                    {
                        printf("Нет данных.");
                    } else 
                    {
                        printf("n_izd\ttname\t\t\ttown\t\t\tave_v_post\n");
                        if (ind_n_izd == -1) 
                        {
                            printf("NULL\t");
                        } else 
                        {
                            printf("%s\t", n_izd);
                        }
                        if (ind_name == -1) 
                        {
                            printf("NULL\t");
                        } else {
                            printf("%s", name);
                        }
                        if (ind_town == -1) 
                        {
                            printf("NULL\t");
                        } else 
                        {
                            printf("%s", town);
                        }
                        if (ind_ave_v_post == -1) 
                        {
                            printf("NULL\t");
                        } else 
                        {
                            printf("%lf\n", ave_v_post);
                        }
                    }
                    while (sqlca.sqlcode == 0) 
                    {
                    exec sql fetch cursor_2 into :n_izd:ind_n_izd, :name:ind_name, :town:ind_town, 
:ave_v_post:ind_ave_v_post;
                        if (sqlca.sqlcode < 0) 
                        {
                            error_msg("Ошибка при чтении курсора");
                            exec sql close cursor_2;
                            exec sql rollback work;
                            break;
                        } else if (sqlca.sqlcode == 0) 
                        {                      
                            if (ind_n_izd == -1) 
                            {
                                printf("NULL\t");
                            } else 
                            {
                                printf("%s\t", n_izd);
                            }
                            if (ind_name == -1) 
                            {
                                printf("NULL\t");
                            } else 
                            {
                                printf("%s", name);
                            }
                            if (ind_town == -1) 
                            {
                                printf("NULL\t");
                            } else 
                            {
                                printf("%s", town);
                            }
                            if (ind_ave_v_post == -1) 
                            {
                                printf("NULL\t");
                            } else 
                            {
                                printf("%lf\n", ave_v_post);
                            }
                        }
                    }
                    exec sql close cursor_2;
                    exec sql commit work;
                    break;
                }
                //запрос номер 3
                case 3: 
                {
                    printf("Задача 3\n");
                    if (!is_task_3_prepaired) 
                    {
                        printf("Задача 3 не может быть выполнена из-за ошибки подготовки запроса.\n");
                        break;
                    }
                    printf("Введите номер изделия: ");
                    scanf("%s", n_izd);
                    

                    //объявление курсора_3
                    exec sql declare cursor_3 cursor for task_3;
                    if(sqlca.sqlcode < 0)
                    {
                        error_msg("Ошибка при объявлении курсора\n");
                        exec sql rollback;
                        break;
                    }

                    // начало транзакции
                    exec sql begin work;

                    //обработка при открытии курсора_3
                    exec sql open cursor_3 using :n_izd, :n_izd;
                    if (sqlca.sqlcode < 0) 
                    {
                        error_msg("Ошибка при открытии курсора.");
                        exec sql close cursor_3;
                        exec sql rollback work;
                        break;
                    }
                    // считывание строки данных и их дальнейшая обработка
                    sqlca.sqlcode = 0;
                    printf("cvet\t\t\tcount_a\tcount_b\tpr\n");
                    int counter = 0;
                    while(sqlca.sqlcode == 0) 
                    {
                        exec sql fetch cursor_3 into :cvet:ind_cvet, :count_a:ind_count_a, 
:count_b:ind_count_b, :pr:ind_pr;
                        if (sqlca.sqlcode < 0) 
                        {
                            error_msg("Ошибка при чтении курсора (FETCH).");
                            exec sql close cursor_3;
                            exec sql rollback work;
                            break;
                        } 
                        else if (sqlca.sqlcode == 100) 
                        {
                            printf("\nКоличество данных: %d\n", counter);
                        } 
                        else if(sqlca.sqlcode != 100)
                        {
                            counter++;
                            if (ind_cvet == -1) 
                            {
                                printf("NULL\t");
                            } else 
                            {
                                printf("%s\t", cvet);
                            }
                            if (ind_count_a == -1) 
                            {
                                printf("NULL\t");
                            } else 
                            {
                                printf("%d\t", count_a);
                            }
                            if (ind_count_b == -1) 
                            {
                                printf("NULL\t");
                            } else 
                            {
                                printf("%d\t", count_b);
                            }
                            if (ind_pr == -1) 
                            {
                                printf("NULL\t");
                            } else 
                            {
                                printf("%.2f\n", pr);
                            }
                        }
                    }
                    exec sql close cursor_3;
                    exec sql commit work;
                    break;
                }
            }
            printf("\n1)Получить наибольший объем поставки для каждого изделия и найти их среднее.");
            printf("\n2)Для указанного поставщика S* найти средний объем его поставок для каждого из изделий (для которых он поставлял детали). Вывести номер изделия, название изделия, город изделия, средний объем поставок для изделия. ");
            printf("\n3)3.	Ввести номер изделия J*. Найти цвета деталей, поставлявшихся для изделия J*, и определить, какой процент поставки деталей каждого цвета составляют от общего числа поставок для изделия. Вывести цвет детали, число поставок деталей этого цвета, общее число поставок для изделия J*, процент.");
            printf("\nВыберите задание (1-3): ");
            
        }
            
    }
    closeBD();
    return 0;
} 
