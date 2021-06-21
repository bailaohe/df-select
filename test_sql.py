from dfselect import parse_select, exec_operators, df_select
import pandas as pd

sql_naive = """
select 3.14 from tbl;
"""

# sql_simple = """
# select a, c from df left join df2 on df.b = df2.b and df.c = df2.c where c in (7,8) and b == 3
# """

sql_simple = """
select a, b, t2.b, b+2, a-b, ifnull(b, 10), if(b > 3,1,-1) from df as t1 left join df2 as t2 on t1.c = t2.c order by a+b desc limit 3
"""
# select a from df as t1 left join df2 as t2 on t1.c = t2.c and t1.b = t2.b where (c in (7,) or c in (8,)) and b in (3, 5) and a = 1 order by c asc, b desc
# select 123 as x, a, f(a, b+1) + 1, b, t2.b, c from df as t1 left join df2 as t2 on t1.c = t2.c where (c in (7,) or c in (8,)) and b in (3, 5) and a = 1 order by c asc, b desc

sql_group = """
select ifnull(b,10), sum(if(b > t2.b,1,-1)) as stat from df as t1 left join df2 as t2 on t1.c = t2.c group by ifnull(b, 10)
"""
# df.groupby('b').agg({'a':'sum'}).reset_index()

sql_in_compound = """
select a.col1, u.col2, a.col1 from @mysql-dev.hogwarts.institution_admin as a
left join @mysql-dev.hogwarts.institution_user u
  on a.phone = u.account_no and a.age = u.age
where a.inst_id > 0 and a.state = FUN(3) and a.inst_id in (
  select inst_id from institution where state = 1 and pid in (
    select pid from p
  )
) limit 10;
"""

if __name__ == '__main__':
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [3, 4, 5], 'c': [8, 8, 7]})
    df2 = pd.DataFrame({'b': [3, 5, 4], 'c': [7, 8, 8], 'd': [10, 11, 12]})

    tables = dict(df=df, df2=df2)
    result = df_select(sql_group, tables=tables)
    print('select result:')
    print(result)


