import pandas as pd
from tabulate import tabulate

def merge(load_dt='20240725'):
    read_df = pd.read_parquet('~/tmp/test_parquet')
    cols = ['movieCd', #영화의 대표코드를 출력합니다.
       'movieNm', #영화명(국문)을 출력합니다.
        'openDt', #영화의 개봉일을 출력합니다.
        'audiCnt', #해당일의 관객수를 출력합니다.
        'load_dt', # 입수일자
        'multiMovieYn', #다양성영화 유무
        'repNationCd', #한국외국영화 유무
       ]

    df = read_df[cols].copy()
    
    df['load_dt'] = df['load_dt'].astype("object")
    #df_where = df[(df['movieCd'] == '20235974') & (df['load_dt'] == int(load_dt))].copy()
    df_where = df[(df['movieCd'] == '20235974') & (df['load_dt'] == int(load_dt))].copy() #날짜조건 load_dt 인자 받기 print(dw)
    df_where['multiMovieYn'] = df_where['multiMovieYn'].astype("object")
    df_where['multiMovieYn'] = df_where['multiMovieYn'].fillna('unknown')

    df_where['repNationCd'] = df_where['repNationCd'].astype("object")
    df_where['repNationCd'] = df_where['repNationCd'].fillna('unknown')
    
    u_multi = df_where[df_where['multiMovieYn'] == 'unknown']
    u_nation = df_where[df_where['repNationCd'] == 'unknown']
  
    unknownindex = 0
    for i in u_multi['multiMovieYn'].index:
            for j in u_nation['repNationCd'].index:
                if i == j:
                    unknownindex=j
                    print(i,j)
                    u_multi = u_multi.drop(index=unknownindex)
                    u_nation = u_nation.drop(index=unknownindex)
    merge_df = pd.merge(u_multi, u_nation, on='movieCd', suffixes=('_m', '_n'))
    headers = ['movieCd', 'movieNm_m', 'openDt_m', 'audiCnt_m', 'load_dt_m', 'multiMovieYn_m', 'repNationCd_m']
    df_cleaned = merge_df[headers] 
    #print(m_df)
    #merge_df = pd.merge(df['multiMovieYn'], df['repNationCd'], on="movieCd")
    #print(df_n_YF)
    #print("\n"*3)
    #print(df_n_YK)
    #print("\n"*3)
    #print(df_n_NF)
    #print("\n"*3)
    #print(df_n_NK)
    #print(u_multi)
    #print(u_nation)
    #print(merge_df)
    tabulate.WIDE_CHARS_MODE = False
    print(tabulate(df_cleaned, headers, tablefmt="rst"))   
    #print(df_where.dtypes)
    return df_cleaned

merge()
