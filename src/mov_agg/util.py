import pandas as pd

def merge(load_dt=20240731):
    read_df = pd.read_parquet('~/tmp/test_parquet')
    cols = ['movieCd', #영화의 대표코드를 출력합니다.
       'movieNm', #영화명(국문)을 출력합니다.
        'openDt', #영화의 개봉일을 출력합니다.
        'audiCnt', #해당일의 관객수를 출력합니다.
        'load_dt', # 입수일자
        'multiMovieYn', #다양성영화 유무
        'repNationCd', #한국외국영화 유무
       ]

    df = read_df[cols]
    temp_df = read_df[cols]
    
    temp_df['load_dt'] = temp_df['load_dt'].astype("object")
    temp_df['load_dt'] = temp_df['load_dt'].fillna(0)
    temp_df['load_dt'].astype(str).astype(int)

    df_where = temp_df[temp_df['load_dt'] == load_dt]
    #print(df_where)
    #df_where['multiMovieYn'] = df_where['multiMovieYn'].astype("object")
    #df_where['multiMovieYn'] = df_where['multiMovieYn'].fillna(0)
    #df_where['multiMovieYn'].astype(str).astype(int)

    #df_where['repNationCd'] = df_where['repNationCd'].astype("object").astype(str).astype(int)
    #df_where['repNationCd'] = df_where['repNationCd'].fillna(0)
    #df_where['repNationCd'].astype(str).astype(int)
    
    #print(df)
    #df_where = df[df['movieCd'] == '20112207']
    #df_mul_Y = df[df['multiMovieYn'] == 'Y']
    #df_mul_N = df[df['multiMovieYn'] == 'N']

    #merge_df = pd.merge(df['multiMovieYn'], df['repNationCd'], on="movieCd")
    #print(df_n_YF)
    #print("\n"*3)
    #print(df_n_YK)
    #print("\n"*3)
    #print(df_n_NF)
    #print("\n"*3)
    #print(df_n_NK)
    #print(df_where)
    #print(df_where.dtypes)
    return df_where
