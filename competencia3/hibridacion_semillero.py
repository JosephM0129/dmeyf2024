#%%
import pandas as pd
import duckdb 

w1= duckdb.read_csv("expw227_SC-0001_tb_future_prediccion_base.txt") 
w2= duckdb.read_csv("expw227_SC-0002_tb_future_prediccion_sin202004202003.txt") 
w3= duckdb.read_csv("expw227_SC-0003_tb_future_prediccion_sumo201902y201901.txt") 
w4= duckdb.read_csv("expw227_SC-0004_tb_future_prediccion_resto202104y202103.txt") 
w5= duckdb.read_csv("expw227_SC-0005_tb_future_prediccion_delta3.txt") 
w6= duckdb.read_csv("expw227_SC-0006_tb_future_prediccion_lineamuerte.txt") 

nombre_entrega_kaggle = 'hibridacion_w1_w2_w3_w4_w5_w6'
s1 = duckdb.sql(f'''
    SELECT numero_de_cliente, sem_1_1 FROM w1 UNION ALL

    SELECT numero_de_cliente, sem_1_1 FROM w2 UNION ALL

   SELECT numero_de_cliente, sem_1_1 FROM w3 UNION ALL
    
   SELECT numero_de_cliente, sem_1_1 FROM w4 UNION ALL
    
   SELECT numero_de_cliente, sem_1_1 FROM w5 UNION ALL
   
   SELECT numero_de_cliente, sem_1_1 FROM w6 

''')

s1_score_prom = duckdb.sql(f'''
    SELECT 
        numero_de_cliente, 
        AVG(sem_1_1) as sem_1_1_promedio
    FROM s1
    GROUP BY ALL
    ORDER BY AVG(sem_1_1) DESC
''')

h1_11000 = duckdb.sql(f'''
    WITH CORTE_11000 AS (
        SELECT *, ROW_NUMBER() OVER (ORDER BY sem_1_1_promedio desc) AS id
        FROM s1_score_prom
    )
    SELECT 
        numero_de_cliente,
        CASE WHEN id <= 11000 THEN 1 ELSE 0 END AS Predicted
    FROM CORTE_11000;
''')

h1_11000.to_df().to_csv(f'{nombre_entrega_kaggle}.csv', index=False)