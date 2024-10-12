import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


# Devuelve la cantidad de items y porcentaje de contenido Free por año para la empresa desarrolladora ingresada como
# parametro.
def developer (desarrollador):
    
    # Importa el archivo 
    df_dev = pd.read_parquet('Archivos API/def_developer.parquet')
        # Columnas:
            # developer
            # item_id
            # release_year
            # price

    # Se crea un DataFrame que contiene solo los registros para el desarrollador especificado
    dev = df_dev[df_dev['developer']==desarrollador]

    # Se crean las columnas 'free' y 'tot' para hacer el conteo de items
    dev['free']=dev['price'].apply(lambda x: 1 if x == 0 else 0) # 1 para cada item Free
    dev['tot']=1 # 1 para todos los items

    # Se agrupan los registros por desarrollador y año. Se suma la cantidad de items Free y se cuentan los Total
    dev = dev.groupby(['developer','release_year']).agg({'free': 'sum','tot':'count'}).reset_index()
    
    # Se calcula el porcentaje de contenido Free
    dev['percentage']=((dev['free']/dev['tot'])*100).round(2)

    # Se borran las columnas auxiliares y se cambian los nombres de las que contienen el resultado
    dev.drop(columns=['developer','free'],inplace=True)
    dev.rename(columns={'release_year':'año','tot':'Cantidad de items','percentage':'Contenido Free (%)'},inplace=True)
    
    return dev # Se devuelve el DataFrame


# Devuelve la cantidad de dinero gastado por el usuario ingresado como parametro, el porcentaje de recomendación en 
# base a reviews.recommend y cantidad de items.
def userdata (user_id):

    # Importa el archivo 
    df_user = pd.read_parquet('Archivos API/def_userdata.parquet')
        # Columnas:
            # user_id
            # item_id
            # recommend
            # price

    # Se crea un DataFrame que contiene solo los registros para el usuario especificado
    u_data = df_user[df_user['user_id']==user_id]

    #Se calculan directamente los valores solicitados y se devuelven en el formato solicitado
    return {"User":user_id,
            "Dinero gastado":float(u_data['price'].sum()),
            "% Recomendacion":(u_data[u_data['recommend']].shape[0]/u_data.shape[0])*100,
            "Cantidad de items":u_data.shape[0]}


# Devuelve el usuario que acumula más horas jugadas para el género ingresado como parametro y una lista de la 
# acumulación de horas jugadas por año de lanzamiento.
def UserForGenre (genero):

    # Importa el archivo
    df_user_gen = pd.read_parquet('Archivos API/def_userforgenre.parquet')
        # Columnas:
            # user_id
            # item_id
            # playtime
            # genres
            # release_year

    # Se crea un DataFrame con los registros que contienen en su lista de generos el genero ingresado por parametro
    df_gen = df_user_gen[df_user_gen['genres'].apply(lambda x: genero in x)]

    # Se agrupan los registros por usuario y se suman los tiempos de juego
    tot = df_gen.groupby(['user_id']).agg({'playtime': 'sum'}).reset_index()

    # Se ordenan los tiempos de juego y se toma el usuario correspondiente al mayor valor
    u_most = tot.sort_values(by='playtime',ascending=False).iloc[0,0]
    
    # Se filtra el DataFrame que contenia solo los registros del genero indicado para el usuario encontrado
    df_user = df_gen[df_gen['user_id']==u_most]

    # Se agrupan los resultados por año
    df_user = df_user.groupby(['release_year']).agg({'playtime': 'sum'}).reset_index()
    
    # Se renombran las columnas y se crea el diccionario para devolver
    df_user.rename(columns={'release_year':'Año','playtime':'Horas'},inplace=True)
    res_dict = df_user.to_dict(orient='records')

    return {f"Usuario con más horas jugadas para Genero '{genero}'" : u_most, 
            "Horas jugadas":res_dict} 


# Devuelve el top 3 de desarrolladores con juegos MÁS recomendados por usuarios para el año dado. Se analiza basado
# en las variablses recommend = True y sentiment_analysis = 2 (positivo).
def best_developer_year (anio):

    # Importa el archivo
    df_best_dev = pd.read_parquet('Archivos API/def_best_dev.parquet')
        # Columnas:
            # developer
            # item_id
            # release_year
            # recommend
            # sentiment_analysis

    # Se crea un DataFrame con los registros que corresponden al año ingresado por parametro y que son recomendados
    # y tienen un analisis de sentimiento positivo
    df_best = df_best_dev[(df_best_dev['recommend'])&(df_best_dev['sentiment_analysis']==2)&(df_best_dev['release_year']==anio)]

    # Se ordenan los developer segun su frecuencia en orden descendente y se toman los 3 primeros
    p1 = df_best['developer'].value_counts().index[0]
    tam = df_best['developer'].value_counts().size
    if  tam > 1:
        p2 = df_best['developer'].value_counts().index[1]
        if tam > 2:
            p3 = df_best['developer'].value_counts().index[2]
        else:
            p3 = None
    else:
        p2 = None
        p3 = None
    
    return [{"Puesto 1":p1,"Puesto 2":p2,"Puesto 3":p3}] # Se devuelven los datos en el formato solicitado


# Devuelve un diccionario con el nombre del desarrollador ingresado por parametro como llave y una lista con la 
# cantidad total de registros de reseñas de usuarios que se encuentren categorizados con un análisis de sentimiento 
# como valor positivo o negativo.
def developer_reviews_analysis (desarrolladora):

    # Importa el archivo
    df_dev_rev = pd.read_parquet('Archivos API/def_dev_rev.parquet')
        # Columnas:
            # developer
            # item_id
            # sentiment_analysis

    # Se crea un DataFrame que contiene solo los registros para la desarrolladora especificada
    df_dev = df_dev_rev[df_dev_rev['developer']==desarrolladora]

    #Se calculan directamente los valores solicitados y se devuelven en el formato solicitado
    return {f"{desarrolladora}":[f'Negative = {df_dev[df_dev['sentiment_analysis']==0].shape[0]}',
                                 f'Positive = {df_dev[df_dev['sentiment_analysis']==2].shape[0]}']}

def recomendacion_juego (id_producto):

    lista_reco = []
    
    return lista_reco