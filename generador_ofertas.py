#script que define el flujo de trabajo de un generador de ofertas consultando una api de ML
from airflow import DAG
from datetime import datetime, date
from airflow.operators.python_operator import PythonOperator
from consultar_api1 import get_most_relevant_items_for_category
from ofertar import analizar_y_ofertar
DATE=str(date.today()).replace('-','')

   
with DAG(
    dag_id="Generador_de_ofertas",
    start_date=datetime(2023, 10, 9),
) as dag:
    def consultar_api(category):
        resultado_consulta = get_most_relevant_items_for_category(category)
        return resultado_consulta

    def ofertando(**kwargs):
        #la siguiente linea se utiliza para recuperar el valor almacenado en XCom de la tarea Consultando_API
        resultado_tarea_1 = kwargs['ti'].xcom_pull(task_ids='Consultando_API') 
        analizar_y_ofertar(resultado_tarea_1)
   
    task_1 = PythonOperator(
        task_id="Consultando_API",
        python_callable=consultar_api,
        op_args=["MLA1055"]
    )
    task_2 = PythonOperator(
        task_id="Generando_oferta",
        python_callable=ofertando,
        provide_context=True  # Esto es necesario para acceder a kwargs

    )
    
    task_1 >> task_2