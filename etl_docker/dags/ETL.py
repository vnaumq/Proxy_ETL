from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import ping3
import time
from google_authorization import google_json

args = {
    'owner': 'Vnaumq',
    'start_date': days_ago(1) # make start date in the past
}

credentials_info = google_json()
credentials = service_account.Credentials.from_service_account_info(credentials_info)

def export_table():
    
    project_id = 'testvizuators'
    client = bigquery.Client(credentials= credentials,project=project_id)
    query_job = client.query("""
            SELECT IP 
            FROM testvizuators.ip_dataset.ip_table    
                            """)
    results = query_job.result()
    ip_gbq = []
    for i in results:
        for j in i:
            ip_gbq.append(j)
    len(ip_gbq)
    # Create an URL object
    url = 'https://free-proxy-list.net/'
    # Create object page
    page = requests.get(url)
    # parser-lxml = Change html to Python friendly format
    # Obtain page's information
    soup = BeautifulSoup(page.text, "html.parser")
    proxy = soup.find('table', class_ = 'table table-striped table-bordered')
    # Obtain every title of columns with tag <th>
    headers = []
    for i in proxy.find_all('th'):
        title = i.text
        headers.append(title)
    headers
    # Create a dataframe
    df = pd.DataFrame(columns=headers)
    rows = proxy.find_all('tr'[0:])
    for row in rows:
        data = row.find_all('td')
        row = [i.text for i in data]
        if len(row) == len(headers) and row[0] not in ip_gbq: 
            df.loc[ len(df.index )] = row
    df = df.to_json()
    return df


def transform_table(**context):
    json_data = context['ti'].xcom_pull(task_ids='export')
    df = pd.read_json(json_data)
    ping_result = []
    a=0
    for ip in df['IP Address']:
        a+=1
        result =  ping3.ping(ip, timeout=4)
        if result is not None:
            print(f'{ip} - ping successful {a}/{df.shape[0]} .')
            ping_result.append(True)
        else:
            print(f'{ip} - ping failed {a}/{df.shape[0]} .')
            ping_result.append(False)
    df['Ping'] = ping_result
    print(df['Ping'])
    print('Ready')
    df.drop(['Last Checked'], axis=1, inplace=True)
    df = df.loc[df['Ping'] == True]
    df.drop(['Ping'], axis=1, inplace=True)
    df.rename(columns={'IP Address' : 'IP'},inplace=True)
    df = df.to_json()
    return df

def load_table(**context):
    json_data = context['ti'].xcom_pull(task_ids='transform')
    df = pd.read_json(json_data)
    df['Port'] = df['Port'].astype(str)
    print(df.dtypes)
    project_id = 'testvizuators'
    client = bigquery.Client(credentials= credentials,project=project_id)
    job_config = bigquery.LoadJobConfig(
        autodetect = True,
        write_disposition = 'WRITE_APPEND'
    )
    print(df.dtypes)
    target_tale_id = 'testvizuators.ip_dataset.ip_table'    
    job = client.load_table_from_dataframe(df, target_tale_id, job_config=job_config)
    while job.state != 'DONE':
        time.sleep(2)
        job.reload()
    print(job.result())
    table = client.get_table(target_tale_id)
    print(
        f'Loaded {df.shape[0]} rows to "{target_tale_id}"'
    )
    

with DAG(
    dag_id='proxy_ETL',
    default_args=args,
    schedule_interval='@daily', # make this workflow happen every day
    tags=["proxy_ETL"],
    catchup = False
) as dag:
    
    export = PythonOperator(
        task_id='export',
        python_callable=export_table
    )

    transform = PythonOperator(
        task_id = 'transform',
        python_callable=transform_table
    )

    load = PythonOperator(
        task_id = 'load',
        python_callable=load_table
    )        

#456

export >> transform >> load

