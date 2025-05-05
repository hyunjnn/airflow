from pandas import json_normalize

__all__ = ["preprocessing"]

# task instance: dag 내의 task 정보를 얻을 수 있는 객체
def preprocessing(ti):
    # cross communication: Operator 간 데이터 전달을 가능하게 하는 도구
    search_result = ti.xcom_pull(task_ids=["crawl_naver"])
    
    if not len(search_result):
        raise ValueError("no search result")
    
    items = search_result[0]["items"]
    processed_items = json_normalize([
        {
            "title" : item["title"],
            "address" : item["address"],
            "category" : item["category"],
            "description" : item["description"],
            "link" : item["link"]
        } for item in items
    ])
    processed_items.to_csv("/opt/airflow/dags/data/processed_result.csv", index=False, header=True)
