import pandas
import json
import numpy as np
import lancedb

db = lancedb.connect("s3://mediumarticle")
table = db.open_table("medium_articles")

def handler(event, context):
    status_code = 200

    if event['query_vector'] is None:
        status_code = 404
        return {
            "statusCode": status_code,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "Error ": "No vector to query was issued"
            })
        }
    
    # Shape of medium_artilces dataset is (64,20000), d=float32
    query_vector = np.array(event['query_vector'], dtype=np.float32)
    
    # 3 best matches	
    rs = table.search(query_vector).limit(3).to_df()

    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": rs.to_json()
    }
