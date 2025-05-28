from flask import Flask,request
from elastic import es
from query_builder import build_query_from_expression

app = Flask(__name__)

@app.post("/screener")
def screener():
    query = request.json.get('query')
    expression = build_query_from_expression(
        expr=query
    )
    response = es.search(index="financial_reports", body=expression,size=2000)
    
    res = []
    
    for hit in response['hits']['hits']:
        res.append(hit['_source'])

    return {"count":len(res),"res":res}

if __name__ == "__main__":
    app.run(port=4000,debug=True)