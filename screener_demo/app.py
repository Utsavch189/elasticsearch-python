from flask import Flask,request
from elastic import es
from query_builder import build_query_from_expression
from fields_map import fields_reverse

app = Flask(__name__)

@app.post("/screener")
def screener():
    page = 1
    page_size = 105
    offset = (page - 1) * page_size

    query = request.json.get('query')
    categories,expression = build_query_from_expression(
        expr=query
    )
    print(expression)
    response = es.search(
        index="financial_data_screener", 
        body=expression,
        from_=offset,
        size=page_size
    )
    
    res = []
    total = response['hits']['total']['value']
    
    for hit in response['hits']['hits']:
        source = hit['_source']
        for cate in categories:
            c = source.pop(cate, {})
            if c:
                c = {fields_reverse[cate+"."+key]:val for key,val in c.items()}
                source.update(c)
        res.append(source) 

    return {"count":total,"res":res}

if __name__ == "__main__":
    app.run(debug=True,port=4000)