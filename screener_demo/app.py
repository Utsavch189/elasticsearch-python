from flask import Flask,request
from elastic import es
from query_builder import build_query_from_expression
from fields_map import fields_reverse
import operator
import re

app = Flask(__name__)

OPS = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "=": operator.eq,
    "==": operator.eq,
}

def get_field_operator_value(condition:str)->list:
    pattern = r'(<=|>=|<|>|=|==)'
    parts = re.split(pattern, condition)
    return parts[0].strip(),parts[1].strip(),parts[2].strip()

def paginate(items, page=1, per_page=20):
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end]

@app.post("/screener")
def screener():
    # page = 1
    # page_size = 105
    # offset = (page - 1) * page_size

    query = request.json.get('query')
    only_app_filter,app_level_filters,categories,expression = build_query_from_expression(
        expr=query
    )
    print("EXPR ===> ",expression)
    print("APP LEVEL FILTERS ==> ",app_level_filters)
    response = es.search(
        index="financial_data_screener", 
        body=expression,
        size=10000
    )
    
    res = []
    total = response['hits']['total']['value']

    app_level_total = 0
    
    for hit in response['hits']['hits']:
        source = hit['_source']

        for cate in categories:
            c = source.pop(cate, {})
            if c:
                c = {fields_reverse[cate+"."+key]:val for key,val in c.items()}
                source.update(c)

        if not len(app_level_filters):
            res.append(source)
        else:
            for filters in app_level_filters:
                statement = filters.get('stmnt')
                opt = filters.get('opt')
                left_part,operators,right_part = get_field_operator_value(statement.strip())
                left_part_val = source.get(left_part)
                right_part_val = source.get(right_part)

                if opt == 'AND':
                    if left_part_val is not None and right_part_val is not None:
                        if OPS[operators](float(left_part_val),float(right_part_val)) == True:
                            res.append(source) 
                            app_level_total += 1
                else:
                    res.append(source) 
                    app_level_total += 1
    
    data = paginate(res,page=1,per_page=20)

    return {"count":len(res),"data":data}

if __name__ == "__main__":
    app.run(debug=True,port=4000)