from elastic import es
import time

def parse_comparison(cond):
    op_map = {
        '>=': 'gte',
        '<=': 'lte',
        '>': 'gt',
        '<': 'lt',
        '=': 'eq',
        '==': 'eq'
    }
    for op in ['>=', '<=', '>', '<', '==', '=']:
        if op in cond:
            field, value = cond.split(op)
            field = field.strip().replace(" ", "_")
            value = float(value.strip().replace('%', ''))
            if op_map[op] == 'eq':
                return {"term": {field: value}}
            else:
                return {"range": {field: {op_map[op]: value}}}
    raise ValueError(f"Invalid condition: {cond}")

def get_fields(cond):
    op_map = {
        '>=': 'gte',
        '<=': 'lte',
        '>': 'gt',
        '<': 'lt',
        '=': 'eq',
        '==': 'eq'
    }
    
    for op in ['>=', '<=', '>', '<', '==', '=']:
        if op in cond:
            field, value = cond.split(op)
            field = field.strip().replace(" ", "_")
            return field

def build_query_from_expression(expr):
    expr = expr.lower().replace(" and ", " AND ").replace(" or ", " OR ")

    or_parts = [part.strip() for part in expr.split("OR")]

    should_clauses = []
    source = ["fincode","year_end_x"]

    for part in or_parts:
        if "AND" in part:
            and_conditions = [c.strip() for c in part.split("AND")]
            must_clauses = [parse_comparison(c) for c in and_conditions]
            should_clauses.append({"bool": {"must": must_clauses}})
            for f in and_conditions:
                source.append(get_fields(f))
        else:
            should_clauses.append(parse_comparison(part))
            source.append(get_fields(part))

    return {
        "_source": source,
        "query": {
            "bool": {
                # "filter": [ 
                #     {"term": {"year_end_x": 202303}}
                # ],
                "should": should_clauses,
                "minimum_should_match": 1
            }
        }
    }

if __name__=="__main__":
    start = time.time()
    # Example usage
    expression = "net_profit_growth_percentage_cons_2year > 10% OR net_margin_growth_percentage_cons_3year > 20%"
    query = build_query_from_expression(expression)
    # print(expression)

    response = es.search(index="financial_reports", body=query,size=2000)

    res = []

    for hit in response['hits']['hits']:
        res.append(hit['_source'])

    print("res length ====> ",len(res))
    # print("res ====> ",res)
    print("Total time in seconds ===>  ",time.time()-start)