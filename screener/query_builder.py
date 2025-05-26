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

def build_query_from_expression(expr):
    expr = expr.lower().replace(" and ", " AND ").replace(" or ", " OR ")

    or_parts = [part.strip() for part in expr.split("OR")]

    should_clauses = []
    for part in or_parts:
        if "AND" in part:
            and_conditions = [c.strip() for c in part.split("AND")]
            must_clauses = [parse_comparison(c) for c in and_conditions]
            should_clauses.append({"bool": {"must": must_clauses}})
        else:
            should_clauses.append(parse_comparison(part))

    return {
        "query": {
            "bool": {
                # "filter": [  # mandatory filter clause
                #     {"term": {"year_end_x": 202303}}
                # ],
                "should": should_clauses,
                "minimum_should_match": 1
            }
        }
    }

start = time.time()
# Example usage
expression = "net_profit_growth_percentage_cons_2year > 10% AND net_margin_growth_percentage_cons_3year > 20%"
query = build_query_from_expression(expression)
print(expression)
# Elasticsearch call
response = es.search(index="financial_reports", body=query,size=300)

res = []
# Print results
for hit in response['hits']['hits']:
    res.append(hit['_source'])

print("res ====> ",len(res))
print("Total time in seconds ===>  ",time.time()-start)