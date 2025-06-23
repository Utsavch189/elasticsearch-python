from fields_map import fields

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
            field = fields[field.strip()]
            if fields.get(value.strip()):
                return {"app_level_filter":cond}
            value = float(value.strip().replace('%', ''))
            if op_map[op] == 'eq':
                return {"term": {field: value}}
            else:
                return {"range": {field: {op_map[op]: value}}}
    raise ValueError(f"Invalid condition: {cond}")

def get_fields(cond,left_side=True):
    
    for op in ['>=', '<=', '>', '<', '==', '=']:
        if op in cond:
            field, value = cond.split(op)
            return fields[field.strip()] if left_side else fields[value.strip()]

def build_query_from_expression(expr):
    expr = expr.replace(" and ", " AND ").replace(" or ", " OR ")
    or_parts = [part.strip() for part in expr.split("OR")]

    should_clauses = []
    source = ["fincode", "compname", "symbol", "industry", "market_cap"]
    categories = []
    app_level_filters = []

    for part in or_parts:
        if "AND" in part:
            and_conditions = [c.strip() for c in part.split("AND")]
            must_clauses = []
            for cond in and_conditions:
                try:
                    clause = parse_comparison(cond)
                    if clause.get('app_level_filter'):
                        app_level_filters.append({'stmnt':clause.get('app_level_filter').strip(),'opt':'AND'})
                        source.append(get_fields(cond))
                        categories.append(get_fields(cond).split('.')[0])

                        source.append(get_fields(cond,left_side=False))
                        categories.append(get_fields(cond,left_side=False).split('.')[0])
                    else:
                        must_clauses.append(clause)
                        source.append(get_fields(cond))
                        categories.append(get_fields(cond).split('.')[0])
                except ValueError:
                    display_field = fields.get(cond.strip())
                    if display_field:
                        source.append(display_field)
                        categories.append(display_field.split('.')[0])
            if len(must_clauses)!=0:
                should_clauses.append({"bool": {"must": must_clauses}})
        else:
            try:
                clause = parse_comparison(part)
                if clause.get('app_level_filter'):
                    app_level_filters.append({'stmnt':clause.get('app_level_filter').strip(),'opt':'OR'})
                    source.append(get_fields(part))
                    categories.append(get_fields(part).split('.')[0])

                    source.append(get_fields(part,left_side=False))
                    categories.append(get_fields(part,left_side=False).split('.')[0])
                else:
                    should_clauses.append(clause)
                    source.append(get_fields(part))
                    categories.append(get_fields(part).split('.')[0])
            except ValueError:
                display_field = fields.get(part.strip())
                if display_field:
                    source.append(display_field)
                    categories.append(display_field.split('.')[0])

    if not len(should_clauses):
        return True,app_level_filters,list(set(categories)), {
            "_source": list(set(source)),
            "query": {
                "bool": {
                     "filter": [ 
                        { "term": { "data_type.keyword": "merged" } },
                        { "term": { "quarter.year_end": 202503 }},
                        { "term": { "nse_sublisting.keyword": "Active" }},
                        { "exists": { "field": "symbol" }},

                    ]
                }
            }
        }
    else:
        return False,app_level_filters,list(set(categories)), {
            "_source": list(set(source)),
            "query": {
                "bool": {
                     "filter": [ 
                        { "term": { "data_type.keyword": "merged" } },
                        { "term": { "quarter.year_end": 202503 }},
                        { "term": { "nse_sublisting.keyword": "Active" }},
                        { "exists": { "field": "symbol" }},

                    ],
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            }
        }