from elasticsearch import helpers
from elastic import es


def upsert_category(company_docs, category):
    actions = []
    for doc in company_docs:
        doc_id = f"{doc['fincode']}_{doc['fiscal_year']}_{doc['type']}"
        category_data = {k: v for k, v in doc.items() if k not in ['company_id', 'fiscal_year', 'type']}
        body = {
            "doc": {
                category: category_data
            },
            "doc_as_upsert": True
        }
        actions.append({
            "_op_type": "update",
            "_index": "financial_data",
            "_id": doc_id,
            **body
        })
    helpers.bulk(es, actions)

if __name__ == "__main__":
    import json

    with open("ratio.json") as f:
        ratio_docs = json.load(f)
    upsert_category(ratio_docs, "ratios")

    with open("growth.json") as f:
        growth_docs = json.load(f)
    upsert_category(growth_docs, "growth")