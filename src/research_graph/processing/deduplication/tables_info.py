REFERENCE_TABLES = {
    "concepts": {
        "id": "concept_id",
        "sortby": "concept_name",
        "buckets_count": 16
        },
    "institutions": {
        "id": "institution_id",
        "sortby": "institution_name",
        "buckets_count": 8
        },
    "sources": {
        "id": "source_id",
         "sortby": "source_name",
         "buckets_count": 8
         },
    "authors": {
        "id": "author_id",
        "sortby": "author_name",
        "buckets_count": 8
        },
    "works": {
        "id": "work_id",
        "sortby": "cited_by_count",
        "buckets_count": 256
    }
}

RELATIONSHIP_TABLES = {
    "affiliations": 8,
    "authorships": 16,
    "citation_edges": 64,
    "scores": 64,
    "selected_scores": 8
}