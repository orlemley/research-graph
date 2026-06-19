TABLES_INFO = {
    "concepts": {
        "deduplication_mode": "key",
        "deduplication_key_sql": "concept_id",
        "order_by_sql": "snapshot_date DESC NULLS LAST, concept_name IS NOT NULL DESC, length(concept_name) DESC",
        "buckets_count": 16
        },
    "institutions": {
        "deduplication_mode": "key",
        "deduplication_key_sql": "institution_id",
        "order_by_sql": "snapshot_date DESC NULLS LAST, institution_name IS NOT NULL DESC, length(institution_name) DESC",
        "buckets_count": 8
        },
    "sources": {
        "deduplication_mode": "key",
        "deduplication_key_sql": "source_id",
         "order_by_sql": "snapshot_date DESC NULLS LAST, source_name IS NOT NULL DESC, length(source_name) DESC",
         "buckets_count": 8
         },
    "authors": {
        "deduplication_mode": "key",
        "deduplication_key_sql": "author_id",
        "order_by_sql": "snapshot_date DESC NULLS LAST, author_name IS NOT NULL DESC, length(author_name) DESC",
        "buckets_count": 8
        },
    "works": {
        "deduplication_mode": "key",
        "deduplication_key_sql": "work_id",
        "order_by_sql": "snapshot_date DESC NULLS LAST, cited_by_count DESC NULLS LAST",
        "buckets_count": 256
    },
    "affiliations": {
        "deduplication_mode": "row",
        "deduplication_key_sql": "work_id, author_id, institution_id",
        "order_by_sql": "snapshot_date DESC NULLS LAST",
        "buckets_count": 8,
    },
    "authorships": {
        "deduplication_mode": "key",
        "deduplication_key_sql": "work_id, author_id",
        "order_by_sql": """
            snapshot_date DESC NULLS LAST,
            is_corresponding DESC NULLS LAST,
            CASE author_position
                WHEN 'first' THEN 1
                WHEN 'last' THEN 2
                WHEN 'middle' THEN 3
                ELSE 4
            END
        """,
        "buckets_count": 8,
    },
    "citation_edges": {
        "deduplication_mode": "row",
        "deduplication_key_sql": "citing_work_id, cited_work_id",
        "order_by_sql": "snapshot_date DESC NULLS LAST",
        "buckets_count": 64,
    },
    "scores": {
        "deduplication_mode": "key",
        "deduplication_key_sql": "work_id, concept_id",
        "order_by_sql": "snapshot_date DESC NULLS LAST, concept_score DESC NULLS LAST",
        "buckets_count": 64,
    },
    "selected_scores": {
        "deduplication_mode": "key",
        "deduplication_key_sql": "work_id, concept_id",
        "order_by_sql": "snapshot_date DESC NULLS LAST, concept_score DESC NULLS LAST",
        "buckets_count": 32,
    }
}