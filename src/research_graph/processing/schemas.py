import pyarrow as pa


WORKS_SCHEMA = pa.schema([
    pa.field('work_id', pa.string()),
    pa.field('publication_year', pa.int32()),
    pa.field('publication_date', pa.string()),
    pa.field('title', pa.string()),
    pa.field('abstract_text', pa.string()),
    pa.field('title_length', pa.int32()),
    pa.field('abstract_length', pa.int32()),
    pa.field('cited_by_count', pa.int32()),
    pa.field('referenced_works_count', pa.int32()),
    pa.field('fwci', pa.float64()),
    pa.field('source_id', pa.string()),
    pa.field('work_type', pa.string()),
    pa.field('is_retracted', pa.bool_()),
    pa.field('is_paratext', pa.bool_()),
    pa.field('language', pa.string()),
    pa.field('doi', pa.string()),
    pa.field('ids', pa.string()),
    pa.field('is_open_access', pa.bool_()),
    pa.field('open_access_url', pa.string()),
    pa.field('landing_page_url', pa.string()),
    pa.field('pdf_url', pa.string()),
    pa.field('created_date', pa.string()),
    pa.field('updated_date', pa.string())
])
CITATION_EDGES_SCHEMA = pa.schema([
    pa.field('cited_work_id', pa.string()),
    pa.field('citing_work_id', pa.string())
])
AUTHORSHIPS_SCHEMA = pa.schema([
    pa.field('work_id', pa.string()),
    pa.field('author_id', pa.string()),
    pa.field('author_position', pa.string()),
    pa.field('author_sequence', pa.int32()),
    pa.field('is_corresponding', pa.bool_())
])
AFFILIATIONS_SCHEMA = pa.schema([
    pa.field('work_id', pa.string()),
    pa.field('author_id', pa.string()),
    pa.field('institution_id', pa.string())
])
INSTITUTIONS_SCHEMA = pa.schema([
    pa.field('institution_id', pa.string()),
    pa.field('institution_name', pa.string()),
    pa.field('country', pa.string())
])
SOURCES_SCHEMA = pa.schema([
    pa.field('source_id', pa.string()),
    pa.field('source_name', pa.string()),
    pa.field('source_type', pa.string()),
    pa.field('linking_issn', pa.string()),
    pa.field('issn_list', pa.string()),
    pa.field('host_organization_id', pa.string()),
    pa.field('is_oa', pa.bool_())
])
CONCEPTS_SCHEMA = pa.schema([
    pa.field('concept_id', pa.string()),
    pa.field('concept_name', pa.string()),
    pa.field('concept_level', pa.int32())
])
SCORES_SCHEMA = pa.schema([
    pa.field('work_id', pa.string()),
    pa.field('concept_id', pa.string()),
    pa.field('concept_score', pa.float64())
])
SELECTED_SCORES_SCHEMA = pa.schema([
    pa.field('work_id', pa.string()),
    pa.field('concept_id', pa.string()),
    pa.field('concept_score', pa.float64())
])
AUTHORS_SCHEMA = pa.schema([
    pa.field('author_id', pa.string()),
    pa.field('author_name', pa.string()),
    pa.field('first_publication_year', pa.int32()),
    pa.field('last_publication_year', pa.int32()),
    pa.field('works_count', pa.int32()),
    pa.field('cited_by_count', pa.int32()),
    pa.field('h_index', pa.int32())
])
ABSTRACT_INVERTED_INDEX_FIELD = pa.field('abstract_inverted_index', pa.string())