import pyarrow as pa


WORKS_SCHEMA = pa.schema([
    pa.field('work_id', pa.string()),
    pa.field('publication_year', pa.int32()),
    pa.field('title', pa.string()),
    pa.field('abstract_inverted_index', pa.string()),
    pa.field('abstract_text', pa.string()),
    pa.field('cited_by_count', pa.int32()),
    pa.field('referenced_works_count', pa.int32()),
    pa.field('venue_id', pa.string()),
    pa.field('work_type', pa.string()),
    pa.field('language', pa.string()),
    pa.field('doi', pa.string())
])
CITATION_EDGES_SCHEMA = pa.schema([
    pa.field('cited_work_id', pa.string()),
    pa.field('citing_work_id', pa.string())
])
AUTHORSHIPS_SCHEMA = pa.schema([
    pa.field('work_id', pa.string()),
    pa.field('author_id', pa.string()),
    pa.field('author_position', pa.string()),
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

VENUES_SCHEMA = pa.schema([
    pa.field('venue_id', pa.string()),
    pa.field('venue_name', pa.string()),
    pa.field('venue_type', pa.string())
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