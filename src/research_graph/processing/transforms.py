import json
import html
import unicodedata
import ftfy
import logging


logger = logging.getLogger(__name__)


def get_concepts_dict(paper):
    return {strip_openalex_id(concept.get('id', None)):concept.get('score', 0.0) for concept in (paper.get('concepts', []) or [])}


def strip_openalex_id(id):
    return id.split("/")[-1] if isinstance(id, str) else None


def basic_text_clean(raw_text):
    if raw_text is None:
        return None
    
    text = html.unescape(raw_text)
    text = ftfy.fix_text(text) #Fixes corrupted unicode
    text = unicodedata.normalize("NFC", text)
    text = text.strip()

    return text

#Dictionary approach is n*log(n) time because of sort. List is faster but "risky" in case inverted index contains very high incorrect position
def invert_abstract(inverted):
    if not inverted or not any(inverted.values()):
        return ""

    '''tokens = [""] * (max(map(max, filter(None, inverted.values()))) + 1)

    for word, positions in inverted.items():
        for pos in positions:
            tokens[pos] = word

    return " ".join(tokens)'''
    
    tokens = {}
    
    for word, positions in inverted.items():
        for position in positions or []:
            if position in tokens:
                logger.warning(f"Multiple words found for position {position} in abstract; keeping first")
                continue
            tokens[position] = word

    if not tokens:
        return ""

    text = " ".join(tokens[i] for i in sorted(tokens))

    text = basic_text_clean(text) or ""

    return text


def get_works_row(paper, include_abstract_inverted):
    work_id = strip_openalex_id(paper.get('id', None))
    source_id = strip_openalex_id((((paper.get('primary_location', {}) or {}).get('source', {}) or {}).get('id', None)))
    raw_title = paper.get('title', None)
    title = basic_text_clean(raw_title) if raw_title is not None else None
    abstract_inverted_index = paper.get('abstract_inverted_index', None)
    abstract_text = invert_abstract(abstract_inverted_index)
    works_row = {
        'work_id': work_id,
        'publication_year': paper.get('publication_year', None),
        'publication_date': paper.get('publication_date', None),
        'title': title,
        'abstract_text': abstract_text,
        'title_length': len(title) if title else 0,
        'abstract_length': len(abstract_text) if abstract_text else 0,
        'cited_by_count': paper.get('cited_by_count', None),
        'referenced_works_count': paper.get('referenced_works_count', None),
        'fwci': paper.get('fwci', None),
        'source_id': source_id,
        'work_type': paper.get('type', None),
        'is_retracted': paper.get('is_retracted', None),
        'is_paratext': paper.get('is_paratext', None),
        'language': paper.get('language', None),
        'doi': paper.get('doi', None),
        'ids': json.dumps(paper.get('ids', {}) or {}),
        'is_open_access': (paper.get('open_access', {}) or {}).get('is_oa', None),
        'open_access_url': (paper.get('open_access', {}) or {}).get('oa_url', None),
        'landing_page_url': (((paper.get('primary_location', {}) or {}).get('landing_page_url', None))),
        'pdf_url': (((paper.get('primary_location', {}) or {}).get('pdf_url', None))),
        'created_date': paper.get('created_date', None),
        'updated_date': paper.get('updated_date', None)
    }
    if include_abstract_inverted:
        works_row["abstract_inverted_index"] = json.dumps(abstract_inverted_index) if abstract_inverted_index is not None else None
    return works_row


def get_citation_edges_rows(paper):
    citation_edges_rows = []
    referenced_works = paper.get('referenced_works', []) or []
    citing_work_id = strip_openalex_id(paper.get('id', None))
    for referenced_work_id in referenced_works:
        citation_edges_row = {
            'cited_work_id': strip_openalex_id(referenced_work_id),
            'citing_work_id': citing_work_id
        }
        citation_edges_rows.append(citation_edges_row)
    return citation_edges_rows


def get_authorships_rows(paper):
    authorships_rows = []
    work_id = strip_openalex_id(paper.get('id', None))
    for index, authorship in enumerate(paper.get('authorships', []) or []):
        author_id = strip_openalex_id((authorship.get('author', {}) or {}).get('id', None))
        authorship_row = {
            'work_id': work_id,
            'author_id': author_id,
            'author_position': authorship.get('author_position', None),
            'author_sequence': index,
            'is_corresponding': authorship.get('is_corresponding', None)
        }
        authorships_rows.append(authorship_row)
    return authorships_rows


def get_affiliations_rows(paper):
    affiliations_rows = []
    work_id = strip_openalex_id(paper.get('id', None))
    for authorship in (paper.get('authorships', []) or []):
        author_id = strip_openalex_id((authorship.get('author', {}) or {}).get('id', None))
        for institution in (authorship.get('institutions', []) or []):
            institution_id = strip_openalex_id(institution.get('id', None))
            affiliations_row = {
                'work_id': work_id,
                'author_id': author_id,
                'institution_id': institution_id
            }
            affiliations_rows.append(affiliations_row)
    return affiliations_rows


def get_institutions_rows(paper):
    institutions_rows = []
    for authorship in (paper.get('authorships', []) or []):
        for institution in (authorship.get('institutions', []) or []):
            institution_id = strip_openalex_id(institution.get('id', None))
            institution_name = basic_text_clean(institution.get('display_name', None))
            institution_country = institution.get('country_code', None)
            institutions_row = {
                'institution_id': institution_id,
                'institution_name': institution_name,
                'country': institution_country
            }
            institutions_rows.append(institutions_row)
    return institutions_rows


def get_sources_row(paper):
    source = (((paper.get('primary_location', {}) or {})).get('source', {}) or {})
    if not source:
        return None
    source_id = strip_openalex_id(source.get('id', None))
    source_name = basic_text_clean(source.get('display_name', None))
    source_type = source.get('type', None)
    linking_issn = source.get('issn_l', None)
    issn_list = json.dumps(source.get('issn', []) or [])
    host_organization = strip_openalex_id(source.get('host_organization', None))
    is_oa = source.get('is_oa', None)
    sources_row = {
        'source_id': source_id,
        'source_name': source_name,
        'source_type': source_type,
        'linking_issn': linking_issn,
        'issn_list': issn_list,
        'host_organization_id': host_organization,
        'is_oa': is_oa
    }
    return sources_row


def get_concepts_rows(paper, concept_score_threshold):
    concepts_rows = []

    for concept in (paper.get('concepts',[]) or []):
        if (concept.get('score', 0.0) or 0.0) >= concept_score_threshold:
            concept_id = strip_openalex_id(concept.get('id', None))
            concepts_row = {
                'concept_id': concept_id,
                'concept_name': basic_text_clean(concept.get('display_name', None)),
                'concept_level': concept.get('level', None),
            }
            concepts_rows.append(concepts_row)
    return concepts_rows


def get_scores_rows(paper, concept_score_threshold):
    scores_rows = []
    work_id = strip_openalex_id(paper.get('id', None))
    
    for concept in (paper.get('concepts',[]) or []):
        if (concept.get('score', 0.0) or 0.0) >= concept_score_threshold:
            concept_id = strip_openalex_id(concept.get('id', None))
            scores_row = {
                'work_id': work_id,
                'concept_id': concept_id,
                'concept_score': concept.get('score', None)
            }
            scores_rows.append(scores_row)
    return scores_rows


def get_selected_scores_rows(paper,selected_concepts):
    concepts_dict = get_concepts_dict(paper)
    work_id = strip_openalex_id(paper.get('id', None))
    selected_scores_rows = []
    if selected_concepts["get_all_concepts"]:
        for concept_id in concepts_dict:
            selected_scores_row = {
                'work_id': work_id,
                'concept_id': concept_id,
                'concept_score': concepts_dict.get(concept_id, 0.0)
            }
            selected_scores_rows.append(selected_scores_row)
    else:
        for concept_id in selected_concepts['concept_ids']:
            selected_scores_row = {
                'work_id': work_id,
                'concept_id': concept_id,
                'concept_score': concepts_dict.get(concept_id, 0.0)
            }
            selected_scores_rows.append(selected_scores_row)
    return selected_scores_rows



def get_authors_row(author):
    return {
        'author_id': strip_openalex_id(author.get('id', None)),
        'author_name': basic_text_clean(author.get('display_name', None)),
        'first_publication_year': author.get('first_publication_year', None),
        'last_publication_year': author.get('last_publication_year', None),
        'works_count': author.get('works_count', None),
        'cited_by_count': author.get('cited_by_count', None),
        'h_index': (author.get('summary_stats', {}) or {}).get('h_index', None)
    }