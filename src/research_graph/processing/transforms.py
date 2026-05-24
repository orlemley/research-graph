import json
import html
import unicodedata
import ftfy


def get_concepts_dict(paper):
    return {strip_openalex_id(concept.get('id', None)):concept.get('score', 0.0) for concept in (paper.get('concepts', []) or [])}


def strip_openalex_id(id):
    return id.split("/")[-1] if isinstance(id, str) else None


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
        word = html.unescape(word)
        word = unicodedata.normalize("NFKC", word)

        for position in positions:
            tokens[position] = word

    text = [tokens[i] for i in sorted(tokens)]
    text = " ".join(text)

    #Fixes corrupted unicode
    text = ftfy.fix_text(text)

    for prefix in ["<JATS", "</JATS", "<jats:", "</jats:"]:
        while prefix in text:
            start = text.find(prefix)
            end = text.find(">", start)

            if end == -1:
                break

            text = text[:start] + text[end + 1:]

    return text


def get_works_row(paper):
    work_id = strip_openalex_id(paper.get('id', None))
    venue_id = strip_openalex_id((((paper.get('primary_location', {}) or {}).get('source', {}) or {}).get('id', None)))
    title = paper.get('title', None)
    abstract_inverted_index = paper.get('abstract_inverted_index', None)
    return {
        'work_id': work_id,
        'publication_year': paper.get('publication_year', None),
        'title': ftfy.fix_text(title),
        'abstract_inverted_index': json.dumps(abstract_inverted_index) if abstract_inverted_index is not None else None,
        'abstract_text': invert_abstract(abstract_inverted_index),
        'cited_by_count': paper.get('cited_by_count', None),
        'referenced_works_count': paper.get('referenced_works_count', None),
        'venue_id': venue_id,
        'work_type': paper.get('type', None),
        'language': paper.get('language', None),
        'doi': paper.get('doi', None)
    }


def get_citation_edges_rows(paper):
    citation_edges_rows = []
    referenced_works = paper.get('referenced_works', [])
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
    for authorship in (paper.get('authorships', []) or []):
        author_id = strip_openalex_id((authorship.get('author', {}) or {}).get('id', None))
        authorship_row = {
            'work_id': work_id,
            'author_id': author_id,
            'author_position': authorship.get('author_position', None),
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
            institution_name = institution.get('display_name', None)
            institution_country = institution.get('country_code', None)
            institutions_row = {
                'institution_id': institution_id,
                'institution_name': institution_name,
                'country': institution_country
            }
            institutions_rows.append(institutions_row)
    return institutions_rows


def get_venues_row(paper):
    venue_id = strip_openalex_id((((paper.get('primary_location', {}) or {}).get('source', {}) or {}).get('id', None)))
    venue_name = (((((paper.get('primary_location', {}) or {})).get('source', {}) or {}))).get('display_name', None)
    venue_type = (((((paper.get('primary_location', {}) or {})).get('source', {}) or {}))).get('type', None)
    venues_row = {
        'venue_id': venue_id,
        'venue_name': venue_name,
        'venue_type': venue_type
    }
    return venues_row


def get_concepts_rows(paper, concept_score_threshold):
    concepts_rows = []

    for concept in (paper.get('concepts',[]) or []):
        if (concept.get('score', 0.0) or 0.0) >= concept_score_threshold:
            concept_id = strip_openalex_id(concept.get('id', None))
            concepts_row = {
                'concept_id': concept_id,
                'concept_name': concept.get('display_name', None),
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
        'author_name': author.get('display_name', None),
        'first_publication_year': author.get('first_publication_year', None),
        'last_publication_year': author.get('last_publication_year', None),
        'works_count': author.get('works_count', None),
        'cited_by_count': author.get('cited_by_count', None),
        'h_index': (author.get('summary_stats', {}) or {}).get('h_index', None)
    }