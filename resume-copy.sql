-- Resume loading from the works table
-- This script clears the works table first to ensure no partial data remains from previous aborted attempts

TRUNCATE openalex.works;

--works

\copy openalex.works (id, doi, title, display_name, publication_year, publication_date, type, cited_by_count, is_retracted, is_paratext, cited_by_api_url, abstract_inverted_index, language) from program 'gunzip -c /app/csv-files/works.csv.gz' csv header
\copy openalex.works_primary_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) from program 'gunzip -c /app/csv-files/works_primary_locations.csv.gz' csv header
\copy openalex.works_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) from program 'gunzip -c /app/csv-files/works_locations.csv.gz' csv header
\copy openalex.works_best_oa_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) from program 'gunzip -c /app/csv-files/works_best_oa_locations.csv.gz' csv header
\copy openalex.works_authorships (work_id, author_position, author_id, institution_id, raw_affiliation_string) from program 'gunzip -c /app/csv-files/works_authorships.csv.gz' csv header
\copy openalex.works_biblio (work_id, volume, issue, first_page, last_page) from program 'gunzip -c /app/csv-files/works_biblio.csv.gz' csv header
\copy openalex.works_topics (work_id, topic_id, score) from program 'gunzip -c /app/csv-files/works_topics.csv.gz' csv header
\copy openalex.works_concepts (work_id, concept_id, score) from program 'gunzip -c /app/csv-files/works_concepts.csv.gz' csv header
\copy openalex.works_ids (work_id, openalex, doi, mag, pmid, pmcid) from program 'gunzip -c /app/csv-files/works_ids.csv.gz' csv header
\copy openalex.works_mesh (work_id, descriptor_ui, descriptor_name, qualifier_ui, qualifier_name, is_major_topic) from program 'gunzip -c /app/csv-files/works_mesh.csv.gz' csv header
\copy openalex.works_open_access (work_id, is_oa, oa_status, oa_url, any_repository_has_fulltext) from program 'gunzip -c /app/csv-files/works_open_access.csv.gz' csv header
\copy openalex.works_referenced_works (work_id, referenced_work_id) from program 'gunzip -c /app/csv-files/works_referenced_works.csv.gz' csv header
\copy openalex.works_related_works (work_id, related_work_id) from program 'gunzip -c /app/csv-files/works_related_works.csv.gz' csv header