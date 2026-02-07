import csv
import glob
import gzip
import json
import os
import shutil
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

SNAPSHOT_DIR = 'openalex-snapshot'
CSV_DIR = 'csv-files'
TEMP_DIR = 'temp-shards'

if not os.path.exists(CSV_DIR):
    os.mkdir(CSV_DIR)

if not os.path.exists(TEMP_DIR):
    os.mkdir(TEMP_DIR)

FILES_PER_ENTITY = int(os.environ.get('OPENALEX_DEMO_FILES_PER_ENTITY', '0'))

csv_files = {
    'authors': {
        'authors': {
            'name': os.path.join(CSV_DIR, 'authors.csv.gz'),
            'columns': [
                'id', 'orcid', 'display_name', 'display_name_alternatives',
                'works_count', 'cited_by_count',
                'last_known_institution', 'works_api_url', 'updated_date',
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'authors_ids.csv.gz'),
            'columns': [
                'author_id', 'openalex', 'orcid', 'scopus', 'twitter',
                'wikipedia', 'mag'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'authors_counts_by_year.csv.gz'),
            'columns': [
                'author_id', 'year', 'works_count', 'cited_by_count',
                'oa_works_count'
            ]
        }
    },
    'concepts': {
        'concepts': {
            'name': os.path.join(CSV_DIR, 'concepts.csv.gz'),
            'columns': [
                'id', 'wikidata', 'display_name', 'level', 'description',
                'works_count', 'cited_by_count', 'image_url',
                'image_thumbnail_url', 'works_api_url', 'updated_date'
            ]
        },
        'ancestors': {
            'name': os.path.join(CSV_DIR, 'concepts_ancestors.csv.gz'),
            'columns': ['concept_id', 'ancestor_id']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'concepts_counts_by_year.csv.gz'),
            'columns': ['concept_id', 'year', 'works_count', 'cited_by_count',
                        'oa_works_count']
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'concepts_ids.csv.gz'),
            'columns': ['concept_id', 'openalex', 'wikidata', 'wikipedia',
                        'umls_aui', 'umls_cui', 'mag']
        },
        'related_concepts': {
            'name': os.path.join(CSV_DIR, 'concepts_related_concepts.csv.gz'),
            'columns': ['concept_id', 'related_concept_id', 'score']
        }
    },
    'topics': {
        'topics': {
            'name': os.path.join(CSV_DIR, 'topics.csv.gz'),
            'columns': ['id', 'display_name', 'subfield_id',
                        'subfield_display_name', 'field_id',
                        'field_display_name',
                        'domain_id', 'domain_display_name', 'description',
                        'keywords', 'works_api_url', 'wikipedia_id',
                        'works_count', 'cited_by_count', 'updated_date', 'siblings']
        }
    },
    'institutions': {
        'institutions': {
            'name': os.path.join(CSV_DIR, 'institutions.csv.gz'),
            'columns': [
                'id', 'ror', 'display_name', 'country_code', 'type',
                'homepage_url', 'image_url', 'image_thumbnail_url',
                'display_name_acronyms', 'display_name_alternatives',
                'works_count', 'cited_by_count', 'works_api_url',
                'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'institutions_ids.csv.gz'),
            'columns': [
                'institution_id', 'openalex', 'ror', 'grid', 'wikipedia',
                'wikidata', 'mag'
            ]
        },
        'geo': {
            'name': os.path.join(CSV_DIR, 'institutions_geo.csv.gz'),
            'columns': [
                'institution_id', 'city', 'geonames_city_id', 'region',
                'country_code', 'country', 'latitude',
                'longitude'
            ]
        },
        'associated_institutions': {
            'name': os.path.join(CSV_DIR,
                                 'institutions_associated_institutions.csv.gz'),
            'columns': [
                'institution_id', 'associated_institution_id', 'relationship'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'institutions_counts_by_year.csv.gz'),
            'columns': [
                'institution_id', 'year', 'works_count', 'cited_by_count',
                'oa_works_count'
            ]
        }
    },
    'publishers': {
        'publishers': {
            'name': os.path.join(CSV_DIR, 'publishers.csv.gz'),
            'columns': [
                'id', 'display_name', 'alternate_titles', 'country_codes',
                'hierarchy_level', 'parent_publisher',
                'works_count', 'cited_by_count', 'sources_api_url',
                'updated_date'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'publishers_counts_by_year.csv.gz'),
            'columns': ['publisher_id', 'year', 'works_count', 'cited_by_count',
                        'oa_works_count']
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'publishers_ids.csv.gz'),
            'columns': ['publisher_id', 'openalex', 'ror', 'wikidata']
        },
    },
    'sources': {
        'sources': {
            'name': os.path.join(CSV_DIR, 'sources.csv.gz'),
            'columns': [
                'id', 'issn_l', 'issn', 'display_name', 'publisher',
                'works_count', 'cited_by_count', 'is_oa',
                'is_in_doaj', 'homepage_url', 'works_api_url', 'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'sources_ids.csv.gz'),
            'columns': ['source_id', 'openalex', 'issn_l', 'issn', 'mag',
                        'wikidata', 'fatcat']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'sources_counts_by_year.csv.gz'),
            'columns': ['source_id', 'year', 'works_count', 'cited_by_count',
                        'oa_works_count']
        },
    },
    'works': {
        'works': {
            'name': os.path.join(CSV_DIR, 'works.csv.gz'),
            'columns': [
                'id', 'doi', 'title', 'display_name', 'publication_year',
                'publication_date', 'type', 'cited_by_count',
                'is_retracted', 'is_paratext', 'cited_by_api_url',
                'abstract_inverted_index', 'language'
            ]
        },
        'primary_locations': {
            'name': os.path.join(CSV_DIR, 'works_primary_locations.csv.gz'),
            'columns': [
                'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa',
                'version', 'license'
            ]
        },
        'locations': {
            'name': os.path.join(CSV_DIR, 'works_locations.csv.gz'),
            'columns': [
                'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa',
                'version', 'license'
            ]
        },
        'best_oa_locations': {
            'name': os.path.join(CSV_DIR, 'works_best_oa_locations.csv.gz'),
            'columns': [
                'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa',
                'version', 'license'
            ]
        },
        'authorships': {
            'name': os.path.join(CSV_DIR, 'works_authorships.csv.gz'),
            'columns': [
                'work_id', 'author_position', 'author_id', 'institution_id',
                'raw_affiliation_string'
            ]
        },
        'biblio': {
            'name': os.path.join(CSV_DIR, 'works_biblio.csv.gz'),
            'columns': [
                'work_id', 'volume', 'issue', 'first_page', 'last_page'
            ]
        },
        'topics': {
            'name': os.path.join(CSV_DIR, 'works_topics.csv.gz'),
            'columns': [
                'work_id', 'topic_id', 'score'
            ]
        },
        'concepts': {
            'name': os.path.join(CSV_DIR, 'works_concepts.csv.gz'),
            'columns': [
                'work_id', 'concept_id', 'score'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'works_ids.csv.gz'),
            'columns': [
                'work_id', 'openalex', 'doi', 'mag', 'pmid', 'pmcid'
            ]
        },
        'mesh': {
            'name': os.path.join(CSV_DIR, 'works_mesh.csv.gz'),
            'columns': [
                'work_id', 'descriptor_ui', 'descriptor_name', 'qualifier_ui',
                'qualifier_name', 'is_major_topic'
            ]
        },
        'open_access': {
            'name': os.path.join(CSV_DIR, 'works_open_access.csv.gz'),
            'columns': [
                'work_id', 'is_oa', 'oa_status', 'oa_url',
                'any_repository_has_fulltext'
            ]
        },
        'referenced_works': {
            'name': os.path.join(CSV_DIR, 'works_referenced_works.csv.gz'),
            'columns': [
                'work_id', 'referenced_work_id'
            ]
        },
        'related_works': {
            'name': os.path.join(CSV_DIR, 'works_related_works.csv.gz'),
            'columns': [
                'work_id', 'related_work_id'
            ]
        },
    },
}

def init_dict_writer(csv_file, file_spec, **kwargs):
    writer = csv.DictWriter(
        csv_file, fieldnames=file_spec['columns'], **kwargs
    )
    writer.writeheader()
    return writer

def merge_shards(entity_type, shard_paths):
    """Merges gzipped shards into final files using binary concatenation."""
    spec = csv_files[entity_type]
    for table_name, table_spec in spec.items():
        final_path = table_spec['name']
        table_shards = [s[table_name] for s in shard_paths if table_name in s]
        
        if not table_shards:
            continue
            
        print(f"Merging {len(table_shards)} shards into {final_path}...")
        
        # Write header to a new file
        with gzip.open(final_path, 'wt', encoding='utf-8') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=table_spec['columns'])
            writer.writeheader()
            
        # Append gzipped shards at binary level
        with open(final_path, 'ab') as f_out:
            for shard_path in table_shards:
                with open(shard_path, 'rb') as f_in:
                    shutil.copyfileobj(f_in, f_out)
                os.remove(shard_path)

def process_single_file(entity_type, jsonl_file_name):
    """Worker function to process a single JSONL file into shards."""
    # Create a unique shard prefix by using the relative path from SNAPSHOT_DIR
    rel_path = os.path.relpath(jsonl_file_name, SNAPSHOT_DIR)
    # Replace slashes and other potentially problematic characters
    unique_name = rel_path.replace(os.sep, '_').replace('.', '_')
    shard_prefix = f"{entity_type}_{unique_name}_"
    file_spec = csv_files[entity_type]
    
    shard_files = {}
    shard_writers = {}
    shard_paths = {table_name: os.path.join(TEMP_DIR, f"{shard_prefix}{table_name}.csv.gz") 
                   for table_name in file_spec}
    
    # Check if this file is already done (resumability)
    if all(os.path.exists(p) and os.path.getsize(p) > 0 for p in shard_paths.values()):
        return shard_paths

    # Open all needed CSV shards for this entity type
    for table_name, table_spec in file_spec.items():
        shard_path = shard_paths[table_name]
        f = gzip.open(shard_path, 'wt', encoding='utf-8')
        shard_files[table_name] = f
        # We ignore extra fields to be robust against schema additions
        shard_writers[table_name] = csv.DictWriter(f, fieldnames=table_spec['columns'], extrasaction='ignore')

    try:
        print(f"Processing {jsonl_file_name}...")
        with gzip.open(jsonl_file_name, 'r') as jsonl_file:
            for line in jsonl_file:
                if not line.strip():
                    continue
                item = json.loads(line)
                
                if entity_type == 'authors':
                    flatten_author_item(item, shard_writers)
                elif entity_type == 'topics':
                    flatten_topic_item(item, shard_writers)
                elif entity_type == 'concepts':
                    flatten_concept_item(item, shard_writers)
                elif entity_type == 'institutions':
                    flatten_institution_item(item, shard_writers)
                elif entity_type == 'publishers':
                    flatten_publisher_item(item, shard_writers)
                elif entity_type == 'sources':
                    flatten_source_item(item, shard_writers)
                elif entity_type == 'works':
                    flatten_work_item(item, shard_writers)
    finally:
        for f in shard_files.values():
            if f: f.close()
            
    return shard_paths

# --- Entity-specific flattening logic (extracted from original script) ---

def flatten_author_item(author, writers):
    if not (author_id := author.get('id')):
        return
    
    author['display_name_alternatives'] = json.dumps(author.get('display_name_alternatives'), ensure_ascii=False)
    author['last_known_institution'] = (author.get('last_known_institution') or {}).get('id')
    writers['authors'].writerow(author)

    if author_ids := author.get('ids'):
        author_ids['author_id'] = author_id
        writers['ids'].writerow(author_ids)

    if counts_by_year := author.get('counts_by_year'):
        for count_by_year in counts_by_year:
            count_by_year['author_id'] = author_id
            writers['counts_by_year'].writerow(count_by_year)

def flatten_topic_item(topic, writers):
    if not (topic_id := topic.get('id')):
        return
    topic['keywords'] = '; '.join(topic.get('keywords') or [])
    for key in ('subfield', 'field', 'domain'):
        if topic.get(key):
            topic[f'{key}_id'] = topic[key].get('id')
            topic[f'{key}_display_name'] = topic[key].get('display_name')
            del topic[key]
    
    # Handle older vs newer OpenAlex schema for 'updated' / 'updated_date'
    if 'updated' in topic:
        topic['updated_date'] = topic['updated']
        del topic['updated']
    
    if topic.get('ids'):
        topic['wikipedia_id'] = topic['ids'].get('wikipedia')
        del topic['ids']
    if 'created_date' in topic: del topic['created_date']
    writers['topics'].writerow(topic)

def flatten_concept_item(concept, writers):
    if not (concept_id := concept.get('id')):
        return
    writers['concepts'].writerow(concept)

    if concept_ids := concept.get('ids'):
        concept_ids['concept_id'] = concept_id
        concept_ids['umls_aui'] = json.dumps(concept_ids.get('umls_aui'), ensure_ascii=False)
        concept_ids['umls_cui'] = json.dumps(concept_ids.get('umls_cui'), ensure_ascii=False)
        writers['ids'].writerow(concept_ids)

    if ancestors := concept.get('ancestors'):
        for ancestor in ancestors:
            if ancestor_id := ancestor.get('id'):
                writers['ancestors'].writerow({'concept_id': concept_id, 'ancestor_id': ancestor_id})

    if counts_by_year := concept.get('counts_by_year'):
        for count_by_year in counts_by_year:
            count_by_year['concept_id'] = concept_id
            writers['counts_by_year'].writerow(count_by_year)

    if related_concepts := concept.get('related_concepts'):
        for related_concept in related_concepts:
            if related_concept_id := related_concept.get('id'):
                writers['related_concepts'].writerow({
                    'concept_id': concept_id,
                    'related_concept_id': related_concept_id,
                    'score': related_concept.get('score')
                })

def flatten_institution_item(institution, writers):
    if not (institution_id := institution.get('id')):
        return
    institution['display_name_acronyms'] = json.dumps(institution.get('display_name_acronyms'), ensure_ascii=False)
    institution['display_name_alternatives'] = json.dumps(institution.get('display_name_alternatives'), ensure_ascii=False)
    writers['institutions'].writerow(institution)

    if institution_ids := institution.get('ids'):
        institution_ids['institution_id'] = institution_id
        writers['ids'].writerow(institution_ids)

    if institution_geo := institution.get('geo'):
        institution_geo['institution_id'] = institution_id
        writers['geo'].writerow(institution_geo)

    associated = institution.get('associated_institutions', institution.get('associated_insitutions'))
    if associated:
        for assoc in associated:
            if assoc_id := assoc.get('id'):
                writers['associated_institutions'].writerow({
                    'institution_id': institution_id,
                    'associated_institution_id': assoc_id,
                    'relationship': assoc.get('relationship')
                })

    if counts_by_year := institution.get('counts_by_year'):
        for count_by_year in counts_by_year:
            count_by_year['institution_id'] = institution_id
            writers['counts_by_year'].writerow(count_by_year)

def flatten_publisher_item(publisher, writers):
    if not (publisher_id := publisher.get('id')):
        return
    publisher['alternate_titles'] = json.dumps(publisher.get('alternate_titles'), ensure_ascii=False)
    publisher['country_codes'] = json.dumps(publisher.get('country_codes'), ensure_ascii=False)
    writers['publishers'].writerow(publisher)

    if publisher_ids := publisher.get('ids'):
        publisher_ids['publisher_id'] = publisher_id
        writers['ids'].writerow(publisher_ids)

    if counts_by_year := publisher.get('counts_by_year'):
        for count_by_year in counts_by_year:
            count_by_year['publisher_id'] = publisher_id
            writers['counts_by_year'].writerow(count_by_year)

def flatten_source_item(source, writers):
    if not (source_id := source.get('id')):
        return
    source['issn'] = json.dumps(source.get('issn'))
    writers['sources'].writerow(source)

    if source_ids := source.get('ids'):
        source_ids['source_id'] = source_id
        source_ids['issn'] = json.dumps(source_ids.get('issn'))
        writers['ids'].writerow(source_ids)

    if counts_by_year := source.get('counts_by_year'):
        for count_by_year in counts_by_year:
            count_by_year['source_id'] = source_id
            writers['counts_by_year'].writerow(count_by_year)

def flatten_work_item(work, writers):
    if not (work_id := work.get('id')):
        return
    
    if (abstract := work.get('abstract_inverted_index')) is not None:
        work['abstract_inverted_index'] = json.dumps(abstract, ensure_ascii=False)
    writers['works'].writerow(work)

    for loc_key in ['primary_location', 'best_oa_location']:
        loc = work.get(loc_key) or {}
        if loc.get('source', {}).get('id'):
            writers[f"{loc_key}s"].writerow({
                'work_id': work_id,
                'source_id': loc['source']['id'],
                'landing_page_url': loc.get('landing_page_url'),
                'pdf_url': loc.get('pdf_url'),
                'is_oa': loc.get('is_oa'),
                'version': loc.get('version'),
                'license': loc.get('license'),
            })

    for loc in work.get('locations', []):
        if loc.get('source', {}).get('id'):
            writers['locations'].writerow({
                'work_id': work_id,
                'source_id': loc['source']['id'],
                'landing_page_url': loc.get('landing_page_url'),
                'pdf_url': loc.get('pdf_url'),
                'is_oa': loc.get('is_oa'),
                'version': loc.get('version'),
                'license': loc.get('license'),
            })

    for authorship in work.get('authorships', []):
        if author_id := authorship.get('author', {}).get('id'):
            insts = authorship.get('institutions') or [None]
            inst_ids = [i.get('id') if i else None for i in insts]
            for inst_id in inst_ids:
                if inst_id or len(inst_ids) == 1:
                    writers['authorships'].writerow({
                        'work_id': work_id,
                        'author_position': authorship.get('author_position'),
                        'author_id': author_id,
                        'institution_id': inst_id,
                        'raw_affiliation_string': authorship.get('raw_affiliation_string'),
                    })

    if biblio := work.get('biblio'):
        biblio['work_id'] = work_id
        writers['biblio'].writerow(biblio)

    for topic in work.get('topics', []):
        if topic_id := topic.get('id'):
            writers['topics'].writerow({'work_id': work_id, 'topic_id': topic_id, 'score': topic.get('score')})

    for concept in work.get('concepts', []):
        if concept_id := concept.get('id'):
            writers['concepts'].writerow({'work_id': work_id, 'concept_id': concept_id, 'score': concept.get('score')})

    if ids := work.get('ids'):
        ids['work_id'] = work_id
        writers['ids'].writerow(ids)

    for mesh in work.get('mesh', []):
        mesh['work_id'] = work_id
        writers['mesh'].writerow(mesh)

    if oa := work.get('open_access'):
        oa['work_id'] = work_id
        writers['open_access'].writerow(oa)

    for ref in work.get('referenced_works', []):
        if ref: writers['referenced_works'].writerow({'work_id': work_id, 'referenced_work_id': ref})

    for rel in work.get('related_works', []):
        if rel: writers['related_works'].writerow({'work_id': work_id, 'related_work_id': rel})

# --- Coordination logic ---

def flatten_entity(entity_type):
    """Level 2 Parallelism: Processes JSONL files for an entity type in parallel."""
    spec = csv_files[entity_type]
    # Check if all final files for this entity already exist (Skip finished entities)
    # We exclude 'works' from this auto-skip because it often exists as a partial file
    if entity_type != 'works' and all(os.path.exists(table['name']) for table in spec.values()):
        print(f"Final files for {entity_type} already exist. Skipping...")
        return

    files = glob.glob(os.path.join(SNAPSHOT_DIR, 'data', entity_type, '*', '*.gz'))
    if FILES_PER_ENTITY:
        files = files[:FILES_PER_ENTITY]
    
    if not files:
        print(f"No files found for {entity_type}")
        return

    # Use a process pool for the files of this entity
    # Use as many workers as possible while leaving a margin of 30 cores
    num_workers = max(1, os.cpu_count() - 30)
    print(f"Starting pool with {num_workers} workers for {entity_type}...")
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        shard_paths = list(executor.map(partial(process_single_file, entity_type), files))
    
    # Merge all shards created for this entity
    merge_shards(entity_type, shard_paths)
    print(f"Finished flattening for {entity_type}.")

if __name__ == '__main__':
    entities = ['authors', 'concepts', 'topics', 'institutions', 'publishers', 'sources', 'works']
    
    for entity in entities:
        print(f"Starting to process entity: {entity}")
        flatten_entity(entity)
        
    # Clean up temp directory
    if os.path.exists(TEMP_DIR) and not os.listdir(TEMP_DIR):
        os.rmdir(TEMP_DIR)
    
    print("All entities processed successfully.")
