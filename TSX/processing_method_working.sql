# Note: We don't need to specify CHARSET and COLLATE on our CREATE TABLE statements if
# the database already has appropriate defaults:
# ALTER DATABASE database_name
# DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

# Create all columns up-front for two reasons:
# 1. Conceptually we can see all outputs without having to examine whole script
# 2. Turns out we can easily populate a lot of summary stats at the same time as
# identifying all of the Taxon x Unit x Source x SearchType combinations, which saves
# a lot of update queries later on.

# Note TEMPORARY TABLE will automatically disappear after this SQL session
# (Not sure how well this works with phpMyAdmin... just get rid of TEMPORARY if
# it's a problem)
# GE - chenged temporary to full table (dropped at the end) because of phpMyAdmin export issue
CREATE TABLE processing_method_working2 (
  taxon_id char(8) NOT NULL,
  unit_id INT DEFAULT NULL,
  unit_type_id INT DEFAULT NULL,
  source_id INT DEFAULT NULL,
  search_type_id INT DEFAULT NULL,
  survey_id_count INT DEFAULT NULL,
  site_id_count INT DEFAULT NULL,
  data_type INT DEFAULT NULL,
  experimental_design_type_id INT DEFAULT 1 NOT NULL, # changed to default 1 - this assumes all data are 'Standardised site' and to be included in the index which is the case as of 2022 (we have not yet used unstandardised sites or grids). If we do this will need to change.
  response_variable_type_id INT DEFAULT 0 NOT NULL
    COMMENT 'new permutation of response varible type - based on unit types',
  response_variable_type_id_previous INT DEFAULT 0 NOT NULL
    COMMENT 'previous permutation of response varible type - from processing_method',
  matches_previous BOOL DEFAULT FALSE
    COMMENT '1 is resp variable as per pre 2022',
  is_orphan BOOL DEFAULT FALSE
    COMMENT 'time-series pre 20220 but is not in current data',
  n_count_values INT,
  min_count DOUBLE,
  n_months INT,
  n_years INT,
  max_count DOUBLE,
  positional_accuracy_threshold_in_m double DEFAULT NULL,
  max_status_id INT,
  taxon_defunct BOOL DEFAULT FALSE,
  UNIQUE (taxon_id, unit_id, source_id, search_type_id, data_type)
);

-- Taxon x Unit x Source x SearchType for type 1 surveys
# Query OK, 1654 rows affected (9.70 sec)
INSERT INTO processing_method_working2 (
  taxon_id,
  unit_id,
  unit_type_id,
  source_id,
  search_type_id,
  survey_id_count,
  site_id_count,
  data_type,
  n_count_values,
  min_count,
  max_count,
  n_months,
  n_years,
  max_status_id)
SELECT
  t1_sighting.taxon_id,
  t1_sighting.unit_id,
  t1_sighting.unit_type_id,
  t1_survey.source_id,
  t1_site.search_type_id,
  COUNT(DISTINCT t1_survey.id) AS survey_id_count,
  COUNT(DISTINCT t1_site.id) AS site_id_count,
  '1' AS data_type,
  COUNT(DISTINCT t1_sighting.count) AS n_count_values,
  MIN(t1_sighting.count) AS min_count,
  MAX(t1_sighting.count) AS max_count,
  COUNT(DISTINCT CONCAT(COALESCE(t1_survey.start_date_m, 0), t1_survey.start_date_y)) AS n_months,
  COUNT(DISTINCT t1_survey.start_date_y) AS n_years,
  taxon.max_status_id
FROM source
  INNER JOIN t1_survey ON source.id = t1_survey.source_id
  INNER JOIN t1_sighting ON t1_survey.id = t1_sighting.survey_id
  INNER JOIN t1_site ON t1_survey.site_id = t1_site.id
  INNER JOIN taxon ON t1_sighting.taxon_id = taxon.id
GROUP BY
  t1_sighting.taxon_id,
  t1_sighting.unit_id,
  t1_sighting.unit_type_id,
  t1_survey.source_id,
  t1_site.search_type_id;


-- Taxon x Unit x Source x SearchType for type 2 surveys
# Query OK, 5088 rows affected (55.11 sec)
INSERT INTO processing_method_working2 (
  taxon_id,
  unit_id,
  unit_type_id,
  source_id,
  search_type_id,
  survey_id_count,
  site_id_count,
  data_type,
  n_count_values,
  min_count,
  max_count,
  n_months,
  n_years,
  max_status_id)
SELECT
  t2_ultrataxon_sighting.taxon_id,
  t2_sighting.unit_id,
  t2_sighting.unit_type_id,
  t2_survey.source_id,
  t2_survey.search_type_id,
  COUNT(DISTINCT t2_survey.id) AS survey_id_count,
  COUNT(DISTINCT t2_survey.site_id) AS site_id_count,
  '2' AS data_type,
  COUNT(DISTINCT t2_sighting.count) AS n_count_values,
  MIN(t2_sighting.count) AS min_count,
  MAX(t2_sighting.count) AS max_count,
  COUNT(DISTINCT CONCAT(COALESCE(t2_survey.start_date_m, 0), t2_survey.start_date_y)) AS n_months,
  COUNT(DISTINCT t2_survey.start_date_y) AS n_years,
  taxon.max_status_id
FROM
  source,
  t2_survey,
  t2_sighting,
  t2_ultrataxon_sighting,
  taxon
WHERE
  source.id = t2_survey.source_id
  AND t2_survey.id = t2_sighting.survey_id
  AND t2_sighting.id = t2_ultrataxon_sighting.sighting_id
  AND taxon.id = t2_ultrataxon_sighting.taxon_id
GROUP BY
  t2_ultrataxon_sighting.taxon_id,
  t2_sighting.unit_id,
  t2_sighting.unit_type_id,
  t2_survey.source_id,
  t2_survey.search_type_id;


# CASE-WHEN construct is really useful for this case -
# can be easier to reason about than a series of update statements

-- ???
UPDATE processing_method_working2
SET response_variable_type_id = CASE
  WHEN source_id IN (178, 179, 184, 187, 188, 189, 191, 192, 193, 293)
    THEN 2
  WHEN unit_type_id = 3
    THEN 3
  ELSE 1
END;

# Add orphans

# It's a bit clunky creating this temporary table, but it is a work around for
# a 'cannot re-open table' error in the next query due to querying and updating
# the same table.
CREATE TEMPORARY TABLE t2 SELECT * FROM processing_method_working2;

# Note this pattern:
#  tableA LEFT JOIN tableB ON ... WHERE tableB.something IS NULL
# This is a handy trick for getting all records from tableB that can't be found in
# tableA (according to the join criteria)
INSERT INTO processing_method_working2 (
  taxon_id,
  unit_id,
  source_id,
  search_type_id,
  data_type,
  experimental_design_type_id,
  response_variable_type_id_previous,
  is_orphan,
  max_status_id,
  taxon_defunct)
SELECT
  t1.taxon_id,
  t1.unit_id,
  t1.source_id,
  t1.search_type_id,
  t1.data_type,
  t1.experimental_design_type_id,
  t1.response_variable_type_id AS response_variable_type_id_previous,
  1 AS is_orphan,
  taxon.max_status_id,
  (taxon.id IS NULL) AS taxon_defunct
FROM processing_method AS t1
LEFT JOIN t2 ON CONCAT(t1.taxon_id, t1.unit_id, t1.source_id, t1.search_type_id) = CONCAT(t2.taxon_id, t2.unit_id, t2.source_id, t2.search_type_id)
LEFT JOIN taxon ON t1.taxon_id = taxon.id
WHERE t2.taxon_id IS NULL;

#Update response_variable_type_id_previous
# Query OK, 1547 rows affected (25.27 sec)
UPDATE processing_method_working2 AS t2
JOIN processing_method AS t1 ON CONCAT(t1.taxon_id, t1.unit_id, t1.source_id, t1.search_type_id) = CONCAT(t2.taxon_id, t2.unit_id, t2.source_id, t2.search_type_id)
SET t2.response_variable_type_id_previous = t1.response_variable_type_id,
t2.matches_previous = (t2.response_variable_type_id = t1.response_variable_type_id);


SELECT
  CONCAT (processing_method_working2.source_id, "_", processing_method_working2.taxon_id, "_", processing_method_working2.search_type_id,  "_", processing_method_working2.unit_type_id, "_", processing_method_working2.unit_id) AS ts_id,
  CONCAT (processing_method_working2.source_id, "_", processing_method_working2.taxon_id) AS data_id,
  processing_method_working2.taxon_defunct,
  taxon.taxonomic_group,
  processing_method_working2.taxon_id,
  taxon.common_name,
  taxon.scientific_name,
  taxon_status.description AS max_taxon_status,
  processing_method_working2.unit_type_id,
  unit_type.description AS unit_type_description,
  processing_method_working2.unit_id,
  unit.description AS unit_description,
  processing_method_working2.source_id,
  source.description AS source_description,
  processing_method_working2.search_type_id,
  search_type.description AS search_type_description,
  processing_method_working2.data_type,
  processing_method_working2.experimental_design_type_id,
  processing_method_working2.response_variable_type_id,
  processing_method_working2.response_variable_type_id_previous,
  processing_method_working2.matches_previous,
  processing_method_working2.is_orphan,
  processing_method_working2.survey_id_count,
  processing_method_working2.site_id_count,
  processing_method_working2.n_count_values,
  processing_method_working2.min_count,
  processing_method_working2.max_count,
  processing_method_working2.n_months,
  processing_method_working2.n_years,
  processing_method_working2.positional_accuracy_threshold_in_m
FROM
  processing_method_working2
LEFT JOIN taxon ON processing_method_working2.taxon_id = taxon.id
LEFT JOIN search_type ON processing_method_working2.search_type_id = search_type.id
LEFT JOIN unit ON processing_method_working2.unit_id = unit.id
LEFT JOIN unit_type ON processing_method_working2.unit_type_id = unit_type.id
LEFT JOIN source ON processing_method_working2.source_id = source.id
LEFT JOIN taxon_status ON processing_method_working2.max_status_id = taxon_status.id
;

DROP TABLE processing_method_working2;