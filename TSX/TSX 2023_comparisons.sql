

-- make integrated table for comparison of aggregated data
DROP TABLE IF EXISTS aggregated;
CREATE TABLE aggregated (
  taxon_id varchar DEFAULT NULL,
  agg_2018_site_id_2023 int DEFAULT NULL,
  agg_2018_search_type int DEFAULT NULL,
  agg_2018_year int DEFAULT NULL,
  agg_2018_unit int DEFAULT NULL,
  agg_2018_response_variable int DEFAULT NULL,
  agg_2023_site_id int DEFAULT NULL,
  -- agg_2023_site_permutation varchar DEFAULT NULL,
  agg_2023_search_type int DEFAULT NULL,
  agg_2023_year int DEFAULT NULL,
  agg_2018_num_surveys int DEFAULT NULL,
  agg_2023_num_surveys int DEFAULT NULL,
  agg_2018_value decimal DEFAULT NULL,
  agg_2023_value decimal DEFAULT NULL
);
  
INSERT INTO aggregated (taxon_id, agg_2018_site_id_2023, agg_2018_search_type, agg_2018_year, agg_2018_unit, agg_2018_response_variable, agg_2023_site_id, agg_2023_search_type, agg_2023_year, agg_2018_num_surveys, agg_2023_num_surveys, agg_2018_value, agg_2023_value)
SELECT
  aggregated_by_year_2023.taxon_id,
  aggregated_by_year_2018.site_id_2023,
  aggregated_by_year_2018.search_type_id,
  aggregated_by_year_2018.start_date_y,
  aggregated_by_year_2018.unit_id,
  aggregated_by_year_2018.response_variable_type_id,
  aggregated_by_year_2023.site_id,
  -- aggregated_by_year_2023.site_permutation,
  aggregated_by_year_2023.search_type_id,
  aggregated_by_year_2023.start_date_y AS year,
  aggregated_by_year_2018.survey_count,
  aggregated_by_year_2023.num_surveys,
  aggregated_by_year_2018.value,
  aggregated_by_year_2023.value
FROM aggregated_by_year_2023
FULL OUTER JOIN aggregated_by_year_2018
  ON aggregated_by_year_2018.site_id_2023 = aggregated_by_year_2023.site_id
  AND aggregated_by_year_2018.search_type_id = aggregated_by_year_2023.search_type_id
  AND aggregated_by_year_2018.start_date_y = aggregated_by_year_2023.start_date_y
WHERE
  aggregated_by_year_2018.taxon_id = aggregated_by_year_2023.taxon_id
  AND aggregated_by_year_2018.start_date_y > 1998
;


-- where site pair exists;
  -- rrΔ (+ is more in 2023)
  ALTER TABLE IF EXISTS aggregated DROP COLUMN IF EXISTS rr_delta;
  ALTER TABLE IF EXISTS aggregated
    ADD COLUMN rr_delta numeric;
  UPDATE aggregated
  SET rr_delta = agg_2023_value - agg_2018_value
  ;

-- make variable aggregates
DROP TABLE IF EXISTS aggregated_deltas;
CREATE TABLE aggregated_deltas (
  taxon_id varchar DEFAULT NULL,
  site_permutation int DEFAULT NULL,
  search_type_id int DEFAULT NULL,
  year int DEFAULT NULL,
  sites DEFAULT NULL,
  sightings DEFAULT NULL,
  mean_rr int DEFAULT NULL,
);

INSERT INTO aggregated_deltas
SELECT
  

  -- # sites and delta


  UPDATE aggregated_deltas
  SET num_sites = sub.num_sites
  FROM



    (SELECT
      taxon_id,

      COUNT(DISTINCT site_id) AS num_sites
    FROM
    WHERE
      agg_2018_site_id_2023 IS NOT NULL
    )sub
  
  
  
  -- return non matching sites

  -- mean yearly rrΔ
    SELECT
      agg_2023_year AS year,
      AVG(agg_2018_value) AS mean_2018_value,
      AVG(agg_2023_value) AS mean_2023_value
    FROM
      aggregated
    WHERE
      agg_2023_site_id IS NOT NULL
      AND agg_2018_year < 2019
      AND agg_2023_site_permutation = 'original'
    GROUP BY
      agg_2023_year
    ORDER BY
      agg_2023_year
    ;


-- filter to comprable time-series

  WHERE
    agg_2023_site_permutation = 'original'
  ;


primary variables (by month)
  make all directionly consistent =repative to 2023 (+ is more in 2023)

  * num-surveys
  * num_sightings
  * rrΔ
  *
  *
  *
  
variable aggregates
  * # sites different (more/less)
  * mean yearly rrΔ

grouping
  * site_id(2023) with geom as option

factors
  * search_type
  * source_id
  * year
  * region (sub-ibra)
 

  ** link non-matching time-series back to raw data via site_id / month / etc

-- possible soures of distinctions

  * 2018 site_ids are inferred from 2023 sites by spatial intersection (see xxxxx). If the 2023 site layer has been modified this may result in some differenses.
  * data additions to birdata - atlas paper form lag, 3rd party data dumps etc



  SELECT
    site_id,
    site_permutation,
    survey_type.name AS search_type,
    AVG(rr) FILTER (WHERE year = 1999) AS "1999",
    AVG(rr) FILTER (WHERE year = 2000) AS "2000",
    AVG(rr) FILTER (WHERE year = 2001) AS "2001",
    AVG(rr) FILTER (WHERE year = 2002) AS "2002",
    AVG(rr) FILTER (WHERE year = 2003) AS "2003",
    AVG(rr) FILTER (WHERE year = 2004) AS "2004",
    AVG(rr) FILTER (WHERE year = 2005) AS "2005",
    AVG(rr) FILTER (WHERE year = 2006) AS "2006",
    AVG(rr) FILTER (WHERE year = 2007) AS "2007",
    AVG(rr) FILTER (WHERE year = 2008) AS "2008",
    AVG(rr) FILTER (WHERE year = 2019) AS "2009",
    AVG(rr) FILTER (WHERE year = 2010) AS "2010",
    AVG(rr) FILTER (WHERE year = 2011) AS "2011",
    AVG(rr) FILTER (WHERE year = 2012) AS "2012",
    AVG(rr) FILTER (WHERE year = 2013) AS "2013",
    AVG(rr) FILTER (WHERE year = 2014) AS "2014",
    AVG(rr) FILTER (WHERE year = 2015) AS "2015",
    AVG(rr) FILTER (WHERE year = 2016) AS "2016",
    AVG(rr) FILTER (WHERE year = 2017) AS "2017",
    AVG(rr) FILTER (WHERE year = 2018) AS "2018"
  FROM
    aggregated_by_year_2023
  JOIN survey_type ON aggregated_by_year_2023.search_type_id = survey_type.id
  WHERE
    site_permutation = 'original'
  GROUP BY
    site_id,
    site_permutation,
    survey_type.name
  ;


-- long
  SELECT
    site_id,
    site_permutation,
    survey_type.name AS search_type,
    rr
  FROM
    aggregated_by_year_2023
  JOIN survey_type ON aggregated_by_year_2023.search_type_id = survey_type.id
  WHERE
    site_permutation = 'original'
;









