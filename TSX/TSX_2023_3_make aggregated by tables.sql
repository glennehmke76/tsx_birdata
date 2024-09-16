-- localhost; 20s
-- AcuGIS;

-- calculate response variables by month
  DROP TABLE IF EXISTS data_by_month;
  CREATE TABLE IF NOT EXISTS data_by_month AS
  SELECT
    surveys_by_month.site_id,
    site_tsx.site_type AS survey_type_id,
    site_tsx.site_permutation,
    -- site_tsx.geom,
    surveys_by_month.year,
    surveys_by_month.month,
    surveys_by_month.taxon_id,
    surveys_by_month.range_id,
    surveys_by_month.num_surveys,
    COALESCE(sightings_by_month.num_sightings, 0) AS num_sightings,
    COALESCE((COALESCE(sightings_by_month.num_sightings, 0) / surveys_by_month.num_surveys :: decimal), 0) AS rr,
    COALESCE(sightings_by_month.mean_of_count, 0) AS mean_of_count, -- not including absences
    COALESCE(sightings_by_month.sum_of_count / (surveys_by_month.num_surveys - sightings_by_month.num_po_sightings) :: decimal, 0) AS mean_abundance -- including absences as 0s
  FROM surveys_by_month
  LEFT JOIN sightings_by_month
    ON surveys_by_month.site_id = sightings_by_month.site_id
    AND surveys_by_month.year = sightings_by_month.year
    AND surveys_by_month.month = sightings_by_month.month
    AND surveys_by_month.taxon_id = sightings_by_month.taxon_id
    AND surveys_by_month.range_id = sightings_by_month.range_id
  JOIN site_tsx ON surveys_by_month.site_id = site_tsx.site_id
  ;

  ALTER TABLE IF EXISTS data_by_month
  ADD CONSTRAINT data_by_month_pkey
    PRIMARY KEY (site_id, taxon_id, range_id, year, month);
  CREATE INDEX IF NOT EXISTS idx_data_by_month_site_id
    ON data_by_month (site_id);
  CREATE INDEX IF NOT EXISTS idx_data_by_month_year
    ON data_by_month (year);
  CREATE INDEX IF NOT EXISTS idx_data_by_month_month
    ON data_by_month (month);
  CREATE INDEX IF NOT EXISTS idx_data_by_month_taxon_id
    ON data_by_month (taxon_id);
  CREATE INDEX IF NOT EXISTS idx_data_by_month_range_id
    ON data_by_month (range_id);

  -- check taxa
    -- SELECT DISTINCT
    --   taxon_id
    -- FROM data_by_month
    -- ORDER BY taxon_id

-- aggregate by year
  DROP TABLE IF EXISTS data_by_year;
  CREATE TABLE IF NOT EXISTS data_by_year AS
  SELECT
    data_by_month.site_id,
    data_by_month.survey_type_id,
    data_by_month.site_permutation,
    -- site_tsx.geom,
    data_by_month.year,
    data_by_month.taxon_id,
    data_by_month.range_id,
    SUM(data_by_month.num_surveys) AS num_surveys,
    SUM(data_by_month.num_sightings) AS num_sightings,
    AVG(data_by_month.rr) AS mean_monthly_rr,
    AVG(data_by_month.mean_of_count) AS monthly_mean_of_mean_of_count,
    AVG(data_by_month.mean_abundance) AS monthly_mean_abundance
  FROM data_by_month
          -- LEFT JOIN sightings_by_month
          --   ON data_by_month.site_id = sightings_by_month.site_id
          --   AND data_by_month.year = sightings_by_month.year
          --   AND data_by_month.taxon_id = sightings_by_month.taxon_id
          --   AND data_by_month.range_id = sightings_by_month.range_id
          -- JOIN site_tsx ON data_by_month.site_id = site_tsx.site_id
  GROUP BY
    data_by_month.site_id,
    data_by_month.survey_type_id,
    data_by_month.site_permutation,
    -- site_tsx.geom,
    data_by_month.year,
    data_by_month.taxon_id,
    data_by_month.range_id
  ;

  ALTER TABLE IF EXISTS data_by_year
  ADD CONSTRAINT data_by_year_pkey
    PRIMARY KEY (site_id, taxon_id, range_id, year);
  CREATE INDEX IF NOT EXISTS idx_data_by_year_site_id
    ON data_by_year (site_id);
  CREATE INDEX IF NOT EXISTS idx_data_by_year_year
    ON data_by_year (year);
  CREATE INDEX IF NOT EXISTS idx_data_by_year_taxon_id
    ON data_by_year (taxon_id);
  CREATE INDEX IF NOT EXISTS idx_data_by_year_range_id
    ON data_by_year (range_id);

  -- add processing method
    -- import table to birdata if required
      -- DROP TABLE IF EXISTS processing_method;
      -- CREATE TABLE processing_method (
      --   taxon_id varchar NOT NULL,
      --   unit_type_id int NOT NULL,
      --   unit_id int NOT NULL,
      --   search_type_id int NOT NULL,
      --   response_variable_type_id int NOT NULL
      -- );
      -- copy processing_method FROM '/Users/glennehmke/Downloads/processing_method.csv' DELIMITER ',' CSV HEADER;
      -- CREATE INDEX IF NOT EXISTS idx_processing_method_taxon_id
      --   ON processing_method (taxon_id);
      -- CREATE INDEX IF NOT EXISTS idx_processing_method_search_type_id
      --   ON processing_method (search_type_id);

    -- add response_variable_type_id to data_by_year
    ALTER TABLE IF EXISTS data_by_year
      ADD COLUMN response_variable_type_id smallint;

    UPDATE data_by_year
    SET response_variable_type_id = processing_method.response_variable_type_id
    FROM processing_method
    WHERE
      processing_method.taxon_id = data_by_year.taxon_id
      AND processing_method.search_type_id = data_by_year.survey_type_id
      AND processing_method.response_variable_type_id = 3
      AND processing_method.unit_type_id = 3
    ;

  -- identify all 0 time-series
  ALTER TABLE IF EXISTS data_by_year
    DROP COLUMN IF EXISTS all0_values;
  ALTER TABLE IF EXISTS data_by_year
    ADD COLUMN all0_values smallint;
    
  UPDATE data_by_year
  SET all0_values = 1
  FROM
      (SELECT
        taxon_id,
        site_id,
        survey_type_id
      FROM data_by_year
      GROUP BY
        taxon_id,
        site_id,
        survey_type_id
      HAVING
        SUM(data_by_year.mean_monthly_rr) = 0
      )sub
  WHERE
    sub.survey_type_id = data_by_year.survey_type_id
    AND sub.site_id = data_by_year.site_id
    AND sub.taxon_id = data_by_year.taxon_id
  ;

  -- summarise and check
  SELECT
    data_by_year.taxon_id,
    wlab.taxon_name,
    data_by_year.response_variable_type_id,
    data_by_year.survey_type_id,
    survey_type.name,
    COUNT(DISTINCT data_by_year.site_id) AS num_sites
  FROM data_by_year
  JOIN wlab ON data_by_year.taxon_id = wlab.taxon_id
  JOIN survey_type ON data_by_year.survey_type_id = survey_type.id
  WHERE
    data_by_year.response_variable_type_id IS NOT NULL
  GROUP BY
    data_by_year.taxon_id,
    wlab.taxon_name,
    data_by_year.response_variable_type_id,
    data_by_year.survey_type_id,
    survey_type.name
  ;