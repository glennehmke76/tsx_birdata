-- bring in nesp_2018 aggregated by year
  -- from TSXdb
  SELECT
    start_date_y,
    site_id,
    search_type_id,
    taxon_id,
    response_variable_type_id,
    value,
    source_id,
    unit_id,
    ST_Y(centroid_coords) AS lat,
    ST_X(centroid_coords) AS lon,
    survey_count
  FROM
    nesp_2018.aggregated_by_year_2018
  WHERE
    source_id = 23
  ;

  DROP TABLE IF EXISTS aggregated_by_year_2018;
  CREATE TABLE IF NOT EXISTS aggregated_by_year_2018
    (
    start_date_y smallint NOT NULL,
    site_id integer,
    search_type_id integer,
    taxon_id character(8) NOT NULL,
    response_variable_type_id integer,
    value numeric(7,4) NOT NULL,
    source_id integer NOT NULL,
    unit_id integer NOT NULL,
    lat numeric NOT NULL,
    lon numeric NOT NULL,
    survey_count integer NOT NULL
    );

  CREATE INDEX IF NOT EXISTS aggregated_by_year_2018_start_date_y
    ON aggregated_by_year_2018 (start_date_y);
  CREATE INDEX IF NOT EXISTS aggregated_by_year_2018_taxon_id
    ON aggregated_by_year_2018 (taxon_id);
    CREATE INDEX IF NOT EXISTS aggregated_by_year_2018_site_id
    ON aggregated_by_year_2018 (site_id);
  CREATE INDEX IF NOT EXISTS aggregated_by_year_2018_search_type_id
    ON aggregated_by_year_2018 (search_type_id);
  CREATE INDEX IF NOT EXISTS aggregated_by_year_2018_unit_id
    ON aggregated_by_year_2018 (unit_id);


  -- round value to match 2023
  UPDATE aggregated_by_year_2018
  SET value = ROUND(value, 4)
  ;

  -- make geom
  ALTER TABLE IF EXISTS aggregated_by_year_2018
    ADD COLUMN geom geometry(Point,4283);
  UPDATE aggregated_by_year_2018
  SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4283);
  CREATE INDEX idx_aggregated_by_year_2018_geom ON aggregated_by_year_2018 USING gist (geom);

  -- back attribute 2023 site_id by intersection to compare apples
  ALTER TABLE IF EXISTS aggregated_by_year_2018
      ADD COLUMN site_id_2023 integer;
  UPDATE aggregated_by_year_2018
  SET site_id_2023 = sub.site_id_2023
  FROM
      (SELECT
        aggregated_by_year_2018.site_id,
        site_tsx.site_id AS site_id_2023
      FROM site_tsx
      JOIN aggregated_by_year_2018 ON ST_Intersects(site_tsx.geom, aggregated_by_year_2018.geom) AND site_tsx.site_type = aggregated_by_year_2018.search_type_id
      )sub
  WHERE
    aggregated_by_year_2018.site_id = sub.site_id
  ;

-- identify all 0 time-series
  ALTER TABLE IF EXISTS aggregated_by_year_2018
    DROP COLUMN all0_values;
  ALTER TABLE IF EXISTS aggregated_by_year_2018
    ADD COLUMN all0_values smallint;

  UPDATE aggregated_by_year_2018
  SET all0_values = 1
  FROM
      (SELECT
        taxon_id,
        site_id,
        search_type_id
      FROM aggregated_by_year_2018
      GROUP BY
        taxon_id,
        site_id,
        search_type_id
      HAVING
        SUM(aggregated_by_year_2018.value) = 0
      )sub
  WHERE
    sub.search_type_id = aggregated_by_year_2018.search_type_id
    AND sub.site_id = aggregated_by_year_2018.site_id
    AND sub.taxon_id = aggregated_by_year_2018.taxon_id
  ;

-- compare summary statistics for all time-series
  WITH summary_2018 AS
    (SELECT
      aggregated_by_year_2018.taxon_id,
      wlab.taxon_name,
      COUNT(DISTINCT site_id) FILTER (WHERE search_type_id = 1) AS num_2ha_sites_2018,
      COUNT(DISTINCT site_id) FILTER (WHERE search_type_id = 2) AS num_500m_sites_2018,
      SUM(survey_count) FILTER (WHERE search_type_id = 1) AS num_surveys_2ha_sites_2018,
      SUM(survey_count) FILTER (WHERE search_type_id = 2) AS num_surveys_500m_sites_2018,
      -- SUM(DISTINCT num_sightings) FILTER (WHERE search_type_id = 1) AS num_sightings_2ha_sites,
      -- SUM(DISTINCT num_sightings) FILTER (WHERE search_type_id = 2) AS num_sightings_500m_sites,
      ROUND(AVG(value) FILTER (WHERE search_type_id = 1), 4) AS mean_monthly_rr_2ha_sites_2018,
      ROUND(AVG(value) FILTER (WHERE search_type_id = 2), 4) AS mean_monthly_rr_500m_sites_2018
    FROM aggregated_by_year_2018
    LEFT JOIN wlab ON aggregated_by_year_2018.taxon_id = wlab.taxon_id
    WHERE
      aggregated_by_year_2018.response_variable_type_id = 3
    GROUP BY
      aggregated_by_year_2018.taxon_id,
      wlab.taxon_name
    )
  SELECT
    summary_2023.taxon_id,
    summary_2023.taxon_name,
    num_2ha_sites_2018,
    num_2ha_sites_2023,
    num_surveys_2ha_sites_2018,
    num_surveys_2ha_sites_2023,
    mean_monthly_rr_2ha_sites_2018,
    mean_monthly_rr_2ha_sites_2023,
    num_500m_sites_2018,
    num_500m_sites_2023,
    num_surveys_500m_sites_2018,
    num_surveys_500m_sites_2023,
    mean_monthly_rr_500m_sites_2018,
    mean_monthly_rr_500m_sites_2023
  FROM
      (SELECT
        data_by_year.taxon_id,
        wlab.taxon_name,
        COUNT(DISTINCT site_id) FILTER (WHERE survey_type_id = 1) AS num_2ha_sites_2023,
        COUNT(DISTINCT site_id) FILTER (WHERE survey_type_id = 2) AS num_500m_sites_2023,
        SUM(num_surveys) FILTER (WHERE survey_type_id = 1) AS num_surveys_2ha_sites_2023,
        SUM(num_surveys) FILTER (WHERE survey_type_id = 2) AS num_surveys_500m_sites_2023,
        SUM(num_sightings) FILTER (WHERE survey_type_id = 1) AS num_sightings_2ha_sites_2023,
        SUM(num_sightings) FILTER (WHERE survey_type_id = 2) AS num_sightings_500m_sites_2023,
        ROUND(AVG(mean_monthly_rr) FILTER (WHERE survey_type_id = 1), 4) AS mean_monthly_rr_2ha_sites_2023,
        ROUND(AVG(mean_monthly_rr) FILTER (WHERE survey_type_id = 2), 4) AS mean_monthly_rr_500m_sites_2023
      FROM data_by_year
      LEFT JOIN wlab ON data_by_year.taxon_id = wlab.taxon_id
      WHERE
        data_by_year.response_variable_type_id IS NOT NULL
        AND year <2018
        AND data_by_year.range_id = 1
        AND site_permutation = 'original'
      GROUP BY
        data_by_year.taxon_id,
        wlab.taxon_name
      )summary_2023
  LEFT JOIN summary_2018 ON summary_2023.taxon_id = summary_2018.taxon_id
  ;

-- identify differing time-series by year (site x year)
  ALTER TABLE IF EXISTS data_by_year
    DROP COLUMN IF EXISTS value_differs_2018;
  ALTER TABLE IF EXISTS data_by_year
    ADD COLUMN value_differs_2018 boolean;

  UPDATE data_by_year
  SET value_differs_2018 = 'true'
  FROM
      (SELECT
        CONCAT(data_by_year.site_id, data_by_year.survey_type_id, data_by_year.year, data_by_year.taxon_id) AS ts_id
      FROM data_by_year
      JOIN aggregated_by_year_2018
        ON aggregated_by_year_2018.site_id_2023 = data_by_year.site_id
        AND aggregated_by_year_2018.search_type_id = data_by_year.survey_type_id
        AND aggregated_by_year_2018.start_date_y = data_by_year.year
        AND aggregated_by_year_2018.taxon_id = data_by_year.taxon_id
      WHERE
        aggregated_by_year_2018.value <> data_by_year.mean_monthly_rr -- assumes equal decimal - if not use ROUND
        AND aggregated_by_year_2018.response_variable_type_id = 3
        AND aggregated_by_year_2018.start_date_y > 1998
        AND data_by_year.range_id = 1
        AND data_by_year.year < (SELECT
                                  MAX(start_date_y) AS max_year
                                FROM aggregated_by_year_2018
                                )
        -- AND data_by_year.all0_values IS NULL -- exclude all 0 time-series
      )unmatched
  WHERE
    unmatched.ts_id = CONCAT(data_by_year.site_id, data_by_year.survey_type_id, data_by_year.year, data_by_year.taxon_id)
  ;

  UPDATE data_by_year
  SET value_differs_2018 = 'false'
  FROM
      (SELECT
        CONCAT(data_by_year.site_id, data_by_year.survey_type_id, data_by_year.year, data_by_year.taxon_id) AS ts_id
      FROM data_by_year
      JOIN aggregated_by_year_2018
        ON aggregated_by_year_2018.site_id_2023 = data_by_year.site_id
        AND aggregated_by_year_2018.search_type_id = data_by_year.survey_type_id
        AND aggregated_by_year_2018.start_date_y = data_by_year.year
        AND aggregated_by_year_2018.taxon_id = data_by_year.taxon_id
      WHERE
        aggregated_by_year_2018.value = data_by_year.mean_monthly_rr -- assumes equal decimal - if not use ROUND
        AND aggregated_by_year_2018.response_variable_type_id = 3
        AND aggregated_by_year_2018.start_date_y > 1998
        AND data_by_year.range_id = 1
        AND data_by_year.year < (SELECT
                                  MAX(start_date_y) AS max_year
                                FROM aggregated_by_year_2018
                                )
        -- AND data_by_year.all0_values IS NULL
        -- AND aggregated_by_year_2018.all0_values IS NULLA
      )unmatched
  WHERE
    unmatched.ts_id = CONCAT(data_by_year.site_id, data_by_year.survey_type_id, data_by_year.year, data_by_year.taxon_id)
  ;

  -- summarise site x year differences by site
    SELECT
      data_by_year.taxon_id,
      wlab.taxon_name,
      survey_type.name,
      COUNT(site_id) FILTER (WHERE value_differs_2018 = true AND all0_values IS NULL) AS num_qualifying_ts_differ,
      COUNT(site_id) FILTER (WHERE value_differs_2018 = true AND all0_values = 1) AS num_all0_ts_differ,
      COUNT(site_id) FILTER (WHERE value_differs_2018 = false AND all0_values IS NULL) AS num_qualifying_ts_match,
      COUNT(site_id) FILTER (WHERE value_differs_2018 = false AND all0_values = 1) AS num_all0_ts_match,
      COUNT(site_id) FILTER (WHERE value_differs_2018 IS NULL AND all0_values IS NULL) AS num_qualifying_ts_match_null,
      COUNT(site_id) FILTER (WHERE value_differs_2018 IS NULL AND all0_values = 1) AS num_all0_ts_match_null
    FROM data_by_year
    LEFT JOIN wlab ON data_by_year.taxon_id = wlab.taxon_id
    JOIN survey_type ON data_by_year.survey_type_id = survey_type.id
    WHERE
      site_permutation = 'original'
      AND response_variable_type_id = 3
      AND data_by_year.year < 2018
    GROUP BY
      data_by_year.taxon_id,
      wlab.taxon_name,
      survey_type.name
    ;

-- compare summary statistics for selected time-series
  -- make ts summaries
    DROP TABLE IF EXISTS time_series_summary_2023;
    CREATE TABLE time_series_summary_2023 AS
    SELECT
      taxon_id,
      data_by_year.site_id,
      site_tsx.site_permutation,
      survey_type.name,
      Sum(num_surveys) AS ts_surveys,
      Sum(num_sightings) AS num_sightings,
      Count(year) FILTER (WHERE mean_monthly_rr = 0) AS num_0s,
      Avg(mean_monthly_rr) AS mean_rr,
      Min(year) AS ts_start_year,
      Max(year) AS ts_end_year,
      Count(year) AS ts_sample_years,
      (Max(year)-Min(year))+1 AS ts_length,
      Count(year)/((Max(year)-Min(year))+1)*100 AS ts_completeness
    FROM
      data_by_year
    JOIN survey_type ON data_by_year.survey_type_id = survey_type.id
    JOIN site_tsx ON data_by_year.site_id = site_tsx.site_id
    WHERE
      site_tsx.site_permutation = 'original'
      AND response_variable_type_id = 3
      AND data_by_year.year < 2018
      AND data_by_year.range_id = 1
    GROUP BY
      taxon_id,
      data_by_year.site_id,
      site_tsx.site_permutation,
      survey_type.name
    ;

    DROP TABLE IF EXISTS time_series_summary_2018;
    CREATE TABLE time_series_summary_2018 AS
    SELECT
      taxon_id,
      aggregated_by_year_2018.site_id_2023 AS site_id,
      site_tsx.site_permutation,
      survey_type.name,
      Sum(survey_count) AS ts_surveys,
      -- Sum(num_sightings) AS num_sightings,
      Count(start_date_y) FILTER (WHERE value = 0) AS num_0s,
      Avg(value) AS mean_rr,
      Min(start_date_y) AS ts_start_year,
      Max(start_date_y) AS ts_end_year,
      Count(start_date_y) AS ts_sample_years,
      (Max(start_date_y)-Min(start_date_y))+1 AS ts_length,
      Count(start_date_y)/((Max(start_date_y)-Min(start_date_y))+1)*100 AS ts_completeness
    FROM
      aggregated_by_year_2018
    JOIN survey_type ON aggregated_by_year_2018.search_type_id = survey_type.id
    JOIN site_tsx ON aggregated_by_year_2018.site_id = site_tsx.site_id
    WHERE
      site_tsx.site_permutation = 'original'
      AND response_variable_type_id = 3
      AND aggregated_by_year_2018.start_date_y < 2018
    GROUP BY
      taxon_id,
      aggregated_by_year_2018.site_id_2023,
      site_tsx.site_permutation,
      survey_type.name
    ;

  -- identify non-matching time-series (including all 0 ts)
    -- ie if not listed then the yearly mean of value is equal - inferring all the months are also so equal
    DROP TABLE IF EXISTS non_matching_ts;
    CREATE TEMPORARY TABLE non_matching_ts AS
    WITH total_ts AS
      (SELECT
        taxon_id,
        COUNT(DISTINCT site_id) AS total_ts
      FROM time_series_summary_2023
      GROUP BY
        taxon_id)

    SELECT
      total_ts.taxon_id,
      wlab.taxon_name,
      total_ts.total_ts,
      Count(unequal_ts.*) AS num_unequal_ts,
      Round(Count(unequal_ts.*) / total_ts.total_ts :: decimal * 100, 2) AS percentage_unequal_ts
    FROM
        (SELECT
          time_series_summary_2023.taxon_id,
          time_series_summary_2023.site_id,
          -- time_series_summary_2018.site_id,
          time_series_summary_2023.mean_rr AS value_2023,
          time_series_summary_2018.mean_rr AS value_2018
        FROM time_series_summary_2023
        FULL OUTER JOIN time_series_summary_2018 -- join type not meaningful as ts are filtered in temp table subqueries
          ON time_series_summary_2018.site_id = time_series_summary_2023.site_id
          AND time_series_summary_2018.taxon_id = time_series_summary_2023.taxon_id
        WHERE
          time_series_summary_2018.mean_rr <> time_series_summary_2023.mean_rr -- assumes equal decimal - if not use ROUND
        )unequal_ts
    RIGHT JOIN total_ts ON unequal_ts.taxon_id = total_ts.taxon_id
    JOIN wlab ON total_ts.taxon_id = wlab.taxon_id
    GROUP BY
      total_ts.taxon_id,
      wlab.taxon_name,
      total_ts
    ;

  -- calculate other summary statistics (in this case for non-all-0 aka qualifying time series) and integrate
    SELECT
      wlab.taxon_id,
      wlab.taxon_name,
      non_matching_ts.total_ts,
      non_matching_ts.num_unequal_ts,
      non_matching_ts.percentage_unequal_ts,
      sub.num_ts_2018 AS num_qualifying_ts_2018,
      sub.num_ts_2023 AS num_qualifying_ts_2023,
      sub.mean_rr_2018,
      sub.mean_rr_2023,
      sub.mean_ts_start_year_2018,
      sub.mean_ts_start_year_2023,
      sub.mean_ts_sample_years_2018,
      sub.mean_ts_sample_years_2023,
      sub.mean_ts_length_2018,
      sub.mean_ts_length_2023
    FROM
        (SELECT
          time_series_summary_2023.taxon_id AS taxon_id_2023,
          Count(time_series_summary_2018.site_id) AS num_ts_2018,
          Count(time_series_summary_2023.site_id) AS num_ts_2023,
          ROUND(Avg(time_series_summary_2018.mean_rr), 4) AS mean_rr_2018,
          ROUND(Avg(time_series_summary_2023.mean_rr), 4) AS mean_rr_2023,
          ROUND(Avg(time_series_summary_2018.ts_start_year), 0) AS mean_ts_start_year_2018,
          ROUND(Avg(time_series_summary_2023.ts_start_year), 0) AS mean_ts_start_year_2023,
          ROUND(Avg(time_series_summary_2018.ts_sample_years), 2) AS mean_ts_sample_years_2018,
          ROUND(Avg(time_series_summary_2023.ts_sample_years), 2) AS mean_ts_sample_years_2023,
          ROUND(Avg(time_series_summary_2018.ts_length), 2) AS mean_ts_length_2018,
          ROUND(Avg(time_series_summary_2023.ts_length), 2) AS mean_ts_length_2023
        FROM time_series_summary_2023
        FULL OUTER JOIN time_series_summary_2018
          ON time_series_summary_2023.site_id = time_series_summary_2018.site_id
          AND time_series_summary_2023.taxon_id = time_series_summary_2018.taxon_id
        WHERE
          time_series_summary_2023.num_sightings > 0
          AND time_series_summary_2023.num_sightings > 0
        GROUP BY
          time_series_summary_2023.taxon_id
        )sub
    JOIN wlab ON sub.taxon_id_2023 = wlab.taxon_id
    JOIN non_matching_ts ON wlab.taxon_id = non_matching_ts.taxon_id
    ;

-- if count data are pursued summarise non-0 time-series length for reporting rate vs mean abundance

-- clean-up
  drop table time_series_summary_2018;
  drop table time_series_summary_2023;
  drop table aggregated_by_year_2018;
