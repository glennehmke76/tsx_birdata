-- make aggregated table of survey count x months by x sites x ultrataxa
  -- birdata sources already in tsx are excluded - see predicates below
  -- localhost; 169,487 rows affected in 58 s 207 ms
  -- AcuGIS;
  DROP TABLE IF EXISTS surveys_by_month;
  CREATE TEMPORARY TABLE surveys_by_month AS
  SELECT
    extract(year from survey.start_date) AS year,
    extract(month from survey.start_date) AS month,
    sites_in_ranges.site_id,
    sites_in_ranges.taxon_id,
    sites_in_ranges.range_id,
    COUNT(survey.id) AS num_surveys
  FROM survey
  JOIN survey_point ON survey.survey_point_id = survey_point.id
  JOIN site_tsx ON ST_Intersects(site_tsx.geom, survey_point.geom)
  JOIN sites_in_ranges
    ON survey.survey_type_id = sites_in_ranges.site_type -- effectively limits surveys to types in use
    AND site_tsx.site_id = sites_in_ranges.site_id
    -- xxxx
    AND ????
  WHERE
    survey.start_date BETWEEN '1999-01-11' AND '2021-12-31'
    AND
      (survey.source_id <> 71 -- LTERN Vic Highlands
      AND survey.source_id <> 77 -- LTERN Desert Program
      AND survey.source_id <> 36 -- Mallee Fire & Biodiversity Project
      AND survey.source_id <> 36 -- LaTrobe - Mallee VIC - fire study
      AND survey.source_id <> 97 --Tern Supersites - Cumberland Plains	
      AND survey.source_id <> 98 -- Tern SuperSites - Samfordy
      AND survey.source_id <> 99 -- Tern SuperSites - Robson Creek
      )
    -- AND sites_in_ranges.taxon_id = 'u967'
  GROUP BY
    extract(year from survey.start_date),
    extract(month from survey.start_date),
    sites_in_ranges.site_id,
    sites_in_ranges.taxon_id,
    sites_in_ranges.range_id
  ;

  ALTER TABLE IF EXISTS surveys_by_month
  ADD CONSTRAINT surveys_by_month_pkey
    PRIMARY KEY (site_id, taxon_id, year, month);
  CREATE INDEX IF NOT EXISTS idx_surveys_by_month_site_id
    ON surveys_by_month (site_id);  
  CREATE INDEX IF NOT EXISTS idx_surveys_by_month_year
    ON surveys_by_month (year);
  CREATE INDEX IF NOT EXISTS idx_surveys_by_month_month
    ON surveys_by_month (month);
  CREATE INDEX IF NOT EXISTS idx_surveys_by_month_taxon_id
    ON surveys_by_month (taxon_id);


-- make aggregated table of sighting counts (for reporting rates) and mean counts (for that response variable type where appropriate) for tsx taxa at the species level x months by x sites
  -- begin by creating an indexed, reduced table of sightings for tsx taxa at species level sightings occurring in tsx sites
    DROP TABLE IF EXISTS sightings_in_sites;
    CREATE TEMPORARY TABLE sightings_in_sites AS
    -- 723,050 rows affected in 1 m 23 s
    SELECT
      sighting.id AS sighting_id,
      sighting.survey_id,
      sighting.species_id AS sp_id,
      sighting.individual_count,
      site_tsx.site_id
    FROM survey
    LEFT JOIN sighting ON survey.id = sighting.survey_id
    -- and why the fuck is this there?
    JOIN site_tsx ON survey.survey_type_id = site_tsx.site_type

    -- or this necessarily?
    JOIN survey_point
      ON survey.survey_point_id = survey_point.id
      AND ST_Intersects(survey_point.geom, site_tsx.geom)
      -- xxxx

    JOIN sites_in_ranges
      ON survey.survey_type_id = sites_in_ranges.site_type -- effectively limits surveys to types in use
      AND site_tsx.site_id = sites_in_ranges.site_id

--        doe sthere then need to be some sort of taxonomic link?... being no taxon_id in sightings how could there be?
      AND ????

    -- manually populated predicate for tsx taxa in this instance as the view wlab_sp may not flow all appropriate taxa at species level through to sightings. This would be better resolved though to avoid manually listing tsx taxa species
    -- JOIN wlab_sp ON sighting.species_id = wlab_sp.sp_id
    -- WHERE
    --   wlab_sp.tsx_taxa IS NOT NULL
    -- ;
    WHERE
      (survey.start_date BETWEEN '1999-01-11' AND '2021-12-31'
      AND
        (survey.source_id <> 71 -- LTERN Vic Highlands
        AND survey.source_id <> 77 -- LTERN Desert Program
        AND survey.source_id <> 36 -- Mallee Fire & Biodiversity Project
        AND survey.source_id <> 36 -- LaTrobe - Mallee VIC - fire study
        AND survey.source_id <> 97 -- Tern Supersites - Cumberland Plains
        AND survey.source_id <> 98 -- Tern SuperSites - Samfordy
        AND survey.source_id <> 99 -- Tern SuperSites - Robson Creek
        )
      AND
        (survey.survey_type_id = 1
        OR survey.survey_type_id = 2
        ))
      AND (sighting.species_id = 39
      OR sighting.species_id = 197
      OR sighting.species_id = 242
      OR sighting.species_id = 268
      OR sighting.species_id = 270
      OR sighting.species_id = 277
      OR sighting.species_id = 278
      OR sighting.species_id = 282
      OR sighting.species_id = 306
      OR sighting.species_id = 334
      OR sighting.species_id = 382
      OR sighting.species_id = 385
      OR sighting.species_id = 386
      OR sighting.species_id = 466
      OR sighting.species_id = 470
      OR sighting.species_id = 475
      OR sighting.species_id = 488
      OR sighting.species_id = 493
      OR sighting.species_id = 506
      OR sighting.species_id = 513
      OR sighting.species_id = 526
      OR sighting.species_id = 529
      OR sighting.species_id = 542
      OR sighting.species_id = 555
      OR sighting.species_id = 582
      OR sighting.species_id = 583
      OR sighting.species_id = 598
      OR sighting.species_id = 620
      OR sighting.species_id = 631
      OR sighting.species_id = 638
      OR sighting.species_id = 652
      OR sighting.species_id = 670
      OR sighting.species_id = 697
      OR sighting.species_id = 710
      OR sighting.species_id = 967)
    ;

    -- ALTER TABLE IF EXISTS sightings_in_sites
    -- ADD CONSTRAINT sightings_in_sites_pkey
    --   PRIMARY KEY (sp_id, site_id);
    CREATE INDEX IF NOT EXISTS idx_sightings_in_sites_survey_id
      ON sightings_in_sites (survey_id);
    CREATE INDEX IF NOT EXISTS idx_sightings_in_sites_sighting_id
      ON sightings_in_sites (sighting_id);
    CREATE INDEX IF NOT EXISTS idx_sightings_in_sites_sp_id
      ON sightings_in_sites (sp_id);
    CREATE INDEX IF NOT EXISTS idx_sightings_in_sites_site_id
      ON sightings_in_sites (site_id);

    -- check species taxa have come through
      -- SELECT
      --   sp_id,
      --   Count(*)
      -- FROM sightings_in_sites
      -- GROUP BY
      --   sp_id
      -- ;

  -- make main table
    -- range_alpha_ultrataxa excludes non-target subspecies
    -- Query returned successfully in 13 secs 728 msec. 65542 rows
    DROP TABLE IF EXISTS sightings_by_month;
    CREATE TEMPORARY TABLE sightings_by_month AS
      WITH ultrataxon_sightings_in_sites AS
        -- 625,466 rows affected in 6 s 263 ms
        (SELECT
          sightings_in_sites.sighting_id,
          sightings_in_sites.survey_id,
          sightings_in_sites.sp_id,
          sightings_in_sites.site_id,
          range_alpha_ultrataxa.taxon_id,
          range_alpha_ultrataxa.range_id,
          sightings_in_sites.individual_count
        FROM 
            (SELECT
              *
            FROM survey
            WHERE
              survey.start_date BETWEEN '1999-01-11' AND '2021-12-31'
              AND
                (survey.source_id <> 71 -- LTERN Vic Highlands
                AND survey.source_id <> 77 -- LTERN Desert Program
                AND survey.source_id <> 36 -- Mallee Fire & Biodiversity Project
                AND survey.source_id <> 36 -- LaTrobe - Mallee VIC - fire study
                AND survey.source_id <> 97 -- Tern Supersites - Cumberland Plains
                AND survey.source_id <> 98 -- Tern SuperSites - Samfordy
                AND survey.source_id <> 99 -- Tern SuperSites - Robson Creek
                )
              AND
                (survey.survey_type_id = 1
                OR survey.survey_type_id = 2
                )
            )survey_sub
        LEFT JOIN sightings_in_sites ON survey_sub.id = sightings_in_sites.survey_id
        JOIN range_alpha_ultrataxa ON sightings_in_sites.sp_id = range_alpha_ultrataxa.sp_id
        )
      SELECT
        extract(year from survey.start_date) AS year,
        extract(month from survey.start_date) AS month,
        ultrataxon_sightings_in_sites.site_id,
        ultrataxon_sightings_in_sites.taxon_id,
        ultrataxon_sightings_in_sites.range_id,
        COUNT(ultrataxon_sightings_in_sites.sighting_id) AS num_sightings,
        COUNT(ultrataxon_sightings_in_sites.sighting_id) FILTER (WHERE ultrataxon_sightings_in_sites.individual_count IS NULL) AS num_po_sightings, -- # presence only sightings
        AVG(ultrataxon_sightings_in_sites.individual_count) :: decimal AS mean_of_count,
        SUM(ultrataxon_sightings_in_sites.individual_count) AS sum_of_count
      FROM ultrataxon_sightings_in_sites
      JOIN survey ON ultrataxon_sightings_in_sites.survey_id = survey.id
      GROUP BY
        extract(year from survey.start_date),
        extract(month from survey.start_date),
        ultrataxon_sightings_in_sites.site_id,
        ultrataxon_sightings_in_sites.taxon_id,
        ultrataxon_sightings_in_sites.range_id
      ;

    ALTER TABLE IF EXISTS sightings_by_month
    ADD CONSTRAINT sightings_by_month_pkey
      PRIMARY KEY (site_id, taxon_id, range_id, year, month);
    CREATE INDEX IF NOT EXISTS idx_sightings_by_month_site_id
      ON sightings_by_month (site_id);
    CREATE INDEX IF NOT EXISTS idx_sightings_by_month_year
      ON sightings_by_month (year);
    CREATE INDEX IF NOT EXISTS idx_sightings_by_month_month
      ON sightings_by_month (month);
    CREATE INDEX IF NOT EXISTS idx_sightings_by_month_taxon_id
      ON sightings_by_month (taxon_id);
    CREATE INDEX IF NOT EXISTS idx_sightings_by_month_range_id
      ON sightings_by_month (range_id);

    -- check ultrataxataxa have come through
      -- SELECT
      --   taxon_id,
      --   Count(*)
      -- FROM sightings_by_month
      -- GROUP BY
      --   taxon_id
      -- ;