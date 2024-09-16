-- cross-tabulate aggregated by year to lpi compliant format
  -- with CTE 138,787 rows retrieved (execution: 4 m 59 s 901 ms, fetching: 6 s 515 ms)
CREATE TABLE tsx_wide_rr AS
  WITH site_region AS
    (SELECT
      site_centroids.site_id,
      region_sibra.id,
      region_sibra.sub_name_7,
      ST_X(ST_Centroid(region_sibra.geom)) AS x,
      ST_Y(ST_Centroid(region_sibra.geom)) AS y
    FROM
        (SELECT
          site_tsx.site_id,
          site_tsx.site_type,
          site_permutation,
          ST_Centroid(geom) AS geom
        FROM site_tsx
        )site_centroids
    JOIN region_sibra ON ST_Intersects(site_centroids.geom, region_sibra.geom)
    )
  SELECT
    wlab.taxon_scientific_name,
    wlab.sp_id,
    data_by_year.taxon_id,
    wlab.taxon_name,
    data_by_year.site_id,
    survey_type.name AS search_type_desc,
    site_tsx.site_permutation,
    -- site_tsx.geom,
    site_region.sub_name_7 AS region_name,
    site_region.x AS region_centroid_longitude,
    site_region.y AS region_centroid_latitude,
    AVG(mean_monthly_rr) FILTER (WHERE year = 1999) AS "1999",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2000) AS "2000",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2001) AS "2001",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2002) AS "2002",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2003) AS "2003",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2004) AS "2004",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2005) AS "2005",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2006) AS "2006",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2007) AS "2007",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2008) AS "2008",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2009) AS "2009",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2010) AS "2010",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2011) AS "2011",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2012) AS "2012",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2013) AS "2013",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2014) AS "2014",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2015) AS "2015",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2016) AS "2016",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2017) AS "2017",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2018) AS "2018",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2019) AS "2019",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2019) AS "2020",
    AVG(mean_monthly_rr) FILTER (WHERE year = 2019) AS "2021"
  FROM data_by_year
  LEFT JOIN site_tsx ON data_by_year.site_id = site_tsx.site_id
  JOIN survey_type ON site_tsx.site_type = survey_type.id
  LEFT JOIN wlab ON data_by_year.taxon_id = wlab.taxon_id
  LEFT JOIN site_region ON site_tsx.site_id = site_region.site_id
  WHERE
    data_by_year.response_variable_type_id IS NOT NULL
    AND data_by_year.range_id = 1
  GROUP BY
    wlab.taxon_scientific_name,
    wlab.sp_id,
    data_by_year.taxon_id,
    wlab.taxon_name,
    data_by_year.site_id,
    survey_type.name,
    site_tsx.site_permutation,
    site_region.sub_name_7,
    site_region.x,
    site_region.y
;

-- add nearest (manually populated nearest after using python nearest in this instance) - better done via ST_Dist and ST_DWithin or some such SQL in future
UPDATE tsx_wide_rr
SET
  region_name = sub.region_name,
  region_centroid_latitude = sub.region_centroid_latitude,
  region_centroid_longitude = sub. region_centroid_longitude
FROM
    (SELECT
       nearest.site_id,
       region_sibra.sub_name_7 AS region_name,
      ST_X(ST_Centroid(region_sibra.geom)) AS region_centroid_latitude,
      ST_Y(ST_Centroid(region_sibra.geom)) AS region_centroid_longitude
    FROM nearest
    JOIN region_sibra ON nearest.sub_code_7 = region_sibra.sub_code_7
    )sub
WHERE
  tsx_wide_rr.site_id = sub.site_id
  AND tsx_wide_rr.region_name IS NULL
;

  -- count ts
  WITH site_region AS
    (SELECT
      site_centroids.site_id,
      region_sibra.id,
      region_sibra.sub_name_7,
      ST_X(ST_Centroid(region_sibra.geom)) AS x,
      ST_Y(ST_Centroid(region_sibra.geom)) AS y
    FROM
        (SELECT
          site_tsx.site_id,
          site_tsx.site_type,
          ST_Centroid(geom) AS geom
        FROM site_tsx
        )site_centroids
    JOIN region_sibra ON ST_Intersects(site_centroids.geom, region_sibra.geom)
    )
  SELECT
    wlab.taxon_scientific_name,
    wlab.sp_id,
    data_by_year.taxon_id,
    wlab.taxon_name,
    data_by_year.site_id,
    survey_type.name AS search_type_desc,
    site_tsx.site_permutation,
    -- site_tsx.geom,
    site_region.sub_name_7 AS region_name,
    site_region.x AS region_centroid_longitude,
    site_region.y AS region_centroid_latitude,
    AVG(monthly_mean_abundance) FILTER (WHERE year = 1999) AS "1999",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2000) AS "2000",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2001) AS "2001",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2002) AS "2002",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2003) AS "2003",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2004) AS "2004",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2005) AS "2005",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2006) AS "2006",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2007) AS "2007",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2008) AS "2008",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2009) AS "2009",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2010) AS "2010",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2011) AS "2011",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2012) AS "2012",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2013) AS "2013",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2014) AS "2014",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2015) AS "2015",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2016) AS "2016",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2017) AS "2017",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2018) AS "2018",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2019) AS "2019",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2019) AS "2020",
    AVG(monthly_mean_abundance) FILTER (WHERE year = 2019) AS "2021"
  FROM data_by_year
  LEFT JOIN site_tsx ON data_by_year.site_id = site_tsx.site_id
  JOIN survey_type ON site_tsx.site_type = survey_type.id
  LEFT JOIN wlab ON data_by_year.taxon_id = wlab.taxon_id
  LEFT JOIN site_region ON site_tsx.site_id = site_region.site_id
  WHERE
    data_by_year.response_variable_type_id IS NOT NULL
    AND data_by_year.range_id = 1
  GROUP BY
    wlab.taxon_scientific_name,
    wlab.sp_id,
    data_by_year.taxon_id,
    wlab.taxon_name,
    data_by_year.site_id,
    survey_type.name,
    site_tsx.site_permutation,
    site_region.sub_name_7,
    site_region.x,
    site_region.y
  ;