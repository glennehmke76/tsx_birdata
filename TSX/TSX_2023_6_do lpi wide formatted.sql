



FROM aggregate4d_xxxx
JOIN 

INNER JOIN taxon ON taxon.id = agg.taxon_id
LEFT JOIN search_type ON search_type.id = agg.search_type_id
INNER JOIN source ON source.id = agg.source_id
INNER JOIN unit ON unit.id = agg.unit_id
LEFT JOIN region ON region.id = agg.region_id
LEFT JOIN region_centroid ON region_centroid.id = agg.region_id
LEFT JOIN taxon_source_alpha_hull alpha ON alpha.taxon_id = agg.taxon_id AND alpha.source_id = agg.source_id AND alpha.data_type = agg.data_type
LEFT JOIN data_source ON data_source.taxon_id = agg.taxon_id AND data_source.source_id = agg.source_id
LEFT JOIN t1_site ON site_id = t1_site.id AND agg.data_type = 1
LEFT JOIN t2_site ON site_id = t2_site.id AND agg.data_type = 2








 SELECT
  time_series_id AS TimeSeriesID,
  taxon.spno AS SpNo,
  taxon.id AS TaxonID,
  taxon.common_name AS CommonName,
  taxon.`order` AS `Order`,
  taxon.scientific_name AS scientific_name,
  taxon.family_scientific_name AS Family,
  taxon.family_common_name AS FamilyCommonName,
    (SELECT
        GROUP_CONCAT(
            CONCAT(taxon_group.group_name, COALESCE(CONCAT(':', taxon_group.subgroup_name), ''))
        )
        FROM taxon_group
        WHERE taxon_group.taxon_id = taxon.id
    ) AS FunctionalGroup,
    taxon.taxonomic_group AS TaxonomicGroup,
    CASE taxon.taxonomic_group
        WHEN 'Birds' THEN 'Aves'
        WHEN 'Mammals' THEN 'Mammalia'
        ELSE ''
    END AS Class,
  taxon.national_priority AS NationalPriorityTaxa,
  (SELECT description FROM taxon_status WHERE taxon_status.id = taxon.epbc_status_id) AS EPBCStatus,
  (SELECT description FROM taxon_status WHERE taxon_status.id = taxon.iucn_status_id) AS IUCNStatus,
  (SELECT description FROM taxon_status WHERE taxon_status.id = taxon.state_status_id) AS StatePlantStatus,
  (SELECT description FROM taxon_status WHERE taxon_status.id = taxon.bird_action_plan_status_id) AS BirdActionPlanStatus,
  (SELECT description FROM taxon_status WHERE taxon_status.id = taxon.max_status_id) AS MaxStatus,
  search_type.id AS SearchTypeID,
  search_type.description AS SearchTypeDesc,
  COALESCE(site_id, grid_cell_id) AS SiteID,
  COALESCE(
      t1_site.name,
      t2_site.name,
      CONCAT('site_', agg.data_type, '_', site_id),
      CONCAT('grid_', grid_cell_id)) AS SiteName,
  COALESCE((SELECT type FROM management WHERE t1_site.management_id = management.id), 'No known management') AS Management,
  COALESCE((SELECT description FROM management WHERE t1_site.management_id = management.id), 'Unknown') AS ManagementCategory,
  t1_site.management_comments AS ManagementCategoryComments,
  source.id AS SourceID,
  source.description AS SourceDesc,
  (SELECT description FROM monitoring_program WHERE source.monitoring_program_id = monitoring_program.id) AS MonitoringProgram,
  unit.id AS UnitID,
  unit.description AS Unit,
  region.name AS Region,
  region.state AS State,
  MIN(region_centroid.x) AS RegionCentroidLongitude,
  MIN(region_centroid.y) AS RegionCentroidLatitude,
  region.positional_accuracy_in_m AS RegionCentroidAccuracy,
  {value_series} AS value_series,
  COUNT(*) AS value_count,
  agg.data_type AS DataType,
  (SELECT description FROM experimental_design_type WHERE agg.experimental_design_type_id = experimental_design_type.id) AS ExperimentalDesignType,
  (SELECT description FROM response_variable_type WHERE agg.response_variable_type_id = response_variable_type.id) AS ResponseVariableType,
  (CASE 
    WHEN taxon.suppress_spatial_representativeness AND alpha.core_range_area_in_m2 THEN NULL
    ELSE ROUND(alpha.alpha_hull_area_in_m2 / alpha.core_range_area_in_m2, 4) END) AS SpatialRepresentativeness,
  data_source.absences_recorded AS AbsencesRecorded,
  data_source.standardisation_of_method_effort_id AS StandardisationOfMethodEffort,
  data_source.objective_of_monitoring_id AS ObjectiveOfMonitoring,
  data_source.consistency_of_monitoring_id AS ConsistencyOfMonitoring,
  data_source.data_agreement_id AS DataAgreement,
  data_source.suppress_aggregated_data AS SuppressAggregatedData,
  MAX(ST_X(agg.centroid_coords)) AS SurveysCentroidLongitude,
  MAX(ST_Y(agg.centroid_coords)) AS SurveysCentroidLatitude,
  MAX(agg.positional_accuracy_in_m) AS SurveysSpatialAccuracy,
  SUM(agg.survey_count) AS SurveyCount,
  data_source.citation AS Citation
FROM
  {aggregated_table} agg
  INNER JOIN taxon ON taxon.id = agg.taxon_id
  LEFT JOIN search_type ON search_type.id = agg.search_type_id
  INNER JOIN source ON source.id = agg.source_id
  INNER JOIN unit ON unit.id = agg.unit_id
  LEFT JOIN region ON region.id = agg.region_id
  LEFT JOIN region_centroid ON region_centroid.id = agg.region_id
  LEFT JOIN taxon_source_alpha_hull alpha ON alpha.taxon_id = agg.taxon_id AND alpha.source_id = agg.source_id AND alpha.data_type = agg.data_type
  LEFT JOIN data_source ON data_source.taxon_id = agg.taxon_id AND data_source.source_id = agg.source_id
  LEFT JOIN t1_site ON site_id = t1_site.id AND agg.data_type = 1
  LEFT JOIN t2_site ON site_id = t2_site.id AND agg.data_type = 2
WHERE agg.taxon_id = :taxon_id
AND start_date_y >= :min_year
AND start_date_y <= :max_year
{where_conditions}
GROUP BY
  agg.source_id,
  agg.search_type_id,
  agg.site_id,
  agg.grid_cell_id,
  agg.experimental_design_type_id,
  agg.response_variable_type_id,
  agg.region_id,
  agg.unit_id,
  agg.data_type
{having_clause}
ORDER BY
  agg.source_id,
  agg.search_type_id,
  agg.site_id,
  agg.grid_cell_id,
  agg.experimental_design_type_id,
  agg.response_variable_type_id,
  agg.region_id,
  agg.unit_id,
  agg.data_type





SELECT t2_processed_survey.id, tmp_taxon_site.taxon_id
		FROM t2_survey
		INNER JOIN t2_survey_site ON t2_survey.id = t2_survey_site.survey_id
		INNER JOIN t2_processed_survey ON t2_survey.id = t2_processed_survey.raw_survey_id AND t2_processed_survey.experimental_design_type_id = 1
		INNER JOIN tmp_taxon_site ON t2_survey_site.site_id = tmp_taxon_site.site_id AND taxon_id = :taxon_id
		LEFT JOIN t2_processed_sighting ON t2_processed_sighting.survey_id = t2_processed_survey.id AND t2_processed_sighting.taxon_id = tmp_taxon_site.taxon_id
		WHERE t2_processed_sighting.id IS NULL





-- filters

CREATE TEMPORARY TABLE tmp_filtered_ts
		( INDEX (time_series_id) )
		SELECT time_series_id
		FROM aggregated_by_year agg
		INNER JOIN taxon ON agg.taxon_id = taxon.id
		LEFT JOIN data_source ON data_source.taxon_id = agg.taxon_id AND data_source.source_id = agg.source_id
		WHERE agg.start_date_y <= COALESCE(data_source.end_year, :max_year)
		AND agg.start_date_y >= COALESCE(data_source.start_year, :min_year)
		AND NOT data_source.exclude_from_analysis
		AND COALESCE(agg.search_type_id, 0) != 6
		AND COALESCE(taxon.max_status_id, 0) NOT IN (0,1,7)
		AND region_id IS NOT NULL
		AND COALESCE(data_source.data_agreement_id, -1) NOT IN (0)
		AND COALESCE(data_source.standardisation_of_method_effort_id, -1) NOT IN (0, 1)
		AND COALESCE(data_source.consistency_of_monitoring_id, -1) NOT IN (0, 1)
		AND experimental_design_type_id = 1
		GROUP BY agg.time_series_id
		HAVING MAX(value) > 0
		AND COUNT(DISTINCT start_date_y) >= :min_tssy;
	, {
		'min_year': min_year,
		'max_year': max_year,
		'min_tssy': min_tssy



    UPDATE aggregated_by_year agg
		LEFT JOIN data_source ON data_source.taxon_id = agg.taxon_id AND data_source.source_id = agg.source_id
		SET agg.include_in_analysis =
			agg.time_series_id IN (SELECT time_series_id FROM tmp_filtered_ts)
			AND agg.start_date_y <= COALESCE(data_source.end_year, :max_year)
			AND agg.start_date_y >= COALESCE(data_source.start_year, :min_year)