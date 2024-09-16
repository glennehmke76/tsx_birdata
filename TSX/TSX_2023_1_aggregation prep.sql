-- define focal taxa in taxonomic list (in this case wlab)
  DROP TABLE IF EXISTS wlab_tsx;
  CREATE TABLE wlab_tsx (
    taxon_id varchar NOT NULL,
    tsx_taxa int NOT NULL,
    PRIMARY KEY (taxon_id)
  );
  copy wlab_tsx FROM '/Users/glennehmke/Downloads/wlab_tsx.csv' DELIMITER ',' CSV HEADER;

  ALTER TABLE IF EXISTS wlab
    DROP COLUMN IF EXISTS tsx_taxa;
  ALTER TABLE IF EXISTS wlab
    ADD COLUMN tsx_taxa smallint;
  UPDATE wlab
  SET tsx_taxa = wlab_tsx.tsx_taxa
  FROM wlab_tsx
  WHERE wlab_tsx.taxon_id = wlab.taxon_id;

-- make a table of sites which occur within focal taxa distrinutions (sites x taxa)
  -- !!!! GE is using his set of alpha hulls in this instance cf the set the TSX workflow produces. These hulls will be broadly indicative, but there may be some differences to what the workflow produces given these alpha hulls are rather old. They should be sufficient for a general check however (and may be all but identical for many taxa)
  -- interesect site centroids with simplified alpha hull gemoetries limited to type 2 data tsx ultrataxa (incl hybrid zone duplication for subspecies where appropriate)
  -- this reduces the complexity of alpha geometries and produces an indexed table of ultrataxon taxon ranges (with range classes) from which to base subsequent processes.
  -- the table is limited to type 2 tsx taxa through wlab.tsx. To achange this alter the wlab_tsx.csv and re-import. 

  -- general notes / issues along the way
    -- Query returned successfully in 1 secs 59 msec. but only with union snap to = 0.1 otherwise ERROR:  lwgeom_unaryunion_prec: GEOS Error: TopologyException: unable to assign free hole to a shell at 1811709 -3109458... but 0.1 degrees tiso too much!...
    -- ... then removing aggregation (dissolve) as below works ...
        -- SELECT
        --   wlab.taxon_id,
        --   range_alpha.rnge AS range_id,
        --   ST_Transform(ST_Simplify(ST_Transform(range_alpha.geom, 3112), 50), 4283) AS geom
        -- FROM range_alpha
        -- JOIN wlab_range ON range_alpha.taxon_id_r = wlab_range.taxon_id_r 
        -- JOIN wlab ON wlab_range.taxon_id = wlab.taxon_id
        -- WHERE 
        --   wlab.tsx_taxa IS NOT NULL
        -- ;
    -- but then tried not bothering to simplify at all which seems to work... does this affect performance down the line though?
      -- but then any level of simplification gets ERROR:  lwgeom_unaryunion_prec: GEOS Error: TopologyException: side location conflict at 152.44397212304659 -26.225221897360768. This can occur if the input geometry is invalid.

-- local; 21s
-- AcuGIS; 2m22s
  DROP TABLE IF EXISTS range_alpha_ultrataxa;
  CREATE TEMPORARY TABLE range_alpha_ultrataxa AS
  WITH sub AS
    (SELECT
      wlab.sp_id,
      wlab.taxon_id,
      range_alpha.rnge AS range_id,
      ST_Union(range_alpha.geom) AS geom
    FROM range_alpha
    JOIN wlab_range ON range_alpha.taxon_id_r = wlab_range.taxon_id_r 
    JOIN wlab ON wlab_range.taxon_id = wlab.taxon_id
    WHERE 
      wlab.tsx_taxa IS NOT NULL
    GROUP BY
      wlab.taxon_id,
      range_alpha.rnge
    )
  SELECT
    sub.sp_id,
    sub.taxon_id,
    sub.range_id,
    ST_Simplify(sub.geom, 0.001) AS geom
  FROM sub
  ;
  ALTER TABLE IF EXISTS range_alpha_ultrataxa
  ADD CONSTRAINT range_alpha_ultrataxa_pkey
    PRIMARY KEY (taxon_id, range_id);
CREATE INDEX IF NOT EXISTS idx_range_alpha_ultrataxa_taxon_id
  ON range_alpha_ultrataxa (taxon_id);
  CREATE INDEX IF NOT EXISTS idx_range_alpha_ultrataxa_sp_id
    ON range_alpha_ultrataxa (sp_id);
  CREATE INDEX IF NOT EXISTS idx_range_alpha_ultrataxa_range_id
    ON range_alpha_ultrataxa (range_id);
  CREATE INDEX idx_range_alpha_ultrataxa_geom ON range_alpha_ultrataxa USING gist (geom);

-- intersect range_alpha_ultrataxa with sites
  DROP TABLE IF EXISTS sites_in_ranges;
  CREATE TEMPORARY TABLE sites_in_ranges AS
    -- 19/9 added sp_id for use in ultrataxon_sightings_in_sites in place of range_alpha_ultrataxa
  -- Successfully run. Total query runtime: 4 secs 169120 rows affected.
  WITH site_centroids AS
    (SELECT
      site_tsx.site_id,
      site_tsx.site_type,
      ST_Centroid(geom) AS geom
    FROM site_tsx
    )
  SELECT
    site_tsx.site_id,
    site_tsx.site_type,
    sub.sp_id,
    sub.taxon_id,
    sub.range_id
  FROM
      (SELECT
        site_centroids.site_id,
        site_centroids.site_type,
        range_alpha_ultrataxa.sp_id,
        range_alpha_ultrataxa.taxon_id,
        range_alpha_ultrataxa.range_id
      FROM site_centroids
      JOIN range_alpha_ultrataxa ON ST_Intersects(site_centroids.geom, range_alpha_ultrataxa.geom)
      )sub
  JOIN site_tsx ON sub.site_id = site_tsx.site_id
  ;

  ALTER TABLE IF EXISTS sites_in_ranges
  ADD CONSTRAINT sites_in_ranges_pkey
    PRIMARY KEY (site_id, taxon_id);
  CREATE INDEX IF NOT EXISTS idx_sites_in_ranges_site_id
    ON sites_in_ranges (site_id);
  CREATE INDEX IF NOT EXISTS idx_sites_in_ranges_sp_id
    ON sites_in_ranges (sp_id);
  CREATE INDEX IF NOT EXISTS idx_sites_in_ranges_taxon_id
    ON sites_in_ranges (taxon_id);
  CREATE INDEX IF NOT EXISTS idx_sites_in_ranges_site_type
    ON sites_in_ranges (site_type);
  CREATE INDEX IF NOT EXISTS idx_sites_in_ranges_range_id
    ON sites_in_ranges (range_id);

  -- check data
    -- SELECT sites_in_ranges.*, site_tsx.geom
    -- FROM sites_in_ranges
    -- JOIN site_tsx ON sites_in_ranges.site_id = site_tsx.site_id