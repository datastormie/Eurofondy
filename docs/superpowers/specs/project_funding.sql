-- Datamart verification SQL — run against data/eufunds.duckdb to sanity-check
-- the 4 mart definitions before scripts/build_datamart.py is written.
-- e.g.: duckdb data/eufunds.duckdb < docs/superpowers/specs/project_funding.sql

CREATE SCHEMA IF NOT EXISTS datamart;

-- 1. regional_funding (one row per project) ------------------------------
-- suma_eu/suma_sr are recomputed from itms21_projekt_financnyplan.zdroj_id
-- (ciselnik 1052), matching website.itms21_projects_current's own derivation
-- ("EÚ"/"ŠR" substring in the zdroj name) -- verified to match exactly for
-- all 4,375 projects.
-- Grain fix vs the earlier draft: itms21_projekt_miestorealizaciefull has
-- multiple location rows per project that can map to the *same* nuts3_id
-- (e.g. several municipalities in one region) -- DISTINCT on (project_id,
-- nuts3_id) below before counting/listagg-ing regions, so region_count and
-- kraj_names aren't inflated by that duplication.

CREATE OR REPLACE TABLE datamart.regional_funding AS
WITH fp AS (
    SELECT
        f.project_id,
        sum(CASE WHEN zd.nazovsk LIKE '%EÚ%' OR zd.nazovsk LIKE '%EU%' THEN f.suma ELSE 0 END) AS suma_eu,
        sum(CASE WHEN zd.nazovsk LIKE '%ŠR%' OR zd.nazovsk LIKE '%SR%' THEN f.suma ELSE 0 END) AS suma_sr
    FROM slovakia.itms21_projekt_financnyplan f
    LEFT JOIN slovakia.itms21_ciselniky_detail zd
        ON zd.ciselnik_kod = '1052' AND zd.id = f.zdroj_id
    GROUP BY f.project_id
),
regions AS (
    SELECT DISTINCT m.project_id, m.nuts3_id, nuts.nazovsk AS kraj_nazov
    FROM slovakia.itms21_projekt_miestorealizaciefull m
    JOIN slovakia.itms21_ciselniky_detail nuts
        ON nuts.ciselnik_kod = '1006' AND nuts.id = m.nuts3_id
    WHERE m.stat_id = 210 AND m.nuts3_id IS NOT NULL
),
region_agg AS (
    SELECT
        project_id,
        count(*) AS region_count,
        string_agg(kraj_nazov, ', ' ORDER BY kraj_nazov) AS kraj_names
    FROM regions
    GROUP BY project_id
)
SELECT
    p.id AS project_id,
    p.kod AS project_kod,
    p.nazov AS project_nazov,
    p.program_id,
    prog.skratka AS program_skratka,
    prog.nazovsk AS program_nazov,
    ben.nazov AS prijimatel_nazov,
    ra.region_count,
    ra.kraj_names,
    fp.suma_eu,
    fp.suma_sr,
    fp.suma_eu + fp.suma_sr AS suma_spolu,
    p.stav,
    p.ukonceny,
    p.vrealizacii
FROM slovakia.itms21_projekt p
JOIN region_agg ra ON ra.project_id = p.id
LEFT JOIN slovakia.itms21_program prog ON prog.id = p.program_id
LEFT JOIN slovakia.itms21_subjekt ben ON ben.id = p.prijimatel_id
LEFT JOIN fp ON fp.project_id = p.id;

SELECT count(*) AS total_projects,
       sum(CASE WHEN region_count = 1 THEN 1 ELSE 0 END) AS single_region_projects,
       sum(CASE WHEN region_count > 1 THEN 1 ELSE 0 END) AS multi_region_projects
FROM datamart.regional_funding;
-- expect: total_projects <= 4375, single_region_projects = 3722


-- 1b. regional_summary (one row per region, for the map) -----------------
-- Grain: one row per Slovak region + one synthetic "nationwide" row.
-- Sums only single-region projects per region (region_count = 1), per the
-- design caveat: multi-region projects would overstate a region's total if
-- summed in -- they're rolled into the nationwide row instead.

CREATE OR REPLACE TABLE datamart.regional_summary AS
SELECT
    r.nuts3_id,
    r.kraj_nazov,
    count(*) AS project_count,
    sum(rf.suma_eu) AS suma_eu,
    sum(rf.suma_sr) AS suma_sr,
    sum(rf.suma_spolu) AS suma_spolu
FROM (
    SELECT DISTINCT m.project_id, m.nuts3_id, nuts.nazovsk AS kraj_nazov
    FROM slovakia.itms21_projekt_miestorealizaciefull m
    JOIN slovakia.itms21_ciselniky_detail nuts
        ON nuts.ciselnik_kod = '1006' AND nuts.id = m.nuts3_id
    WHERE m.stat_id = 210 AND m.nuts3_id IS NOT NULL
) r
JOIN datamart.regional_funding rf ON rf.project_id = r.project_id AND rf.region_count = 1
GROUP BY r.nuts3_id, r.kraj_nazov
UNION ALL
SELECT
    NULL AS nuts3_id,
    'Celoštátne (viacregionálne)' AS kraj_nazov,
    count(*) AS project_count,
    sum(suma_eu) AS suma_eu,
    sum(suma_sr) AS suma_sr,
    sum(suma_spolu) AS suma_spolu
FROM datamart.regional_funding
WHERE region_count > 1;

SELECT * FROM datamart.regional_summary ORDER BY nuts3_id;
-- expect: 8 region rows + 1 nationwide row, region project_count summing
-- with the nationwide row's project_count to total_projects above


-- 2. beneficiary_funding -------------------------------------------------

CREATE OR REPLACE TABLE datamart.beneficiary_funding AS
WITH fp AS (
    SELECT
        f.project_id,
        sum(CASE WHEN zd.nazovsk LIKE '%EÚ%' OR zd.nazovsk LIKE '%EU%' THEN f.suma ELSE 0 END) AS suma_eu,
        sum(CASE WHEN zd.nazovsk LIKE '%ŠR%' OR zd.nazovsk LIKE '%SR%' THEN f.suma ELSE 0 END) AS suma_sr
    FROM slovakia.itms21_projekt_financnyplan f
    LEFT JOIN slovakia.itms21_ciselniky_detail zd
        ON zd.ciselnik_kod = '1052' AND zd.id = f.zdroj_id
    GROUP BY f.project_id
),
proj AS (
    SELECT p.id AS project_id, p.prijimatel_id, p.planovanarealizaciazaciatok,
           fp.suma_eu, fp.suma_sr, (fp.suma_eu + fp.suma_sr) AS suma_spolu
    FROM slovakia.itms21_projekt p
    LEFT JOIN fp ON fp.project_id = p.id
    WHERE p.prijimatel_id IS NOT NULL
),
sector_counts AS (
    SELECT p.prijimatel_id, hc.nazovsk AS sector, count(*) AS n
    FROM slovakia.itms21_projekt p
    JOIN slovakia.itms21_projekt_hospodarskacinnost h ON h.project_id = p.id
    JOIN slovakia.itms21_ciselniky_detail hc
        ON hc.ciselnik_kod = '1038' AND hc.id = h.hospodarskacinnost_id
    WHERE p.prijimatel_id IS NOT NULL
    GROUP BY 1, 2
),
primary_sector AS (
    SELECT prijimatel_id, sector
    FROM (
        SELECT prijimatel_id, sector,
               row_number() OVER (PARTITION BY prijimatel_id ORDER BY n DESC) AS rn
        FROM sector_counts
    )
    WHERE rn = 1
)
SELECT
    s.id AS subjekt_id,
    s.nazov,
    s.ico,
    pf.nazovsk AS pravnaforma_nazov,
    s.adresa_obec,
    ps.sector AS primary_sector,
    count(*) AS project_count,
    sum(proj.suma_eu) AS suma_eu,
    sum(proj.suma_sr) AS suma_sr,
    sum(proj.suma_spolu) AS suma_spolu,
    min(year(epoch_ms(proj.planovanarealizaciazaciatok))) AS first_project_year,
    max(year(epoch_ms(proj.planovanarealizaciazaciatok))) AS last_project_year
FROM proj
JOIN slovakia.itms21_subjekt s ON s.id = proj.prijimatel_id
LEFT JOIN slovakia.itms21_ciselniky_detail pf
    ON pf.ciselnik_kod = '1009' AND pf.id = s.pravnaforma_id
LEFT JOIN primary_sector ps ON ps.prijimatel_id = s.id
GROUP BY s.id, s.nazov, s.ico, pf.nazovsk, s.adresa_obec, ps.sector;

SELECT count(*) AS total_beneficiaries FROM datamart.beneficiary_funding;
-- expect: 2121


-- 3. procurement_contracts ------------------------------------------------

CREATE OR REPLACE TABLE datamart.procurement_contracts AS
SELECT
    z.id AS contract_id,
    z.kod AS contract_kod,
    z.nazov AS contract_nazov,
    z.cislozmluvy,
    z.celkovasumazmluvy,
    z.sumabezdph,
    TRY_CAST(z.datumucinnosti AS BIGINT) AS datumucinnosti,
    TRY_CAST(z.datumplatnosti AS BIGINT) AS datumplatnosti,
    z.urlodkaznazmluvu AS url_zmluva,
    COALESCE(sup_s.nazov, sup_d.nazov) AS supplier_nazov,
    COALESCE(sup_s.ico, sup_d.ico) AS supplier_ico,
    vo.id AS procurement_id,
    vo.kod AS procurement_kod,
    vo.nazov AS procurement_nazov,
    vo.stav AS procurement_stav,
    cpv.nazovsk AS cpv_nazov,
    proj.id AS project_id,
    proj.kod AS project_kod,
    proj.nazov AS project_nazov,
    prog.skratka AS program_skratka
FROM slovakia.itms21_zmluvaverejneobstaravanie z
LEFT JOIN slovakia.itms21_subjekt sup_s ON sup_s.id = z.hlavnydodavatelsubjekt_id
LEFT JOIN slovakia.itms21_dodavatelobstaravatel sup_d ON sup_d.id = z.hlavnydodavateldodavatelobstaravatel_id
LEFT JOIN slovakia.itms21_verejneobstaravanie_detail vo ON vo.id = z.verejneobstaravanie_id
LEFT JOIN slovakia.itms21_ciselniky_detail cpv
    ON cpv.ciselnik_kod = '1049' AND cpv.id = vo.hlavnypredmethlavnyslovnik_id
LEFT JOIN slovakia.itms21_verejneobstaravanie_detail_projekty link
    ON link.verejneobstaravanie_detail_id = vo.id
LEFT JOIN slovakia.itms21_projekt proj ON proj.id = link.id
LEFT JOIN slovakia.itms21_program prog ON prog.id = proj.program_id;

SELECT count(*) AS total_contracts,
       sum(CASE WHEN supplier_nazov IS NULL THEN 1 ELSE 0 END) AS missing_supplier
FROM datamart.procurement_contracts;
-- expect: total_contracts = 7714, missing_supplier = 0


-- 4. payment_disbursements -------------------------------------------------

CREATE OR REPLACE TABLE datamart.payment_disbursements AS
SELECT
    z.id AS zop_id,
    z.kod,
    z.typ,
    z.projekt_id,
    p.kod AS project_kod,
    p.nazov AS project_nazov,
    p.program_id,
    prog.skratka AS program_skratka,
    s.nazov AS prijimatel_nazov,
    z.narokovanasuma,
    COALESCE(
        TRY_CAST(z.datumuhrady AS BIGINT),
        TRY_CAST(z.datumprijatia AS BIGINT),
        TRY_CAST(z.createdat AS BIGINT)
    ) AS event_date,
    z.neuhradena,
    z.zopjezaverecna
FROM slovakia.itms21_zop z
LEFT JOIN slovakia.itms21_projekt p ON p.id = z.projekt_id
LEFT JOIN slovakia.itms21_program prog ON prog.id = p.program_id
LEFT JOIN slovakia.itms21_subjekt s ON s.id = z.prijimatel_id;

SELECT count(*) AS total_payments,
       sum(CASE WHEN projekt_id IS NULL THEN 1 ELSE 0 END) AS missing_project
FROM datamart.payment_disbursements;
-- expect: total_payments = 22970, missing_project = 0
