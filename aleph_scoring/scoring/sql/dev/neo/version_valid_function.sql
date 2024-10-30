CREATE OR REPLACE FUNCTION is_version_valid(software_name varchar, version_name varchar, version_date timestamp with time zone)
RETURNS integer
LANGUAGE plpgsql
AS
$$
DECLARE
    annotation integer;
BEGIN
    SELECT CASE
               WHEN EXISTS (
                   SELECT 1
                   FROM software_versions sv
                   WHERE sv.software = software_name
                     AND sv.name = version_name
                     AND sv.released_on <= version_date
                     AND (sv.replaced_on IS NULL OR version_date < sv.replaced_on)) THEN 1
               WHEN EXISTS (
                   SELECT 1
                   FROM software_versions sv
                   WHERE sv.software = software_name
                     AND sv.name = version_name
                     AND sv.replaced_on IS NOT NULL
                     AND sv.replaced_on <= version_date
                     AND version_date < sv.replaced_on + INTERVAL '14 days') THEN 1
               ELSE 0
           END
    INTO annotation;

    RETURN annotation;
END
$$;

CREATE INDEX IF NOT EXISTS idx_software_versions ON software_versions (software, name, released_on, replaced_on);
CREATE INDEX IF NOT EXISTS idx_sv_software_name_released_on ON software_versions (software, name, released_on);
CREATE INDEX IF NOT EXISTS idx_sv_software_name_replaced_on ON software_versions (software, name, replaced_on);
CREATE INDEX IF NOT EXISTS idx_posts_owner_type_creation_datetime ON posts(owner, type, creation_datetime);
CREATE INDEX IF NOT EXISTS idx_posts_content_metrics_crn ON posts USING GIN ((content -> 'metrics' -> 'crn'));