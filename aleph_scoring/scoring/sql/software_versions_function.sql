CREATE OR REPLACE FUNCTION annotate_version(software_name varchar, version_name varchar, version_date timestamp with time zone)
    RETURNS VARCHAR
    language plpgsql
as
$$
DECLARE
    annotation varchar;
BEGIN
    SELECT CASE
               WHEN
                    software_version.released_on <= version_date
                        AND (software_version.replaced_on is null OR version_date < software_version.replaced_on)
                    THEN
                        CASE
                            WHEN software_version.prerelease
                                THEN 'prerelease'
                            ELSE 'latest'
                        END
               WHEN software_version.replaced_on <= version_date
                        AND version_date < software_version.replaced_on + (interval '14' day)
                   THEN 'outdated'
               WHEN software_version.replaced_on + (interval '14' day) <= version_date
                   THEN 'obsolete'
               ELSE 'other'
               END
    INTO annotation
    FROM software_versions as software_version
    WHERE software_name = software_version.software
      AND version_name = software_version.name
    ORDER BY software_version.released_on DESC;

    IF NOT FOUND THEN
        RAISE NOTICE 'No matching records found for version_name: % and version_date: %', version_name, version_date;
        RETURN 'unknown';
    END IF;

    RETURN annotation;
END
$$;
