CREATE TABLE software_versions
(
    software    varchar(30)        not null,
    name        varchar(30)        not null,
    released_on timestamp          not null,
    replaced_on timestamp,
    prerelease  bool default false not null,
    UNIQUE (software, name)
);
