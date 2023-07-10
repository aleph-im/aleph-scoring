BEGIN;
       DELETE FROM software_versions WHERE software = 'pyaleph';

       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.5.0-rc2', '2023-04-14T16:50:26Z', null, true);
       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.5.0-rc1', '2023-03-28T13:16:11Z', '2023-04-14T16:50:26Z', true);
       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.4.7', '2023-03-21T11:31:26Z', null, false);
       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.4.6', '2023-03-20T17:19:44Z', '2023-03-21T11:31:26Z', false);
       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.4.5', '2023-03-20T16:50:50Z', '2023-03-20T17:19:44Z', false);
       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.4.4', '2023-02-03T15:40:38Z', '2023-03-20T16:50:50Z', false);
       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.4.3', '2022-10-20T10:53:26Z', '2023-02-03T15:40:38Z', false);
       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.4.2', '2022-10-18T10:17:56Z', '2022-10-20T10:53:26Z', false);
       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.4.1', '2022-10-17T13:11:47Z', '2022-10-18T10:17:56Z', false);
       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.4.0', '2022-10-17T10:25:44Z', '2022-10-17T13:11:47Z', false);
       INSERT INTO software_versions
       VALUES ('pyaleph', 'v0.3.3', '2022-09-02T12:35:25Z', '2022-10-17T10:25:44Z', false);

       INSERT INTO software_versions
       VALUES ('aleph-vm', '0.2.5', '2022-10-06T20:58:31Z', null, false);
       INSERT INTO software_versions
       VALUES ('aleph-vm', '0.2.4', '2022-07-27T08:55:17Z', '2022-10-06T20:58:31Z', false);
       INSERT INTO software_versions
       VALUES ('aleph-vm', '0.2.3', '2022-07-05T22:32:16Z', '2022-07-27T08:55:17Z', false);
       INSERT INTO software_versions
       VALUES ('aleph-vm', '0.2.2', '2022-06-28T10:05:10Z', '2022-07-05T22:32:16Z', false);
       INSERT INTO software_versions
       VALUES ('aleph-vm', '0.2.1', '2022-05-19T16:02:22Z', '2022-06-28T10:05:10Z', false);
       INSERT INTO software_versions
       VALUES ('aleph-vm', '0.2.0', '2022-03-31T09:55:27Z', '2022-05-19T16:02:22Z', false);
COMMIT;


SELECT annotate_version('pyaleph', 'v0.5.0-rc1', '2023-04-10'::timestamp) as prerelease_1,
       annotate_version('pyaleph', 'v0.5.0-rc1', '2023-04-15'::timestamp) as outdated_prerelease_1,
       annotate_version('pyaleph', 'v0.5.0-rc1', to_timestamp(1681514423.850069)::timestamp without time zone) as outdated_prerelease_2,
       annotate_version('pyaleph', 'v0.5.0-rc2', to_timestamp(1681514423.850069)::timestamp without time zone) as prerelease_3,
       annotate_version('pyaleph', 'v0.5.0-rc2', '2023-04-15'::timestamp) as prerelease_2,
       annotate_version('pyaleph', 'v0.A.B', '2023-03-30'::timestamp)     as unknown,
       annotate_version('pyaleph', 'v0.4.7', '2023-03-30'::timestamp)     as latest,

       annotate_version('aleph-vm', '0.2.5', '2023-04-15'::timestamp)     as vm_latest,
       annotate_version('aleph-vm', '0.2.5', '2021-03-30'::timestamp)     as vm_other
;
