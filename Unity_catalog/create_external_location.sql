-- External location points to a safe root path for Unity-managed tables
CREATE EXTERNAL LOCATION external_lv426
URL 'abfss://external-ingest@datalakelv426.dfs.core.windows.net/thebetty'
WITH (STORAGE CREDENTIAL lv426_cred)
COMMENT 'External location for Unity Catalog to manage /thebetty/'