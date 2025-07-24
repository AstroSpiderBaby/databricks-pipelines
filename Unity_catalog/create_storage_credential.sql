-- Run this once as a metastore admin (adminspider)
CREATE STORAGE CREDENTIAL lv426_cred
WITH AZURE_STORAGE_ACCOUNT_KEY = (
  SECRET SCOPE databricks-secrets-lv426
  SECRET KEY lv426-storage-key
)
COMMENT "Credential for lv426 Data Lake Gen2"
