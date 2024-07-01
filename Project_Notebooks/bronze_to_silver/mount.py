# Databricks notebook source
storage_account_name = "projectstorage7"
storage_account_access_key = dbutils.secrets.get(scope='vtex-data-secret-scope', key='vtex-data-secret')
container_name = "vtex-data"

# Create the DBFS mount point
mount_point = f"/mnt/{container_name}"

dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = mount_point,
  extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)

# COMMAND ----------

