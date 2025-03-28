#set airflow environment
HM_AF_CONFIG=/opt/repositories/vm06-prod/ingest-pipeline/src/ingest-pipeline/airflow/airflow.cfg
HM_AF_HOME=/opt/repositories/vm06-prod/ingest-pipeline/src/ingest-pipeline/airflow

HM_AF_METHOD='conda'
HM_AF_ENV_NAME="condaEnv_python_${HUBMAP_PYTHON_VERSION}_prod"

HM_AF_CONN_INGEST_API_CONNECTION=http://vm06.moonshot.pitt.edu:7777/
HM_AF_CONN_UUID_API_CONNECTION=http://https%3a%2f%2fuuid.api.sennetconsortium.org/
HM_AF_CONN_FILES_API_CONNECTION=http://https%3a%2f%2ffiles.api.sennetconsortium.org/
HM_AF_CONN_SPATIAL_API_CONNECTION=http://https%3a%2f%2fspatial.api.sennetconsortium.org/
HM_AF_CONN_CELLS_API_CONNECTION=http://https%3a%2f%2fcells.api.sennetconsortium.org/
HM_AF_CONN_SEARCH_API_CONNECTION=http://https%3a%2f%2fontology.api.sennetconsortium.org/
HM_AF_CONN_ENTITY_API_CONNECTION=http://https%3a%2f%2fentity.api.sennetconsortium.org/