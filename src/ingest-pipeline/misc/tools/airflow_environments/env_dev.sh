#set airflow environment
HM_AF_CONFIG=/opt/repositories/vm05-dev/ingest-pipeline/src/ingest-pipeline/airflow/airflow.cfg
HM_AF_HOME=/opt/repositories/vm05-dev/ingest-pipeline/src/ingest-pipeline/airflow

HM_AF_METHOD='conda'
HM_AF_ENV_NAME="condaEnv_python_${HUBMAP_PYTHON_VERSION}_dev"

HM_AF_CONN_INGEST_API_CONNECTION=http://https%3a%2f%2fingest-api.dev.sennetconsortium.org/
HM_AF_CONN_UUID_API_CONNECTION=http://https%3a%2f%2fuuid-api.dev.sennetconsortium.org/
HM_AF_CONN_FILES_API_CONNECTION=http://https%3a%2f%2ffiles-api.dev.sennetconsortium.org/
HM_AF_CONN_SPATIAL_API_CONNECTION=http://https%3a%2f%2fspatial-api.dev.sennetconsortium.org/
HM_AF_CONN_CELLS_API_CONNECTION=http://https%3a%2f%2fcells-api.dev.sennetconsortium.org/
HM_AF_CONN_SEARCH_API_CONNECTION=http://https%3a%2f%2fontology-api.dev.sennetconsortium.org/
HM_AF_CONN_ENTITY_API_CONNECTION=http://https%3a%2f%2fentity-api.dev.sennetconsortium.org/
