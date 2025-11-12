# Search service set up 
import os
from snowflake.core import Root
from snowflake.snowpark import Session

CONNECTION_PARAMETERS = {
	"account": os.environ["your_account_info"],
	"user": os.environ["your_username"],
	"password": os.environ["your_password"],
	"role": "test_role",
	"warehouse": "test_warehouse",
	"database": "MH_PUBLICATIONS",
	"schema": "DATA",
}

session = Session.builder.configs(CONNECTION_PARAMETERS).create()
root = Root(session)

# fetch service
my_service = (root
	.databases["MH_PUBLICATIONS"]
	.schemas["DATA"]
	.cortex_search_services["MH_PUBLICATIONS_SEARCH_SERVICE"]
)

# query service
resp = my_service.search(
	query="<your query here>",
	columns=["CHUNK", "RELATIVE_PATH", "CHUNK_ORDER", "EFF_CODE_FINAL_DATE"],
	limit=10,
)

print(resp.to_json())