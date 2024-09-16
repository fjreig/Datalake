from dremio_simple_query.connect import get_token, DremioConnection
import polars as pl

# Dremio login details
login_endpoint = "http://dremio:9047/apiv2/login"
payload = {
    "userName": "admin",  # Dremio username
    "password": "password1"  # Dremio password
}

# Get the token
token = get_token(uri=login_endpoint, payload=payload)

# Dremio Arrow Flight endpoint (no SSL for local setup)
arrow_endpoint = "grpc://dremio:32010"

# Create the connection
dremio = DremioConnection(token, arrow_endpoint)

# Query dataset
query = "SELECT * FROM nessie.pabat.aarr;"
df = dremio.toPolars(query)

# Display the Polars DataFrame
print(df)