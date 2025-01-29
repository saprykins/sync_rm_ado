import pandas as pd
import sys

if len(sys.argv) > 1:
    fn_latest_version = sys.argv[1]
    fn_current_state = sys.argv[2]
    fn_output = sys.argv[3]

# Read the current_state and latest_version files into pandas DataFrames
current_state = pd.read_excel(fn_current_state)
latest_version = pd.read_csv(fn_latest_version)

# Initialize an empty list to store new rows
rows = []

# Check for applications in current_state that are not in latest_version
for _, row in current_state.iterrows():
    app_tuple = (row['app_name'], row['Entity'])
    if app_tuple not in set(zip(latest_version['App'], latest_version['Entity'])):
        # Application exists in current_state but not in latest_version
        rows.append({
            'sserver_id_ado': row['sserver_id_ado'],  # Use current state ID
            'sserver_name': row['sserver_name'],
            'action_server': 'delete',
            'env_id_ado': row['env_id_ado'],  # Use current state ID
            'env_name': row['env_name'],
            'action_environment': 'delete',
            'app_sys_id': row['app_sys_id'],
            'app_id_ado': row['app_id_ado'],
            'app_name': row['app_name'],
            'entity': row['Entity'],
            'action_app': 'delete'
        })

# Check for applications in latest_version that are not in current_state
for _, row in latest_version.iterrows():
    app_tuple = (row['App'], row['Entity'])
    if app_tuple not in set(zip(current_state['app_name'], current_state['Entity'])):
        # Application exists in latest_version but not in current_state
        # Get the env_id_ado from the current state if it matches the env_name
        env_id = None
        for _, current_row in current_state.iterrows():
            if current_row['env_name'] == row['Env'] and current_row['Entity'] == row['Entity']:
                env_id = current_row['env_id_ado']
                break

        rows.append({
            'sserver_id_ado': None,  # Set to None as you wanted it to be empty
            'sserver_name': row['Hostname'],
            'action_server': 'added',
            'env_id_ado': env_id,  # Use the retrieved env_id if found
            'env_name': row['Env'],
            'action_environment': 'added',
            'app_sys_id': None,
            'app_id_ado': None,
            'app_name': row['App'],
            'entity': row['Entity'],
            'action_app': 'added'
        })

# Now handle applications that stay
# Group current state by app_name and entity, and environment
current_grouped = current_state.groupby(['app_name', 'Entity', 'env_name']).agg(
    app_id_ado=('app_id_ado', 'first'),  # Assuming app_id_ado is the same for the same app_name and entity
    servers=('sserver_name', list),  # Collect all related servers in this environment
    sserver_id_ado=('sserver_id_ado', list),  # Collect server IDs
    env_id_ado=('env_id_ado', 'first')  # Assuming env_id_ado is the same for the same env_name
).reset_index()

# Group latest version by app_name and entity, and environment
latest_grouped = latest_version.groupby(['App', 'Entity', 'Env']).agg(
    servers=('Hostname', list)  # Collect all related servers in this environment
).reset_index()

# Loop through grouped current state
for _, current_row in current_grouped.iterrows():
    app_name, entity, env_name = current_row['app_name'], current_row['Entity'], current_row['env_name']
    app_id_ado = current_row['app_id_ado']
    current_servers = current_row['servers']

    # Check if the application is also in the latest version
    latest_row = latest_grouped[(latest_grouped['App'] == app_name) & (latest_grouped['Entity'] == entity) & (latest_grouped['Env'] == env_name)]
    
    if not latest_row.empty:
        latest_env_servers = latest_row['servers'].values[0]

        # Compare servers in the current and latest environments
        for server in current_servers:
            if server in latest_env_servers:
                # Server exists in both current and latest
                rows.append({
                    'sserver_id_ado': current_row['sserver_id_ado'][0],  # Get ID from current state
                    'sserver_name': server,
                    'action_server': 'stay',
                    'env_id_ado': current_row['env_id_ado'],  # Get ID from current state
                    'env_name': env_name,
                    'action_environment': 'stay',
                    'app_sys_id': None,
                    'app_id_ado': app_id_ado,
                    'app_name': app_name,
                    'entity': entity,
                    'action_app': 'stay'
                })
            else:
                # Server exists in current but not in latest
                rows.append({
                    'sserver_id_ado': current_row['sserver_id_ado'][0],  # Get ID from current state
                    'sserver_name': server,
                    'action_server': 'delete',
                    'env_id_ado': current_row['env_id_ado'],  # Get ID from current state
                    'env_name': env_name,
                    'action_environment': 'stay',
                    'app_sys_id': None,
                    'app_id_ado': app_id_ado,
                    'app_name': app_name,
                    'entity': entity,
                    'action_app': 'stay'
                })

        # Check for additional servers that are in the latest but not in current
        for server in latest_env_servers:
            if server not in current_servers:
                rows.append({
                    'sserver_id_ado': None,  # Set to None as you wanted it to be empty
                    'sserver_name': server,
                    'action_server': 'added',
                    'env_id_ado': current_row['env_id_ado'],  # Use the env_id_ado from current state
                    'env_name': env_name,
                    'action_environment': 'stay',
                    'app_sys_id': None,
                    'app_id_ado': app_id_ado,
                    'app_name': app_name,
                    'entity': entity,
                    'action_app': 'stay'
                })

# Create a DataFrame from the list of rows
result = pd.DataFrame(rows)

# Write the updated DataFrame to a new CSV file
result.to_csv(fn_output, index=False)
