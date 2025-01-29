import pandas as pd

# File names
fn_current_state = 'ado.xlsx'
fn_latest_version = 'rm.csv'
fn_output = 'output.csv'

# Read the current_state and latest_version files into pandas DataFrames
current_state = pd.read_excel(fn_current_state)
latest_version = pd.read_csv(fn_latest_version)

# Columns mapping, update these based on the actual columns in your files
columns_mapping = {
    'entity': 'Entity',
    'app': ('app_name', 'App'),
    'environment': ('env_name', 'Env'),
    'server': ('sserver_name', 'Hostname'),
    'id_ado': 'sserver_id_ado'
}

# Unique entities
entities = set(current_state[columns_mapping['entity']]).union(set(latest_version[columns_mapping['entity']]))

# Output data placeholder
output_data = []

for entity in entities:
    # Filtering data by entity
    current_entity_data = current_state[current_state[columns_mapping['entity']] == entity]
    latest_entity_data = latest_version[latest_version[columns_mapping['entity']] == entity]
    
    # Print entity and corresponding data
    print(f"Processing entity: {entity}")
    print("Current entity data:", current_entity_data)
    print("Latest entity data:", latest_entity_data)
    
    # Unique applications within the entity
    apps = set(current_entity_data[columns_mapping['app'][0]]).union(set(latest_entity_data[columns_mapping['app'][1]]))
    
    print("Unique apps:", apps)

    for app in apps:
        current_app_data = current_entity_data[current_entity_data[columns_mapping['app'][0]] == app]
        latest_app_data = latest_entity_data[latest_entity_data[columns_mapping['app'][1]] == app]
        
        # Print app and corresponding data
        print(f"  Processing app: {app}")
        print("  Current app data:", current_app_data)
        print("  Latest app data:", latest_app_data)

        if not current_app_data.empty and latest_app_data.empty:
            for _, row in current_app_data.iterrows():
                action = 'delete application'
                print(f"  App '{app}' exists in current_state but not in latest_version. Action: {action}")
                
                output_data.append({
                    'sserver_id_ado': row[columns_mapping['id_ado']] if columns_mapping['id_ado'] in current_app_data else None,
                    'sserver_name': row[columns_mapping['server'][0]],
                    'action_server': 'delete',
                    'env_id_ado': row['env_id_ado'] if 'env_id_ado' in current_app_data else None,
                    'env_name': row[columns_mapping['environment'][0]],
                    'action_environment': 'delete',
                    'app_sys_id': row['app_sys_id'] if 'app_sys_id' in current_app_data else None,
                    'app_id_ado': row['app_id_ado'] if 'app_id_ado' in current_app_data else None,
                    'app_name': row[columns_mapping['app'][0]],
                    'entity': row[columns_mapping['entity']],
                    'action_app': 'delete',
                })
        
        elif current_app_data.empty and not latest_app_data.empty:
            for _, row in latest_app_data.iterrows():
                action = 'add application'
                print(f"  App '{app}' exists in latest_version but not in current_state. Action: {action}")
                
                output_data.append({
                    'sserver_id_ado': None,
                    'sserver_name': row[columns_mapping['server'][1]],
                    'action_server': 'add',
                    'env_id_ado': None,
                    'env_name': row[columns_mapping['environment'][1]],
                    'action_environment': 'add',
                    'app_sys_id': None,
                    'app_id_ado': None,
                    'app_name': row[columns_mapping['app'][1]],
                    'entity': row[columns_mapping['entity']],
                    'action_app': 'add',
                })
        
        elif not current_app_data.empty and not latest_app_data.empty:
            environments = set(current_app_data[columns_mapping['environment'][0]]).union(set(latest_app_data[columns_mapping['environment'][1]]))
            
            print("  Unique environments:", environments)
            # app_id is defined here
            app_id = current_app_data.iloc[0]['app_id_ado'] if 'app_id_ado' in current_app_data else latest_app_data.iloc[0]['app_id_ado'] if 'app_id_ado' in latest_app_data else None
            
            for env in environments:
                current_env_data = current_app_data[current_app_data[columns_mapping['environment'][0]] == env]
                latest_env_data = latest_app_data[latest_app_data[columns_mapping['environment'][1]] == env]
                
                print(f"    Processing environment: {env}")
                print("    Current environment data:", current_env_data)
                print("    Latest environment data:", latest_env_data)

                if not current_env_data.empty and latest_env_data.empty:
                    action = 'delete environment'
                    env_id = current_env_data.iloc[0]['env_id_ado'] if 'env_id_ado' in current_env_data else None
                    for _, row in current_env_data.iterrows():
                        print(f"    Environment '{env}' exists in current_state but not in latest_version. Action: {action}")
                        output_data.append({
                            'sserver_id_ado': row[columns_mapping['id_ado']] if columns_mapping['id_ado'] in current_env_data else None,
                            'sserver_name': row[columns_mapping['server'][0]],
                            'action_server': 'delete',
                            'env_id_ado': env_id,
                            'env_name': row[columns_mapping['environment'][0]],
                            'action_environment': 'delete',
                            'app_sys_id': row['app_sys_id'] if 'app_sys_id' in current_env_data else None,
                            'app_id_ado': app_id,
                            'app_name': row[columns_mapping['app'][0]],
                            'entity': row[columns_mapping['entity']],
                            'action_app': 'stay',
                        })

                elif current_env_data.empty and not latest_env_data.empty:
                    action = 'add environment'
                    env_id = latest_env_data.iloc[0]['env_id_ado'] if 'env_id_ado' in latest_env_data else None
                    for _, row in latest_env_data.iterrows():
                        print(f"    Environment '{env}' exists in latest_version but not in current_state. Action: {action}")
                        output_data.append({
                            'sserver_id_ado': None,
                            'sserver_name': row[columns_mapping['server'][1]],
                            'action_server': 'add',
                            'env_id_ado': env_id,
                            'env_name': row[columns_mapping['environment'][1]],
                            'action_environment': 'add',
                            'app_sys_id': None,
                            'app_id_ado': app_id,
                            'app_name': row[columns_mapping['app'][1]],
                            'entity': row[columns_mapping['entity']],
                            'action_app': 'stay',
                        })

                elif not current_env_data.empty and not latest_env_data.empty:
                    servers = set(current_env_data[columns_mapping['server'][0]]).union(set(latest_env_data[columns_mapping['server'][1]]))
                    
                    print("      Unique servers:", servers)
                    # env_id and app_id are defined here
                    env_id = current_env_data.iloc[0]['env_id_ado'] if 'env_id_ado' in current_env_data else latest_env_data.iloc[0]['env_id_ado'] if 'env_id_ado' in latest_env_data else None
                    app_id = current_env_data.iloc[0]['app_id_ado'] if 'app_id_ado' in current_env_data else latest_env_data.iloc[0]['app_id_ado'] if 'app_id_ado' in latest_env_data else None
                    
                    for server in servers:
                        current_server_data = current_env_data[current_env_data[columns_mapping['server'][0]] == server]
                        latest_server_data = latest_env_data[latest_env_data[columns_mapping['server'][1]] == server]
                        
                        print(f"        Processing server: {server}")
                        print("        Current server data:", current_server_data)
                        print("        Latest server data:", latest_server_data)

                        if not current_server_data.empty and latest_server_data.empty:
                            action = 'delete server'
                            row = current_server_data.iloc[0]
                            print(f"        Server '{server}' exists in current_state but not in latest_version. Action: {action}")
                            output_data.append({
                                'sserver_id_ado': row[columns_mapping['id_ado']] if columns_mapping['id_ado'] in current_server_data else None,
                                'sserver_name': row[columns_mapping['server'][0]],
                                'action_server': 'delete',
                                'env_id_ado': env_id,
                                'env_name': row[columns_mapping['environment'][0]],
                                'action_environment': 'stay',
                                'app_sys_id': row['app_sys_id'] if 'app_sys_id' in current_server_data else None,
                                'app_id_ado': app_id,
                                'app_name': row[columns_mapping['app'][0]],
                                'entity': row[columns_mapping['entity']],
                                'action_app': 'stay',
                            })

                        elif current_server_data.empty and not latest_server_data.empty:
                            action = 'add server'
                            row = latest_server_data.iloc[0]
                            print(f"        Server '{server}' exists in latest_version but not in current_state. Action: {action}")
                            output_data.append({
                                'sserver_id_ado': None,
                                'sserver_name': row[columns_mapping['server'][1]],
                                'action_server': 'add',
                                'env_id_ado': env_id,
                                'env_name': row[columns_mapping['environment'][1]],
                                'action_environment': 'stay',
                                'app_sys_id': None,
                                'app_id_ado': app_id,
                                'app_name': row[columns_mapping['app'][1]],
                                'entity': row[columns_mapping['entity']],
                                'action_app': 'stay',
                            })

                        elif not current_server_data.empty and not latest_server_data.empty:
                            action = 'stay server'
                            row = current_server_data.iloc[0]
                            print(f"        Server '{server}' exists in both current_state and latest_version. Action: {action}")
                            output_data.append({
                                'sserver_id_ado': row[columns_mapping['id_ado']] if columns_mapping['id_ado'] in current_server_data else None,
                                'sserver_name': row[columns_mapping['server'][0]],
                                'action_server': 'stay',
                                'env_id_ado': env_id,
                                'env_name': row[columns_mapping['environment'][0]],
                                'action_environment': 'stay',
                                'app_sys_id': row['app_sys_id'] if 'app_sys_id' in current_server_data else None,
                                'app_id_ado': app_id,
                                'app_name': row[columns_mapping['app'][0]],
                                'entity': row[columns_mapping['entity']],
                                'action_app': 'stay',
                            })

# Convert the output data to a DataFrame and save to CSV
output_df = pd.DataFrame(output_data)
output_df.to_csv(fn_output, index=False)

print("Final output data:", output_df)
