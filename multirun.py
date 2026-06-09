import os
import subprocess
import glob
import sys

def read_divergent_designs(config_path: str, route_path: str) -> dict[str, list[str, str]]:
    configs = glob.glob(f'{config_path}/*.csv')
    routes = glob.glob(f'{route_path}/*.csv')

    if len(configs) != len(routes):
        raise FileNotFoundError('some index configurations are missing routing tables (or vice versa!)')
    
    config_names = [os.path.basename(c) for c in configs]
    route_names = [os.path.basename(r) for r in routes]

    designs = {}

    for i_c, config in enumerate(configs):
        try:
            i_r = route_names.index(config_names[i_c])
        except ValueError:
            raise FileNotFoundError(f'no routing table found for index configuration {config}')
        designs[config_names[i_c].strip('.csv')] = [configs[i_c], routes[i_r]]
    
    return designs

if __name__ == '__main__':
    dd = read_divergent_designs('./configs', './routes')

    for key, design in dd.items():
        print('*' * 20, 'EVALUATING DESIGN', key, '*' * 20)
        
        with open(f'./design_{key}.log', 'a') as outfile:
            subprocess.run(['python', 'run.py', *sys.argv[1:], '--index-config', design[0], '--routing-table', design[1]])
