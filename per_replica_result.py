import re
import sys

def get_results(infile, n_replicas):
    with open(infile, 'r') as i_f:
        log = i_f.read()
        times = ['nan' for _ in range(n_replicas)]
        for i in range(n_replicas):
            match = re.search(f'replica {i} completed in (.*)s\n', log, re.IGNORECASE)
            times[i] = match.group(1)
    return times

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('usage:', sys.argv[0], '[filename] [n_replicas]')
        exit(0)
    print('--'.join(get_results(sys.argv[1], int(sys.argv[2]))))
