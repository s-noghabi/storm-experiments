import os
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

LOG_DIR="./logs/"
PLOT_DIR="./plots/"

def parse():
    times = {}
    for fname in os.listdir(LOG_DIR):
        if "machines" not in fname:
            continue

        times[fname] = {}

        with open(LOG_DIR + fname) as f:
            for line in f:
                _line = line.split()

                if "output" not in _line[5] or "execute_count" not in _line[6]:
                    continue

                time = datetime.strptime(' '.join(_line[:2]), "%Y-%m-%d %H:%M:%S,%f")
                host = _line[4]
                task_name = _line[5].split(':')[0]
                bolt_name = _line[5].split(':')[1]
                num_tuples = _line[2]

                if bolt_name not in times[fname]:
                    times[fname][bolt_name] = {}
                if task_name not in times[fname][bolt_name]:
                    times[fname][bolt_name][task_name] = []

                times[fname][bolt_name][task_name].append(num_tuples)

    return times

def plot(times):
    fig, ax = plt.subplots()
    width = 0.35
    N = len(times)
    ind = np.arange(N)
    fname_index = 0
    num_output_bolts = 0
    count = 0

    for fname, fname_times in times.items():
        means = []
        stds = []

        if "machines" not in fname:
            continue

        for bolt_name, bolt_times in fname_times.items():
            time_series = []
            for task_name, task_times in bolt_times.items():
                time_series.extend([float(second) - float(first) for first, second in \
                    zip(task_times[:-1], task_times[1:])])

            means.append(np.average(time_series))
            stds.append(np.std(time_series))

        bar_step = np.array([count + width * i for i in range(len(fname_times))])
        # ax.bar(bar_step, means, width, color='r', yerr=stds)
        ax.bar(bar_step, means, width, color='r')

        count += (len(fname_times)+ 1) * width
        num_output_bolts = len(fname_times)

    ax.set_ylabel('Throughput')
    ax.set_xlabel('Output bolts')

    # Label the step in x-axis
    name_step = np.array([((num_output_bolts + 1) * i + num_output_bolts/2) * width for i in range(len(os.listdir(LOG_DIR)))])
    ax.set_xticks(name_step)
    ax.set_xticklabels([fname for fname in os.listdir(LOG_DIR) if "machines" in fname])

    plt.grid()
    plt.savefig(PLOT_DIR + 'load.png', format='png')

if __name__ == '__main__':
    times = parse()
    plot(times)
