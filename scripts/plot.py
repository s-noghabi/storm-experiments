import os
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

LOG_DIR="./logs/"
PLOT_DIR="./plots/"

def parse_bar():
    times = {}
    for fname in os.listdir(LOG_DIR):
        if "." in fname or os.path.isdir(LOG_DIR + fname):
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
                num_tuples = _line[7]

                if bolt_name not in times[fname]:
                    times[fname][bolt_name] = {}
                if task_name not in times[fname][bolt_name]:
                    times[fname][bolt_name][task_name] = []

                times[fname][bolt_name][task_name].append(num_tuples)

    return times

def parse_line():
    times = {}
    for fname in os.listdir(LOG_DIR):
        if "." in fname or os.path.isdir(LOG_DIR + fname):
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
                num_tuples = _line[7]

                if bolt_name not in times[fname]:
                    times[fname][bolt_name] = {}
                if task_name not in times[fname][bolt_name]:
                    times[fname][bolt_name][task_name] = {}

                times[fname][bolt_name][task_name][time] = num_tuples

    return times

def plot_bar(times):
    fig, ax = plt.subplots()
    width = 0.5
    N = len(times)
    ind = np.arange(N)
    fname_index = 0
    num_output_bolts = 0
    count = 0
    file_labels = [_[:-len("_load")] for _ in sorted(times.keys())]

    for fname in sorted(times.keys()):
        fname_times = times[fname]
        means = []
        stds = []

        for bolt_name, bolt_times in fname_times.items():
            time_series = []
            for task_name, task_times in bolt_times.items():
                time_series.extend([float(_)/float(fname.split('_')[0]) for _ in task_times])

            means.append(np.average(time_series))
            stds.append(np.std(time_series))

        bar_step = np.array([count + width * i for i in range(len(fname_times))])
        # ax.bar(bar_step, means, width, color='r', yerr=stds)
        ax.bar(bar_step, means, width, color='r')

        count += (len(fname_times) + 2) * width
        num_output_bolts = len(fname_times)

    ax.set_ylabel('Throughput')
    ax.set_xlabel('Machines_Load')

    # Label the step in x-axis
    name_step = np.array([((num_output_bolts + 2) * i + num_output_bolts/2 + 1) * width \
        for i in range(len(file_labels))])
    ax.set_xticks(name_step)
    ax.set_xticklabels(file_labels)

    plt.grid()
    plt.savefig(PLOT_DIR + 'load.png', format='png')

def plot_line(times):
    fig, ax = plt.subplots()
    width = 0.5
    N = len(times)
    ind = np.arange(N)
    fname_index = 0
    num_output_bolts = 0
    count = 0
    file_labels = [_[:-len("_load")] for _ in sorted(times.keys())]

    for fname in sorted(times.keys()):
        fname_times = times[fname]
        means = []
        stds = []

        for bolt_name, bolt_times in fname_times.items():
            time_series = {}
            min_time = datetime.now()

            for task_name, task_times in bolt_times.items():
                for time, num_tuples in task_times.items():
                    min_time = min(min_time, time)

            for task_name, task_times in bolt_times.items():
                for time, num_tuples in task_times.items():
                    time_series[time - min_time] = float(num_tuples)/float(fname.split('_')[0])

            x_axis = [float(_.microseconds)/1000000 + float(_.seconds) for _ in sorted(time_series.keys())]
            y_axis = [time_series[_] for _ in sorted(time_series.keys())]
            plt.plot(x_axis, y_axis)

    ax.set_ylabel('Throughput')
    ax.set_xlabel('Time(minutes)')

    plt.grid()
    plt.savefig(PLOT_DIR + 'scale_in.png', format='png')

if __name__ == '__main__':
    times = parse_line()
    plot_line(times)

    # times = parse_bar()
    # plot_bar(times)
