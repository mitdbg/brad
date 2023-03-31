from absl import app
from absl import flags
import IMDB.parameters
from IMDB.olap_workload_simulator import simulate_olap
from IMDB.oltp_workload_simulator import simulate_oltp

FLAGS = flags.FLAGS
flags.DEFINE_string('run', 'Default', 'Experiment config to run.')


def Main(argv):
    del argv  # Unused.
    name = FLAGS.run
    print('Looking up params by name:', name)
    p = IMDB.parameters.Get(name)
    if p.simulate_olap:
        simulate_olap(p)
    if p.simulate_oltp:
        simulate_oltp(p)

if __name__ == '__main__':
    app.run(Main)
