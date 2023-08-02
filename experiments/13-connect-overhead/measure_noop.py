import argparse
import time
from brad.grpc_client import BradGrpcClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=1000)
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=6583)
    args = parser.parse_args()

    with BradGrpcClient(args.host, args.port) as client:
        start = time.time()
        for _ in range(args.repetitions):
            client.run_query_json("BRAD_NOOP")
        end = time.time()

    total = end - start
    avg_lat = total / args.repetitions

    print("reps,total_time_s,avg_lat_s")
    print("{},{},{}".format(args.repetitions, total, avg_lat))


if __name__ == "__main__":
    main()
